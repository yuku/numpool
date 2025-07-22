package numpool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yuku/numpool/internal/sqlc"
	"github.com/yuku/numpool/internal/waitqueue"
)

// Setup creates a new Numpool manager and initializes the numpool table in the database.
func Setup(ctx context.Context, pool *pgxpool.Pool) (*Manager, error) {
	manager := &Manager{pool: pool}
	if err := manager.setup(ctx); err != nil {
		return nil, fmt.Errorf("failed to setup numpool: %w", err)
	}
	return manager, nil
}

// Manager is the main entry point for interacting with the numpool system.
// It is responsible for managing pools and their resources.
// It does not close the underlying database connection pool as it is expected
// to be managed by the caller.
type Manager struct {
	// pool is the underlying database connection pool.
	// It is expected to be managed by the caller.
	pool *pgxpool.Pool

	// numpools holds the list of Numpool instances managed by this manager.
	// This is used to track all pools created by this manager.
	numpools []*Numpool

	// closed indicates whether the manager is closed.
	closed bool

	// mu protects the closed state of the manager.
	mu sync.RWMutex
}

// setup initializes the numpool table in the database.
// It uses PostgreSQL advisory locks to prevent concurrent setup attempts.
// If the table already exists, it does nothing.
func (m *Manager) setup(ctx context.Context) error {
	// Use advisory lock to prevent concurrent schema creation
	// Lock ID 12345 is arbitrary but must be consistent across all processes
	const lockID int64 = 12345

	return pgx.BeginFunc(ctx, m.pool, func(tx pgx.Tx) error {
		q := sqlc.New(tx)

		// Try to acquire exclusive advisory lock
		if err := q.AcquireAdvisoryLock(ctx, lockID); err != nil {
			return fmt.Errorf("failed to acquire advisory lock: %w", err)
		}

		ok, err := q.CheckNumpoolTableExist(ctx)
		if err != nil {
			return fmt.Errorf("failed to check if numpool table exists: %w", err)
		}
		if ok {
			return nil // Table already exists, no need to set up
		}
		if err := q.CreateTable(ctx); err != nil {
			return fmt.Errorf("failed to create numpool table: %w", err)
		}
		return nil
	})
}

type Config struct {
	// ID is the unique identifier for the pool. Required.
	ID string

	// MaxResourcesCount is the maximum number of resources that can be in the pool.
	// It must be between 1 and MaxResourcesLimit (inclusive).
	// If the pool already exists with a different MaxResourcesCount,
	// an error will be returned.
	MaxResourcesCount int32

	// Metadata is optional JSON metadata associated with the pool.
	// It can be used to store additional information about the pool.
	// It is not used by the library itself, but can be useful for clients.
	// If the pool already exists, this field will be ignored.
	Metadata json.RawMessage

	// NoStartListening indicates whether the listener should be started automatically.
	// If true, the caller must call Listen explicitly. This is useful for
	// controlling when the listener starts and timeouts.
	NoStartListening bool
}

const (
	// MaxResourcesLimit is the maximum number of resources that can be in a pool.
	// This limit is due to the bit representation used for tracking resource usage.
	MaxResourcesLimit = 64
)

func (c Config) Validate() error {
	if c.ID == "" {
		return fmt.Errorf("pool ID cannot be empty")
	}
	if c.MaxResourcesCount <= 0 || MaxResourcesLimit < c.MaxResourcesCount {
		return fmt.Errorf("max resources count must be between 1 and %d: given %d",
			MaxResourcesLimit, c.MaxResourcesCount,
		)
	}
	return nil
}

// GetOrCreate retrieves a Numpool instance by its ID, creating it if it does not exist.
// It returns an error if the pool already exists with a different MaxResourcesCount.
func (m *Manager) GetOrCreate(ctx context.Context, conf Config) (*Numpool, error) {
	if m.Closed() {
		return nil, fmt.Errorf("manager is closed")
	}

	if err := conf.Validate(); err != nil {
		return nil, fmt.Errorf("invalid pool configuration: %w", err)
	}

	metadata, err := m.createIfNotExists(ctx, conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create: %w", err)
	}

	result := &Numpool{
		id:            conf.ID,
		metadata:      metadata,
		manager:       m,
		listenHandler: &waitqueue.ListenHandler{},
	}

	if !conf.NoStartListening {
		// Start listening in a separate goroutine
		go func() {
			if err := result.Listen(ctx); err != nil {
				// panic in the goroutine will terminate the entire program. If this is
				// not desired, set NoStartListening to true and call Listen explicitly.
				panic(err)
			}
		}()
	}

	m.mu.Lock()
	m.numpools = append(m.numpools, result)
	m.mu.Unlock()

	return result, nil
}

func (m *Manager) remove(pool *Numpool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.numpools = slices.DeleteFunc(m.numpools, func(np *Numpool) bool {
		return np.id == pool.id
	})
}

func (m *Manager) createIfNotExists(ctx context.Context, conf Config) (json.RawMessage, error) {
	var metadata json.RawMessage
	err := pgx.BeginFunc(ctx, m.pool, func(tx pgx.Tx) error {
		q := sqlc.New(tx)
		if err := q.LockNumpoolTableInShareMode(ctx); err != nil {
			return fmt.Errorf("failed to lock numpool table in share mode: %w", err)
		}

		m, err := q.GetNumpool(ctx, conf.ID)
		if err == nil {
			if m.MaxResourcesCount != conf.MaxResourcesCount {
				return fmt.Errorf("pool %s already exists with different max resources count: %d, expected: %d",
					conf.ID, m.MaxResourcesCount, conf.MaxResourcesCount,
				)
			}
			metadata = m.Metadata
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("failed to get numpool: %w", err)
		}
		// Pool does not exist, create it
		if conf.Metadata != nil {
			metadata = conf.Metadata
		}
		return q.CreateNumpool(ctx, sqlc.CreateNumpoolParams{
			ID:                conf.ID,
			MaxResourcesCount: conf.MaxResourcesCount,
			Metadata:          metadata,
		})
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create numpool: %w", err)
	}
	return metadata, nil
}

// Close closes m and releases any resources it holds.
// It does not close the underlying database connection pool as it is expected
// to be managed by the caller.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return
	}
	m.closed = true

	for _, np := range m.numpools {
		np.Close() // Close each Numpool instance
	}
	m.numpools = nil
}

// Closed returns if the manager is closed.
func (m *Manager) Closed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.closed
}

// Delete removes a Numpool instance by its ID.
// It closes the pool if it exists in the manager's tracked pools and deletes it from the database.
// It returns an error if the pool is not managed by this manager or if the deletion fails.
func (m *Manager) Delete(ctx context.Context, poolID string) error {
	if m.Closed() {
		return fmt.Errorf("manager is closed")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Find the pool in the managed pools
	var poolToClose *Numpool
	poolIndex := -1
	for i, np := range m.numpools {
		if np.id == poolID {
			poolToClose = np
			poolIndex = i
			break
		}
	}

	// Return error if pool is not managed by this manager
	if poolToClose == nil {
		return fmt.Errorf("pool %s is not managed by this manager", poolID)
	}

	// Close the pool instance first
	poolToClose.Close()

	// Remove from managed pools list
	m.numpools = append(m.numpools[:poolIndex], m.numpools[poolIndex+1:]...)

	// Delete from database
	affected, err := sqlc.New(m.pool).DeleteNumpool(ctx, poolID)
	if err != nil {
		return fmt.Errorf("failed to delete pool %s: %w", poolID, err)
	}
	if affected == 0 {
		return fmt.Errorf("pool %s does not exist in database", poolID)
	}
	return nil
}
