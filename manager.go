package numpool

import (
	"context"
	"errors"
	"fmt"

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

type Manager struct {
	pool *pgxpool.Pool
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
	MaxResourcesCount int32

	// NoStartListening indicates whether the listener should be started automatically.
	// If true, the caller must call Listen/Start explicitly. This is useful for
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
	if err := conf.Validate(); err != nil {
		return nil, fmt.Errorf("invalid pool configuration: %w", err)
	}

	if err := m.createIfNotExists(ctx, conf); err != nil {
		return nil, fmt.Errorf("failed to create: %w", err)
	}

	resource := &Numpool{
		id:            conf.ID,
		manager:       m,
		listenHandler: &waitqueue.ListenHandler{},
	}
	if !conf.NoStartListening {
		resource.Start(ctx) // Start the listener in a separate goroutine
	}

	return resource, nil
}

func (m *Manager) createIfNotExists(ctx context.Context, conf Config) error {
	return pgx.BeginFunc(ctx, m.pool, func(tx pgx.Tx) error {
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
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("failed to get numpool: %w", err)
		}
		// Pool does not exist, create it
		return q.CreateNumpool(ctx, sqlc.CreateNumpoolParams{
			ID:                conf.ID,
			MaxResourcesCount: conf.MaxResourcesCount,
		})
	})
}

func (m *Manager) Close() {
	m.pool.Close()
}
