package numpool

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
	"github.com/yuku/numpool/internal/sqlc"
)

// Pool represents a pool of resources that can be acquired and released.
type Pool struct {
	id       string
	pool     *pgxpool.Pool
	listener *pgxlisten.Listener
	mu       sync.Mutex
	
	// notifyHandlers maps client IDs to notification channels
	notifyHandlers map[string]chan struct{}
}

type Config struct {
	Pool              *pgxpool.Pool
	ID                string
	MaxResourcesCount int32
}

const (
	maxResourcesCount = 64 // Maximum number of resources in the pool
)

func (c Config) Validate() error {
	if c.Pool == nil {
		return fmt.Errorf("pool cannot be nil")
	}
	if c.ID == "" {
		return fmt.Errorf("pool ID cannot be empty")
	}
	if c.MaxResourcesCount <= 0 || maxResourcesCount < c.MaxResourcesCount {
		return fmt.Errorf("max resources count must be between 1 and %d: given %d",
			maxResourcesCount, c.MaxResourcesCount,
		)
	}
	return nil
}

// CreateOrOpen creates a new pool or opens an existing one based on the
// provided configuration. If the pool already exists with a different
// MaxResourcesCount, it returns an error.
func CreateOrOpen(ctx context.Context, conf Config) (*Pool, error) {
	if err := conf.Validate(); err != nil {
		return nil, fmt.Errorf("invalid pool configuration: %w", err)
	}

	// Check if the pool already exists
	conn, err := conf.Pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection from pool: %w", err)
	}
	defer conn.Release()

	q := sqlc.New(conn.Conn())

	row, err := q.GetNumpool(ctx, conf.ID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("failed to get numpool: %w", err)
		}
		// Pool does not exist, create it
		err = q.CreateNumpool(ctx, sqlc.CreateNumpoolParams{
			ID:                conf.ID,
			MaxResourcesCount: conf.MaxResourcesCount,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create numpool: %w", err)
		}
	} else {
		if row.MaxResourcesCount != conf.MaxResourcesCount {
			return nil, fmt.Errorf("pool %s already exists with different max resources count: %d, expected: %d",
				conf.ID, row.MaxResourcesCount, conf.MaxResourcesCount,
			)
		}
	}

	// Create listener for LISTEN/NOTIFY
	listener := &pgxlisten.Listener{
		Connect: func(ctx context.Context) (*pgx.Conn, error) {
			config := conf.Pool.Config().ConnConfig.Copy()
			return pgx.ConnectConfig(ctx, config)
		},
	}
	
	pool := &Pool{
		id:             conf.ID,
		pool:           conf.Pool,
		listener:       listener,
		notifyHandlers: make(map[string]chan struct{}),
	}
	
	// Set up notification handler
	// PostgreSQL channel names have a limit, so we use a shorter format
	channelName := fmt.Sprintf("np_%s", conf.ID)
	listener.Handle(channelName, pgxlisten.HandlerFunc(pool.handleNotification))
	
	// Start listening in background
	go listener.Listen(context.Background())
	
	return pool, nil
}

// ID returns the unique identifier of the pool.
func (p *Pool) ID() string {
	return p.id
}

// Acquire acquires a resource from the pool.
func (p *Pool) Acquire(ctx context.Context) (*Resource, error) {
	for {
		// Try to acquire a resource
		resource, err := p.tryAcquire(ctx)
		if err != nil {
			return nil, err
		}
		if resource != nil {
			return resource, nil
		}

		// No resources available, wait for one
		clientID := uuid.New()
		notifyChan := make(chan struct{}, 1)

		// Register the notification handler
		p.mu.Lock()
		p.notifyHandlers[clientID.String()] = notifyChan
		p.mu.Unlock()

		// Add to wait queue
		err = p.enqueueClient(ctx, clientID)
		if err != nil {
			p.mu.Lock()
			delete(p.notifyHandlers, clientID.String())
			p.mu.Unlock()
			return nil, fmt.Errorf("failed to enqueue client: %w", err)
		}

		// Wait for notification or context cancellation
		select {
		case <-notifyChan:
			// Notification received, clean up and try again
			p.mu.Lock()
			delete(p.notifyHandlers, clientID.String())
			p.mu.Unlock()
			continue
		case <-ctx.Done():
			// Context cancelled, clean up
			p.mu.Lock()
			delete(p.notifyHandlers, clientID.String())
			p.mu.Unlock()
			
			// Remove from wait queue
			_ = p.removeFromWaitQueue(context.Background(), clientID)
			
			return nil, ctx.Err()
		}
	}
}

// tryAcquire attempts to acquire a resource without blocking
func (p *Pool) tryAcquire(ctx context.Context) (*Resource, error) {
	var resource *Resource

	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		q := sqlc.New(tx)

		// Get the pool with lock
		numpool, err := q.GetNumpoolForUpdate(ctx, p.id)
		if err != nil {
			return fmt.Errorf("failed to get numpool for update: %w", err)
		}

		// Find the first unused resource
		index := numpool.FindUnusedResourceIndex()
		if index == -1 {
			// No resources available
			return nil
		}

		// Try to acquire this resource
		affected, err := q.AcquireResource(ctx, sqlc.AcquireResourceParams{
			ID:            p.id,
			ResourceIndex: index,
		})
		if err != nil {
			return fmt.Errorf("failed to acquire resource: %w", err)
		}

		if affected == 0 {
			// This shouldn't happen with proper locking
			return fmt.Errorf("resource at index %d was already in use", index)
		}

		resource = &Resource{
			pool:  p,
			index: index,
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return resource, nil
}

// enqueueClient adds a client to the wait queue
func (p *Pool) enqueueClient(ctx context.Context, clientID uuid.UUID) error {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	q := sqlc.New(conn.Conn())
	pgUUID := pgtype.UUID{Valid: true}
	copy(pgUUID.Bytes[:], clientID[:])
	return q.EnqueueWaitingClient(ctx, sqlc.EnqueueWaitingClientParams{
		ID:       p.id,
		ClientID: pgUUID,
	})
}

// removeFromWaitQueue removes a client from the wait queue
func (p *Pool) removeFromWaitQueue(ctx context.Context, clientID uuid.UUID) error {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	q := sqlc.New(conn.Conn())
	pgUUID := pgtype.UUID{Valid: true}
	copy(pgUUID.Bytes[:], clientID[:])
	return q.RemoveFromWaitQueue(ctx, sqlc.RemoveFromWaitQueueParams{
		ID:       p.id,
		ClientID: pgUUID,
	})
}

// release releases a resource back to the pool.
func (p *Pool) release(ctx context.Context, r *Resource) error {
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		q := sqlc.New(tx)

		affected, err := q.ReleaseResource(ctx, sqlc.ReleaseResourceParams{
			ID:            p.id,
			ResourceIndex: r.index,
		})
		if err != nil {
			return fmt.Errorf("failed to release resource: %w", err)
		}

		if affected == 0 {
			return fmt.Errorf("resource was not in use")
		}

		// Check if there are waiting clients
		clientIDRaw, err := q.DequeueWaitingClient(ctx, p.id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// No waiting clients
				return nil
			}
			return fmt.Errorf("failed to dequeue waiting client: %w", err)
		}

		// Convert the raw UUID to string
		var clientIDStr string
		switch v := clientIDRaw.(type) {
		case [16]byte:
			clientID := uuid.UUID(v)
			clientIDStr = clientID.String()
		case string:
			clientIDStr = v
		default:
			return fmt.Errorf("unexpected client ID type: %T", clientIDRaw)
		}

		// Notify the waiting client
		channelName := fmt.Sprintf("np_%s", p.id)
		_, err = tx.Exec(ctx, "SELECT pg_notify($1, $2)", channelName, clientIDStr)
		if err != nil {
			return fmt.Errorf("failed to notify waiting client: %w", err)
		}

		return nil
	})
}

// handleNotification handles incoming notifications from PostgreSQL
func (p *Pool) handleNotification(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
	p.mu.Lock()
	ch, exists := p.notifyHandlers[notification.Payload]
	p.mu.Unlock()

	if exists {
		select {
		case ch <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
