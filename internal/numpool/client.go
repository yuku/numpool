package numpool

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/yuku/numpool/internal/sqlc"
)

// Client represents a client that can acquire resources from a pool.
type Client struct {
	id     uuid.UUID
	pool   *Pool
	notify chan struct{}
}

// NewClient creates a new client for the given pool.
func NewClient(pool *Pool) *Client {
	return &Client{
		id:     uuid.New(),
		pool:   pool,
		notify: make(chan struct{}, 1),
	}
}

// ID returns the client's unique identifier.
func (c *Client) ID() uuid.UUID {
	return c.id
}

// Acquire acquires a resource from the pool.
func (c *Client) Acquire(ctx context.Context) (*Resource, error) {
	for {
		// Try to acquire a resource
		resource, err := c.tryAcquire(ctx)
		if err != nil {
			return nil, err
		}
		if resource != nil {
			return resource, nil
		}

		// No resources available, register for notifications
		c.pool.registerClient(c.id.String(), c.notify)
		defer c.pool.unregisterClient(c.id.String())

		// Add to wait queue
		err = c.enqueue(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to enqueue client: %w", err)
		}

		// Wait for notification or context cancellation
		select {
		case <-c.notify:
			// Notification received, try again
			continue
		case <-ctx.Done():
			// Context cancelled, remove from wait queue
			_ = c.removeFromQueue(context.Background())
			return nil, ctx.Err()
		}
	}
}

// tryAcquire attempts to acquire a resource without blocking.
func (c *Client) tryAcquire(ctx context.Context) (*Resource, error) {
	var resource *Resource

	err := pgx.BeginFunc(ctx, c.pool.pool, func(tx pgx.Tx) error {
		q := sqlc.New(tx)

		// Get the pool with lock
		numpool, err := q.GetNumpoolForUpdate(ctx, c.pool.id)
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
			ID:            c.pool.id,
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
			pool:  c.pool,
			index: index,
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return resource, nil
}

// enqueue adds the client to the wait queue.
func (c *Client) enqueue(ctx context.Context) error {
	conn, err := c.pool.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	q := sqlc.New(conn.Conn())
	pgUUID := pgtype.UUID{Valid: true}
	copy(pgUUID.Bytes[:], c.id[:])
	return q.EnqueueWaitingClient(ctx, sqlc.EnqueueWaitingClientParams{
		ID:       c.pool.id,
		ClientID: pgUUID,
	})
}

// removeFromQueue removes the client from the wait queue.
func (c *Client) removeFromQueue(ctx context.Context) error {
	conn, err := c.pool.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	q := sqlc.New(conn.Conn())
	pgUUID := pgtype.UUID{Valid: true}
	copy(pgUUID.Bytes[:], c.id[:])
	return q.RemoveFromWaitQueue(ctx, sqlc.RemoveFromWaitQueueParams{
		ID:       c.pool.id,
		ClientID: pgUUID,
	})
}