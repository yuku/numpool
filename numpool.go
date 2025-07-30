package numpool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgxlisten"
	"github.com/yuku/numpool/internal/sqlc"
	"github.com/yuku/numpool/internal/waitqueue"
)

// Numpool represents a pool of resources that can be acquired and released.
type Numpool struct {
	id string

	// metadata is optional metadata associated with the pool.
	metadata json.RawMessage

	manager *Manager

	// listenHandler is used to handle LISTEN/NOTIFY notifications for this pool.
	// It is set when the pool is created and used to listen for resource availability.
	// It becomes nil when the pool is closed.
	listenHandler *waitqueue.ListenHandler

	// cancelListen is a function to cancel the listening goroutine.
	// It is set while pgxlisten.Listener is running.
	cancelListen context.CancelFunc

	// resources holds the resources acquired from the pool.
	resources []*Resource

	// mu protects the state of the Numpool.
	mu sync.RWMutex
}

// ID returns the unique identifier of the pool.
func (p *Numpool) ID() string {
	return p.id
}

// Metadata returns the metadata associated with the pool.
func (p *Numpool) Metadata() json.RawMessage {
	return p.metadata
}

// Listen starts listening for notifications on the pool's channel.
// It blocks until the context is cancelled or an fatal error occurs.
func (p *Numpool) Listen(ctx context.Context) error {
	p.mu.Lock()
	if p.cancelListen != nil {
		p.mu.Unlock()
		return fmt.Errorf("listener for pool %s is already running", p.id)
	}
	ctx, p.cancelListen = context.WithCancel(ctx)
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.cancelListen = nil
		p.mu.Unlock()
	}()

	// Create listener for LISTEN/NOTIFY
	listener := &pgxlisten.Listener{
		Connect: func(ctx context.Context) (*pgx.Conn, error) {
			config := p.manager.pool.Config().ConnConfig.Copy()
			return pgx.ConnectConfig(ctx, config)
		},
	}

	// Check if already closed before setting up listener
	p.mu.RLock()
	if p.listenHandler == nil {
		p.mu.RUnlock()
		return nil // Already closed
	}
	handler := p.listenHandler
	p.mu.RUnlock()

	listener.Handle(p.channelName(), handler)

	if err := listener.Listen(ctx); err != nil {
		p.manager.remove(p)
		p.Close()

		// If the context is cancelled, we can ignore the error.
		if errors.Is(err, context.Canceled) {
			return nil // Listener was cancelled, nothing to do
		}

		// LISTEN/NOTIFY is critical for the current implementation.
		// Without it, clients will block indefinitely waiting for resources.
		//
		// TODO: Implement polling as a fallback mechanism if LISTEN/NOTIFY fails.
		// This would involve periodically checking for available resources
		// instead of waiting for notifications.
		return fmt.Errorf("listener failed: %v", err)
	}

	return nil // never reached
}

// Close stops the listener and releases any resources held by the Numpool.
// It does not Close the underlying database connection pool as it is expected
// to be managed by the caller.
// It is safe to call Close multiple times; subsequent calls will have no effect.
// After calling Close, the Numpool cannot be used to acquire or release resources.
func (p *Numpool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cancelListen != nil {
		p.cancelListen()
		p.cancelListen = nil
	}

	for _, r := range p.resources {
		r.Close()
	}
	p.resources = nil

	p.listenHandler = nil
}

// Listening returns true if the Numpool is currently listening for notifications.
func (p *Numpool) Listening() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.cancelListen != nil
}

// Closed returns true if the Numpool is closed and cannot be used to acquire or
// release resources.
func (p *Numpool) Closed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.listenHandler == nil
}

// Acquire acquires a resource from the pool.
func (p *Numpool) Acquire(ctx context.Context) (*Resource, error) {
	if p.Closed() {
		return nil, fmt.Errorf("cannot acquire resource from closed pool %s", p.id)
	}

	tx, err := p.manager.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	queries := sqlc.New(tx)

	// Get the pool with lock
	model, err := queries.GetNumpoolForUpdate(ctx, p.id)
	if err != nil {
		return nil, fmt.Errorf("failed to get numpool with lock: %w", err)
	}

	if resourceIndex := model.FindUnusedResourceIndex(); resourceIndex != -1 && len(model.WaitQueue) == 0 {
		// We can acquire a resource immediately only if no one is waiting
		affected, err := queries.AcquireResource(ctx, sqlc.AcquireResourceParams{
			ID:            p.id,
			ResourceIndex: resourceIndex,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to acquire resource: %w", err)
		}
		if affected == 0 {
			return nil, fmt.Errorf("failed to acquire resource, it might be in use by another transaction")
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit transaction after acquiring resource: %w", err)
		}

		// Track resource in memory after successful database acquisition.
		// The database transaction ensures acquisition atomicity, and the mutex
		// ensures tracking atomicity. We only track successfully acquired resources.
		p.mu.Lock()
		resource := &Resource{
			pool:  p,
			index: resourceIndex,
		}
		p.resources = append(p.resources, resource)
		p.mu.Unlock()
		return resource, nil
	}

	if !p.Listening() {
		return nil, fmt.Errorf("listener is not running, cannot acquire resource")
	}

	waiterID := uuid.NewString()

	err = queries.EnqueueWaitingClient(ctx, sqlc.EnqueueWaitingClientParams{
		ID:       p.id,
		WaiterID: waiterID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to enqueue waiting client: %w", err)
	}

	err = waitqueue.Wait(ctx, p.listenHandler,
		waitqueue.WithID(waiterID),
		waitqueue.WithAfterRegister(func() error {
			// When we start waiting, commit the transaction and release the lock.
			// This allows other clients to acquire the lock and potentially notify us.
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("failed to commit transaction after enqueuing waiting client: %w", err)
			}
			return nil
		}),
	)
	if err != nil {
		// Remove the client from the wait queue if we failed to wait.
		// We use a new background context here because the original context might be cancelled.
		if cleanupErr := p.removeFromWaitQueue(context.Background(), waiterID); cleanupErr != nil {
			// Log the cleanup error but don't fail the operation
			_ = cleanupErr // TODO: Add proper logging when available
		}
		return nil, fmt.Errorf("failed to wait for resource: %w", err)
	}

	// At this point, we are the first in the wait queue.
	return p.acquireAsFirstInQueue(ctx, waiterID)
}

// WithLock runs a function exclusively across all numpool instances with
// the same ID.
func (p *Numpool) WithLock(ctx context.Context, fn func() error) error {
	return pgx.BeginFunc(ctx, p.manager.pool, func(tx pgx.Tx) error {
		if _, err := sqlc.New(tx).GetNumpoolForUpdate(ctx, p.id); err != nil {
			return fmt.Errorf("failed to get numpool with lock: %w", err)
		}
		return fn()
	})
}

func (p *Numpool) removeFromWaitQueue(ctx context.Context, waiterID string) error {
	// Remove the client from the wait queue
	return pgx.BeginFunc(ctx, p.manager.pool, func(tx pgx.Tx) error {
		queries := sqlc.New(tx)
		if _, err := queries.GetNumpoolForUpdate(ctx, p.id); err != nil {
			return fmt.Errorf("failed to get numpool with lock: %w", err)
		}
		return queries.RemoveFromWaitQueue(ctx, sqlc.RemoveFromWaitQueueParams{
			ID:       p.id,
			WaiterID: waiterID,
		})
	})
}

func (p *Numpool) acquireAsFirstInQueue(ctx context.Context, waiterID string) (*Resource, error) {
	var resourceIndex int

	err := pgx.BeginFunc(ctx, p.manager.pool, func(tx pgx.Tx) error {
		queries := sqlc.New(tx)

		// Get the pool with lock
		model, err := queries.GetNumpoolForUpdate(ctx, p.id)
		if err != nil {
			return fmt.Errorf("failed to get numpool with lock: %w", err)
		}

		// Check if we are the first in the wait queue and if there are unused resources.
		// Should never happen if we are using the wait queue correctly.
		if len(model.WaitQueue) == 0 {
			return fmt.Errorf("expected to be first in the wait queue, but no waiters found")
		}
		if model.WaitQueue[0] != waiterID {
			return fmt.Errorf("not the first in the wait queue: expected %s, got %s", model.WaitQueue[0], waiterID)
		}
		resourceIndex = model.FindUnusedResourceIndex()
		if resourceIndex == -1 {
			return fmt.Errorf("no unused resources available")
		}

		affected, err := queries.AcquireResourceAndDequeueFirstWaiter(ctx, sqlc.AcquireResourceAndDequeueFirstWaiterParams{
			ID:            p.id,
			WaiterID:      waiterID,
			ResourceIndex: int32(resourceIndex),
		})
		if err != nil {
			return fmt.Errorf("failed to acquire resource and dequeue first waiter: %w", err)
		}
		if affected == 0 {
			// This should not happen with proper locking
			return fmt.Errorf("failed to acquire resource, something went wrong")
		}

		// CRITICAL FIX: After acquiring a resource, check if there are still waiters
		// and available resources. If so, notify the next waiter to prevent deadlock.
		// This fixes the chain notification bug where waiters get stuck when resources
		// are available but no release events trigger notifications.
		updatedModel, err := queries.GetNumpoolForUpdate(ctx, p.id)
		if err != nil {
			return fmt.Errorf("failed to get updated model: %w", err)
		}

		// If there are still waiters and available resources, notify the next waiter
		if len(updatedModel.WaitQueue) > 0 && updatedModel.FindUnusedResourceIndex() != -1 {
			err = queries.NotifyWaiters(ctx, sqlc.NotifyWaitersParams{
				ChannelName: p.channelName(),
				WaiterID:    updatedModel.WaitQueue[0],
			})
			if err != nil {
				return fmt.Errorf("failed to notify next waiter: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Track resource in memory after successful database acquisition.
	// Same pattern as above - database transaction ensures acquisition atomicity.
	p.mu.Lock()
	resource := &Resource{
		pool:  p,
		index: resourceIndex,
	}
	p.resources = append(p.resources, resource)
	p.mu.Unlock()
	return resource, nil
}

// release releases a resource back to the pool.
func (p *Numpool) release(ctx context.Context, r *Resource) error {
	return pgx.BeginFunc(ctx, p.manager.pool, func(tx pgx.Tx) error {
		q := sqlc.New(tx)

		// Get the pool with lock
		model, err := q.GetNumpoolForUpdate(ctx, p.id)
		if err != nil {
			return fmt.Errorf("failed to get numpool with lock: %w", err)
		}

		affected, err := q.ReleaseResource(ctx, sqlc.ReleaseResourceParams{
			ID:            p.id,
			ResourceIndex: r.index,
		})
		if err != nil {
			return fmt.Errorf("failed to release resource: %w", err)
		}
		if affected == 0 {
			// Resource was already released - this is idempotent and should not be an error
			return nil
		}

		if len(model.WaitQueue) == 0 {
			// No waiters, nothing to notify
			return nil
		}

		// Notify the first waiter in the queue
		err = q.NotifyWaiters(ctx, sqlc.NotifyWaitersParams{
			ChannelName: p.channelName(),
			WaiterID:    model.WaitQueue[0],
		})
		if err != nil {
			return fmt.Errorf("failed to notify waiting client: %w", err)
		}

		return nil
	})
}

func (p *Numpool) channelName() string {
	// PostgreSQL channel names have a limit, so we use a shorter format
	return fmt.Sprintf("np_%s", p.id)
}

// UpdateMetadata updates the metadata of the Numpool instance.
// It returns an error if the update fails or if the metadata has been modified by another transaction.
// Pass nil to set the metadata to null in the database.
func (m *Numpool) UpdateMetadata(ctx context.Context, metadata json.RawMessage) error {
	return pgx.BeginFunc(ctx, m.manager.pool, func(tx pgx.Tx) error {
		q := sqlc.New(tx)

		// Get the pool with lock
		model, err := q.GetNumpoolForUpdate(ctx, m.id)
		if err != nil {
			return fmt.Errorf("failed to get numpool with lock: %w", err)
		}

		// Check if the current metadata in DB matches what we have in memory (optimistic locking)
		// We need to compare semantically, not byte-wise, since JSON formatting can differ
		if !jsonEqual(model.Metadata, m.metadata) {
			// Another transaction has modified the metadata
			// If it was modified to the same value we want to set, that's fine
			if jsonEqual(model.Metadata, metadata) {
				// Update our in-memory copy to match the database and return success
				m.metadata = model.Metadata
				return nil
			}
			return fmt.Errorf("metadata for pool %s has been modified by another transaction", m.id)
		}

		err = q.UpdateNumpoolMetadata(ctx, sqlc.UpdateNumpoolMetadataParams{
			ID:       m.id,
			Metadata: metadata,
		})
		if err != nil {
			return fmt.Errorf("failed to update metadata: %w", err)
		}

		// Update the in-memory metadata to reflect the new value
		m.metadata = metadata
		return nil
	})
}

// jsonEqual compares two JSON byte slices for semantic equality.
// This handles cases where JSON formatting or key ordering differs.
func jsonEqual(a, b json.RawMessage) bool {
	// Handle nil cases
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Parse both JSON values
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}

	// Compare the parsed values
	return reflect.DeepEqual(va, vb)
}
