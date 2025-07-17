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

	manager       *Manager
	listenHandler *waitqueue.ListenHandler

	mu        sync.Mutex // Protects access to the pool's resources
	listening bool       // Indicates if the listener is currently active
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
	if err := p.startListen(); err != nil {
		return err
	}
	defer func() { p.listening = false }()

	// Create listener for LISTEN/NOTIFY
	listener := &pgxlisten.Listener{
		Connect: func(ctx context.Context) (*pgx.Conn, error) {
			config := p.manager.pool.Config().ConnConfig.Copy()
			return pgx.ConnectConfig(ctx, config)
		},
	}
	listener.Handle(p.channelName(), p.listenHandler)

	if err := listener.Listen(ctx); err != nil {
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

func (p *Numpool) startListen() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.listening {
		return fmt.Errorf("listener for pool %s is already running", p.id)
	}
	p.listening = true
	return nil
}

// Acquire acquires a resource from the pool.
func (p *Numpool) Acquire(ctx context.Context) (*Resource, error) {
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

	if resourceIndex := model.FindUnusedResourceIndex(); resourceIndex != -1 {
		// We can acquire a resource immediately
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
		return &Resource{
			pool:  p,
			index: resourceIndex,
		}, nil
	}

	if !p.listening {
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
		if err := p.removeFromWaitQueue(context.Background(), waiterID); err != nil {
			return nil, fmt.Errorf("failed to remove client from wait queue: %w", err)
		}
		return nil, fmt.Errorf("failed to wait for resource: %w", err)
	}

	// At this point, we are the first in the wait queue.
	return p.acquireAsFirstInQueue(ctx, waiterID)
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
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &Resource{
		pool:  p,
		index: resourceIndex,
	}, nil
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
			return fmt.Errorf("resource was not in use")
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

// Delete removes a Numpool instance by its ID.
// It returns an error if the pool does not exist or if the deletion fails.
func (m *Numpool) Delete(ctx context.Context) error {
	affected, err := sqlc.New(m.manager.pool).DeleteNumpool(ctx, m.id)
	if err != nil {
		return fmt.Errorf("failed to delete pool %s: %w", m.id, err)
	}
	if affected == 0 {
		return fmt.Errorf("pool %s does not exist", m.id)
	}
	return nil
}

// UpdateMetadata updates the metadata of the Numpool instance.
// It returns an error if the update fails or if the metadata has been modified by another transaction.
func (m *Numpool) UpdateMetadata(ctx context.Context, metadata json.RawMessage) error {
	return pgx.BeginFunc(ctx, m.manager.pool, func(tx pgx.Tx) error {
		q := sqlc.New(tx)

		// Convert nil to JSON null representation
		if metadata == nil {
			metadata = json.RawMessage("null")
		}

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
