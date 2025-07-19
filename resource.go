package numpool

import (
	"context"
	"sync"
)

// Resource represents a resource acquired from the pool.
type Resource struct {
	pool   *Numpool
	index  int
	closed bool
	mu     sync.RWMutex
}

// Index returns the index of the resource in the pool.
func (r *Resource) Index() int {
	return r.index
}

// Release releases r back to the pool.
// It is safe to call Release multiple times; subsequent calls will be no-ops.
func (r *Resource) Release(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil // Already released
	}

	if err := r.pool.release(ctx, r); err != nil {
		return err
	}
	r.closed = true
	return nil
}

// Close releases the resource back to the pool, ignoring any errors.
// This method is provided for convenience with defer statements.
// It is equivalent to calling Release with a background context and ignoring the error.
func (r *Resource) Close() {
	_ = r.Release(context.Background())
}

// Closed returns if the resource is closed.
func (r *Resource) Closed() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.closed
}
