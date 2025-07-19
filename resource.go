package numpool

import (
	"context"
	"sync"
)

// Resource represents a resource acquired from the pool.
type Resource struct {
	pool       *Numpool
	index      int
	closed     bool
	releaseErr error
	mu         sync.RWMutex
}

// Index returns the index of the resource in the pool.
func (r *Resource) Index() int {
	return r.index
}

// Release releases r back to the pool.
// It is safe to call Release multiple times; subsequent calls will return the original error if any.
func (r *Resource) Release(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return r.releaseErr // Return the stored error if already released
	}

	r.releaseErr = r.pool.release(ctx, r) // Store the error from release
	if r.releaseErr == nil {
		r.closed = true // Mark as closed only on successful release
	}
	return r.releaseErr
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
