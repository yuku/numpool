package numpool

import (
	"context"
	"sync"
)

// Resource represents a resource acquired from the pool.
type Resource struct {
	pool        *Numpool
	index       int
	releaseOnce sync.Once
	releaseErr  error
}

// Index returns the index of the resource in the pool.
func (r *Resource) Index() int {
	return r.index
}

// Release releases r back to the pool.
// It is safe to call Release multiple times; subsequent calls will be no-ops.
// This allows for both defer r.Release(ctx) and explicit release patterns.
func (r *Resource) Release(ctx context.Context) error {
	r.releaseOnce.Do(func() {
		r.releaseErr = r.pool.release(ctx, r)
	})
	return r.releaseErr
}

// Close releases the resource back to the pool, ignoring any errors.
// This method is provided for convenience with defer statements.
// It is equivalent to calling Release with a background context and ignoring the error.
func (r *Resource) Close() {
	_ = r.Release(context.Background())
}
