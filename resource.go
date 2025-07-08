package numpool

import "context"

// Resource represents a resource acquired from the pool.
type Resource struct {
	pool  *Pool
	index int
}

// Index returns the index of the resource in the pool.
func (r *Resource) Index() int {
	return r.index
}

// Release releases r back to the pool.
func (r *Resource) Release(ctx context.Context) error {
	return r.pool.release(ctx, r)
}
