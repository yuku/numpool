package numpool

import (
	"context"

	"github.com/yuku/numpool/internal/numpool"
)

// Pool represents a pool of resources that can be acquired and released.
// This is a wrapper around the internal implementation.
type Pool = numpool.Pool

// Resource represents a resource acquired from the pool.
// This is a wrapper around the internal implementation.
type Resource = numpool.Resource

// Config holds the configuration for creating or opening a pool.
type Config = numpool.Config

// CreateOrOpen creates a new pool or opens an existing one based on the
// provided configuration. If the pool already exists with a different
// MaxResourcesCount, it returns an error.
func CreateOrOpen(ctx context.Context, conf Config) (*Pool, error) {
	return numpool.CreateOrOpen(ctx, conf)
}