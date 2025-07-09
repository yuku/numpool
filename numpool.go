// Package numpool provides a distributed resource pool implementation backed by PostgreSQL.
// It allows multiple processes to share a finite set of resources with automatic
// blocking when resources are unavailable and fair distribution using a wait queue.
//
// The pool uses PostgreSQL's transactional guarantees and LISTEN/NOTIFY mechanism
// to ensure safe concurrent access and efficient resource allocation across multiple
// application instances.
//
// Setup:
//
// Before using numpool, you need to set up the required database table:
//
//	conn, err := pgx.Connect(ctx, databaseURL)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer conn.Close(ctx)
//
//	if err := numpool.Setup(ctx, conn); err != nil {
//		log.Fatal(err)
//	}
//
// Basic usage:
//
//	pool, err := numpool.CreateOrOpen(ctx, numpool.Config{
//		Pool:              dbPool,
//		ID:                "my-resource-pool",
//		MaxResourcesCount: 10,
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Acquire a resource
//	resource, err := pool.Acquire(ctx)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer resource.Close() // or defer resource.Release(ctx)
//
//	// Use the resource
//	fmt.Printf("Using resource %d\n", resource.Index())
package numpool

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/yuku/numpool/internal/numpool"
	"github.com/yuku/numpool/internal/statedb"
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

// Setup initializes the numpool table in the database.
// This function should be called once before using any pools.
// It is safe to call multiple times as it will not recreate existing tables.
func Setup(ctx context.Context, conn *pgx.Conn) error {
	return statedb.Setup(ctx, conn)
}