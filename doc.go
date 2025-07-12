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
