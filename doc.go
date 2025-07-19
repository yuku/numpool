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
// Before using numpool, you need to set up the required database table and get a manager:
//
//	dbPool, err := pgxpool.New(ctx, databaseURL)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer dbPool.Close()
//
//	manager, err := numpool.Setup(ctx, dbPool)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer manager.Close()
//
// Basic usage:
//
//	pool, err := manager.GetOrCreate(ctx, numpool.Config{
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
//
// Lifecycle management:
//
//	// Delete a pool permanently from the database
//	err = manager.Delete(ctx, "my-resource-pool")
//
//	// Close a pool instance (keeps database record)
//	pool.Close()
//
//	// Close manager and all its pools (keeps database records)
//	manager.Close()
package numpool
