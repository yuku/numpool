package numpool_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool"
	"github.com/yuku/numpool/internal"
	"github.com/yuku/numpool/internal/sqlc"
)

// TestSequentialResourceAcquisition tests acquiring resources sequentially
// from a pool and ensures that the resources are acquired in the expected order.
func TestSequentialResourceAcquisition(t *testing.T) {
	ctx := context.Background()
	poolID := fmt.Sprintf("test_pool_%s", t.Name())
	dbPool := internal.MustGetPoolWithCleanup(t)

	// Clean up any existing pool with the same ID
	conn, err := dbPool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire connection")
	queries := sqlc.New(conn.Conn())
	_ = queries.DeleteNumpool(ctx, poolID)
	conn.Release()

	pool, err := numpool.CreateOrOpen(ctx, numpool.Config{
		Pool:              dbPool,
		ID:                poolID,
		MaxResourcesCount: 2,
	})
	require.NoError(t, err, "failed to create or open pool")

	// Acquire the first resource0
	resource0, err := pool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire resource")
	require.NotNil(t, resource0, "acquired resource should not be nil")
	require.Equal(t, 0, resource0.Index(), "first acquired resource should have index 0")

	// Acquire the second resource
	resource1, err := pool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire second resource")
	require.NotNil(t, resource1, "second acquired resource should not be nil")
	require.Equal(t, 1, resource1.Index(), "second acquired resource should have index 1")

	// Release the first resource
	err = resource0.Release(ctx)
	require.NoError(t, err, "failed to release resource 0")

	// Acquire another resource, which should be the first one again
	resource2, err := pool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire resource after releasing")
	require.NotNil(t, resource2, "acquired resource after release should not be nil")
	require.Equal(t, 0, resource2.Index(), "acquired resource after release should have index 0")
}

// TestParallelResourceAcquisition tests acquiring resources in parallel
// without resource contention, ensuring that each goroutine
// acquires a unique resource from the pool.
func TestParallelResourceAcquisition(t *testing.T) {
	ctx := context.Background()
	poolID := fmt.Sprintf("test_pool_%s", t.Name())
	dbPool := internal.MustGetPoolWithCleanup(t)

	// Clean up any existing pool with the same ID
	conn, err := dbPool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire connection")
	queries := sqlc.New(conn.Conn())
	_ = queries.DeleteNumpool(ctx, poolID)
	conn.Release()

	pool, err := numpool.CreateOrOpen(ctx, numpool.Config{
		Pool:              dbPool,
		ID:                poolID,
		MaxResourcesCount: 2,
	})
	require.NoError(t, err, "failed to create or open pool")

	// Acquire resources in parallel
	var resource0, resource1 *numpool.Resource
	errs := make(chan error, 2)

	go func() {
		var err0 error
		resource0, err0 = pool.Acquire(ctx)
		errs <- err0
	}()

	go func() {
		var err1 error
		resource1, err1 = pool.Acquire(ctx)
		errs <- err1
	}()

	for range 2 {
		err := <-errs
		require.NoError(t, err, "failed to acquire resource in parallel")
	}

	require.NotNil(t, resource0, "first acquired resource should not be nil")
	require.NotNil(t, resource1, "second acquired resource should not be nil")
	require.NotEqual(t, resource0.Index(), resource1.Index(), "acquired resources should have different indices")
}

// TestResourceContentionWithMaxResourceCountOne tests resource contention
// with MaxResourceCount=1, ensuring proper blocking and release behavior.
func TestResourceContentionWithMaxResourceCountOne(t *testing.T) {
	ctx := context.Background()
	poolID := fmt.Sprintf("test_pool_%s", t.Name())
	dbPool := internal.MustGetPoolWithCleanup(t)

	// Clean up any existing pool with the same ID
	conn, err := dbPool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire connection")
	queries := sqlc.New(conn.Conn())
	_ = queries.DeleteNumpool(ctx, poolID)
	conn.Release()

	pool, err := numpool.CreateOrOpen(ctx, numpool.Config{
		Pool:              dbPool,
		ID:                poolID,
		MaxResourcesCount: 1,
	})
	require.NoError(t, err, "failed to create or open pool")

	// Channel to synchronize goroutines
	firstAcquired := make(chan struct{})
	secondBlocked := make(chan struct{})
	firstReleased := make(chan struct{})
	secondAcquired := make(chan struct{})

	var resource1, resource2 *numpool.Resource
	var err1, err2 error

	// First goroutine - acquires first
	go func() {
		resource1, err1 = pool.Acquire(ctx)
		close(firstAcquired)

		// Wait for second goroutine to be blocked on acquire
		<-secondBlocked

		// Add a small delay to ensure second is truly blocked
		time.Sleep(50 * time.Millisecond)

		// Release after ensuring second is waiting
		err := resource1.Release(ctx)
		if err != nil {
			panic(err)
		}
		close(firstReleased)
	}()

	// Second goroutine - waits for first to acquire, then tries to acquire
	go func() {
		// Ensure first goroutine acquires first
		<-firstAcquired

		// Start a goroutine to signal when we're about to block
		go func() {
			// Small delay to ensure we're in the Acquire call
			time.Sleep(10 * time.Millisecond)
			close(secondBlocked)
		}()

		// This should block until first releases
		resource2, err2 = pool.Acquire(ctx)
		close(secondAcquired)
	}()

	// Wait for everything to complete
	<-firstReleased
	<-secondAcquired

	// Verify results
	require.NoError(t, err1, "first goroutine failed to acquire resource")
	require.NoError(t, err2, "second goroutine failed to acquire resource")
	require.NotNil(t, resource1, "first acquired resource should not be nil")
	require.NotNil(t, resource2, "second acquired resource should not be nil")
	require.Equal(t, 0, resource1.Index(), "first acquired resource should have index 0")
	require.Equal(t, 0, resource2.Index(), "second acquired resource should have index 0 (reused after release)")
}

// TestMultiplePoolInstancesWithSameID tests that multiple pool instances
// with the same ID can work together correctly, sharing the same underlying
// resources and wait queue.
func TestMultiplePoolInstancesWithSameID(t *testing.T) {
	ctx := context.Background()
	poolID := fmt.Sprintf("test_pool_%s", t.Name())
	dbPool := internal.MustGetPoolWithCleanup(t)

	// Clean up any existing pool with the same ID
	conn, err := dbPool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire connection")
	queries := sqlc.New(conn.Conn())
	_ = queries.DeleteNumpool(ctx, poolID)
	conn.Release()

	// Create first pool instance
	pool1, err := numpool.CreateOrOpen(ctx, numpool.Config{
		Pool:              dbPool,
		ID:                poolID,
		MaxResourcesCount: 2,
	})
	require.NoError(t, err, "failed to create first pool instance")

	// Create second pool instance with the same ID
	pool2, err := numpool.CreateOrOpen(ctx, numpool.Config{
		Pool:              dbPool,
		ID:                poolID,
		MaxResourcesCount: 2,
	})
	require.NoError(t, err, "failed to create second pool instance")

	// Acquire resource from pool1
	resource1, err := pool1.Acquire(ctx)
	require.NoError(t, err, "failed to acquire resource from pool1")
	require.NotNil(t, resource1, "resource from pool1 should not be nil")
	require.Equal(t, 0, resource1.Index(), "resource from pool1 should have index 0")

	// Acquire resource from pool2 - should get different resource
	resource2, err := pool2.Acquire(ctx)
	require.NoError(t, err, "failed to acquire resource from pool2")
	require.NotNil(t, resource2, "resource from pool2 should not be nil")
	require.Equal(t, 1, resource2.Index(), "resource from pool2 should have index 1")

	// Try to acquire third resource from pool1 - should block
	acquireStarted := make(chan struct{})
	acquireCompleted := make(chan struct{})
	var resource3 *numpool.Resource
	var err3 error

	go func() {
		close(acquireStarted)
		resource3, err3 = pool1.Acquire(ctx)
		close(acquireCompleted)
	}()

	// Wait for acquire to start
	<-acquireStarted
	time.Sleep(50 * time.Millisecond)

	// Verify that acquire is still blocked
	select {
	case <-acquireCompleted:
		t.Fatal("acquire should be blocked")
	default:
		// Expected - acquire is blocked
	}

	// Release resource from pool2
	err = resource2.Release(ctx)
	require.NoError(t, err, "failed to release resource from pool2")

	// Now pool1 should be able to acquire the released resource
	<-acquireCompleted
	require.NoError(t, err3, "failed to acquire resource from pool1 after release")
	require.NotNil(t, resource3, "resource from pool1 after release should not be nil")
	require.Equal(t, 1, resource3.Index(), "resource from pool1 after release should have index 1")

	// Cleanup
	err = resource1.Release(ctx)
	require.NoError(t, err, "failed to release resource1")
	err = resource3.Release(ctx)
	require.NoError(t, err, "failed to release resource3")
}

// TestIdempotentRelease tests that Release can be called multiple times safely.
func TestIdempotentRelease(t *testing.T) {
	ctx := context.Background()
	poolID := fmt.Sprintf("test_pool_%s", t.Name())
	dbPool := internal.MustGetPoolWithCleanup(t)

	// Clean up any existing pool with the same ID
	conn, err := dbPool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire connection")
	queries := sqlc.New(conn.Conn())
	_ = queries.DeleteNumpool(ctx, poolID)
	conn.Release()

	pool, err := numpool.CreateOrOpen(ctx, numpool.Config{
		Pool:              dbPool,
		ID:                poolID,
		MaxResourcesCount: 1,
	})
	require.NoError(t, err, "failed to create pool")

	// Acquire a resource
	resource, err := pool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire resource")
	require.NotNil(t, resource, "acquired resource should not be nil")

	// First release should succeed
	err = resource.Release(ctx)
	require.NoError(t, err, "first release should succeed")

	// Second release should also succeed (no-op)
	err = resource.Release(ctx)
	require.NoError(t, err, "second release should succeed (no-op)")

	// Third release should also succeed (no-op)
	err = resource.Release(ctx)
	require.NoError(t, err, "third release should succeed (no-op)")

	// Verify that the resource is actually available for re-acquisition
	resource2, err := pool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire resource after release")
	require.NotNil(t, resource2, "re-acquired resource should not be nil")
	require.Equal(t, 0, resource2.Index(), "re-acquired resource should have same index")

	// Clean up
	err = resource2.Release(ctx)
	require.NoError(t, err, "failed to release resource2")
}

// TestResourceCloseMethod tests that Close() method works correctly.
func TestResourceCloseMethod(t *testing.T) {
	ctx := context.Background()
	poolID := fmt.Sprintf("test_pool_%s", t.Name())
	dbPool := internal.MustGetPoolWithCleanup(t)

	// Clean up any existing pool with the same ID
	conn, err := dbPool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire connection")
	queries := sqlc.New(conn.Conn())
	_ = queries.DeleteNumpool(ctx, poolID)
	conn.Release()

	pool, err := numpool.CreateOrOpen(ctx, numpool.Config{
		Pool:              dbPool,
		ID:                poolID,
		MaxResourcesCount: 1,
	})
	require.NoError(t, err, "failed to create pool")

	// Test pattern: acquire and use Close() with defer
	func() {
		resource, err := pool.Acquire(ctx)
		require.NoError(t, err, "failed to acquire resource")
		require.NotNil(t, resource, "acquired resource should not be nil")
		defer resource.Close() // Simple defer pattern

		// Use resource...
		require.Equal(t, 0, resource.Index())
	}()

	// Verify resource was released and can be re-acquired
	resource2, err := pool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire resource after Close()")
	require.NotNil(t, resource2, "re-acquired resource should not be nil")
	require.Equal(t, 0, resource2.Index(), "re-acquired resource should have same index")

	// Test that Close() is idempotent
	resource2.Close()
	require.NotPanics(t, func() {
		resource2.Close()
	}, "Close() should be idempotent and not panic on multiple calls")
}
