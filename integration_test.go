package numpool_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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

// TestMultipleConcurrentAcquires tests that multiple goroutines can acquire
// resources concurrently and that all waiting goroutines eventually get resources.
func TestMultipleConcurrentAcquires(t *testing.T) {
	ctx := context.Background()
	poolID := fmt.Sprintf("test_pool_%s", t.Name())
	dbPool := internal.MustGetPoolWithCleanup(t)

	// Clean up any existing pool with the same ID
	conn, err := dbPool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire connection")
	queries := sqlc.New(conn.Conn())
	_ = queries.DeleteNumpool(ctx, poolID)
	conn.Release()

	const maxResources = 2
	const numGoroutines = 10

	pool, err := numpool.CreateOrOpen(ctx, numpool.Config{
		Pool:              dbPool,
		ID:                poolID,
		MaxResourcesCount: maxResources,
	})
	require.NoError(t, err, "failed to create pool")

	// Use a wait group to track all goroutines
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Track resources and errors for each goroutine
	resources := make([]*numpool.Resource, numGoroutines)
	errors := make([]error, numGoroutines)

	// Channel to coordinate resource holding time
	holdResource := make(chan int, numGoroutines)
	releaseResource := make(chan int, numGoroutines)

	// Start goroutines that will compete for resources
	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()

			// Try to acquire a resource
			resources[id], errors[id] = pool.Acquire(ctx)
			if errors[id] != nil {
				return
			}

			// Signal that we're holding a resource
			holdResource <- id

			// Wait for signal to release
			<-releaseResource

			// Release the resource
			err := resources[id].Release(ctx)
			if err != nil {
				t.Logf("goroutine %d failed to release: %v", id, err)
			}
		}(i)
	}

	// First maxResources goroutines should acquire immediately
	acquired := make([]int, 0, maxResources)
	for range maxResources {
		select {
		case id := <-holdResource:
			acquired = append(acquired, id)
			t.Logf("goroutine %d acquired resource (batch 1)", id)
		case <-time.After(1 * time.Second):
			t.Fatal("first batch should acquire immediately")
		}
	}

	// Verify no more have acquired yet
	select {
	case id := <-holdResource:
		t.Fatalf("goroutine %d should not have acquired yet", id)
	case <-time.After(100 * time.Millisecond):
		// Good, no more acquired
	}

	// Release first resource and wait for next acquisition
	releaseResource <- acquired[0]
	select {
	case id := <-holdResource:
		t.Logf("goroutine %d acquired resource (batch 2)", id)
	case <-time.After(2 * time.Second):
		t.Fatal("waiting goroutine should acquire after release")
	}

	// Release second resource and wait for next acquisition
	releaseResource <- acquired[1]
	select {
	case id := <-holdResource:
		t.Logf("goroutine %d acquired resource (batch 3)", id)
	case <-time.After(2 * time.Second):
		t.Fatal("waiting goroutine should acquire after release")
	}

	// Now we should have exactly one more waiting
	// Release remaining resources to let last goroutine acquire
	for i := range 2 {
		releaseResource <- i // dummy value, goroutines just need the signal
	}

	// Wait for the last one
	select {
	case id := <-holdResource:
		t.Logf("goroutine %d acquired resource (final)", id)
	case <-time.After(2 * time.Second):
		t.Fatal("last goroutine should acquire")
	}

	// Signal all remaining goroutines to release
	close(releaseResource)

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify all succeeded
	successCount := 0
	for i := range numGoroutines {
		if errors[i] == nil {
			successCount++
			require.NotNil(t, resources[i], "goroutine %d should have acquired a resource", i)
		}
	}
	require.Equal(t, numGoroutines, successCount, "all goroutines should have acquired eventually")
}

// TestMultiplePoolInstancesConcurrentAcquires tests that multiple pool instances
// with the same ID correctly share resources and handle concurrent acquisitions.
func TestMultiplePoolInstancesConcurrentAcquires(t *testing.T) {
	ctx := context.Background()
	poolID := fmt.Sprintf("test_pool_%s", t.Name())
	dbPool := internal.MustGetPoolWithCleanup(t)

	// Clean up any existing pool with the same ID
	conn, err := dbPool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire connection")
	queries := sqlc.New(conn.Conn())
	_ = queries.DeleteNumpool(ctx, poolID)
	conn.Release()

	const maxResources = 3
	const numPools = 4

	// Create multiple pool instances with the same ID
	pools := make([]*numpool.Pool, numPools)
	for i := 0; i < numPools; i++ {
		pool, err := numpool.CreateOrOpen(ctx, numpool.Config{
			Pool:              dbPool,
			ID:                poolID,
			MaxResourcesCount: maxResources,
		})
		require.NoError(t, err, "failed to create pool %d", i)
		pools[i] = pool
	}

	// Test 1: Verify pools share the same resources
	// Acquire all resources using different pool instances
	resources := make([]*numpool.Resource, maxResources)
	for i := 0; i < maxResources; i++ {
		poolIdx := i % numPools // Use different pools
		resources[i], err = pools[poolIdx].Acquire(ctx)
		require.NoError(t, err, "failed to acquire resource %d from pool %d", i, poolIdx)
		require.NotNil(t, resources[i], "resource %d should not be nil", i)
		t.Logf("Acquired resource %d from pool %d", resources[i].Index(), poolIdx)
	}

	// Try to acquire one more - should block
	done := make(chan struct{})
	var blockedResource *numpool.Resource
	var blockedErr error
	
	go func() {
		// Use a different pool instance
		blockedResource, blockedErr = pools[numPools-1].Acquire(ctx)
		close(done)
	}()

	// Should not complete immediately
	select {
	case <-done:
		t.Fatal("acquisition should have blocked")
	case <-time.After(100 * time.Millisecond):
		// Good, it's blocking
	}

	// Release one resource from a different pool
	err = resources[0].Release(ctx)
	require.NoError(t, err, "failed to release resource")

	// Now the blocked acquisition should complete
	select {
	case <-done:
		require.NoError(t, blockedErr, "blocked acquisition should succeed")
		require.NotNil(t, blockedResource, "blocked acquisition should return a resource")
		t.Logf("Blocked goroutine acquired resource %d", blockedResource.Index())
	case <-time.After(2 * time.Second):
		t.Fatal("blocked acquisition should complete after release")
	}

	// Clean up
	for _, res := range resources[1:] {
		if res != nil {
			res.Release(ctx)
		}
	}
	if blockedResource != nil {
		blockedResource.Release(ctx)
	}

	// Test 2: Concurrent acquisitions from multiple pools
	var wg sync.WaitGroup
	successCount := int32(0)
	totalAttempts := numPools * 2 // 2 attempts per pool

	wg.Add(totalAttempts)
	for poolIdx := 0; poolIdx < numPools; poolIdx++ {
		for attempt := 0; attempt < 2; attempt++ {
			go func(pool *numpool.Pool, poolIdx, attempt int) {
				defer wg.Done()
				
				// Try to acquire with a timeout
				acquireCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				defer cancel()
				
				resource, err := pool.Acquire(acquireCtx)
				if err == nil && resource != nil {
					atomic.AddInt32(&successCount, 1)
					t.Logf("Pool %d attempt %d: acquired resource %d", poolIdx, attempt, resource.Index())
					// Hold briefly then release
					time.Sleep(50 * time.Millisecond)
					resource.Release(ctx)
				} else {
					t.Logf("Pool %d attempt %d: failed to acquire (expected for some)", poolIdx, attempt)
				}
			}(pools[poolIdx], poolIdx, attempt)
		}
	}

	wg.Wait()
	
	// At least maxResources acquisitions should have succeeded
	finalCount := atomic.LoadInt32(&successCount)
	require.GreaterOrEqual(t, finalCount, int32(maxResources), 
		"at least %d acquisitions should succeed", maxResources)
	t.Logf("Total successful acquisitions: %d out of %d attempts", finalCount, totalAttempts)
}
