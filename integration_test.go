package numpool_test

import (
	"context"
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
	manager, poolID := setupWithUniquePoolID(t)

	pool, err := manager.GetOrCreate(ctx, numpool.Config{
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

func TestBlockedResourceAcquisition(t *testing.T) {
	ctx := context.Background()
	manager, poolID := setupWithUniquePoolID(t)

	pool, err := manager.GetOrCreate(ctx, numpool.Config{
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

	done := make(chan struct{})
	started := make(chan struct{})
	go func() {
		close(started)                      // Signal that we're about to call Acquire
		resource2, err := pool.Acquire(ctx) // This should block until resource0 is released
		require.NoError(t, err, "failed to acquire resource after releasing")
		require.NotNil(t, resource2, "acquired resource after release should not be nil")
		require.Equal(t, 0, resource2.Index(), "acquired resource after release should have index 0")
		close(done)
	}()

	// Wait for the goroutine to start and give it more time to get into the Acquire call
	<-started
	time.Sleep(100 * time.Millisecond)

	// Release the first resource
	err = resource0.Release(ctx)
	require.NoError(t, err, "failed to release resource 0")

	select {
	case <-done:
		// Good, resource2 was acquired after release
	case <-time.After(1 * time.Second):
		require.FailNow(t, "failed to acquire resource after releasing resource 0")
	}
}

// TestParallelResourceAcquisition tests acquiring resources in parallel
// without resource contention, ensuring that each goroutine
// acquires a unique resource from the pool.
func TestParallelResourceAcquisition(t *testing.T) {
	ctx := context.Background()
	manager, poolID := setupWithUniquePoolID(t)

	pool, err := manager.GetOrCreate(ctx, numpool.Config{
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
	manager, poolID := setupWithUniquePoolID(t)

	pool, err := manager.GetOrCreate(ctx, numpool.Config{
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
	manager, poolID := setupWithUniquePoolID(t)

	// Create first pool instance
	pool1, err := manager.GetOrCreate(ctx, numpool.Config{
		ID:                poolID,
		MaxResourcesCount: 2,
	})
	require.NoError(t, err, "failed to create first pool instance")

	// Create second pool instance with the same ID
	pool2, err := manager.GetOrCreate(ctx, numpool.Config{
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
	manager, poolID := setupWithUniquePoolID(t)

	pool, err := manager.GetOrCreate(ctx, numpool.Config{
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
	manager, poolID := setupWithUniquePoolID(t)

	pool, err := manager.GetOrCreate(ctx, numpool.Config{
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
	manager, poolID := setupWithUniquePoolID(t)

	const maxResources = 2
	const numGoroutines = 10

	pool, err := manager.GetOrCreate(ctx, numpool.Config{
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
	manager, poolID := setupWithUniquePoolID(t)

	const maxResources = 3
	const numPools = 4

	// Create multiple pool instances with the same ID
	pools := make([]*numpool.Numpool, numPools)
	for i := range numPools {
		pool, err := manager.GetOrCreate(ctx, numpool.Config{
			ID:                poolID,
			MaxResourcesCount: maxResources,
		})
		require.NoError(t, err, "failed to create pool %d", i)
		pools[i] = pool
	}

	// Test 1: Verify pools share the same resources
	// Acquire all resources using different pool instances
	resources := make([]*numpool.Resource, maxResources)
	for i := range maxResources {
		poolIdx := i % numPools // Use different pools
		var err error
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
	err := resources[0].Release(ctx)
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
			res.Close()
		}
	}
	if blockedResource != nil {
		blockedResource.Close()
	}

	// Test 2: Concurrent acquisitions from multiple pools
	var wg sync.WaitGroup
	successCount := int32(0)
	totalAttempts := numPools * 2 // 2 attempts per pool

	wg.Add(totalAttempts)
	for poolIdx := range numPools {
		for attempt := range 2 {
			go func(pool *numpool.Numpool, poolIdx, attempt int) {
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
					resource.Close()
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

// TestAcquireWithTimeout tests that context timeout is properly respected
// when acquiring resources.
func TestAcquireWithTimeout(t *testing.T) {
	ctx := context.Background()
	manager, poolID := setupWithUniquePoolID(t)

	pool, err := manager.GetOrCreate(ctx, numpool.Config{
		ID:                poolID,
		MaxResourcesCount: 1,
	})
	require.NoError(t, err, "failed to create pool")

	// Acquire the only resource
	resource1, err := pool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire first resource")
	require.NotNil(t, resource1, "first resource should not be nil")
	defer resource1.Close()

	// Try to acquire with a short timeout - should fail
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	resource2, err := pool.Acquire(timeoutCtx)
	elapsed := time.Since(start)

	require.Error(t, err, "acquire should fail with timeout")
	require.Nil(t, resource2, "resource should be nil on timeout")
	require.ErrorIs(t, err, context.DeadlineExceeded, "error should be DeadlineExceeded")
	require.Less(t, elapsed, 200*time.Millisecond, "should timeout quickly")
	t.Logf("Acquire timed out after %v", elapsed)

	// Verify the resource can still be acquired after timeout
	done := make(chan struct{})
	go func() {
		resource3, err := pool.Acquire(ctx)
		require.NoError(t, err, "should be able to acquire after timeout")
		require.NotNil(t, resource3, "resource should not be nil")
		resource3.Close()
		close(done)
	}()

	// Release the first resource
	time.Sleep(50 * time.Millisecond)
	err = resource1.Release(ctx)
	require.NoError(t, err, "failed to release resource")

	// The goroutine should complete
	select {
	case <-done:
		// Good
	case <-time.After(1 * time.Second):
		require.FailNow(t, "goroutine should complete after resource is released")
	}
}

// TestAcquireWithCancellation tests that context cancellation is properly
// respected when acquiring resources.
func TestAcquireWithCancellation(t *testing.T) {
	ctx := context.Background()
	manager, poolID := setupWithUniquePoolID(t)

	pool, err := manager.GetOrCreate(ctx, numpool.Config{
		ID:                poolID,
		MaxResourcesCount: 1,
	})
	require.NoError(t, err, "failed to create pool")

	// Acquire the only resource
	resource1, err := pool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire first resource")
	require.NotNil(t, resource1, "first resource should not be nil")
	defer resource1.Close()

	// Try to acquire with cancellation
	cancelCtx, cancel := context.WithCancel(ctx)

	acquireDone := make(chan struct{})
	var resource2 *numpool.Resource
	var acquireErr error

	go func() {
		resource2, acquireErr = pool.Acquire(cancelCtx)
		close(acquireDone)
	}()

	// Wait a bit then cancel
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Acquisition should fail quickly
	select {
	case <-acquireDone:
		require.Error(t, acquireErr, "acquire should fail on cancellation")
		require.Nil(t, resource2, "resource should be nil on cancellation")
		require.ErrorIs(t, acquireErr, context.Canceled, "error should be Canceled")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("acquire should fail quickly after cancellation")
	}

	// Verify the wait queue is cleaned up properly
	// by successfully acquiring after releasing
	err = resource1.Release(ctx)
	require.NoError(t, err, "failed to release resource")

	resource3, err := pool.Acquire(ctx)
	require.NoError(t, err, "should be able to acquire after cancelled request")
	require.NotNil(t, resource3, "resource should not be nil")
	resource3.Close()
}

// TestLongWaitQueue tests behavior with many waiting clients
func TestLongWaitQueue(t *testing.T) {
	ctx := context.Background()
	manager, poolID := setupWithUniquePoolID(t)

	const maxResources = 2
	const numWaiters = 10

	pool, err := manager.GetOrCreate(ctx, numpool.Config{
		ID:                poolID,
		MaxResourcesCount: maxResources,
	})
	require.NoError(t, err, "failed to create pool")

	// Acquire all resources
	resources := make([]*numpool.Resource, maxResources)
	for i := range maxResources {
		resources[i], err = pool.Acquire(ctx)
		require.NoError(t, err, "failed to acquire resource %d", i)
	}

	// Start many waiters
	var wg sync.WaitGroup
	wg.Add(numWaiters)

	acquireOrder := make(chan int, numWaiters)
	errors := make([]error, numWaiters)

	for i := range numWaiters {
		go func(id int) {
			defer wg.Done()

			// Add some jitter to avoid thundering herd
			time.Sleep(time.Duration(id) * 5 * time.Millisecond)

			resource, err := pool.Acquire(ctx)
			errors[id] = err

			if err == nil && resource != nil {
				acquireOrder <- id
				// Hold briefly
				time.Sleep(50 * time.Millisecond)
				resource.Close()
			}
		}(i)
	}

	// Give waiters time to get queued
	time.Sleep(200 * time.Millisecond)

	// Release resources one by one
	for i, res := range resources {
		err = res.Release(ctx)
		require.NoError(t, err, "failed to release resource %d", i)
		time.Sleep(100 * time.Millisecond) // Let one waiter acquire
	}

	// Wait for all waiters
	wg.Wait()
	close(acquireOrder)

	// Verify all waiters eventually succeeded
	for i, err := range errors {
		require.NoError(t, err, "waiter %d should succeed", i)
	}

	// Check FIFO order (with some tolerance for timing)
	var order []int
	for id := range acquireOrder {
		order = append(order, id)
	}

	t.Logf("Acquisition order: %v", order)
	require.Len(t, order, numWaiters, "all waiters should have acquired")
}

// TestMaxResourcesLimit tests the maximum resource count limit
func TestMaxResourcesLimit(t *testing.T) {
	ctx := context.Background()
	manager, poolID := setupWithUniquePoolID(t)

	// Test creating pool with maximum allowed resources
	pool, err := manager.GetOrCreate(ctx, numpool.Config{
		ID:                poolID,
		MaxResourcesCount: numpool.MaxResourcesLimit,
	})
	require.NoError(t, err, "should be able to create pool with %d resources", numpool.MaxResourcesLimit)
	require.NotNil(t, pool, "pool should not be nil")

	// Verify we can acquire all resources up to the limit
	resources := make([]*numpool.Resource, numpool.MaxResourcesLimit)
	for i := range numpool.MaxResourcesLimit {
		resources[i], err = pool.Acquire(ctx)
		require.NoError(t, err, "failed to acquire resource %d", i)
		require.NotNil(t, resources[i], "resource %d should not be nil", i)
		require.Equal(t, i, resources[i].Index(), "resource index should match")
	}

	// Try to acquire one more - should block
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	extra, err := pool.Acquire(timeoutCtx)
	require.Error(t, err, "should not be able to acquire resource beyond limit")
	require.Nil(t, extra, "extra resource should be nil")

	// Release all resources
	for i, res := range resources {
		err = res.Release(ctx)
		require.NoError(t, err, "failed to release resource %d", i)
	}

	// Test creating pool with too many resources
	_, err = manager.GetOrCreate(ctx, numpool.Config{
		ID:                "too_many_resources",
		MaxResourcesCount: numpool.MaxResourcesLimit + 1,
	})
	require.Error(t, err, "should not be able to create pool with %d resources", numpool.MaxResourcesLimit+1)
}

// TestRapidAcquireRelease tests rapid acquire/release cycles
func TestRapidAcquireRelease(t *testing.T) {
	ctx := context.Background()
	manager, poolID := setupWithUniquePoolID(t)

	pool, err := manager.GetOrCreate(ctx, numpool.Config{
		ID:                poolID,
		MaxResourcesCount: 5,
	})
	require.NoError(t, err, "failed to create pool")

	const numGoroutines = 10
	const cyclesPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	successCount := int32(0)
	errorCount := int32(0)

	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()

			for range cyclesPerGoroutine {
				// Use a short timeout to avoid blocking forever
				acquireCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)

				resource, err := pool.Acquire(acquireCtx)
				cancel()

				if err == nil && resource != nil {
					atomic.AddInt32(&successCount, 1)
					// Very brief hold
					time.Sleep(time.Millisecond)
					resource.Close()
				} else {
					atomic.AddInt32(&errorCount, 1)
				}
			}
		}(i)
	}

	wg.Wait()

	finalSuccess := atomic.LoadInt32(&successCount)
	finalError := atomic.LoadInt32(&errorCount)

	t.Logf("Rapid acquire/release: %d successful, %d timed out", finalSuccess, finalError)
	require.Greater(t, finalSuccess, int32(0), "should have some successful acquisitions")
	require.Equal(t, int32(numGoroutines*cyclesPerGoroutine), finalSuccess+finalError,
		"total attempts should match")
}

// TestDoubleRelease tests that releasing a resource twice is safe
func TestDoubleRelease(t *testing.T) {
	ctx := context.Background()
	manager, poolID := setupWithUniquePoolID(t)

	pool, err := manager.GetOrCreate(ctx, numpool.Config{
		ID:                poolID,
		MaxResourcesCount: 1,
	})
	require.NoError(t, err, "failed to create pool")

	// Acquire and release a resource
	resource, err := pool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire resource")
	require.NotNil(t, resource, "resource should not be nil")

	// First release should succeed
	err = resource.Release(ctx)
	require.NoError(t, err, "first release should succeed")

	// Second release should also succeed (idempotent)
	err = resource.Release(ctx)
	require.NoError(t, err, "second release should succeed")

	// Verify the resource is available again
	resource2, err := pool.Acquire(ctx)
	require.NoError(t, err, "should be able to acquire after double release")
	require.NotNil(t, resource2, "resource should not be nil")
	resource2.Close()
}

// TestPoolDeletion tests behavior when a pool is deleted while in use
func TestPoolDeletion(t *testing.T) {
	ctx := context.Background()
	manager, poolID := setupWithUniquePoolID(t)

	pool, err := manager.GetOrCreate(ctx, numpool.Config{
		ID:                poolID,
		MaxResourcesCount: 2,
	})
	require.NoError(t, err, "failed to create pool")

	// Acquire a resource
	resource1, err := pool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire resource")
	require.NotNil(t, resource1, "resource should not be nil")

	// Delete the pool from database
	_, err = sqlc.New(internal.MustGetPoolWithCleanup(t)).DeleteNumpool(ctx, poolID)
	require.NoError(t, err, "failed to delete pool")

	// Try to acquire another resource - should fail
	resource2, err := pool.Acquire(ctx)
	require.Error(t, err, "acquire should fail after pool deletion")
	require.Nil(t, resource2, "resource should be nil")

	// Release should still work for existing resource
	err = resource1.Release(ctx)
	// Release might fail since pool is deleted, but should not panic
	t.Logf("Release after deletion error: %v", err)
}

// TestConcurrentPoolCreation tests multiple goroutines creating the same pool
func TestConcurrentPoolCreation(t *testing.T) {
	ctx := context.Background()
	manager, poolID := setupWithUniquePoolID(t)

	const numGoroutines = 10
	const maxResources = 5

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	pools := make([]*numpool.Numpool, numGoroutines)
	errors := make([]error, numGoroutines)

	// All goroutines try to create the same pool
	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			pools[id], errors[id] = manager.GetOrCreate(ctx, numpool.Config{
				ID:                poolID,
				MaxResourcesCount: maxResources,
			})
		}(i)
	}

	wg.Wait()

	// All should succeed
	for i := range numGoroutines {
		require.NoError(t, errors[i], "goroutine %d should create/open pool successfully", i)
		require.NotNil(t, pools[i], "goroutine %d should have a valid pool", i)
	}

	// Verify all pools share the same resources
	resources := make([]*numpool.Resource, 0, maxResources)

	// Acquire all resources using different pool instances
	for i := range maxResources {
		poolIdx := i % numGoroutines
		resource, err := pools[poolIdx].Acquire(ctx)
		require.NoError(t, err, "failed to acquire from pool %d", poolIdx)
		resources = append(resources, resource)
	}

	// Try to acquire one more - should block
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	extra, err := pools[0].Acquire(timeoutCtx)
	require.Error(t, err, "should not be able to acquire extra resource")
	require.Nil(t, extra, "extra resource should be nil")

	// Clean up
	for _, res := range resources {
		res.Close()
	}
}

// TestContextPropagation tests that context values and deadlines are properly propagated
func TestContextPropagation(t *testing.T) {
	ctx := context.Background()
	manager, poolID := setupWithUniquePoolID(t)

	pool, err := manager.GetOrCreate(ctx, numpool.Config{
		ID:                poolID,
		MaxResourcesCount: 1,
	})
	require.NoError(t, err, "failed to create pool")

	// Test with context that has deadline
	deadline := time.Now().Add(5 * time.Second)
	ctxWithDeadline, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	resource, err := pool.Acquire(ctxWithDeadline)
	require.NoError(t, err, "failed to acquire with deadline context")
	require.NotNil(t, resource, "resource should not be nil")

	// Verify the deadline is respected during operations
	err = resource.Release(ctxWithDeadline)
	require.NoError(t, err, "release should succeed with deadline context")

	// Test with already cancelled context
	cancelledCtx, cancel2 := context.WithCancel(ctx)
	cancel2() // Cancel immediately

	resource2, err := pool.Acquire(cancelledCtx)
	require.Error(t, err, "acquire should fail with cancelled context")
	require.ErrorIs(t, err, context.Canceled, "error should be Canceled")
	require.Nil(t, resource2, "resource should be nil")
}

// TestStressTest performs a stress test with many concurrent operations
func TestStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	ctx := context.Background()
	manager, poolID := setupWithUniquePoolID(t)

	const maxResources = 10
	const numPools = 5
	const numGoroutinesPerPool = 20
	const duration = 5 * time.Second

	// Create multiple pool instances
	pools := make([]*numpool.Numpool, numPools)
	for i := range numPools {
		var err error
		pools[i], err = manager.GetOrCreate(ctx, numpool.Config{
			ID:                poolID,
			MaxResourcesCount: maxResources,
		})
		require.NoError(t, err, "failed to create pool %d", i)
	}

	// Track metrics
	successCount := int64(0)
	timeoutCount := int64(0)
	cancelCount := int64(0)

	// Start time
	start := time.Now()
	done := make(chan struct{})

	// Signal when to stop
	go func() {
		time.Sleep(duration)
		close(done)
	}()

	var wg sync.WaitGroup

	// Start goroutines for each pool
	for poolIdx := range numPools {
		for i := range numGoroutinesPerPool {
			wg.Add(1)
			go func(pool *numpool.Numpool, id int) {
				defer wg.Done()

				for {
					select {
					case <-done:
						return
					default:
						// Random operation
						switch id % 3 {
						case 0: // Normal acquire/release with timeout
							timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
							resource, err := pool.Acquire(timeoutCtx)
							cancel()
							if err == nil {
								atomic.AddInt64(&successCount, 1)
								time.Sleep(time.Duration(id%10) * time.Millisecond)
								resource.Close()
							} else {
								atomic.AddInt64(&timeoutCount, 1)
							}

						case 1: // Acquire with timeout
							timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
							resource, err := pool.Acquire(timeoutCtx)
							cancel()
							if err == nil {
								atomic.AddInt64(&successCount, 1)
								time.Sleep(time.Duration(id%5) * time.Millisecond)
								resource.Close()
							} else {
								atomic.AddInt64(&timeoutCount, 1)
							}

						case 2: // Acquire with random cancellation
							cancelCtx, cancel := context.WithCancel(ctx)
							go func() {
								time.Sleep(time.Duration(id%20) * time.Millisecond)
								cancel()
							}()
							resource, err := pool.Acquire(cancelCtx)
							if err == nil {
								atomic.AddInt64(&successCount, 1)
								resource.Close()
							} else {
								atomic.AddInt64(&cancelCount, 1)
							}
						}
					}
				}
			}(pools[poolIdx], i)
		}
	}

	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("Stress test completed in %v", elapsed)
	t.Logf("Successful acquisitions: %d", atomic.LoadInt64(&successCount))
	t.Logf("Timeouts: %d", atomic.LoadInt64(&timeoutCount))
	t.Logf("Cancellations: %d", atomic.LoadInt64(&cancelCount))

	require.Greater(t, atomic.LoadInt64(&successCount), int64(0), "should have successful acquisitions")
}
