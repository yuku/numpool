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
		resource0, err = pool.Acquire(ctx)
		errs <- err
	}()

	go func() {
		resource1, err = pool.Acquire(ctx)
		errs <- err
	}()

	for range 2 {
		err = <-errs
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
