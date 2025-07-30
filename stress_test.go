package numpool_test

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool"
	"github.com/yuku/numpool/internal/sqlc"
)

// TestStressTest performs a stress test with many concurrent operations
func TestStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const managersCount = 10
	const maxResources = 3          // Tight resource constraint
	const numPools = 15             // Good pool diversity
	const numGoroutinesPerPool = 25 // High concurrency per pool

	ctx := context.Background()
	managers := make([]*numpool.Manager, managersCount)
	for i := range managersCount {
		var err error
		managers[i], err = numpool.Setup(ctx, connPool)
		require.NoError(t, err, "failed to create Numpool manager %d", i)
	}

	poolID := fmt.Sprintf("test_pool_%s_%d", t.Name(), time.Now().UnixNano())
	_, err := sqlc.New(connPool).DeleteNumpool(ctx, poolID)
	require.NoError(t, err)

	// Ensure cleanup after test
	// defer func() {
	// 	// Close all managers to ensure proper resource cleanup
	// 	for _, manager := range managers {
	// 		manager.Close()
	// 	}
	// 	_, err := sqlc.New(connPool).DeleteNumpool(context.Background(), poolID)
	// 	require.NoError(t, err, "failed to clean up test pool %s", poolID)
	// }()

	// Create multiple pool instances
	pools := make([]*numpool.Numpool, numPools)
	for i := range numPools {
		var err error
		pools[i], err = managers[i%managersCount].GetOrCreate(ctx, numpool.Config{
			ID:                poolID,
			MaxResourcesCount: maxResources,
		})
		require.NoError(t, err, "failed to create pool %d", i)
	}

	// Track metrics
	successCount := int64(0)
	failureCount := int64(0)

	var wg sync.WaitGroup

	// Start goroutines for each pool
	for poolIdx := range numPools {
		for i := range numGoroutinesPerPool {
			wg.Add(1)
			go func(pool *numpool.Numpool, id int) {
				defer wg.Done()

				resource, err := pool.Acquire(ctx)
				if err != nil {
					atomic.AddInt64(&failureCount, 1)
					return
				}
				defer func() {
					if err := resource.Release(context.Background()); err != nil {
						log.Fatalf("failed to release resource: %v", err)
					}
				}()
				atomic.AddInt64(&successCount, 1)
			}(pools[poolIdx], i)
		}
	}

	wg.Wait()

	assert.Equal(t, int64(numPools*numGoroutinesPerPool), successCount, "Expected all goroutines to succeed")
	assert.Zero(t, failureCount, "Expected no failures during resource acquisition")
}
