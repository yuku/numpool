package numpool_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool"
	"github.com/yuku/numpool/internal"
)

// TestSequentialResourceAcquisition tests acquiring resources sequentially
// from a pool and ensures that the resources are acquired in the expected order.
func TestSequentialResourceAcquisition(t *testing.T) {
	ctx := context.Background()
	poolID := fmt.Sprintf("test_pool_%s", t.Name())
	conn := internal.MustGetConnectionWithCleanup(t)

	pool, err := numpool.CreateOrOpen(ctx, numpool.Config{
		Conn:              conn,
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
