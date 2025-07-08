package numpool

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool/internal"
	"github.com/yuku/numpool/internal/sqlc"
)

func TestCreateOrOpen(t *testing.T) {
	ctx := context.Background()
	poolID := fmt.Sprintf("test_pool_%s", t.Name())
	dbPool := internal.MustGetPoolWithCleanup(t)
	
	// Get a connection for sqlc queries
	conn, err := dbPool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire connection")
	defer conn.Release()
	q := sqlc.New(conn.Conn())

	// Check if the pool already exists
	_, err = q.GetNumpool(ctx, poolID)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	// Create a new pool
	pool, err := CreateOrOpen(ctx, Config{
		Pool:              dbPool,
		ID:                poolID,
		MaxResourcesCount: 8,
	})
	require.NoError(t, err, "failed to create or open pool")
	t.Cleanup(func() { _ = q.DeleteNumpool(ctx, poolID) })
	require.NotNil(t, pool, "pool should not be nil")
	require.Equal(t, poolID, pool.ID(), "pool ID should match")

	// Verify the pool was created correctly
	row, err := q.GetNumpool(ctx, poolID)
	require.NoError(t, err, "failed to get numpool")
	require.EqualValues(t, 8, row.MaxResourcesCount, "max resources count should match")

	// Open the same pool again
	pool2, err := CreateOrOpen(ctx, Config{
		Pool:              dbPool,
		ID:                poolID,
		MaxResourcesCount: 8,
	})
	require.NoError(t, err, "failed to open existing pool")
	require.NotNil(t, pool2, "opened pool should not be nil")
	require.Equal(t, poolID, pool2.ID(), "opened pool ID should match original")

	// Open the pool with a different MaxResourcesCount
	_, err = CreateOrOpen(ctx, Config{
		Pool:              dbPool,
		ID:                poolID,
		MaxResourcesCount: 16,
	})
	require.Error(t, err, "should not open pool with different MaxResourcesCount")
}
