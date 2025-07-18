package sqlc_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool/internal/sqlc"
)

func TestCreateDeleteNumpool(t *testing.T) {
	ctx := context.Background()
	poolID := fmt.Sprintf("test_pool_%s", t.Name())

	q := sqlc.New(mustGetPoolWithCleanup(t))

	// When creating a numpool
	err := q.CreateNumpool(ctx, sqlc.CreateNumpoolParams{
		ID:                poolID,
		MaxResourcesCount: 8,
	})
	require.NoError(t, err, "failed to create numpool")

	// Then check that the numpool exists
	row, err := q.GetNumpool(ctx, poolID)
	require.NoError(t, err, "failed to get numpool")
	require.EqualValues(t, 8, row.MaxResourcesCount,
		"max resources count should match",
	)
	require.EqualValues(t, 64, row.ResourceUsageStatus.Len,
		"resource usage status length should be 64 bits",
	)
	zero := make([]byte, row.ResourceUsageStatus.Len/8)
	require.Equal(t, zero, row.ResourceUsageStatus.Bytes,
		"resource usage status should be zero for new pool",
	)

	// When deleting the numpool
	_, err = q.DeleteNumpool(ctx, poolID)
	require.NoError(t, err, "failed to delete numpool")

	// Then check that the numpool no longer exists
	_, err = q.GetNumpool(ctx, poolID)
	require.ErrorIs(t, err, pgx.ErrNoRows,
		"should not find numpool after deletion",
	)
}

func TestAcquireResource(t *testing.T) {
	ctx := context.Background()
	poolID := fmt.Sprintf("test_pool_%s", t.Name())

	q := sqlc.New(mustGetPoolWithCleanup(t))

	err := q.CreateNumpool(ctx, sqlc.CreateNumpoolParams{
		ID:                poolID,
		MaxResourcesCount: 8,
	})
	t.Cleanup(func() { _, _ = q.DeleteNumpool(ctx, poolID) })
	require.NoError(t, err, "failed to create numpool")

	row, err := q.GetNumpool(ctx, poolID)
	require.NoError(t, err, "failed to get numpool")
	zero := make([]byte, row.ResourceUsageStatus.Len/8)
	require.Equal(t, zero, row.ResourceUsageStatus.Bytes,
		"resource usage status should be zero for new pool",
	)

	// When acquiring a resource
	rowsAffected, err := q.AcquireResource(ctx, sqlc.AcquireResourceParams{
		ID:            poolID,
		ResourceIndex: 0, // Acquire the first resource (bit 0)
	})
	require.NoError(t, err, "failed to acquire resource")
	require.EqualValues(t, 1, rowsAffected, "should acquire one resource")

	// Then check the resource usage status
	row, err = q.GetNumpool(ctx, poolID)
	require.NoError(t, err, "failed to get numpool after acquiring resource")
	require.Equal(t, []byte{0x80, 0, 0, 0, 0, 0, 0, 0}, row.ResourceUsageStatus.Bytes,
		"resource usage status should have the first bit set after acquiring resource (big-endian)",
	)

	// When trying to acquire the same resource again
	rowsAffected, err = q.AcquireResource(ctx, sqlc.AcquireResourceParams{
		ID:            poolID,
		ResourceIndex: 0, // Try to acquire the same resource again
	})
	require.NoError(t, err, "failed to acquire resource again")
	require.EqualValues(t, 0, rowsAffected,
		"should not acquire resource again since it's already acquired",
	)

	// Check that the resource usage status is unchanged
	row, err = q.GetNumpool(ctx, poolID)
	require.NoError(t, err, "failed to get numpool after trying to acquire resource again")
	require.Equal(t, []byte{0x80, 0, 0, 0, 0, 0, 0, 0}, row.ResourceUsageStatus.Bytes,
		"resource usage status should remain unchanged after trying to acquire already acquired resource",
	)

	// When acquiring another resource
	rowsAffected, err = q.AcquireResource(ctx, sqlc.AcquireResourceParams{
		ID:            poolID,
		ResourceIndex: 3, // Acquire the 4th resource (bit 3)
	})
	require.NoError(t, err, "failed to acquire another resource")
	require.EqualValues(t, 1, rowsAffected, "should acquire another resource")

	// Then check the resource usage status again
	row, err = q.GetNumpool(ctx, poolID)
	require.NoError(t, err, "failed to get numpool after acquiring another resource")
	expectedStatus := []byte{0x80 | 0x10, 0, 0, 0, 0, 0, 0, 0} // bits 0 and 3 set in big-endian
	require.Equal(t, expectedStatus, row.ResourceUsageStatus.Bytes,
		"resource usage status should have the first and fourth bits set after acquiring another resource",
	)
}

func TestReleaseResource(t *testing.T) {
	ctx := context.Background()
	poolID := fmt.Sprintf("test_pool_%s", t.Name())

	q := sqlc.New(mustGetPoolWithCleanup(t))

	// Create a numpool
	err := q.CreateNumpool(ctx, sqlc.CreateNumpoolParams{
		ID:                poolID,
		MaxResourcesCount: 8,
	})
	t.Cleanup(func() { _, _ = q.DeleteNumpool(ctx, poolID) })
	require.NoError(t, err, "failed to create numpool")

	// First acquire some resources
	_, err = q.AcquireResource(ctx, sqlc.AcquireResourceParams{
		ID:            poolID,
		ResourceIndex: 0,
	})
	require.NoError(t, err, "failed to acquire resource 0")

	_, err = q.AcquireResource(ctx, sqlc.AcquireResourceParams{
		ID:            poolID,
		ResourceIndex: 3,
	})
	require.NoError(t, err, "failed to acquire resource 3")

	// Verify resources are acquired
	row, err := q.GetNumpool(ctx, poolID)
	require.NoError(t, err, "failed to get numpool")
	require.Equal(t, []byte{0x80 | 0x10, 0, 0, 0, 0, 0, 0, 0}, row.ResourceUsageStatus.Bytes,
		"resources 0 and 3 should be acquired (big-endian)")

	// When releasing resource 0
	rowsAffected, err := q.ReleaseResource(ctx, sqlc.ReleaseResourceParams{
		ResourceIndex: 0,
		ID:            poolID,
	})
	require.NoError(t, err, "failed to release resource 0")
	require.EqualValues(t, 1, rowsAffected, "should release one resource")

	// Then verify resource 0 is released
	row, err = q.GetNumpool(ctx, poolID)
	require.NoError(t, err, "failed to get numpool after releasing resource")
	require.Equal(t, []byte{0x10, 0, 0, 0, 0, 0, 0, 0}, row.ResourceUsageStatus.Bytes,
		"only resource 3 should be acquired after releasing resource 0 (big-endian)")

	// When releasing resource 3
	rowsAffected, err = q.ReleaseResource(ctx, sqlc.ReleaseResourceParams{
		ResourceIndex: 3,
		ID:            poolID,
	})
	require.NoError(t, err, "failed to release resource 3")
	require.EqualValues(t, 1, rowsAffected, "should release one resource")

	// Then verify all resources are released
	row, err = q.GetNumpool(ctx, poolID)
	require.NoError(t, err, "failed to get numpool after releasing all resources")
	zero := make([]byte, row.ResourceUsageStatus.Len/8)
	require.Equal(t, zero, row.ResourceUsageStatus.Bytes,
		"all resources should be released")

	// Test releasing an already released resource (should not error)
	rowsAffected, err = q.ReleaseResource(ctx, sqlc.ReleaseResourceParams{
		ResourceIndex: 0,
		ID:            poolID,
	})
	require.NoError(t, err,
		"releasing an already released resource should not error",
	)
	require.EqualValues(t, 0, rowsAffected,
		"should not release already released resource",
	)

	// Verify status remains unchanged
	row, err = q.GetNumpool(ctx, poolID)
	require.NoError(t, err, "failed to get numpool")
	require.Equal(t, zero, row.ResourceUsageStatus.Bytes,
		"resource usage status should remain zero",
	)
}

func TestRemoveFromWaitQueue(t *testing.T) {
	ctx := context.Background()
	poolID := fmt.Sprintf("test_pool_%s", t.Name())

	q := sqlc.New(mustGetPoolWithCleanup(t))

	// Create a numpool
	err := q.CreateNumpool(ctx, sqlc.CreateNumpoolParams{
		ID:                poolID,
		MaxResourcesCount: 1,
	})
	t.Cleanup(func() { _, _ = q.DeleteNumpool(ctx, poolID) })
	require.NoError(t, err, "failed to create numpool")

	// Generate test UUIDs
	waiterID1 := "waiter1"
	waiterID2 := "waiter2"
	waiterID3 := "waiter3"

	// Enqueue three waiters
	err = q.EnqueueWaitingClient(ctx, sqlc.EnqueueWaitingClientParams{
		ID:       poolID,
		WaiterID: waiterID1,
	})
	require.NoError(t, err, "failed to enqueue first waiter")

	err = q.EnqueueWaitingClient(ctx, sqlc.EnqueueWaitingClientParams{
		ID:       poolID,
		WaiterID: waiterID2,
	})
	require.NoError(t, err, "failed to enqueue second waiter")

	err = q.EnqueueWaitingClient(ctx, sqlc.EnqueueWaitingClientParams{
		ID:       poolID,
		WaiterID: waiterID3,
	})
	require.NoError(t, err, "failed to enqueue third waiter")

	// Verify all waiters are in the queue
	row, err := q.GetNumpool(ctx, poolID)
	require.NoError(t, err, "failed to get numpool")
	require.Len(t, row.WaitQueue, 3, "wait queue should have three waiters")

	// Remove the middle waiter (waiterID2)
	err = q.RemoveFromWaitQueue(ctx, sqlc.RemoveFromWaitQueueParams{
		ID:       poolID,
		WaiterID: waiterID2,
	})
	require.NoError(t, err, "failed to remove waiter from wait queue")

	// Verify the waiter was removed and order is preserved
	row, err = q.GetNumpool(ctx, poolID)
	require.NoError(t, err, "failed to get numpool after removal")
	require.Len(t, row.WaitQueue, 2, "wait queue should have two waiters after removal")
	require.Equal(t, waiterID1, row.WaitQueue[0], "first waiter should still be first")
	require.Equal(t, waiterID3, row.WaitQueue[1], "third waiter should now be second")

	// Remove a waiter that doesn't exist (should not error)
	nonExistentClient := pgtype.UUID{
		Bytes: [16]byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9},
		Valid: true,
	}
	err = q.RemoveFromWaitQueue(ctx, sqlc.RemoveFromWaitQueueParams{
		ID:       poolID,
		WaiterID: nonExistentClient,
	})
	require.NoError(t, err, "removing non-existent waiter should not error")

	// Verify the queue is unchanged
	row, err = q.GetNumpool(ctx, poolID)
	require.NoError(t, err, "failed to get numpool")
	require.Len(t, row.WaitQueue, 2, "wait queue should still have two waiters")

	// Remove all remaining waiters
	err = q.RemoveFromWaitQueue(ctx, sqlc.RemoveFromWaitQueueParams{
		ID:       poolID,
		WaiterID: waiterID1,
	})
	require.NoError(t, err, "failed to remove first waiter")

	err = q.RemoveFromWaitQueue(ctx, sqlc.RemoveFromWaitQueueParams{
		ID:       poolID,
		WaiterID: waiterID3,
	})
	require.NoError(t, err, "failed to remove third waiter")

	// Verify the queue is empty
	row, err = q.GetNumpool(ctx, poolID)
	require.NoError(t, err, "failed to get numpool after removing all waiters")
	require.Len(t, row.WaitQueue, 0, "wait queue should be empty")
}
