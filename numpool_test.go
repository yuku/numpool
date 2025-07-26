package numpool_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool"
	"github.com/yuku/numpool/internal/sqlc"
)

func TestNumpool_Delete(t *testing.T) {
	ctx := context.Background()
	queries := sqlc.New(connPool)

	manager, err := numpool.Setup(ctx, connPool)
	require.NoError(t, err, "Setup should not return an error")

	t.Run("deletes existing pool", func(t *testing.T) {
		// Given
		conf := numpool.Config{
			ID:                t.Name(),
			MaxResourcesCount: 5,
		}
		model, err := manager.GetOrCreate(ctx, conf)
		require.NoError(t, err, "GetOrCreate should not return an error")
		assert.NotNil(t, model, "GetOrCreate should return a valid Numpool instance")

		// When
		err = manager.Delete(ctx, model.ID())

		// Then
		assert.NoError(t, err, "Delete should not return an error")
		exists, err := queries.CheckNumpoolExists(ctx, conf.ID)
		assert.NoError(t, err, "CheckNumpoolExists should not return an error after deletion")
		assert.False(t, exists, "Numpool should not exist after deletion")

		// Verify the pool instance is closed
		assert.True(t, model.Closed(), "Pool should be closed after deletion")

		t.Run("returns error for non-existing pool", func(t *testing.T) {
			// When (pool was already deleted in parent test)
			err := manager.Delete(ctx, model.ID())

			// Then
			assert.Error(t, err, "Delete should return an error for non-existing pool")
			assert.Contains(t, err.Error(), "is not managed by this manager", "Error should indicate pool is not managed")
		})
	})

	t.Run("returns error when pool exists in DB but not managed by this manager", func(t *testing.T) {
		// Given: Create a pool with one manager
		conf := numpool.Config{
			ID:                "unmanaged-pool",
			MaxResourcesCount: 3,
		}

		manager1, err := numpool.Setup(ctx, connPool)
		require.NoError(t, err, "Setup should not return an error")

		_, err = manager1.GetOrCreate(ctx, conf)
		require.NoError(t, err, "GetOrCreate should not return an error")

		// Create a different manager that doesn't manage this pool
		manager2, err := numpool.Setup(ctx, connPool)
		require.NoError(t, err, "Setup should not return an error")

		// When: Try to delete with manager2 (pool exists in DB but not managed by manager2)
		err = manager2.Delete(ctx, conf.ID)

		// Then: Should return error indicating pool is not managed
		assert.Error(t, err, "Delete should return an error for unmanaged pool")
		assert.Contains(t, err.Error(), "is not managed by this manager", "Error should indicate pool is not managed")

		// Verify pool still exists in database
		exists, err := queries.CheckNumpoolExists(ctx, conf.ID)
		assert.NoError(t, err, "CheckNumpoolExists should not return an error")
		assert.True(t, exists, "Pool should still exist in database after failed delete")

		// Cleanup: Delete with the managing manager
		err = manager1.Delete(ctx, conf.ID)
		assert.NoError(t, err, "Delete should succeed with managing manager")
	})
}

func TestNumpool_UpdateMetadata(t *testing.T) {
	ctx := context.Background()
	pool := connPool

	manager, err := numpool.Setup(ctx, pool)
	require.NoError(t, err, "Setup should not return an error")

	t.Run("updates metadata successfully", func(t *testing.T) {
		// Given
		initialMetadata := map[string]string{"version": "1.0", "description": "initial"}
		initialMetadataBytes, err := json.Marshal(initialMetadata)
		require.NoError(t, err, "JSON marshal should not fail")
		conf := numpool.Config{
			ID:                t.Name(),
			MaxResourcesCount: 5,
			Metadata:          initialMetadataBytes,
		}
		model, err := manager.GetOrCreate(ctx, conf)
		require.NoError(t, err, "GetOrCreate should not return an error")
		t.Cleanup(func() { _ = manager.Delete(ctx, model.ID()) })

		// Verify initial metadata
		var initial map[string]string
		err = json.Unmarshal(model.Metadata(), &initial)
		require.NoError(t, err, "Should unmarshal initial metadata")
		assert.Equal(t, initialMetadata, initial, "Initial metadata should match")

		// When - update metadata
		newMetadata := map[string]string{"version": "2.0", "description": "updated"}
		newMetadataBytes, err := json.Marshal(newMetadata)
		require.NoError(t, err, "JSON marshal should not fail")
		err = model.UpdateMetadata(ctx, newMetadataBytes)

		// Then
		assert.NoError(t, err, "UpdateMetadata should not return an error")

		// Verify metadata was updated in memory
		var updated map[string]string
		err = json.Unmarshal(model.Metadata(), &updated)
		require.NoError(t, err, "Should unmarshal updated metadata")
		assert.Equal(t, newMetadata, updated, "Metadata should be updated")

		// Verify metadata was updated in database by getting fresh instance
		freshModel, err := manager.GetOrCreate(ctx, numpool.Config{
			ID:                conf.ID,
			MaxResourcesCount: conf.MaxResourcesCount,
		})
		require.NoError(t, err, "GetOrCreate should not return an error for existing pool")
		var fresh map[string]string
		err = json.Unmarshal(freshModel.Metadata(), &fresh)
		require.NoError(t, err, "Should unmarshal fresh metadata")
		assert.Equal(t, newMetadata, fresh, "Fresh metadata should match updated value")
	})

	t.Run("returns error for non-existent pool", func(t *testing.T) {
		// Given
		conf := numpool.Config{
			ID:                t.Name(),
			MaxResourcesCount: 5,
		}
		model, err := manager.GetOrCreate(ctx, conf)
		require.NoError(t, err, "GetOrCreate should not return an error")

		// Delete the pool
		err = manager.Delete(ctx, model.ID())
		require.NoError(t, err, "Delete should not return an error")

		// When - try to update metadata of deleted pool
		metadataBytes, err := json.Marshal(map[string]string{"key": "value"})
		require.NoError(t, err, "JSON marshal should not fail")
		err = model.UpdateMetadata(ctx, metadataBytes)

		// Then
		assert.Error(t, err, "UpdateMetadata should return an error for deleted pool")
		assert.Contains(t, err.Error(), "failed to get numpool with lock", "Error should indicate pool not found")
	})

	t.Run("returns error for invalid metadata", func(t *testing.T) {
		// Given
		conf := numpool.Config{
			ID:                t.Name(),
			MaxResourcesCount: 5,
		}
		model, err := manager.GetOrCreate(ctx, conf)
		require.NoError(t, err, "GetOrCreate should not return an error")
		t.Cleanup(func() { _ = manager.Delete(ctx, model.ID()) })

		// When - try to update with invalid JSON
		// Use invalid JSON directly since we can't marshal circular references
		err = model.UpdateMetadata(ctx, json.RawMessage(`{invalid json}`))

		// Then
		assert.Error(t, err, "UpdateMetadata should return an error for invalid JSON")
		// The error will come from database operations or JSON comparison, not marshaling
	})

	t.Run("handles nil metadata", func(t *testing.T) {
		// Given
		conf := numpool.Config{
			ID:                t.Name(),
			MaxResourcesCount: 5,
		}
		model, err := manager.GetOrCreate(ctx, conf)
		require.NoError(t, err, "GetOrCreate should not return an error")
		t.Cleanup(func() { _ = manager.Delete(ctx, model.ID()) })

		// When - update with nil metadata
		err = model.UpdateMetadata(ctx, nil)

		// Then
		assert.NoError(t, err, "UpdateMetadata should handle nil metadata")
		assert.Nil(t, model.Metadata(), "Metadata should be nil")
	})

	t.Run("handles empty metadata", func(t *testing.T) {
		// Given
		conf := numpool.Config{
			ID:                t.Name(),
			MaxResourcesCount: 5,
		}
		model, err := manager.GetOrCreate(ctx, conf)
		require.NoError(t, err, "GetOrCreate should not return an error")
		t.Cleanup(func() { _ = manager.Delete(ctx, model.ID()) })

		// When - update with empty map
		emptyMap := make(map[string]string)
		emptyMapBytes, err := json.Marshal(emptyMap)
		require.NoError(t, err, "JSON marshal should not fail")
		err = model.UpdateMetadata(ctx, emptyMapBytes)

		// Then
		assert.NoError(t, err, "UpdateMetadata should handle empty metadata")
		var result map[string]string
		err = json.Unmarshal(model.Metadata(), &result)
		require.NoError(t, err, "Should unmarshal empty metadata")
		assert.Empty(t, result, "Metadata should be empty map")
	})

	t.Run("succeeds when another transaction updated to same value", func(t *testing.T) {
		// Given
		initialMetadata := map[string]string{"version": "1.0"}
		initialMetadataBytes, err := json.Marshal(initialMetadata)
		require.NoError(t, err, "JSON marshal should not fail")
		conf := numpool.Config{
			ID:                t.Name(),
			MaxResourcesCount: 5,
			Metadata:          initialMetadataBytes,
		}
		model1, err := manager.GetOrCreate(ctx, conf)
		require.NoError(t, err, "GetOrCreate should not return an error")
		t.Cleanup(func() { _ = manager.Delete(ctx, model1.ID()) })

		// Get a second instance of the same pool
		model2, err := manager.GetOrCreate(ctx, numpool.Config{
			ID:                conf.ID,
			MaxResourcesCount: conf.MaxResourcesCount,
		})
		require.NoError(t, err, "GetOrCreate should not return an error for existing pool")

		// When - model1 updates metadata first
		newMetadata := map[string]string{"version": "2.0", "updated": "true"}
		newMetadataBytes, err := json.Marshal(newMetadata)
		require.NoError(t, err, "JSON marshal should not fail")
		err = model1.UpdateMetadata(ctx, newMetadataBytes)
		require.NoError(t, err, "First update should succeed")

		// Then - model2 tries to update to the same value (should succeed)
		err = model2.UpdateMetadata(ctx, newMetadataBytes)
		assert.NoError(t, err, "Second update to same value should succeed")

		// Verify both instances have the correct metadata
		var result1, result2 map[string]string
		err = json.Unmarshal(model1.Metadata(), &result1)
		require.NoError(t, err, "Should unmarshal model1 metadata")
		err = json.Unmarshal(model2.Metadata(), &result2)
		require.NoError(t, err, "Should unmarshal model2 metadata")

		assert.Equal(t, newMetadata, result1, "Model1 metadata should match")
		assert.Equal(t, newMetadata, result2, "Model2 metadata should match")
	})

	t.Run("fails when another transaction updated to different value", func(t *testing.T) {
		// Given
		initialMetadata := map[string]string{"version": "1.0"}
		initialMetadataBytes, err := json.Marshal(initialMetadata)
		require.NoError(t, err, "JSON marshal should not fail")
		conf := numpool.Config{
			ID:                t.Name(),
			MaxResourcesCount: 5,
			Metadata:          initialMetadataBytes,
		}
		model1, err := manager.GetOrCreate(ctx, conf)
		require.NoError(t, err, "GetOrCreate should not return an error")
		t.Cleanup(func() { _ = manager.Delete(ctx, model1.ID()) })

		// Get a second instance of the same pool
		model2, err := manager.GetOrCreate(ctx, numpool.Config{
			ID:                conf.ID,
			MaxResourcesCount: conf.MaxResourcesCount,
		})
		require.NoError(t, err, "GetOrCreate should not return an error for existing pool")

		// When - model1 updates metadata first
		metadata1 := map[string]string{"version": "2.0", "updated_by": "model1"}
		metadata1Bytes, err := json.Marshal(metadata1)
		require.NoError(t, err, "JSON marshal should not fail")
		err = model1.UpdateMetadata(ctx, metadata1Bytes)
		require.NoError(t, err, "First update should succeed")

		// Then - model2 tries to update to a different value (should fail)
		metadata2 := map[string]string{"version": "3.0", "updated_by": "model2"}
		metadata2Bytes, err := json.Marshal(metadata2)
		require.NoError(t, err, "JSON marshal should not fail")
		err = model2.UpdateMetadata(ctx, metadata2Bytes)
		assert.Error(t, err, "Second update to different value should fail")
		assert.Contains(t, err.Error(), "has been modified by another transaction", "Error should indicate concurrent modification")

		// Verify model1 metadata is unchanged
		var result1 map[string]string
		err = json.Unmarshal(model1.Metadata(), &result1)
		require.NoError(t, err, "Should unmarshal model1 metadata")
		assert.Equal(t, metadata1, result1, "Model1 metadata should be unchanged")
	})
}

func TestNumpool_Close(t *testing.T) {
	ctx := context.Background()

	t.Run("closes the numpool and releases resources without closing underlying connection pool", func(t *testing.T) {
		manager, err := numpool.Setup(ctx, connPool)
		require.NoError(t, err, "Setup should not return an error")
		t.Cleanup(manager.Close)

		// Given: a Numpool instance and acquire a resource
		config := numpool.Config{ID: "numpool-close", MaxResourcesCount: 1}
		np, err := manager.GetOrCreate(ctx, config)
		require.NoError(t, err, "GetOrCreate should not return an error")
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		t.Cleanup(cancel)
		resource, err := np.Acquire(ctxWithTimeout)
		require.NoError(t, err, "Acquire should not return an error")

		// When: close the Numpool
		np.Close()

		// Then: the underlying connection pool should still be operational
		assert.NotNil(t, connPool, "Pool should not be nil after manager close")
		assert.NoError(t, connPool.Ping(ctx), "Pool should still be operational after manager close")
		exists, err := sqlc.New(connPool).CheckNumpoolTableExist(ctx)
		assert.NoError(t, err, "CheckNumpoolTableExist should not return an error after manager close")
		assert.True(t, exists, "Numpool table should still exist after manager close")

		// Then: the Numpool should be closed and the resource should be released
		assert.True(t, np.Closed(), "Numpool should be closed after Close()")
		assert.True(t, resource.Closed(), "Resource should be closed after Numpool close")

		// Verify that the resource can be acquired
		otherNp, err := manager.GetOrCreate(ctx, config)
		require.NoError(t, err, "GetOrCreate should not return an error for existing pool")
		ctxWithTimeout, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
		t.Cleanup(cancel)
		otherResource, err := otherNp.Acquire(ctxWithTimeout)
		require.NoError(t, err, "Acquire should not return an error for new resource")
		assert.NotNil(t, otherResource, "Should acquire a new resource from the same pool")
	})

	t.Run("does not delete the numpool record from database", func(t *testing.T) {
		manager, err := numpool.Setup(ctx, connPool)
		require.NoError(t, err, "Setup should not return an error")
		t.Cleanup(manager.Close)

		// Given: a Numpool instance without auto-starting listener to avoid race condition
		config := numpool.Config{ID: "numpool-close-no-delete", MaxResourcesCount: 3, NoStartListening: true}
		np, err := manager.GetOrCreate(ctx, config)
		require.NoError(t, err, "GetOrCreate should not return an error")

		// Verify the pool exists in the database before closing
		queries := sqlc.New(connPool)
		existsBefore, err := queries.CheckNumpoolExists(ctx, config.ID)
		require.NoError(t, err, "CheckNumpoolExists should not return an error")
		assert.True(t, existsBefore, "Pool should exist in database before close")

		// When: close the Numpool (this should NOT delete the database record)
		np.Close()

		// Then: the database record should still exist
		existsAfter, err := queries.CheckNumpoolExists(ctx, config.ID)
		require.NoError(t, err, "CheckNumpoolExists should not return an error after close")
		assert.True(t, existsAfter, "Pool record should still exist in database after Close()")

		// Verify we can still get the pool data from database
		poolData, err := queries.GetNumpool(ctx, config.ID)
		require.NoError(t, err, "GetNumpool should not return an error after close")
		assert.Equal(t, config.ID, poolData.ID, "Pool ID should match")
		assert.Equal(t, config.MaxResourcesCount, poolData.MaxResourcesCount, "MaxResourcesCount should match")

		// Verify that a new manager can still access the existing pool
		otherManager, err := numpool.Setup(ctx, connPool)
		require.NoError(t, err, "Setup should not return an error")
		t.Cleanup(otherManager.Close)

		otherNp, err := otherManager.GetOrCreate(ctx, config)
		require.NoError(t, err, "GetOrCreate should work with existing pool record")
		assert.Equal(t, config.ID, otherNp.ID(), "Pool ID should match existing record")
	})
}

func TestNumpool_WithLock(t *testing.T) {
	ctx := context.Background()

	n := 5

	numpools := make([]*numpool.Numpool, 0, n)
	for range n {
		manager, err := numpool.Setup(ctx, connPool)
		require.NoError(t, err, "Setup should not return an error")
		t.Cleanup(manager.Close)

		np, err := manager.GetOrCreate(ctx, numpool.Config{
			ID:                "with-lock",
			MaxResourcesCount: 1,
		})
		require.NoError(t, err, "GetOrCreate should not return an error")
		numpools = append(numpools, np)
	}

	wg := sync.WaitGroup{}
	count := 0
	m := 5

	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range m {
				err := numpools[i].WithLock(ctx, func() error {
					require.Equal(t, 0, count)
					count++
					require.Equal(t, 1, count)
					time.Sleep(10 * time.Millisecond) // Simulate some work
					require.Equal(t, 1, count)
					count--
					require.Equal(t, 0, count)
					return nil
				})
				require.NoError(t, err, "WithLock should not return an error")
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, 0, count, "Count should be zero after all goroutines complete")
}
