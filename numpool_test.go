package numpool_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool"
	"github.com/yuku/numpool/internal"
	"github.com/yuku/numpool/internal/sqlc"
)

func TestNumpool_Delete(t *testing.T) {
	ctx := context.Background()
	pool := internal.MustGetPoolWithCleanup(t)
	queries := sqlc.New(pool)

	manager, err := numpool.Setup(ctx, pool)
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
		err = model.Delete(ctx)

		// Then
		assert.NoError(t, err, "Delete should not return an error")
		exists, err := queries.CheckNumpoolExists(ctx, conf.ID)
		assert.NoError(t, err, "CheckNumpoolExists should not return an error after deletion")
		assert.False(t, exists, "Numpool should not exist after deletion")

		t.Run("returns error for non-existing pool", func(t *testing.T) {
			// When
			err := model.Delete(ctx)

			// Then
			assert.Error(t, err, "Delete should return an error for non-existing pool")
		})
	})
}

func TestNumpool_UpdateMetadata(t *testing.T) {
	ctx := context.Background()
	pool := internal.MustGetPoolWithCleanup(t)

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
		t.Cleanup(func() { _ = model.Delete(ctx) })

		// Verify initial metadata
		var initial map[string]string
		err = json.Unmarshal(model.Metadata(), &initial)
		require.NoError(t, err, "Should unmarshal initial metadata")
		assert.Equal(t, initialMetadata, initial, "Initial metadata should match")

		// When - update metadata
		newMetadata := map[string]string{"version": "2.0", "description": "updated"}
		err = model.UpdateMetadata(ctx, newMetadata)

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
		err = model.Delete(ctx)
		require.NoError(t, err, "Delete should not return an error")

		// When - try to update metadata of deleted pool
		err = model.UpdateMetadata(ctx, map[string]string{"key": "value"})

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
		t.Cleanup(func() { _ = model.Delete(ctx) })

		// When - try to update with invalid metadata (circular reference)
		invalidData := make(map[string]any)
		invalidData["self"] = invalidData // Creates circular reference
		err = model.UpdateMetadata(ctx, invalidData)

		// Then
		assert.Error(t, err, "UpdateMetadata should return an error for invalid metadata")
		assert.Contains(t, err.Error(), "failed to marshal metadata", "Error should indicate marshaling failure")
	})

	t.Run("handles nil metadata", func(t *testing.T) {
		// Given
		conf := numpool.Config{
			ID:                t.Name(),
			MaxResourcesCount: 5,
		}
		model, err := manager.GetOrCreate(ctx, conf)
		require.NoError(t, err, "GetOrCreate should not return an error")
		t.Cleanup(func() { _ = model.Delete(ctx) })

		// When - update with nil metadata
		err = model.UpdateMetadata(ctx, nil)

		// Then
		assert.NoError(t, err, "UpdateMetadata should handle nil metadata")
		assert.Equal(t, json.RawMessage("null"), model.Metadata(), "Metadata should be null JSON")
	})

	t.Run("handles empty metadata", func(t *testing.T) {
		// Given
		conf := numpool.Config{
			ID:                t.Name(),
			MaxResourcesCount: 5,
		}
		model, err := manager.GetOrCreate(ctx, conf)
		require.NoError(t, err, "GetOrCreate should not return an error")
		t.Cleanup(func() { _ = model.Delete(ctx) })

		// When - update with empty map
		emptyMap := make(map[string]string)
		err = model.UpdateMetadata(ctx, emptyMap)

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
		t.Cleanup(func() { _ = model1.Delete(ctx) })

		// Get a second instance of the same pool
		model2, err := manager.GetOrCreate(ctx, numpool.Config{
			ID:                conf.ID,
			MaxResourcesCount: conf.MaxResourcesCount,
		})
		require.NoError(t, err, "GetOrCreate should not return an error for existing pool")

		// When - model1 updates metadata first
		newMetadata := map[string]string{"version": "2.0", "updated": "true"}
		err = model1.UpdateMetadata(ctx, newMetadata)
		require.NoError(t, err, "First update should succeed")

		// Then - model2 tries to update to the same value (should succeed)
		err = model2.UpdateMetadata(ctx, newMetadata)
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
		t.Cleanup(func() { _ = model1.Delete(ctx) })

		// Get a second instance of the same pool
		model2, err := manager.GetOrCreate(ctx, numpool.Config{
			ID:                conf.ID,
			MaxResourcesCount: conf.MaxResourcesCount,
		})
		require.NoError(t, err, "GetOrCreate should not return an error for existing pool")

		// When - model1 updates metadata first
		metadata1 := map[string]string{"version": "2.0", "updated_by": "model1"}
		err = model1.UpdateMetadata(ctx, metadata1)
		require.NoError(t, err, "First update should succeed")

		// Then - model2 tries to update to a different value (should fail)
		metadata2 := map[string]string{"version": "3.0", "updated_by": "model2"}
		err = model2.UpdateMetadata(ctx, metadata2)
		assert.Error(t, err, "Second update to different value should fail")
		assert.Contains(t, err.Error(), "has been modified by another transaction", "Error should indicate concurrent modification")

		// Verify model1 metadata is unchanged
		var result1 map[string]string
		err = json.Unmarshal(model1.Metadata(), &result1)
		require.NoError(t, err, "Should unmarshal model1 metadata")
		assert.Equal(t, metadata1, result1, "Model1 metadata should be unchanged")
	})
}
