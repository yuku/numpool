package numpool_test

import (
	"context"
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
