package numpool_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool"
	"github.com/yuku/numpool/internal"
	"github.com/yuku/numpool/internal/sqlc"
)

func TestSetup(t *testing.T) {
	defaultConn := internal.MustGetConnectionWithCleanup(t)

	ctx := context.Background()
	dbname := fmt.Sprintf("numpool_test_%d", rand.IntN(1000000)) // Randomize database name to avoid conflicts

	// Create separate database to avoid dropping the table affecting other tests.
	_, err := defaultConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbname))
	require.NoErrorf(t, err, "failed to create %s database", dbname)
	t.Cleanup(func() {
		_, _ = defaultConn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	})

	// Connect to the new database directly
	config := defaultConn.Config().Copy()
	config.Database = dbname
	pool, err := pgxpool.New(ctx, config.ConnString())
	require.NoError(t, err, "failed to connect to test database")
	t.Cleanup(pool.Close)

	// Given
	// If the table already exists, we can drop it to ensure a clean setup.
	_, err = pool.Exec(ctx, "DROP TABLE IF EXISTS numpool")
	require.NoError(t, err, "failed to drop numpool table before setup")

	// When
	_, err = numpool.Setup(ctx, pool)
	require.NoError(t, err, "Setup should not return an error")

	// Then
	exists, err := sqlc.New(pool).CheckNumpoolTableExist(ctx)
	require.NoError(t, err, "failed to check if numpool table exists after setup")
	require.True(t, exists, "numpool table should exist after setup")
}

// TestSetup_Concurrent tests the Setup function in parallel to ensure it can handle concurrent calls.
func TestSetup_Concurrent(t *testing.T) {
	defaultConn := internal.MustGetConnectionWithCleanup(t)

	ctx := context.Background()
	dbname := fmt.Sprintf("numpool_test_%d", rand.IntN(1000000)) // Randomize database name to avoid conflicts

	// Create separate database to avoid dropping the table affecting other tests.
	_, err := defaultConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbname))
	require.NoErrorf(t, err, "failed to create %s database", dbname)
	t.Cleanup(func() {
		_, _ = defaultConn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	})

	// Connect to the new database directly
	config := defaultConn.Config().Copy()
	config.Database = dbname
	pool, err := pgxpool.New(ctx, config.ConnString())
	require.NoError(t, err, "failed to connect to test database")
	t.Cleanup(pool.Close)

	n := 10
	var wg sync.WaitGroup
	wg.Add(n)

	for range n {
		go func() {
			defer wg.Done()
			// If the table already exists, we can drop it to ensure a clean setup.
			_, err := pool.Exec(ctx, "DROP TABLE IF EXISTS numpool")
			require.NoError(t, err, "failed to drop numpool table before setup")

			_, err = numpool.Setup(ctx, pool)
			require.NoError(t, err, "Setup should not return an error")
		}()
	}

	wg.Wait()

	// Check if the numpool table exists after all setups
	exists, err := sqlc.New(pool).CheckNumpoolTableExist(ctx)
	require.NoError(t, err, "failed to check if numpool table exists after parallel setup")
	require.True(t, exists, "numpool table should exist after parallel setup")
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		conf    numpool.Config
		wantErr bool
	}{
		{
			name:    "valid config with min resources",
			conf:    numpool.Config{ID: "pool1", MaxResourcesCount: 1},
			wantErr: false,
		},
		{
			name:    "valid config with max resources",
			conf:    numpool.Config{ID: "pool2", MaxResourcesCount: numpool.MaxResourcesLimit},
			wantErr: false,
		},
		{
			name:    "empty ID",
			conf:    numpool.Config{ID: "", MaxResourcesCount: 10},
			wantErr: true,
		},
		{
			name:    "zero resources",
			conf:    numpool.Config{ID: "pool3", MaxResourcesCount: 0},
			wantErr: true,
		},
		{
			name:    "negative resources",
			conf:    numpool.Config{ID: "pool4", MaxResourcesCount: -5},
			wantErr: true,
		},
		{
			name:    "exceeds max resources",
			conf:    numpool.Config{ID: "pool5", MaxResourcesCount: numpool.MaxResourcesLimit + 1},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			err := tt.conf.Validate()
			if tt.wantErr {
				require.Error(t, err, "expected error for config: %+v", tt.conf)
			} else {
				require.NoError(t, err, "unexpected error for config: %+v", tt.conf)
			}
		})
	}
}

func TestManager_GetOrCreate(t *testing.T) {
	ctx := context.Background()
	pool := internal.MustGetPoolWithCleanup(t)
	queries := sqlc.New(pool)

	manager, err := numpool.Setup(ctx, pool)
	require.NoError(t, err, "Setup should not return an error")

	t.Run("creates new pool if it does not exist", func(t *testing.T) {
		t.Parallel()

		// Given
		type Metadata struct {
			Name string
		}
		metadataBytes, err := json.Marshal(&Metadata{Name: "Test Pool"})
		require.NoError(t, err, "JSON marshal should not fail")
		conf := numpool.Config{
			ID:                t.Name(),
			MaxResourcesCount: 5,
			Metadata:          metadataBytes,
		}
		_, err = queries.DeleteNumpool(ctx, conf.ID)
		require.NoError(t, err, "DeleteNumpool should not return an error")

		// When
		model, err := manager.GetOrCreate(ctx, conf)

		// Then
		assert.NoError(t, err, "GetOrCreate should not return an error")
		if assert.NotNil(t, model, "GetOrCreate should return a valid Numpool instance") {
			assert.Equal(t, conf.ID, model.ID(), "ID should match the configuration")
		}
		exists, err := queries.CheckNumpoolExists(ctx, conf.ID)
		assert.NoError(t, err, "CheckNumpoolExists should not return an error after creation")
		assert.True(t, exists, "record should exist after creation")
		var metadata Metadata
		err = json.Unmarshal(model.Metadata(), &metadata)
		if assert.NoError(t, err, "Unmarshal should not return an error for metadata") {
			assert.Equal(t, "Test Pool", metadata.Name, "Metadata should match the configuration")
		}

		t.Run("returns existing pool if it already exists", func(t *testing.T) {
			t.Parallel()

			// When
			modifiedMetadataBytes, err2 := json.Marshal(&Metadata{Name: "Modified"})
			require.NoError(t, err2, "JSON marshal should not fail")
			newModel, err := manager.GetOrCreate(ctx, numpool.Config{
				ID:                conf.ID,
				MaxResourcesCount: conf.MaxResourcesCount,
				Metadata:          modifiedMetadataBytes,
			})

			// Then
			assert.NoError(t, err, "GetOrCreate should not return an error for existing pool")
			if assert.NotNil(t, newModel, "GetOrCreate should return a valid Numpool instance") {
				assert.Equal(t, conf.ID, newModel.ID(), "Existing pool ID should match the created pool ID")
				assert.NotSame(t, model, newModel, "should return a new instance for existing pool")
			}
			var metadata Metadata
			err = json.Unmarshal(model.Metadata(), &metadata)
			if assert.NoError(t, err, "Unmarshal should not return an error for metadata") {
				assert.Equal(t, "Test Pool", metadata.Name, "Metadata should not change for existing pool")
			}
		})

		t.Run("returns error for different MaxResourcesCount", func(t *testing.T) {
			t.Parallel()

			// Given
			confDifferent := numpool.Config{
				ID:                conf.ID,
				MaxResourcesCount: conf.MaxResourcesCount + 1,
			}

			// When
			newModel, err := manager.GetOrCreate(ctx, confDifferent)

			// Then
			assert.Error(t, err, "GetOrCreate should return an error for different MaxResourcesCount")
			assert.Contains(t, err.Error(), "already exists with different max resources count", "Error message should indicate conflict")
			assert.Nil(t, newModel, "should not return a Numpool instance on error")
		})
	})

	t.Run("returns error for invalid configuration", func(t *testing.T) {
		t.Parallel()

		// Given
		invalidConf := numpool.Config{
			ID:                "test",
			MaxResourcesCount: 0,
		}

		// When
		model, err := manager.GetOrCreate(ctx, invalidConf)

		// Then
		assert.Error(t, err, "GetOrCreate should return an error for invalid configuration")
		assert.Nil(t, model, "should not return a Numpool instance on error")
	})
}

func TestManager_GetOrCreate_Concurrent(t *testing.T) {
	ctx := context.Background()
	pool := internal.MustGetPoolWithCleanup(t)
	queries := sqlc.New(pool)

	n := 5  // Number of manager
	m := 10 // Number of concurrent GetOrCreate calls per manager
	var wg sync.WaitGroup
	wg.Add(n * m)

	getTableName := func(i int) string {
		return fmt.Sprintf("test_getorcreate_concurrent_%d", i)
	}

	// Delete any existing pools to ensure a clean start
	for i := range m {
		_, err := queries.DeleteNumpool(ctx, getTableName(i))
		require.NoError(t, err, "DeleteNumpool should not return an error")
	}

	for range n {
		go func() {
			manager, err := numpool.Setup(ctx, pool)
			require.NoError(t, err, "Setup should not return an error")

			for i := range m {
				go func() {
					defer wg.Done()

					conf := numpool.Config{
						ID:                getTableName(i),
						MaxResourcesCount: 5,
					}
					model, err := manager.GetOrCreate(ctx, conf)
					require.NoError(t, err, "GetOrCreate should not return an error")
					if assert.NotNil(t, model, "GetOrCreate should return a valid Numpool instance") {
						assert.Equal(t, conf.ID, model.ID(), "ID should match the configuration")
					}
				}()
			}
		}()
	}

	wg.Wait()

	// Check if all pools were created
	for i := range m {
		exists, err := queries.CheckNumpoolExists(ctx, getTableName(i))
		require.NoError(t, err, "CheckNumpoolExists should not return an error")
		assert.True(t, exists, "Numpool should exist after concurrent GetOrCreate")
	}
}

func TestManager_Close(t *testing.T) {
	ctx := context.Background()
	pool := internal.MustGetPoolWithCleanup(t)

	manager, err := numpool.Setup(ctx, pool)
	require.NoError(t, err, "Setup should not return an error")

	// Close the manager
	manager.Close()

	// Verify that the pool is still open
	assert.NotNil(t, pool, "Pool should not be nil after manager close")
	assert.NoError(t, pool.Ping(ctx), "Pool should still be operational after manager close")

	// Verify that the manager does not close the underlying pool
	_, err = sqlc.New(pool).CheckNumpoolTableExist(ctx)
	assert.NoError(t, err, "CheckNumpoolTableExist should not return an error after manager close")
}
