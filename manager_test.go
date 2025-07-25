package numpool_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

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
	queries := sqlc.New(connPool)

	manager, err := numpool.Setup(ctx, connPool)
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
	queries := sqlc.New(connPool)

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
			manager, err := numpool.Setup(ctx, connPool)
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

	t.Run("closes manager without closing underlying connection pool", func(t *testing.T) {
		// Given: a manager setup with a connection pool
		manager, err := numpool.Setup(ctx, connPool)
		require.NoError(t, err, "Setup should not return an error")

		// When: close the manager
		manager.Close()

		// Then: the connection pool and db table should still be operational
		assert.NotNil(t, connPool, "Pool should not be nil after manager close")
		assert.NoError(t, connPool.Ping(ctx), "Pool should still be operational after manager close")
		exists, err := sqlc.New(connPool).CheckNumpoolTableExist(ctx)
		assert.NoError(t, err, "CheckNumpoolTableExist should not return an error after manager close")
		assert.True(t, exists, "Numpool table should still exist after manager close")
	})

	t.Run("closes manager and releases resources", func(t *testing.T) {
		// Given: a manager setup with a connection pool and a Numpool instance
		// and acquire a resource
		manager, err := numpool.Setup(ctx, connPool)
		require.NoError(t, err, "Setup should not return an error")

		config := numpool.Config{ID: t.Name(), MaxResourcesCount: 1}
		np, err := manager.GetOrCreate(ctx, config)
		require.NoError(t, err, "GetOrCreate should not return an error")
		assert.NotNil(t, np, "GetOrCreate should return a valid Numpool instance")

		ctxWithTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		t.Cleanup(cancel)
		resource, err := np.Acquire(ctxWithTimeout)
		require.NoError(t, err, "Acquire should not return an error")
		require.Equal(t, 0, resource.Index(), "Resource index should be 0 for single resource pool")

		// Verify that manager and Numpool are not closed yet
		assert.False(t, manager.Closed(), "Manager should not be closed before Close()")
		assert.False(t, np.Closed(), "Numpool should not be closed before Close()")
		assert.False(t, resource.Closed(), "Resource should not be closed before Close()")

		// When: close the manager
		manager.Close()

		// Then: the manager, Numpool, and resource should be closed
		assert.True(t, manager.Closed(), "Manager should be closed after Close()")
		assert.True(t, np.Closed(), "Numpool should be closed after manager close")
		assert.True(t, resource.Closed(), "Resource should be closed after manager close")

		// Verify the resource is released back to the pool
		otherManager, err := numpool.Setup(ctx, connPool)
		require.NoError(t, err, "Setup should not return an error")
		t.Cleanup(otherManager.Close)
		otherNp, err := otherManager.GetOrCreate(ctx, config)
		require.NoError(t, err, "GetOrCreate should not return an error")
		otherCtxWithTimeout, otherCancel := context.WithTimeout(ctx, 100*time.Millisecond)
		t.Cleanup(otherCancel)
		otherResource, err := otherNp.Acquire(otherCtxWithTimeout)
		require.NoError(t, err, "Acquire should not return an error for the released resource")
		assert.NotNil(t, otherResource, "Acquire should return a valid Resource instance")
		assert.Equal(t, 0, otherResource.Index(), "Resource index should be 0 for single resource pool")
	})
}

func TestCleanup(t *testing.T) {
	ctx := context.Background()

	// Given: a manager setup with a connection pool
	_, err := numpool.Setup(ctx, connPool)
	require.NoError(t, err, "Setup should not return an error")
	exists, err := sqlc.New(connPool).CheckNumpoolTableExist(ctx)
	require.NoError(t, err, "CheckNumpoolTableExist should not return an error")
	assert.True(t, exists, "Numpool table should not exist after Cleanup")

	// When: cleanup the manager
	require.NoError(t, numpool.Cleanup(ctx, connPool))

	// Then: that the numpools table is dropped
	exists, err = sqlc.New(connPool).CheckNumpoolTableExist(ctx)
	require.NoError(t, err, "CheckNumpoolTableExist should not return an error")
	assert.False(t, exists, "Numpool table should not exist after Cleanup")
}

func TestManager_Cleanup(t *testing.T) {
	ctx := context.Background()

	// Given: a manager setup with a connection pool
	manager, err := numpool.Setup(ctx, connPool)
	require.NoError(t, err, "Setup should not return an error")

	// When: cleanup the manager
	require.NoError(t, manager.Cleanup(ctx))

	// Then: the manager should be closed
	assert.True(t, manager.Closed(), "Manager should be closed after Cleanup")

	// Verify that the underlying connection pool is still operational
	assert.NotNil(t, connPool, "Pool should not be nil after Cleanup")
	assert.NoError(t, connPool.Ping(ctx), "Pool should still be operational after Cleanup")

	// Verify that the numpools table is dropped
	exists, err := sqlc.New(connPool).CheckNumpoolTableExist(ctx)
	require.NoError(t, err, "CheckNumpoolTableExist should not return an error")
	assert.False(t, exists, "Numpool table should not exist after Cleanup")
}

func TestManager_ListPools(t *testing.T) {
	ctx := context.Background()
	queries := sqlc.New(connPool)

	manager, err := numpool.Setup(ctx, connPool)
	require.NoError(t, err, "Setup should not return an error")

	// Create test pools with different names
	testPools := []string{
		"test_pool_1",
		"test_pool_2",
		"production_pool_1",
		"staging_pool_1",
		"another_test_pool",
	}

	// Clean up any existing pools first
	for _, poolName := range testPools {
		_, _ = queries.DeleteNumpool(ctx, poolName)
	}

	// Create the test pools
	for _, poolName := range testPools {
		conf := numpool.Config{
			ID:                poolName,
			MaxResourcesCount: 5,
		}
		_, err := manager.GetOrCreate(ctx, conf)
		require.NoError(t, err, "GetOrCreate should not return an error for pool %s", poolName)
	}

	t.Run("lists all pools when prefix is empty", func(t *testing.T) {
		pools, err := manager.ListPools(ctx, "")
		require.NoError(t, err, "ListPools should not return an error")

		// Should contain at least our test pools (may contain others from concurrent tests)
		for _, expectedPool := range testPools {
			assert.Contains(t, pools, expectedPool, "Should contain test pool %s", expectedPool)
		}

		// Verify ordering - pools should be in alphabetical order
		if len(pools) > 1 {
			for i := 1; i < len(pools); i++ {
				assert.LessOrEqual(t, pools[i-1], pools[i], "Pools should be ordered alphabetically")
			}
		}
	})

	t.Run("lists pools with specific prefix", func(t *testing.T) {
		pools, err := manager.ListPools(ctx, "test_")
		require.NoError(t, err, "ListPools should not return an error")

		expectedPools := []string{"test_pool_1", "test_pool_2"}
		for _, expectedPool := range expectedPools {
			assert.Contains(t, pools, expectedPool, "Should contain pool %s", expectedPool)
		}

		// Should not contain pools that don't match prefix
		assert.NotContains(t, pools, "production_pool_1", "Should not contain production pool")
		assert.NotContains(t, pools, "staging_pool_1", "Should not contain staging pool")

		// "another_test_pool" should not be included as it doesn't start with "test_"
		assert.NotContains(t, pools, "another_test_pool", "Should not contain another_test_pool")
	})

	t.Run("lists pools with prefix that matches multiple words", func(t *testing.T) {
		pools, err := manager.ListPools(ctx, "production_")
		require.NoError(t, err, "ListPools should not return an error")

		assert.Contains(t, pools, "production_pool_1", "Should contain production pool")
		assert.NotContains(t, pools, "test_pool_1", "Should not contain test pool")
	})

	t.Run("returns empty list for non-matching prefix", func(t *testing.T) {
		pools, err := manager.ListPools(ctx, "nonexistent_")
		require.NoError(t, err, "ListPools should not return an error")
		assert.Empty(t, pools, "Should return empty list for non-matching prefix")
	})

	t.Run("returns error when manager is closed", func(t *testing.T) {
		closedManager, err := numpool.Setup(ctx, connPool)
		require.NoError(t, err, "Setup should not return an error")
		closedManager.Close()

		pools, err := closedManager.ListPools(ctx, "")
		assert.Error(t, err, "ListPools should return error when manager is closed")
		assert.Contains(t, err.Error(), "manager is closed", "Error should indicate manager is closed")
		assert.Nil(t, pools, "Should return nil pools on error")
	})
}

func TestManager_DeletePool(t *testing.T) {
	ctx := context.Background()
	queries := sqlc.New(connPool)

	manager, err := numpool.Setup(ctx, connPool)
	require.NoError(t, err, "Setup should not return an error")

	t.Run("deletes existing pool successfully", func(t *testing.T) {
		// Create a test pool
		poolName := "test_delete_pool_1"
		conf := numpool.Config{
			ID:                poolName,
			MaxResourcesCount: 5,
		}

		// Clean up any existing pool first
		_, _ = queries.DeleteNumpool(ctx, poolName)

		numPool, err := manager.GetOrCreate(ctx, conf)
		require.NoError(t, err, "GetOrCreate should not return an error")
		require.NotNil(t, numPool, "GetOrCreate should return a valid Numpool instance")

		// Verify pool exists
		exists, err := queries.CheckNumpoolExists(ctx, poolName)
		require.NoError(t, err, "CheckNumpoolExists should not return an error")
		assert.True(t, exists, "Pool should exist before deletion")

		// Delete the pool
		err = manager.DeletePool(ctx, poolName)
		require.NoError(t, err, "DeletePool should not return an error")

		// Verify pool no longer exists in database
		exists, err = queries.CheckNumpoolExists(ctx, poolName)
		require.NoError(t, err, "CheckNumpoolExists should not return an error")
		assert.False(t, exists, "Pool should not exist after deletion")

		// Verify the numpool instance was closed
		assert.True(t, numPool.Closed(), "Numpool should be closed after deletion")
	})

	t.Run("deletes pool that exists in database but not tracked by manager", func(t *testing.T) {
		// Create a pool directly in database (bypass manager tracking)
		poolName := "test_delete_untracked_pool"
		err := queries.CreateNumpool(ctx, sqlc.CreateNumpoolParams{
			ID:                poolName,
			MaxResourcesCount: 3,
			Metadata:          []byte(`{}`),
		})
		require.NoError(t, err, "CreateNumpool should not return an error")

		// Verify pool exists
		exists, err := queries.CheckNumpoolExists(ctx, poolName)
		require.NoError(t, err, "CheckNumpoolExists should not return an error")
		assert.True(t, exists, "Pool should exist before deletion")

		// Delete the pool
		err = manager.DeletePool(ctx, poolName)
		require.NoError(t, err, "DeletePool should not return an error for untracked pool")

		// Verify pool no longer exists
		exists, err = queries.CheckNumpoolExists(ctx, poolName)
		require.NoError(t, err, "CheckNumpoolExists should not return an error")
		assert.False(t, exists, "Pool should not exist after deletion")
	})

	t.Run("returns error for non-existent pool", func(t *testing.T) {
		err := manager.DeletePool(ctx, "non_existent_pool")
		assert.Error(t, err, "DeletePool should return error for non-existent pool")
		assert.Contains(t, err.Error(), "does not exist", "Error should indicate pool does not exist")
	})

	t.Run("returns error for empty pool name", func(t *testing.T) {
		err := manager.DeletePool(ctx, "")
		assert.Error(t, err, "DeletePool should return error for empty pool name")
		assert.Contains(t, err.Error(), "pool name cannot be empty", "Error should indicate empty pool name")
	})

	t.Run("returns error when manager is closed", func(t *testing.T) {
		closedManager, err := numpool.Setup(ctx, connPool)
		require.NoError(t, err, "Setup should not return an error")
		closedManager.Close()

		err = closedManager.DeletePool(ctx, "some_pool")
		assert.Error(t, err, "DeletePool should return error when manager is closed")
		assert.Contains(t, err.Error(), "manager is closed", "Error should indicate manager is closed")
	})

	t.Run("handles pool with active resources gracefully", func(t *testing.T) {
		// Create a test pool and acquire a resource
		poolName := "test_delete_with_resource"
		conf := numpool.Config{
			ID:                poolName,
			MaxResourcesCount: 2,
		}

		// Clean up any existing pool first
		_, _ = queries.DeleteNumpool(ctx, poolName)

		numPool, err := manager.GetOrCreate(ctx, conf)
		require.NoError(t, err, "GetOrCreate should not return an error")

		// Acquire a resource
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		resource, err := numPool.Acquire(ctxWithTimeout)
		require.NoError(t, err, "Acquire should not return an error")
		require.NotNil(t, resource, "Acquire should return a valid resource")

		// Delete the pool (should handle active resources gracefully)
		err = manager.DeletePool(ctx, poolName)
		require.NoError(t, err, "DeletePool should not return an error even with active resources")

		// Verify pool no longer exists
		exists, err := queries.CheckNumpoolExists(ctx, poolName)
		require.NoError(t, err, "CheckNumpoolExists should not return an error")
		assert.False(t, exists, "Pool should not exist after deletion")

		// Verify the numpool and resource were closed
		assert.True(t, numPool.Closed(), "Numpool should be closed after deletion")
		assert.True(t, resource.Closed(), "Resource should be closed after pool deletion")
	})
}

func TestManager_DeletePool_Concurrent(t *testing.T) {
	ctx := context.Background()
	queries := sqlc.New(connPool)

	manager, err := numpool.Setup(ctx, connPool)
	require.NoError(t, err, "Setup should not return an error")

	// Create multiple pools for concurrent deletion
	poolCount := 5
	var poolNames []string
	for i := 0; i < poolCount; i++ {
		poolName := fmt.Sprintf("test_concurrent_delete_%d", i)
		poolNames = append(poolNames, poolName)

		// Clean up any existing pool first
		_, _ = queries.DeleteNumpool(ctx, poolName)

		conf := numpool.Config{
			ID:                poolName,
			MaxResourcesCount: 3,
		}
		_, err := manager.GetOrCreate(ctx, conf)
		require.NoError(t, err, "GetOrCreate should not return an error for pool %s", poolName)
	}

	// Verify all pools exist
	for _, poolName := range poolNames {
		exists, err := queries.CheckNumpoolExists(ctx, poolName)
		require.NoError(t, err, "CheckNumpoolExists should not return an error")
		assert.True(t, exists, "Pool %s should exist before concurrent deletion", poolName)
	}

	// Delete pools concurrently
	var wg sync.WaitGroup
	wg.Add(poolCount)

	for _, poolName := range poolNames {
		go func(name string) {
			defer wg.Done()
			err := manager.DeletePool(ctx, name)
			assert.NoError(t, err, "DeletePool should not return an error for pool %s", name)
		}(poolName)
	}

	wg.Wait()

	// Verify all pools are deleted
	for _, poolName := range poolNames {
		exists, err := queries.CheckNumpoolExists(ctx, poolName)
		require.NoError(t, err, "CheckNumpoolExists should not return an error")
		assert.False(t, exists, "Pool %s should not exist after concurrent deletion", poolName)
	}
}
