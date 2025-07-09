package statedb_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool/internal"
	"github.com/yuku/numpool/internal/sqlc"
	"github.com/yuku/numpool/internal/statedb"
)

func TestSetup(t *testing.T) {
	defaultConn := internal.MustGetConnectionWithCleanup(t)

	ctx := context.Background()
	dbname := fmt.Sprintf("numpool_test_%d", rand.IntN(1000000)) // Randomize database name to avoid conflicts

	// Create separate database to avoid dropping the table affecting other tests.
	_, err := defaultConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbname))
	require.NoError(t, err, "failed to create setup_test database")
	t.Cleanup(func() {
		_, _ = defaultConn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	})
	
	// Connect to the new database directly
	config := defaultConn.Config().Copy()
	config.Database = dbname
	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err, "failed to connect to test database")
	t.Cleanup(func() { _ = conn.Close(ctx) })

	// Given
	q := sqlc.New(conn)
	exists, err := q.DoesNumpoolTableExist(ctx)
	require.NoError(t, err, "failed to check if numpool table exists")
	if exists {
		// If the table already exists, we can drop it to ensure a clean setup.
		_, err = conn.Exec(ctx, "DROP TABLE IF EXISTS numpool")
		require.NoError(t, err, "failed to drop numpool table before setup")
	}

	// When
	require.NoError(t, statedb.Setup(ctx, conn))

	// Then
	exists, err = q.DoesNumpoolTableExist(ctx)
	require.NoError(t, err, "failed to check if numpool table exists after setup")
	require.True(t, exists, "numpool table should exist after setup")
}
