package statedb_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool/internal"
	"github.com/yuku/numpool/internal/sqlc"
	"github.com/yuku/numpool/internal/statedb"
)

func TestSetup(t *testing.T) {
	defaultConn := internal.MustGetConnectionWithCleanup(t)

	// Create separete database to avoid dropping the table affecting other tests.
	_, err := defaultConn.Exec(context.Background(), "CREATE DATABASE setup_test")
	require.NoError(t, err, "failed to create setup_test database")
	t.Cleanup(func() {
		_, _ = defaultConn.Exec(context.Background(), "DROP DATABASE IF EXISTS setup_test")
	})
	os.Setenv("PGDATABASE", "setup_test")
	conn := internal.MustGetConnectionWithCleanup(t)

	// Given
	q := sqlc.New(conn)
	exists, err := q.DoesNumpoolTableExist(context.Background())
	require.NoError(t, err, "failed to check if numpool table exists")
	if exists {
		// If the table already exists, we can drop it to ensure a clean setup.
		_, err = conn.Exec(context.Background(), "DROP TABLE IF EXISTS numpool")
		require.NoError(t, err, "failed to drop numpool table before setup")
		exists = false // Reset exists to false since we just dropped the table
	}

	// When
	require.NoError(t, statedb.Setup(context.Background(), conn))

	// Then
	exists, err = q.DoesNumpoolTableExist(context.Background())
	require.NoError(t, err, "failed to check if numpool table exists after setup")
	require.True(t, exists, "numpool table should exist after setup")
}
