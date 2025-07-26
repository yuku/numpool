package sqlc_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool/internal"
)

const testDBName = "numpool_internal_sqlc"

func TestMain(m *testing.M) {
	if err := internal.SetupTestDatabase(testDBName); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func mustGetPoolWithCleanup(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := internal.GetPool(context.Background(), testDBName)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}
