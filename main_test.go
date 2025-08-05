package numpool_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool"
	"github.com/yuku/numpool/internal"
	"github.com/yuku/numpool/internal/sqlc"
)

// Shared connection pool
var connPool *pgxpool.Pool

func TestMain(m *testing.M) {
	pool, err := internal.GetPool(context.Background())
	if err != nil {
		log.Fatalf("failed to get connection pool: %v", err)
	}
	connPool = pool

	code := m.Run()
	os.Exit(code)
}

func setupWithUniquePoolID(t *testing.T) (*numpool.Manager, string) {
	t.Helper()

	ctx := context.Background()
	poolID := fmt.Sprintf("test_pool_%s", t.Name())

	manager, err := numpool.Setup(ctx, connPool)
	require.NoError(t, err, "Failed to create Numpool manager")
	t.Cleanup(manager.Close)

	// Clean up any existing pool with the same ID
	_, err = sqlc.New(connPool).DeleteNumpool(ctx, poolID)
	require.NoError(t, err)

	return manager, poolID
}
