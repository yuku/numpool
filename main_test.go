package numpool_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool"
	"github.com/yuku/numpool/internal"
	"github.com/yuku/numpool/internal/sqlc"
)

func setupWithUniquePoolID(t *testing.T) (*numpool.Manager, string) {
	t.Helper()

	ctx := context.Background()
	dbPool := internal.MustGetPoolWithCleanup(t)
	poolID := fmt.Sprintf("test_pool_%s", t.Name())

	manager, err := numpool.Setup(ctx, dbPool)
	require.NoError(t, err, "Failed to create Numpool manager")

	// Clean up any existing pool with the same ID
	_, err = sqlc.New(dbPool).DeleteNumpool(ctx, poolID)
	require.NoError(t, err)

	return manager, poolID
}
