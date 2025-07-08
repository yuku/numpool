package numpool

import (
	"context"
	"os"
	"testing"

	"github.com/yuku/numpool/internal"
	"github.com/yuku/numpool/internal/statedb"
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Setup the database connection and schema before running tests
	conn := internal.MustGetConnection(ctx)
	defer func() {
		_ = conn.Close(ctx)
	}()

	if err := statedb.Setup(ctx, conn); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}
