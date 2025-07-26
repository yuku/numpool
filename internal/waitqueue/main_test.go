package waitqueue_test

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yuku/numpool/internal"
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
