package internal

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yuku/numpool/internal/statedb"
)

// GetConnection returns a connection to the PostgreSQL database.
// The returned connection must have full privileges to create databases and
// manage the pool.
func GetConnection(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, getConnString())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	return conn, nil
}

// MustGetConnection returns a connection to the PostgreSQL database and panics
// if the connection cannot be established.
func MustGetConnection(ctx context.Context) *pgx.Conn {
	conn, err := GetConnection(ctx)
	if err != nil {
		panic(err)
	}
	return conn
}

func MustGetConnectionWithCleanup(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	conn := MustGetConnection(ctx)
	t.Cleanup(func() { _ = conn.Close(ctx) })
	return conn
}

// MustGetPoolWithCleanup returns a connection pool to the PostgreSQL database
// and automatically cleans it up when the test completes.
func MustGetPoolWithCleanup(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, getConnString())
	if err != nil {
		t.Fatalf("failed to create connection pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	return pool
}

func getConnString() string {
	if connStr := os.Getenv("DATABASE_URL"); connStr != "" {
		return connStr
	}

	host := getEnvOrDefault("PGHOST", "localhost")
	port := getEnvOrDefault("PGPORT", "5432")
	user := getEnvOrDefault("PGUSER", "postgres")
	password := getEnvOrDefault("PGPASSWORD", "postgres")
	database := getEnvOrDefault("PGDATABASE", "postgres")

	if password != "" {
		return fmt.Sprintf(
			"postgres://%s:%s@%s:%s/%s?sslmode=disable",
			user, password, host, port, database,
		)
	}
	return fmt.Sprintf(
		"postgres://%s@%s:%s/%s?sslmode=disable",
		user, host, port, database,
	)
}

// getEnvOrDefault retrieves an environment variable or returns a default value
// if the variable is not set.
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

var setupOnce sync.Once

// SetupTestDatabase ensures the database schema is set up for tests.
// It uses PostgreSQL advisory locks to prevent concurrent setup attempts
// and sync.Once to ensure it runs only once per process.
func SetupTestDatabase() {
	setupOnce.Do(func() {
		ctx := context.Background()
		conn := MustGetConnection(ctx)
		defer func() {
			_ = conn.Close(ctx)
		}()

		// Use advisory lock to prevent concurrent schema creation
		// Lock ID 12345 is arbitrary but must be consistent across all test processes
		const lockID int64 = 12345
		
		// Try to acquire exclusive advisory lock
		_, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockID)
		if err != nil {
			panic(fmt.Sprintf("failed to acquire advisory lock: %v", err))
		}
		defer func() {
			_, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockID)
		}()

		if err := statedb.Setup(ctx, conn); err != nil {
			panic(fmt.Sprintf("failed to setup database: %v", err))
		}
	})
}
