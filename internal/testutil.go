package internal

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// GetConnection returns a connection to the PostgreSQL database.
// The returned connection must have full privileges to create databases and
// manage the pool.
func GetConnection(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, getConnString())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database")
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
		panic(fmt.Sprintf("failed to get database connection: %v", err))
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
