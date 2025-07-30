package internal

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yuku/numpool"
)

// GetConnection returns a connection to the PostgreSQL database.
func GetConnection(ctx context.Context, dbnames ...string) (*pgx.Conn, error) {
	if len(dbnames) == 0 {
		dbnames = append(dbnames, getEnvOrDefault("PGDATABASE", "postgres")) // Default database
	}
	if len(dbnames) > 1 {
		return nil, fmt.Errorf("only one database name is allowed, got %d", len(dbnames))
	}
	conn, err := pgx.Connect(ctx, getConnString(dbnames[0]))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	return conn, nil
}

func MustGetConnectionWithCleanup(t testing.TB, dbnames ...string) *pgx.Conn {
	t.Helper()
	conn, err := GetConnection(context.Background(), dbnames...)
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func GetPool(ctx context.Context, dbnames ...string) (*pgxpool.Pool, error) {
	if len(dbnames) == 0 {
		dbnames = append(dbnames, getEnvOrDefault("PGDATABASE", "postgres")) // Default database
	}
	if len(dbnames) > 1 {
		return nil, fmt.Errorf("only one database name is allowed, got %d", len(dbnames))
	}
	
	// Parse the connection string and configure a larger pool for tests
	config, err := pgxpool.ParseConfig(getConnString(dbnames[0]))
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	
	// Set larger pool size to handle multiple listeners and concurrent operations
	// For stress tests with 10-20 Numpool instances each with listeners
	config.MaxConns = 50
	config.MinConns = 5
	
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	return pool, nil
}

func getConnString(database string) string {
	if connStr := os.Getenv("DATABASE_URL"); connStr != "" {
		return connStr
	}

	host := getEnvOrDefault("PGHOST", "localhost")
	port := getEnvOrDefault("PGPORT", "5432")
	user := getEnvOrDefault("PGUSER", "postgres")
	password := getEnvOrDefault("PGPASSWORD", "postgres")

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

// SetupTestDatabase ensures the database schema is set up for tests.
func SetupTestDatabase(dbname string) error {
	ctx := context.Background()
	defaultPool, err := GetPool(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection pool: %w", err)
	}
	defer defaultPool.Close()

	// Drop then recreate the numpool_internal_sqlc database to ensure a clean state.
	_, err = defaultPool.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
	if err != nil {
		return fmt.Errorf("failed to drop numpool_internal_sqlc database: %w", err)
	}
	_, err = defaultPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbname))
	if err != nil {
		return fmt.Errorf("failed to create numpool_internal_sqlc database: %w", err)
	}

	pool, err := GetPool(ctx, dbname)
	if err != nil {
		return fmt.Errorf("failed to connect to %s database: %w", dbname, err)
	}
	if _, err = numpool.Setup(ctx, pool); err != nil {
		return fmt.Errorf("failed to setup database: %w", err)
	}
	return nil
}
