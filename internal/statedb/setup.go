package statedb

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/yuku/numpool/internal/sqlc"
)

//go:embed schema.sql
var schemaSQL string

// Setup initializes the numpool table.
// It uses PostgreSQL advisory locks to prevent concurrent setup attempts.
func Setup(ctx context.Context, conn *pgx.Conn) error {
	// Use advisory lock to prevent concurrent schema creation
	// Lock ID 12345 is arbitrary but must be consistent across all processes
	const lockID int64 = 12345
	
	// Try to acquire exclusive advisory lock
	_, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockID)
	if err != nil {
		return fmt.Errorf("failed to acquire advisory lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockID)
	}()

	q := sqlc.New(conn)

	ok, err := q.DoesNumpoolTableExist(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if numpool table exists: %w", err)
	}

	if ok {
		return nil // Table already exists, no need to set up
	}

	if _, err := conn.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("failed to create numpool table: %w", err)
	}
	return nil
}
