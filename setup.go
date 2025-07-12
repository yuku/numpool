package numpool

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/yuku/numpool/internal/sqlc"
)

// Setup initializes the numpool table in the database.
// It uses PostgreSQL advisory locks to prevent concurrent setup attempts.
// If the table already exists, it does nothing.
// This method should be called once at application startup to ensure the table
// is ready for use.
func Setup(ctx context.Context, conn *pgx.Conn) error {
	// Use advisory lock to prevent concurrent schema creation
	// Lock ID 12345 is arbitrary but must be consistent across all processes
	const lockID int64 = 12345

	return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		// Try to acquire exclusive advisory lock
		_, err := conn.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockID)
		if err != nil {
			return fmt.Errorf("failed to acquire advisory lock: %w", err)
		}

		q := sqlc.New(tx)
		ok, err := q.DoesNumpoolTableExist(ctx)
		if err != nil {
			return fmt.Errorf("failed to check if numpool table exists: %w", err)
		}
		if ok {
			return nil // Table already exists, no need to set up
		}
		if err := q.CreateTable(ctx); err != nil {
			return fmt.Errorf("failed to create numpool table: %w", err)
		}
		return nil
	})
}
