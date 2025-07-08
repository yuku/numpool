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
func Setup(ctx context.Context, conn *pgx.Conn) error {
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
