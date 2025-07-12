package sqlc

import (
	"context"
	_ "embed"
)

//go:embed schema.sql
var schemaSQL string

// CreateTable creates the numpool table in the database.
func (q *Queries) CreateTable(ctx context.Context) error {
	if _, err := q.db.Exec(ctx, schemaSQL); err != nil {
		return err
	}
	return nil
}
