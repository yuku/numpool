// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.29.0
// source: query.sql

package sqlc

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

const acquireResource = `-- name: AcquireResource :execrows
UPDATE numpool
SET resource_usage_status = resource_usage_status | (1::BIT(64) << (63 - $2))
WHERE id = $1
  AND (resource_usage_status & (1::BIT(64) << (63 - $2))) = 0::BIT(64)
`

type AcquireResourceParams struct {
	ID            string
	ResourceIndex interface{}
}

// AcquireResource attempts to acquire a resource from the numpool.
// It returns the id of the numpool if successful, or NULL if no resources are available.
func (q *Queries) AcquireResource(ctx context.Context, arg AcquireResourceParams) (int64, error) {
	result, err := q.db.Exec(ctx, acquireResource, arg.ID, arg.ResourceIndex)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const createNumpool = `-- name: CreateNumpool :exec
INSERT INTO numpool (id, max_resources_count)
VALUES ($1, $2)
`

type CreateNumpoolParams struct {
	ID                string
	MaxResourcesCount int32
}

// CreateNumpool creates a new numpool with the specified id and max_resources_count.
func (q *Queries) CreateNumpool(ctx context.Context, arg CreateNumpoolParams) error {
	_, err := q.db.Exec(ctx, createNumpool, arg.ID, arg.MaxResourcesCount)
	return err
}

const deleteNumpool = `-- name: DeleteNumpool :exec
DELETE FROM numpool WHERE id = $1
`

// DeleteNumpool deletes the numpool with the specified id.
func (q *Queries) DeleteNumpool(ctx context.Context, id string) error {
	_, err := q.db.Exec(ctx, deleteNumpool, id)
	return err
}

const dequeueWaitingClient = `-- name: DequeueWaitingClient :one
WITH first_client AS (
  SELECT wait_queue[1] AS client_id
  FROM numpool
  WHERE id = $1 AND cardinality(wait_queue) > 0
  FOR UPDATE
)
UPDATE numpool
SET wait_queue = wait_queue[2:]
FROM first_client
WHERE numpool.id = $1 AND cardinality(wait_queue) > 0
RETURNING first_client.client_id::UUID
`

// DequeueWaitingClient removes and returns the first client from the wait queue.
func (q *Queries) DequeueWaitingClient(ctx context.Context, id string) (pgtype.UUID, error) {
	row := q.db.QueryRow(ctx, dequeueWaitingClient, id)
	var first_client_client_id pgtype.UUID
	err := row.Scan(&first_client_client_id)
	return first_client_client_id, err
}

const doesNumpoolTableExist = `-- name: DoesNumpoolTableExist :one
SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'numpool'
) AS exists
`

// DoesNumpoolTableExist checks if "public"."numpool" exists.
func (q *Queries) DoesNumpoolTableExist(ctx context.Context) (bool, error) {
	row := q.db.QueryRow(ctx, doesNumpoolTableExist)
	var exists bool
	err := row.Scan(&exists)
	return exists, err
}

const enqueueWaitingClient = `-- name: EnqueueWaitingClient :exec
UPDATE numpool
SET wait_queue = array_append(wait_queue, $2)
WHERE id = $1
`

type EnqueueWaitingClientParams struct {
	ID       string
	ClientID interface{}
}

// EnqueueWaitingClient adds a client UUID to the wait queue.
func (q *Queries) EnqueueWaitingClient(ctx context.Context, arg EnqueueWaitingClientParams) error {
	_, err := q.db.Exec(ctx, enqueueWaitingClient, arg.ID, arg.ClientID)
	return err
}

const getNumpool = `-- name: GetNumpool :one
SELECT id, max_resources_count, resource_usage_status, wait_queue FROM numpool WHERE id = $1
`

// GetNumpoolForUpdate retrieves the numpool row with the given id without locking it.
func (q *Queries) GetNumpool(ctx context.Context, id string) (Numpool, error) {
	row := q.db.QueryRow(ctx, getNumpool, id)
	var i Numpool
	err := row.Scan(
		&i.ID,
		&i.MaxResourcesCount,
		&i.ResourceUsageStatus,
		&i.WaitQueue,
	)
	return i, err
}

const getNumpoolForUpdate = `-- name: GetNumpoolForUpdate :one
SELECT id, max_resources_count, resource_usage_status, wait_queue FROM numpool WHERE id = $1 FOR UPDATE
`

// GetNumpoolForUpdate retrieves the numpool row with the given id and locks it for update.
func (q *Queries) GetNumpoolForUpdate(ctx context.Context, id string) (Numpool, error) {
	row := q.db.QueryRow(ctx, getNumpoolForUpdate, id)
	var i Numpool
	err := row.Scan(
		&i.ID,
		&i.MaxResourcesCount,
		&i.ResourceUsageStatus,
		&i.WaitQueue,
	)
	return i, err
}

const releaseResource = `-- name: ReleaseResource :execrows
UPDATE numpool
SET resource_usage_status = resource_usage_status & ~(1::BIT(64) << (63 - $2))
WHERE id = $1
	AND (resource_usage_status & (1::BIT(64) << (63 - $2))) <> 0::BIT(64)
`

type ReleaseResourceParams struct {
	ID            string
	ResourceIndex interface{}
}

// ReleaseResource releases a resource back to the numpool.
func (q *Queries) ReleaseResource(ctx context.Context, arg ReleaseResourceParams) (int64, error) {
	result, err := q.db.Exec(ctx, releaseResource, arg.ID, arg.ResourceIndex)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const removeFromWaitQueue = `-- name: RemoveFromWaitQueue :exec
UPDATE numpool
SET wait_queue = array_remove(wait_queue, $2)
WHERE id = $1
`

type RemoveFromWaitQueueParams struct {
	ID       string
	ClientID interface{}
}

// RemoveFromWaitQueue removes a specific client UUID from the wait queue.
func (q *Queries) RemoveFromWaitQueue(ctx context.Context, arg RemoveFromWaitQueueParams) error {
	_, err := q.db.Exec(ctx, removeFromWaitQueue, arg.ID, arg.ClientID)
	return err
}
