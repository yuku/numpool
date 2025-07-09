-- name: DoesNumpoolTableExist :one
-- DoesNumpoolTableExist checks if "public"."numpool" exists.
SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'numpool'
) AS exists;

-- name: GetNumpoolForUpdate :one
-- GetNumpoolForUpdate retrieves the numpool row with the given id and locks it for update.
SELECT * FROM numpool WHERE id = $1 FOR UPDATE;

-- name: GetNumpool :one
-- GetNumpoolForUpdate retrieves the numpool row with the given id without locking it.
SELECT * FROM numpool WHERE id = $1;

-- name: CreateNumpool :exec
-- CreateNumpool creates a new numpool with the specified id and max_resources_count.
INSERT INTO numpool (id, max_resources_count)
VALUES ($1, $2);

-- name: DeleteNumpool :exec
-- DeleteNumpool deletes the numpool with the specified id.
DELETE FROM numpool WHERE id = $1;

-- name: AcquireResource :execrows
-- AcquireResource attempts to acquire a resource from the numpool.
-- It returns the id of the numpool if successful, or NULL if no resources are available.
UPDATE numpool
SET resource_usage_status = resource_usage_status | (1::BIT(64) << (63 - @resource_index))
WHERE id = $1
  AND (resource_usage_status & (1::BIT(64) << (63 - @resource_index))) = 0::BIT(64);

-- name: ReleaseResource :execrows
-- ReleaseResource releases a resource back to the numpool.
UPDATE numpool
SET resource_usage_status = resource_usage_status & ~(1::BIT(64) << (63 - @resource_index))
WHERE id = $1
	AND (resource_usage_status & (1::BIT(64) << (63 - @resource_index))) <> 0::BIT(64);

-- name: EnqueueWaitingClient :exec
-- EnqueueWaitingClient adds a client UUID to the wait queue.
UPDATE numpool
SET wait_queue = array_append(wait_queue, @client_id)
WHERE id = $1;

-- name: DequeueWaitingClient :one
-- DequeueWaitingClient removes and returns the first client from the wait queue.
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
RETURNING first_client.client_id;

-- name: RemoveFromWaitQueue :exec
-- RemoveFromWaitQueue removes a specific client UUID from the wait queue.
UPDATE numpool
SET wait_queue = array_remove(wait_queue, @client_id)
WHERE id = $1;
