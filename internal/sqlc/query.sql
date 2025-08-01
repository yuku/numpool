-- name: CheckNumpoolTableExist :one
-- CheckNumpoolTableExist checks if "public"."numpool" exists.
SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'numpools'
) AS exists;

-- name: GetNumpoolForUpdate :one
-- GetNumpoolForUpdate retrieves the numpool row with the given id and locks it for update.
SELECT * FROM numpools WHERE id = $1 FOR UPDATE;

-- name: GetNumpool :one
-- GetNumpoolForUpdate retrieves the numpool row with the given id without locking it.
SELECT * FROM numpools WHERE id = $1;

-- name: CheckNumpoolExists :one
-- CheckNumpoolExists checks if a numpool with the given id exists.
SELECT EXISTS (
    SELECT 1
    FROM numpools
    WHERE id = $1
) AS exists;

-- name: CreateNumpool :exec
-- CreateNumpool creates a new numpool with the specified id and max_resources_count.
INSERT INTO numpools (id, max_resources_count, metadata)
VALUES ($1, $2, $3);

-- name: UpdateNumpoolMetadata :exec
-- UpdateNumpoolMetadata updates the metadata of the numpool with the specified id.
UPDATE numpools
SET metadata = $2
WHERE id = $1;

-- name: DeleteNumpool :execrows
-- DeleteNumpool deletes the numpool with the specified id.
DELETE FROM numpools WHERE id = $1;

-- name: AcquireResource :execrows
-- AcquireResource attempts to acquire a resource from the numpool.
-- It returns the id of the numpool if successful, or NULL if no resources are available.
UPDATE numpools
SET resource_usage_status = resource_usage_status | (1::BIT(64) << (63 - @resource_index))
WHERE id = $1
  AND (resource_usage_status & (1::BIT(64) << (63 - @resource_index))) = 0::BIT(64);

-- name: ReleaseResource :execrows
-- ReleaseResource releases a resource back to the numpool.
UPDATE numpools
SET resource_usage_status = resource_usage_status & ~(1::BIT(64) << (63 - @resource_index))
WHERE id = $1
	AND (resource_usage_status & (1::BIT(64) << (63 - @resource_index))) <> 0::BIT(64);

-- name: EnqueueWaitingClient :exec
-- EnqueueWaitingClient adds a waiter ID to the wait queue.
UPDATE numpools
SET wait_queue = array_append(wait_queue, @waiter_id::VARCHAR(100))
WHERE id = $1;

-- name: AcquireResourceAndDequeueFirstWaiter :execrows
-- AcquireResourceAndDequeueFirstWaiter attempts to acquire a resource and dequeue the first client
-- from the wait queue if successful.
UPDATE numpools
SET
  resource_usage_status = resource_usage_status | (1::BIT(64) << (63 - @resource_index::INTEGER)),
  wait_queue = wait_queue[2:]
WHERE id = $1
  AND (resource_usage_status & (1::BIT(64) << (63 - @resource_index))) = 0::BIT(64)
  AND cardinality(wait_queue) > 0
  AND wait_queue[1] = @waiter_id::VARCHAR(100);

-- name: RemoveFromWaitQueue :exec
-- RemoveFromWaitQueue removes a specific waiter UUID from the wait queue.
UPDATE numpools
SET wait_queue = array_remove(wait_queue, @waiter_id)
WHERE id = $1;

-- name: NotifyWaiters :exec
SELECT pg_notify(@channel_name, @waiter_id);

-- name: LockNumpoolTableInShareMode :exec
LOCK TABLE numpools IN SHARE ROW EXCLUSIVE MODE;

-- name: AcquireAdvisoryLock :exec
SELECT pg_advisory_xact_lock(@lock_id);

-- name: DropNumpoolTable :exec
-- DropNumpoolTable drops the numpool table if it exists.
DROP TABLE IF EXISTS numpools;

-- name: ListPools :many
-- ListPools returns a list of pool names that start with the given prefix.
-- If prefix is empty, returns all pools ordered by pool name.
SELECT id FROM numpools
WHERE id LIKE @prefix || '%'
ORDER BY id;
