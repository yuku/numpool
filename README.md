# numpool

[![Go Reference](https://pkg.go.dev/badge/github.com/yuku/numpool.svg)](https://pkg.go.dev/github.com/yuku/numpool)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/yuku/numpool)

A distributed resource pool implementation backed by PostgreSQL for Go applications.

## Overview

numpool provides a way to manage a finite set of resources across multiple processes or servers.
It uses PostgreSQL as a backend to ensure consistency and provides automatic blocking when resources are unavailable.

Key features:
- **Distributed**: Multiple processes can share the same pool
- **Fair queuing**: Blocked clients are served in FIFO order
- **Automatic cleanup**: Resources are released on process termination
- **PostgreSQL-backed**: Leverages PostgreSQL's ACID properties
- **LISTEN/NOTIFY**: Efficient blocking without polling
- **Metadata support**: Store JSON metadata with each pool

## Installation

```bash
go get github.com/yuku/numpool
```

## Quick Start

### 1. Set up the database

Initialize the required database table programmatically:

```go
import (
    "context"
    "log"
    
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/yuku/numpool"
)

func main() {
    ctx := context.Background()
    
    // Create a connection pool
    dbPool, err := pgxpool.New(ctx, "postgres://user:password@localhost/dbname")
    if err != nil {
        log.Fatal(err)
    }
    defer dbPool.Close()
    
    // Set up the numpool table and get a manager
    manager, err := numpool.Setup(ctx, dbPool)
    if err != nil {
        log.Fatal(err)
    }
    defer manager.Close() // This does NOT close the database pool
}
```

### 2. Create and use a pool

```go
import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/yuku/numpool"
)

func main() {
    ctx := context.Background()
    
    // Create a connection pool
    dbPool, err := pgxpool.New(ctx, "postgres://user:password@localhost/dbname")
    if err != nil {
        log.Fatal(err)
    }
    defer dbPool.Close()
    
    // Set up the numpool table and get a manager
    manager, err := numpool.Setup(ctx, dbPool)
    if err != nil {
        log.Fatal(err)
    }
    defer manager.Close() // This does NOT close the database pool
    
    // Create or get a resource pool
    metadataBytes, _ := json.Marshal(map[string]string{"description": "API rate limiter"})
    pool, err := manager.GetOrCreate(ctx, numpool.Config{
        ID:                "my-resources",
        MaxResourcesCount: 10, // Allow up to 10 resources
        Metadata:          metadataBytes,
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // Acquire a resource
    resource, err := pool.Acquire(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer resource.Close() // Always release when done
    
    // Use the resource
    fmt.Printf("Got resource #%d from pool: %s\n", resource.Index(), pool.ID())
    
    // Access pool metadata
    fmt.Printf("Pool metadata: %s\n", pool.Metadata())
    
    // Do work with the resource...
}
```

## How It Works

1. **Resource Tracking**: Each pool maintains a bitmap in PostgreSQL tracking which resources are in use
2. **Acquisition**: When acquiring a resource, numpool finds the first available slot atomically
3. **Blocking**: If no resources are available, the client is added to a wait queue
4. **Notification**: When a resource is released, the first waiting client is notified via PostgreSQL's LISTEN/NOTIFY
5. **Fairness**: The wait queue ensures resources are distributed in the order requested

## Use Cases

- **Connection pooling**: Manage a limited number of connections to external services
- **License management**: Distribute a finite number of software licenses
- **Worker coordination**: Coordinate access to limited computational resources
- **Rate limiting**: Implement distributed rate limiting across services
- **Configuration management**: Store and update pool-specific configuration data

## Configuration

### Pool Configuration

```go
type Config struct {
    // Required: Unique identifier for this resource pool
    ID string
    
    // Required: Maximum number of resources (1-64)
    MaxResourcesCount int32
    
    // Optional: JSON metadata associated with the pool.
    // Set only during pool creation, ignored for existing pools.
    // Must be valid JSON bytes (json.RawMessage).
    Metadata json.RawMessage
    
    // Optional: If true, prevents automatic listener startup.
    // You must call Listen() manually when NoStartListening is true.
    NoStartListening bool
}
```

### Manager and Manual Listener Control

For advanced use cases, you can control the listener manually:

```go
// Create a pool without starting the listener
pool, err := manager.GetOrCreate(ctx, numpool.Config{
    ID:                "my-resources",
    MaxResourcesCount: 10,
    NoStartListening:  true, // Prevents automatic listener startup
})
if err != nil {
    log.Fatal(err)
}

// Start the listener manually in a separate goroutine
go func() {
    if err := pool.Listen(ctx); err != nil {
        log.Printf("Listener error: %v", err)
    }
}()

// Now you can acquire resources as normal
resource, err := pool.Acquire(ctx)
// ... use resource
```

### Pool Methods

```go
// Get the pool ID
id := pool.ID()

// Get the pool metadata as JSON
metadata := pool.Metadata()

// Update the pool metadata (uses optimistic locking)
metadataBytes, _ := json.Marshal(map[string]string{
    "version": "2.0",
    "updated": "2024-01-01",
})
err := pool.UpdateMetadata(ctx, metadataBytes)

// Check pool status
if pool.Listening() {
    log.Println("Pool is listening for notifications")
}
if pool.Closed() {
    log.Println("Pool is closed")
}

// Manually close a pool (stops listening and releases resources)
pool.Close()
```

### Manager Methods

```go
// Delete a pool by ID (returns error if pool doesn't exist)
err := manager.Delete(ctx, "pool-id")

// Check if manager is closed
if manager.Closed() {
    log.Println("Manager is closed")
}

// Close the manager (closes all managed pools but not the database pool)
manager.Close()
```

### Resource Methods

```go
// Get the resource index (0-based)
index := resource.Index()

// Release with error handling
err := resource.Release(ctx)

// Release without error handling (for defer)
resource.Close()

// Check if resource is closed/released
if resource.Closed() {
    log.Println("Resource has been released")
}
```

## Metadata Management

numpool supports optional JSON metadata that can be associated with each pool. This metadata is stored in PostgreSQL and can be used to store configuration, descriptions, or any other relevant information.

### Setting Metadata During Creation

```go
// Create a pool with initial metadata
metadataBytes, _ := json.Marshal(map[string]any{
    "description": "Rate limiter for external API calls",
    "rate_limit":  1000,
    "created_by":  "api-service",
    "tags":        []string{"production", "api", "rate-limiting"},
})
pool, err := manager.GetOrCreate(ctx, numpool.Config{
    ID:                "api-limiter",
    MaxResourcesCount: 100,
    Metadata:          metadataBytes,
})
```

### Reading Metadata

```go
import "encoding/json"

// Get metadata as raw JSON
metadataBytes := pool.Metadata()

// Unmarshal into a struct
type PoolConfig struct {
    Description string   `json:"description"`
    RateLimit   int      `json:"rate_limit"`
    CreatedBy   string   `json:"created_by"`
    Tags        []string `json:"tags"`
}

var config PoolConfig
err := json.Unmarshal(pool.Metadata(), &config)
if err != nil {
    log.Fatal(err)
}
```

### Updating Metadata

The `UpdateMetadata` method uses optimistic locking to prevent concurrent modifications:

```go
import "time"

// Update metadata safely
newConfig := map[string]any{
    "description": "Updated rate limiter",
    "rate_limit":  2000,
    "updated_at":  time.Now(),
}

metadataBytes, _ := json.Marshal(newConfig)
err := pool.UpdateMetadata(ctx, metadataBytes)
if err != nil {
    // Handle error - might be due to concurrent modification
    log.Printf("Failed to update metadata: %v", err)
}

// To clear metadata (set to null in database)
err = pool.UpdateMetadata(ctx, nil)
if err != nil {
    log.Printf("Failed to clear metadata: %v", err)
}
```

### Metadata Behavior

- **Type Safety**: Metadata must be provided as `json.RawMessage` (valid JSON bytes), ensuring type safety and preventing marshaling errors.
- **Creation**: Metadata is set only when creating a new pool. If a pool already exists, the metadata parameter in `Config` is ignored.
- **Concurrent Updates**: Uses optimistic locking to detect concurrent modifications. If another transaction updates metadata to the same value you're trying to set, the operation succeeds (idempotent).
- **JSON Storage**: Metadata is stored as JSONB in PostgreSQL, allowing for efficient queries and indexing.
- **Null Handling**: Pools can have null metadata, which is returned as `nil` from the `Metadata()` method.
- **Nil Support**: The `UpdateMetadata` method accepts nil to set metadata to null in the database.

## Lifecycle Management

The Manager provides proper lifecycle management for Numpool instances:

```go
// The manager tracks all Numpool instances created through it
manager, err := numpool.Setup(ctx, dbPool)
if err != nil {
    log.Fatal(err)
}

// Create multiple pools
pool1, _ := manager.GetOrCreate(ctx, numpool.Config{ID: "pool1", MaxResourcesCount: 5})
pool2, _ := manager.GetOrCreate(ctx, numpool.Config{ID: "pool2", MaxResourcesCount: 10})

// Closing the manager closes all managed pools but leaves the database pool open
manager.Close()

// Check if manager/pools are closed
if manager.Closed() {
    log.Println("Manager is closed")
}
if pool1.Closed() {
    log.Println("Pool1 is closed")
}
```

**Important Note**: The Manager and Numpool instances do **NOT** close the underlying `pgxpool.Pool` when their `Close()` methods are called. The database connection pool lifecycle is the caller's responsibility. This design allows multiple managers to share the same database pool and gives you full control over when to close the database connections.

## Testing

Run the test suite:

```bash
# Run all tests
go test ./...

# Run with race detector
go test -race ./...

# Run integration tests only
go test ./integration_test.go
```

## License

MIT License - see LICENSE file for details.
