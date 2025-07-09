# numpool

[![Go Reference](https://pkg.go.dev/badge/github.com/yuku/numpool.svg)](https://pkg.go.dev/github.com/yuku/numpool)

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

## Installation

```bash
go get github.com/yuku/numpool
```

## Quick Start

### 1. Set up the database

First, initialize the required database table. You can do this either via CLI or programmatically:

#### Via CLI:

```bash
# Using go run
go run github.com/yuku/numpool/cmd/numpool setup

# Or install the binary first
go install github.com/yuku/numpool/cmd/numpool@latest
numpool setup
```

The CLI respects standard PostgreSQL environment variables:
- `DATABASE_URL`: Full connection string
- `PGHOST`: Database host (default: localhost)
- `PGPORT`: Database port (default: 5432)
- `PGUSER`: Database user (default: postgres)
- `PGPASSWORD`: Database password
- `PGDATABASE`: Database name (default: postgres)

#### Programmatically:

```go
import (
    "context"
    "log"
    
    "github.com/jackc/pgx/v5"
    "github.com/yuku/numpool"
)

func main() {
    ctx := context.Background()
    
    // Connect to PostgreSQL
    conn, err := pgx.Connect(ctx, "postgres://user:password@localhost/dbname")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close(ctx)
    
    // Set up the numpool table
    if err := numpool.Setup(ctx, conn); err != nil {
        log.Fatal(err)
    }
}
```

### 2. Create and use a pool

```go
import (
    "context"
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
    
    // Create or open a resource pool
    pool, err := numpool.CreateOrOpen(ctx, numpool.Config{
        Pool:              dbPool,
        ID:                "my-resources",
        MaxResourcesCount: 10, // Allow up to 10 resources
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
    fmt.Printf("Got resource #%d\n", resource.Index())
    
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

## Configuration

### Pool Configuration

```go
type Config struct {
    // Required: PostgreSQL connection pool
    Pool *pgxpool.Pool
    
    // Required: Unique identifier for this resource pool
    ID string
    
    // Required: Maximum number of resources (1-64)
    MaxResourcesCount int32
}
```

### Resource Methods

```go
// Get the resource index (0-based)
index := resource.Index()

// Release with error handling
err := resource.Release(ctx)

// Release without error handling (for defer)
resource.Close()
```

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
