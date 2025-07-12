// Command numpool provides CLI utilities for managing numpool database schema.
//
// Usage:
//
//	numpool <command>
//
// Commands:
//
//	setup    Initialize the numpool database schema
//
// The numpool command respects standard PostgreSQL environment variables:
//   - DATABASE_URL: Full connection string (overrides all other variables)
//   - PGHOST: Database host (default: localhost)
//   - PGPORT: Database port (default: 5432)
//   - PGUSER: Database user (default: postgres)
//   - PGPASSWORD: Database password (default: postgres)
//   - PGDATABASE: Database name (default: postgres)
//
// Example:
//
//	# Using DATABASE_URL
//	DATABASE_URL=postgres://user:pass@host:5432/db numpool setup
//
//	# Using individual variables
//	PGHOST=db.example.com PGUSER=myuser PGPASSWORD=mypass numpool setup
//
//	# Using defaults (connects to localhost:5432 as postgres user)
//	numpool setup
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/yuku/numpool"
	"github.com/yuku/numpool/internal"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <command>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  setup    Initialize the numpool database schema\n")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "setup":
		if err := runSetup(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Setup completed successfully")
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		os.Exit(1)
	}
}

// runSetup initializes the numpool database schema.
// It uses internal.GetConnection which automatically reads PostgreSQL
// connection parameters from environment variables.
func runSetup() error {
	ctx := context.Background()

	pool, err := internal.GetPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	if _, err := numpool.Setup(ctx, pool); err != nil {
		return fmt.Errorf("failed to setup database: %w", err)
	}

	return nil
}
