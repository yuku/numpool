package numpool

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/yuku/numpool/internal/sqlc"
)

// Pool represents a pool of resources that can be acquired and released.
type Pool struct {
	id   string
	conn *pgx.Conn
}

type Config struct {
	Conn              *pgx.Conn
	ID                string
	MaxResourcesCount int32
}

const (
	maxResourcesCount = 64 // Maximum number of resources in the pool
)

func (c Config) Validate() error {
	if c.Conn == nil {
		return fmt.Errorf("connection cannot be nil")
	}
	if c.ID == "" {
		return fmt.Errorf("pool ID cannot be empty")
	}
	if c.MaxResourcesCount <= 0 || maxResourcesCount < c.MaxResourcesCount {
		return fmt.Errorf("max resources count must be between 1 and %d: given %d",
			maxResourcesCount, c.MaxResourcesCount,
		)
	}
	return nil
}

// CreateOrOpen creates a new pool or opens an existing one based on the
// provided configuration. If the pool already exists with a different
// MaxResourcesCount, it returns an error.
func CreateOrOpen(ctx context.Context, conf Config) (*Pool, error) {
	if err := conf.Validate(); err != nil {
		return nil, fmt.Errorf("invalid pool configuration: %w", err)
	}

	// Check if the pool already exists
	q := sqlc.New(conf.Conn)

	row, err := q.GetNumpool(ctx, conf.ID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("failed to get numpool: %w", err)
		}
		// Pool does not exist, create it
		err = q.CreateNumpool(ctx, sqlc.CreateNumpoolParams{
			ID:                conf.ID,
			MaxResourcesCount: conf.MaxResourcesCount,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create numpool: %w", err)
		}
	} else {
		if row.MaxResourcesCount != conf.MaxResourcesCount {
			return nil, fmt.Errorf("pool %s already exists with different max resources count: %d, expected: %d",
				conf.ID, row.MaxResourcesCount, conf.MaxResourcesCount,
			)
		}
	}

	return &Pool{id: conf.ID, conn: conf.Conn}, nil
}

// ID returns the unique identifier of the pool.
func (p *Pool) ID() string {
	return p.id
}

// Acquire acquires a resource from the pool.
func (p *Pool) Acquire(ctx context.Context) (*Resource, error) {
	return nil, fmt.Errorf("not implemented")
}

// release releases a resource back to the pool.
func (p *Pool) release(ctx context.Context, r *Resource) error {
	return fmt.Errorf("not implemented")
}
