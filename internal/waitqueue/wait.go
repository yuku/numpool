package waitqueue

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

func Wait(ctx context.Context, handler *ListenHandler, opts ...WaitOption) error {
	if handler == nil {
		return errors.New("handler cannot be nil")
	}

	options := &WaitOptions{}
	for _, opt := range opts {
		opt(options)
	}

	if options.id == "" {
		options.id = uuid.NewString()
	}

	notify := make(chan struct{}, 1)

	err := handler.Register(options.id, func(ctx context.Context) error {
		select {
		case notify <- struct{}{}:
			return nil // Successfully sent notification to the waiter
		case <-ctx.Done():
			return ctx.Err() // Context was cancelled
		}
	})
	if err != nil {
		return fmt.Errorf("failed to register waiter: %w", err)
	}
	defer handler.Unregister(options.id)

	if options.afterRegister != nil {
		if err := options.afterRegister(); err != nil {
			return fmt.Errorf("after register callback failed: %w", err)
		}
	}

	// Wait for a notification or context cancellation
	select {
	case <-notify:
		return nil // Resource is available
	case <-ctx.Done():
		return ctx.Err() // Context was cancelled
	}
}

type WaitOptions struct {
	// id is the unique identifier for the waiter.
	id string

	// afterRegister is a callback that will be called after the waiter is registered.
	afterRegister func() error
}

type WaitOption func(*WaitOptions)

// WithID allows setting a unique identifier for the waiter.
func WithID(id string) WaitOption {
	return func(opts *WaitOptions) {
		opts.id = id
	}
}

// WithAfterRegister allows setting a callback to be called after the waiter is
// registered.
func WithAfterRegister(callback func() error) WaitOption {
	return func(opts *WaitOptions) {
		opts.afterRegister = callback
	}
}
