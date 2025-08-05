package waitqueue_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool/internal/waitqueue"
)

func TestListenHandler_HandleNotification(t *testing.T) {
	t.Parallel()

	t.Run("returns nil if no callback is registered", func(t *testing.T) {
		// Given
		handler := &waitqueue.ListenHandler{}

		// When
		err := handler.HandleNotification(context.Background(), &pgconn.Notification{Payload: "nonexistent"}, nil)

		// Then
		assert.NoError(t, err, "HandleNotification should return nil if no callback is registered")
	})

	t.Run("returns nil if callback is nil", func(t *testing.T) {
		// Given
		handler := &waitqueue.ListenHandler{}
		require.NoError(t, handler.Register("test", nil))

		// When
		err := handler.HandleNotification(context.Background(), &pgconn.Notification{Payload: "test"}, nil)

		// Then
		assert.NoError(t, err, "HandleNotification should return nil if callback is nil")
	})

	t.Run("calls registered callback with context", func(t *testing.T) {
		// Given
		handler := &waitqueue.ListenHandler{}
		var called bool
		var mu sync.Mutex
		done := make(chan struct{})

		err := handler.Register("test", func(ctx context.Context) error {
			mu.Lock()
			called = true
			mu.Unlock()
			close(done)
			return nil
		})
		require.NoError(t, err)

		// When
		err = handler.HandleNotification(context.Background(), &pgconn.Notification{Payload: "test"}, nil)

		// Then
		assert.NoError(t, err, "HandleNotification should not return an error")

		// Wait for async callback
		select {
		case <-done:
			// Success
		case <-time.After(1 * time.Second):
			t.Fatal("Callback was not called within timeout")
		}

		mu.Lock()
		assert.True(t, called, "Registered callback should be called")
		mu.Unlock()
	})

	t.Run("ignores callback errors in async processing", func(t *testing.T) {
		// Given
		handler := &waitqueue.ListenHandler{}
		done := make(chan struct{})

		err := handler.Register("test", func(ctx context.Context) error {
			close(done)
			return errors.New("callback error") // This error is ignored in async processing
		})
		require.NoError(t, err)

		// When
		err = handler.HandleNotification(context.Background(), &pgconn.Notification{Payload: "test"}, nil)

		// Then
		assert.NoError(t, err, "HandleNotification should not return callback errors in async mode")

		// Wait for async callback
		select {
		case <-done:
			// Success - callback was called even though it returned an error
		case <-time.After(1 * time.Second):
			t.Fatal("Callback was not called within timeout")
		}
	})

	t.Run("handles multiple callbacks", func(t *testing.T) {
		// Given
		handler := &waitqueue.ListenHandler{}
		var called1, called2 bool
		var mu sync.Mutex
		done1 := make(chan struct{})

		err := handler.Register("test1", func(ctx context.Context) error {
			mu.Lock()
			called1 = true
			mu.Unlock()
			close(done1)
			return nil
		})
		require.NoError(t, err)
		err = handler.Register("test2", func(ctx context.Context) error {
			mu.Lock()
			called2 = true
			mu.Unlock()
			return nil
		})
		require.NoError(t, err)

		// When
		err = handler.HandleNotification(context.Background(), &pgconn.Notification{Payload: "test1"}, nil)

		// Then
		assert.NoError(t, err, "HandleNotification should not return an error")

		// Wait for async callback
		select {
		case <-done1:
			// Success
		case <-time.After(1 * time.Second):
			t.Fatal("Callback was not called within timeout")
		}

		mu.Lock()
		assert.True(t, called1, "Callback for test1 should be called")
		assert.False(t, called2, "Callback for test2 should not be called")
		mu.Unlock()
	})
}
