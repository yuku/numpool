package waitqueue_test

import (
	"context"
	"errors"
	"testing"

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
		err := handler.Register("test", func(ctx context.Context) error {
			called = true
			return nil
		})
		require.NoError(t, err)

		// When
		err = handler.HandleNotification(context.Background(), &pgconn.Notification{Payload: "test"}, nil)

		// Then
		assert.NoError(t, err, "HandleNotification should not return an error")
		assert.True(t, called, "Registered callback should be called")
	})

	t.Run("returns error if callback returns error", func(t *testing.T) {
		// Given
		handler := &waitqueue.ListenHandler{}
		expectedErr := errors.New("callback error")
		err := handler.Register("test", func(ctx context.Context) error {
			return expectedErr
		})
		require.NoError(t, err)

		// When
		err = handler.HandleNotification(context.Background(), &pgconn.Notification{Payload: "test"}, nil)

		// Then
		assert.Error(t, err, "HandleNotification should return an error if callback returns an error")
		assert.Equal(t, expectedErr, err, "Error returned should match the one from the callback")
	})

	t.Run("handles multiple callbacks", func(t *testing.T) {
		// Given
		handler := &waitqueue.ListenHandler{}
		var called1, called2 bool
		err := handler.Register("test1", func(ctx context.Context) error {
			called1 = true
			return nil
		})
		require.NoError(t, err)
		err = handler.Register("test2", func(ctx context.Context) error {
			called2 = true
			return nil
		})
		require.NoError(t, err)

		// When
		err = handler.HandleNotification(context.Background(), &pgconn.Notification{Payload: "test1"}, nil)

		// Then
		assert.NoError(t, err, "HandleNotification should not return an error")
		assert.True(t, called1, "Callback for test1 should be called")
		assert.False(t, called2, "Callback for test2 should not be called")
	})
}
