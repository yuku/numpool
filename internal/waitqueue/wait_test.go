package waitqueue_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool/internal/waitqueue"
)

func TestWait(t *testing.T) {
	t.Parallel()

	t.Run("blocks until notification received", func(t *testing.T) {
		handler := &waitqueue.ListenHandler{}
		ctx := context.Background()

		errs := make(chan error, 1)
		returned := false

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			err := waitqueue.Wait(ctx, handler,
				waitqueue.WithID("test"),
				waitqueue.WithAfterRegister(func() error {
					wg.Done()
					return nil
				}),
			)
			returned = true
			errs <- err
		}()

		wg.Wait() // Ensure the waiter is registered

		require.False(t, returned, "Wait should block until notification is received")
		err := handler.HandleNotification(ctx, &pgconn.Notification{Payload: "test"}, nil)
		require.NoError(t, err, "HandleNotification should not return an error")

		select {
		case err = <-errs:
			require.NoError(t, err, "Wait should not return an error")
			require.True(t, returned, "Wait should return after notification is received")
		case <-time.After(1 * time.Second):
			t.Fatal("Wait did not return after notification was sent")
		}
	})

	t.Run("returns error if afterRegister callback fails", func(t *testing.T) {
		handler := &waitqueue.ListenHandler{}
		ctx := context.Background()

		err := waitqueue.Wait(ctx, handler,
			waitqueue.WithAfterRegister(func() error {
				return errors.New("callback error")
			}),
		)
		require.ErrorContains(t, err, "callback error")
	})

	t.Run("returns error if duplicate ID is used", func(t *testing.T) {
		handler := &waitqueue.ListenHandler{}
		ctx := context.Background()

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			_ = waitqueue.Wait(ctx, handler,
				waitqueue.WithID("test"),
				waitqueue.WithAfterRegister(func() error {
					wg.Done()
					return nil
				}),
			)
		}()

		wg.Wait() // Ensure the waiter is registered

		err := waitqueue.Wait(ctx, handler, waitqueue.WithID("test"))
		require.ErrorContains(t, err, "duplicate id: test")
	})

	t.Run("waits with context cancellation", func(t *testing.T) {
		handler := &waitqueue.ListenHandler{}
		waitCtx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		errs := make(chan error, 1)

		go func() {
			errs <- waitqueue.Wait(waitCtx, handler,
				waitqueue.WithID("test"),
				waitqueue.WithAfterRegister(func() error {
					wg.Done()
					return nil
				}),
			)
		}()

		wg.Wait() // Ensure the waiter is registered

		cancel() // Cancel the context before notification is sent

		select {
		case err := <-errs:
			require.ErrorIs(t, err, context.Canceled, "Wait should return context.Canceled error")
			require.False(t, handler.Has("test"), "Waiter should be unregistered after context cancellation")
		case <-time.After(1 * time.Second):
			t.Fatal("Wait did not return after context cancellation")
		}
	})

	t.Run("concurrent waiters", func(t *testing.T) {
		handler := &waitqueue.ListenHandler{}
		ctx := context.Background()

		n := 10
		var registered, called sync.WaitGroup
		count := int32(0)

		for i := range n {
			registered.Add(1)
			called.Add(1)
			go func() {
				err := waitqueue.Wait(ctx, handler,
					waitqueue.WithID(fmt.Sprintf("test-%d", i)),
					waitqueue.WithAfterRegister(func() error {
						registered.Done()
						return nil
					}),
				)
				require.NoError(t, err, "Wait should not return an error")
				atomic.AddInt32(&count, 1)
				called.Done()
			}()
		}

		registered.Wait() // Ensure all waiters are registered

		// Send notifications concurrently
		for i := range n {
			go func() {
				err := handler.HandleNotification(ctx,
					&pgconn.Notification{Payload: fmt.Sprintf("test-%d", i)},
					nil,
				)
				require.NoError(t, err, "HandleNotification should not return an error")
			}()
		}

		called.Wait() // Wait for all callbacks to be called
		require.EqualValues(t, n, count, "All waiters should have been notified")
	})
}
