package waitqueue_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgxlisten"
	"github.com/stretchr/testify/require"
	"github.com/yuku/numpool/internal"
	"github.com/yuku/numpool/internal/sqlc"
	"github.com/yuku/numpool/internal/waitqueue"
)

// TestSingleListenerWithPostgres is an integration test that checks if the
// listener can handle multiple waiters and notifications correctly using
// a PostgreSQL database.
func TestSingleListenerWithPostgres(t *testing.T) {
	t.Parallel()

	const channel = "test_channel1"
	handler := &waitqueue.ListenHandler{}

	n := 10
	var registered, called sync.WaitGroup
	registered.Add(n)
	called.Add(n)
	count := int32(0)

	for i := range n {
		go func() {
			err := waitqueue.Wait(context.Background(), handler,
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

	go func() {
		listener := &pgxlisten.Listener{
			Connect: func(ctx context.Context) (*pgx.Conn, error) {
				return internal.GetConnection(ctx)
			},
		}
		listener.Handle(channel, handler)
		err := listener.Listen(context.Background())
		require.NoError(t, err, "Listener should not return an error")
	}()
	time.Sleep(100 * time.Millisecond) // Give the listener some time to start

	queries := sqlc.New(connPool)
	for i := range n {
		go func() {
			err := queries.NotifyWaiters(context.Background(), sqlc.NotifyWaitersParams{
				ChannelName: channel,
				WaiterID:    fmt.Sprintf("test-%d", i),
			})
			require.NoError(t, err, "NotifyWaiters should not return an error")
		}()
	}

	called.Wait() // Wait for all callbacks to be called
	require.EqualValues(t, n, count, "All waiters should have been notified")
}

// TestMultipleListenersWithPostgres is an integration test that checks if
// multiple listeners can handle concurrent waiters and notifications correctly
// using a PostgreSQL database.
func TestMultipleListenersWithPostgres(t *testing.T) {
	t.Parallel()

	const channel = "test_channel2"

	m := 5 // Number of listeners
	handlers := make([]*waitqueue.ListenHandler, 0, m)
	for range m {
		handlers = append(handlers, &waitqueue.ListenHandler{})
	}

	n := 100 // Number of waiters
	var registered, called sync.WaitGroup
	registered.Add(n)
	called.Add(n)
	count := int32(0)

	for i := range n {
		go func() {
			err := waitqueue.Wait(context.Background(), handlers[i%m],
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

	for _, handler := range handlers {
		go func() {
			listener := &pgxlisten.Listener{
				Connect: func(ctx context.Context) (*pgx.Conn, error) {
					return internal.GetConnection(ctx)
				},
			}
			listener.Handle(channel, handler)
			err := listener.Listen(context.Background())
			require.NoError(t, err, "Listener should not return an error")
		}()
	}
	time.Sleep(100 * time.Millisecond) // Give the listener some time to start

	queries := sqlc.New(connPool)
	for i := range n {
		go func() {
			err := queries.NotifyWaiters(context.Background(), sqlc.NotifyWaitersParams{
				ChannelName: channel,
				WaiterID:    fmt.Sprintf("test-%d", i),
			})
			require.NoError(t, err, "NotifyWaiters should not return an error")
		}()
	}

	called.Wait() // Wait for all callbacks to be called
	require.EqualValues(t, n, count, "All waiters should have been notified")
}
