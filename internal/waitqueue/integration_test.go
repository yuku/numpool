package waitqueue_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgxlisten"
	"github.com/stretchr/testify/assert"
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

// TestWaitQueueStressTest performs stress testing on the waitqueue system
// to verify reliability under high concurrent load
func TestWaitQueueStressTest(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping waitqueue stress test in short mode")
	}

	const numWaiters = 50
	const numNotifications = 50

	handler := &waitqueue.ListenHandler{}
	ctx := context.Background()

	// Track successful notifications
	var successCount int64
	var timeoutCount int64
	var wg sync.WaitGroup

	// Create many waiters that will compete for notifications
	for i := 0; i < numWaiters; i++ {
		wg.Add(1)
		go func(waiterID int) {
			defer wg.Done()

			waiterIDStr := fmt.Sprintf("stress-waiter-%d", waiterID)
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()

			err := waitqueue.Wait(waitCtx, handler, waitqueue.WithID(waiterIDStr))
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					atomic.AddInt64(&timeoutCount, 1)
				}
				return
			}

			atomic.AddInt64(&successCount, 1)
		}(i)
	}

	// Give waiters time to register
	time.Sleep(50 * time.Millisecond)

	// Start PostgreSQL listener
	const stressChannel = "stress_test_channel"
	go func() {
		listener := &pgxlisten.Listener{
			Connect: func(ctx context.Context) (*pgx.Conn, error) {
				return internal.GetConnection(ctx)
			},
		}
		listener.Handle(stressChannel, handler)
		_ = listener.Listen(ctx)
	}()
	time.Sleep(100 * time.Millisecond) // Give listener time to start

	// Send notifications to waiters via PostgreSQL
	queries := sqlc.New(connPool)
	go func() {
		for i := 0; i < numNotifications; i++ {
			// Pick a waiter to notify
			targetWaiter := i % numWaiters // Ensure each waiter gets at least one chance
			waiterID := fmt.Sprintf("stress-waiter-%d", targetWaiter)

			// Send PostgreSQL notification
			_ = queries.NotifyWaiters(ctx, sqlc.NotifyWaitersParams{
				ChannelName: stressChannel,
				WaiterID:    waiterID,
			})

			// Small delay between notifications
			time.Sleep(20 * time.Millisecond)
		}
	}()

	wg.Wait()

	finalSuccess := atomic.LoadInt64(&successCount)
	finalTimeout := atomic.LoadInt64(&timeoutCount)

	t.Logf("Stress test results: %d successful, %d timeouts out of %d waiters",
		finalSuccess, finalTimeout, numWaiters)

	// We expect reasonable number of successes since we cycle through all waiters
	assert.GreaterOrEqual(t, finalSuccess, int64(numWaiters/2),
		"Should have reasonable number of successful notifications")
	assert.Equal(t, int64(numWaiters), finalSuccess+finalTimeout,
		"Total should equal number of waiters")
}
