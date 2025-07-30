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
// to verify reliability under high concurrent load with multiple handlers
func TestWaitQueueStressTest(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping waitqueue stress test in short mode")
	}

	const numWaiters = 200
	const numNotifications = 200
	const numHandlers = 5 // Multiple handlers to simulate multiple processes

	handlers := make([]*waitqueue.ListenHandler, numHandlers)
	for i := range numHandlers {
		handlers[i] = &waitqueue.ListenHandler{}
	}
	ctx := context.Background()

	// Track successful notifications
	var successCount int64
	var timeoutCount int64
	var wg sync.WaitGroup

	// Create many waiters that will compete for notifications
	// Distribute waiters across multiple handlers
	for i := 0; i < numWaiters; i++ {
		wg.Add(1)
		go func(waiterID int) {
			defer wg.Done()

			// Use different handlers to simulate multiple processes
			handlerIndex := waiterID % numHandlers
			handler := handlers[handlerIndex]

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

	// Start multiple PostgreSQL listeners (one per handler)
	const stressChannel = "stress_test_channel"
	for i, handler := range handlers {
		go func(handlerIndex int, h *waitqueue.ListenHandler) {
			listener := &pgxlisten.Listener{
				Connect: func(ctx context.Context) (*pgx.Conn, error) {
					return internal.GetConnection(ctx)
				},
			}
			listener.Handle(stressChannel, h)
			_ = listener.Listen(ctx)
		}(i, handler)
	}
	time.Sleep(100 * time.Millisecond) // Give listeners time to start

	// Send notifications to waiters via PostgreSQL in parallel
	queries := sqlc.New(connPool)
	var notifyWg sync.WaitGroup
	
	// Create a channel to distribute notification work
	notificationChan := make(chan int, numNotifications)
	for i := 0; i < numNotifications; i++ {
		notificationChan <- i
	}
	close(notificationChan)

	// Start multiple goroutines to send notifications in parallel
	const numNotifiers = 10
	for i := 0; i < numNotifiers; i++ {
		notifyWg.Add(1)
		go func() {
			defer notifyWg.Done()
			for notificationID := range notificationChan {
				// Pick a waiter to notify
				targetWaiter := notificationID % numWaiters // Ensure each waiter gets at least one chance
				waiterID := fmt.Sprintf("stress-waiter-%d", targetWaiter)

				// Send PostgreSQL notification
				_ = queries.NotifyWaiters(ctx, sqlc.NotifyWaitersParams{
					ChannelName: stressChannel,
					WaiterID:    waiterID,
				})

				// Small delay between notifications from this goroutine
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	notifyWg.Wait()
	wg.Wait()

	finalSuccess := atomic.LoadInt64(&successCount)
	finalTimeout := atomic.LoadInt64(&timeoutCount)

	t.Logf("Stress test results: %d successful, %d timeouts out of %d waiters (using %d handlers, %d notifiers)",
		finalSuccess, finalTimeout, numWaiters, numHandlers, numNotifiers)

	// We expect reasonable number of successes since we cycle through all waiters
	assert.GreaterOrEqual(t, finalSuccess, int64(numWaiters/2),
		"Should have reasonable number of successful notifications")
	assert.Equal(t, int64(numWaiters), finalSuccess+finalTimeout,
		"Total should equal number of waiters")
}
