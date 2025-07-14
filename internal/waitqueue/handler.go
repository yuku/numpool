package waitqueue

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgxlisten"
)

type ListenHandler struct {
	mu sync.RWMutex

	waiters map[string]func(context.Context) error
}

var _ pgxlisten.Handler = (*ListenHandler)(nil)

// HandleNotification implements the pgxlisten.Handler interface.
func (h *ListenHandler) HandleNotification(ctx context.Context, notification *pgconn.Notification, _ *pgx.Conn) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if callback := h.waiters[notification.Payload]; callback != nil {
		return callback(ctx)
	}

	return nil
}

// Register registers a w to receive notifications.
func (h *ListenHandler) Register(id string, callback func(context.Context) error) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.waiters == nil {
		h.waiters = make(map[string]func(context.Context) error)
	}
	if _, exists := h.waiters[id]; exists {
		return fmt.Errorf("duplicate id: %s", id)
	}
	h.waiters[id] = callback
	return nil
}

// Has checks if a waiter with the given ID is registered.
func (h *ListenHandler) Has(id string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, exists := h.waiters[id]
	return exists
}

// Unregister unregisters a w from receiving notifications.
func (h *ListenHandler) Unregister(id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, exists := h.waiters[id]; !exists {
		return false
	}
	delete(h.waiters, id)
	return true
}
