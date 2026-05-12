package fanout

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/store/subscriptions"
)

const (
	rebuildInterval = 30 * time.Second
	listenChannel   = "subscription_changes"
)

// Cache is an in-memory subscription cache invalidated via PG LISTEN/NOTIFY.
// Exact lookups are O(1). Wildcard ('*') lookups are O(n) over the tenant's
// wildcard subscriptions — acceptable because a tenant won't have thousands of them.
type Cache struct {
	mu       sync.RWMutex
	exact    map[string][]subscriptions.ActiveChannel // "tenantID:userID:eventType" → channels
	wildcard map[string][]subscriptions.ActiveChannel // "tenantID:userID" → wildcard channels

	subs *subscriptions.Repo
	pool *pgxpool.Pool
}

func NewCache(pool *pgxpool.Pool, subs *subscriptions.Repo) *Cache {
	return &Cache{
		exact:    make(map[string][]subscriptions.ActiveChannel),
		wildcard: make(map[string][]subscriptions.ActiveChannel),
		subs:     subs,
		pool:     pool,
	}
}

// Start warms the cache and runs the background refresh loop until ctx is cancelled.
func (c *Cache) Start(ctx context.Context) error {
	if err := c.rebuild(ctx); err != nil {
		return fmt.Errorf("initial cache build: %w", err)
	}

	go c.listen(ctx)
	return nil
}

// ListActiveForEvent satisfies the fanout.Lookup interface.
func (c *Cache) ListActiveForEvent(_ context.Context, tenantID, userID, eventType string) ([]subscriptions.ActiveChannel, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	exactKey := tenantID + ":" + userID + ":" + eventType
	wildcardKey := tenantID + ":" + userID

	var out []subscriptions.ActiveChannel
	out = append(out, c.exact[exactKey]...)
	out = append(out, c.wildcard[wildcardKey]...)
	return out, nil
}

func (c *Cache) rebuild(ctx context.Context) error {
	entries, err := c.subs.ListAllForCache(ctx)
	if err != nil {
		return err
	}

	exact := make(map[string][]subscriptions.ActiveChannel, len(entries))
	wildcard := make(map[string][]subscriptions.ActiveChannel)

	for _, e := range entries {
		ac := e.ActiveChannel
		if e.EventType == "*" {
			key := e.TenantID + ":" + e.UserID
			wildcard[key] = append(wildcard[key], ac)
		} else {
			key := e.TenantID + ":" + e.UserID + ":" + e.EventType
			exact[key] = append(exact[key], ac)
		}
	}

	c.mu.Lock()
	c.exact = exact
	c.wildcard = wildcard
	c.mu.Unlock()
	return nil
}

// listen blocks, listening for PG NOTIFY on subscription_changes.
// WaitForNotification is given a rebuildInterval timeout so the cache rebuilds
// periodically even if no notification arrives — guards against missed notifies.
func (c *Cache) listen(ctx context.Context) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		slog.Error("cache listen: acquire conn", "err", err)
		return
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN "+listenChannel); err != nil {
		slog.Error("cache listen: LISTEN failed", "err", err)
		return
	}

	for {
		if ctx.Err() != nil {
			return
		}

		// Bound the wait so we rebuild periodically even without a notification.
		waitCtx, cancel := context.WithTimeout(ctx, rebuildInterval)
		_, err := conn.Conn().WaitForNotification(waitCtx)
		cancel()

		if ctx.Err() != nil {
			return
		}
		// err is nil (notification) or a deadline exceeded (timeout) — both
		// mean we should rebuild. A real connection error is logged and we exit.
		if err != nil && ctx.Err() == nil {
			slog.Error("cache listen: wait error", "err", err)
		}
		if err := c.rebuild(ctx); err != nil {
			slog.Error("cache rebuild", "err", err)
		}
	}
}
