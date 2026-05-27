// cache.go — in-memory subscription cache invalidated via PG LISTEN/NOTIFY.
// The cache is rebuilt from Postgres on startup and after each notification.

package fanout

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/schema"
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

	subs      *subscriptions.Repo
	pool      *pgxpool.Pool
	listenDSN string // direct Postgres DSN for LISTEN/NOTIFY, bypasses PgBouncer
	reg       *schema.Registry
}

// NewCache creates a cache backed by pool for query traffic.
// listenDSN must be a direct Postgres DSN (not PgBouncer) because LISTEN/NOTIFY
// requires a persistent session that PgBouncer transaction mode doesn't support.
func NewCache(pool *pgxpool.Pool, subs *subscriptions.Repo, listenDSN string, reg *schema.Registry) *Cache {
	if listenDSN == "" {
		listenDSN = pool.Config().ConnConfig.ConnString()
	}
	return &Cache{
		exact:     make(map[string][]subscriptions.ActiveChannel),
		wildcard:  make(map[string][]subscriptions.ActiveChannel),
		subs:      subs,
		pool:      pool,
		listenDSN: listenDSN,
		reg:       reg,
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

// rebuild rebuilds the cache by listing all active subscriptions and building the exact and wildcard maps.
func (c *Cache) rebuild(ctx context.Context) error {
	entries, err := c.subs.ListAllForCache(ctx)
	if err != nil {
		return err
	}

	exact := make(map[string][]subscriptions.ActiveChannel, len(entries))
	wildcard := make(map[string][]subscriptions.ActiveChannel)

	for _, e := range entries {
		// Gate 1: skip channel types not allowed by the static registry.
		if !c.reg.IsAllowed(e.TenantID, e.EventType, e.ChannelType) {
			continue
		}
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

// listen opens a dedicated raw connection to Postgres (bypassing PgBouncer) and
// blocks on LISTEN subscription_changes. WaitForNotification is bounded by
// rebuildInterval so the cache rebuilds periodically even if no NOTIFY arrives.
func (c *Cache) listen(ctx context.Context) {
	conn, err := pgx.Connect(ctx, c.listenDSN)
	if err != nil {
		slog.Error("cache listen: connect", "err", err)
		return
	}
	defer conn.Close(ctx)

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
		_, err := conn.WaitForNotification(waitCtx)
		cancel()

		if ctx.Err() != nil {
			return
		}
		// waitCtx.Err() is non-nil when our own deadline fired (expected periodic tick).
		// If err is non-nil and waitCtx is still healthy, it's a real connection problem.
		if err != nil && waitCtx.Err() == nil {
			slog.Error("cache listen: connection error", "err", err)
			return
		}
		if err := c.rebuild(ctx); err != nil {
			slog.Error("cache rebuild", "err", err)
		}
	}
}
