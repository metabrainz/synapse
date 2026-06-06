//go:build integration

// Package e2e_test contains end-to-end integration tests that exercise the full
// Synapse pipeline: HTTP API → Postgres → outbox relay → RabbitMQ → worker → adapter.
// Run with: go test -tags integration ./e2e/...
package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/metabrainz/synapse/internal/adapter"
	"github.com/metabrainz/synapse/internal/api"
	"github.com/metabrainz/synapse/internal/broker"
	"github.com/metabrainz/synapse/internal/broker/rabbitmq"
	"github.com/metabrainz/synapse/internal/config"
	"github.com/metabrainz/synapse/internal/dedup"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/oauth"
	"github.com/metabrainz/synapse/internal/schema"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/outbox"
	"github.com/metabrainz/synapse/internal/store/subscriptions"
	"github.com/metabrainz/synapse/internal/store/userchannels"
	"github.com/metabrainz/synapse/internal/store/usereventsubs"
	"github.com/metabrainz/synapse/internal/store/users"
	"github.com/metabrainz/synapse/internal/store/usertenant"
	"github.com/metabrainz/synapse/internal/worker"
	"github.com/metabrainz/synapse/testutil"
)

const (
	testTenantID = "listenbrainz"
	testAPIKey   = "test-lb-api-key"
)

// env holds all live infrastructure a test needs to drive the full Synapse stack.
type env struct {
	ctx     context.Context
	t       *testing.T
	pool    *pgxpool.Pool
	server  *httptest.Server
	amqpURL string
	apiKey  string              // static tenant API key for testTenantID
	fan     *fanout.Fanout      // shared with the HTTP server — cache-backed in tests
	deduper *dedup.Deduper      // shared with workers — backed by the test Redis instance
	pub     *rabbitmq.Publisher // long-lived publisher used by relayTick
	stub    *stubIntrospector
}

// setup spins up real infrastructure (Postgres, Redis, RabbitMQ), runs migrations,
// wires the full HTTP router, and returns a ready env. Cleanup is registered via
// t.Cleanup in LIFO order: cancel → pool.Close → container teardown.
func setup(t *testing.T) *env {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	pool := testutil.NewTestPool(ctx, t)
	// cancel runs BEFORE pool.Close (LIFO) so the cache LISTEN goroutine releases
	// its connection before the pool waits on open connections during Close.
	t.Cleanup(cancel)

	// Truncate all surviving tables so tests that share a local Postgres instance don't bleed.
	if _, err := pool.Exec(ctx,
		`TRUNCATE outbox, deliveries, events,
		          user_event_subscriptions, user_tenant_channel_mapping,
		          user_channels, users RESTART IDENTITY CASCADE`,
	); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}

	rdbAddr, redisCleanup, err := testutil.StartRedis(ctx)
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(redisCleanup)

	amqpURL, amqpCleanup, err := testutil.StartRabbitMQ(ctx)
	if err != nil {
		t.Fatalf("start rabbitmq: %v", err)
	}
	t.Cleanup(amqpCleanup)

	// Delete queues before declaring topology to evict stale consumers from any
	// previous test run that targeted the same shared local RabbitMQ instance.
	if err := deleteQueues(amqpURL); err != nil {
		t.Fatalf("delete queues: %v", err)
	}
	if err := rabbitmq.Setup(amqpURL, adapter.ChannelTypes()); err != nil {
		t.Fatalf("rabbitmq topology: %v", err)
	}

	rdb, err := store.NewRedis(ctx, config.RedisConfig{Addr: rdbAddr})
	if err != nil {
		t.Fatalf("redis client: %v", err)
	}
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}

	// Build registry with a fixed test API key for the listenbrainz tenant.
	tenants := schema.KnownTenants
	for i := range tenants {
		if tenants[i].ID == testTenantID {
			tenants[i].APIKey = testAPIKey
		}
	}
	reg := schema.New(tenants)

	subRepo := subscriptions.New(pool)
	// "" → listenDSN derived from pool config; correct when there is no PgBouncer.
	cache := fanout.NewCache(pool, subRepo, "", reg)
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("fanout cache: %v", err)
	}

	if err := adapter.Build(ctx, adapter.Options{Redis: rdb}); err != nil {
		t.Fatalf("adapter.Build: %v", err)
	}

	fan := fanout.New(cache, adapter.MaxAttemptsFor, reg)
	deduper := dedup.New(rdb)

	stub := newStubIntrospector()

	router := api.NewRouter(
		api.Config{
			Introspector: stub,
		},
		pool,
		rdb,
		users.New(pool),
		userchannels.New(pool),
		usertenant.New(pool),
		usereventsubs.New(pool),
		fan,
		deduper,
		reg,
	)

	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)

	pub, err := rabbitmq.New(amqpURL)
	if err != nil {
		t.Fatalf("relay publisher: %v", err)
	}
	t.Cleanup(func() { pub.Close() })

	return &env{
		ctx:     ctx,
		t:       t,
		pool:    pool,
		server:  srv,
		amqpURL: amqpURL,
		apiKey:  testAPIKey,
		fan:     fan,
		deduper: deduper,
		pub:     pub,
		stub:    stub,
	}
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

func (e *env) do(method, path string, body any, headers map[string]string) *http.Response {
	e.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			e.t.Fatalf("encode request body: %v", err)
		}
	}
	req, err := http.NewRequestWithContext(e.ctx, method, e.server.URL+path, &buf)
	if err != nil {
		e.t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("do request: %v", err)
	}
	return resp
}

func (e *env) tenantDo(method, path string, body any, apiKey string) *http.Response {
	return e.do(method, path, body, map[string]string{"Authorization": "Bearer " + apiKey})
}

// userDo makes an OAuth-authenticated request (Bearer token, /v1/me/* routes).
func (e *env) userDo(method, path string, body any, token string) *http.Response {
	return e.do(method, path, body, map[string]string{"Authorization": "Bearer " + token})
}

func decodeJSON(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode response (status %d): %v", resp.StatusCode, err)
	}
}

// ── Pipeline helpers ──────────────────────────────────────────────────────────

// relayTick runs one outbox flush: claim → PublishBatch (with confirms) → delete confirmed.
// Uses the real three-phase protocol so tests exercise the same code path as production.
func (e *env) relayTick() int {
	e.t.Helper()

	msgs, err := outbox.ClaimBatch(e.ctx, e.pool, "test-relay", 100)
	if err != nil {
		e.t.Fatalf("relayTick: claim: %v", err)
	}
	if len(msgs) == 0 {
		return 0
	}

	batch := make([]broker.BatchMsg, len(msgs))
	for i, m := range msgs {
		batch[i] = broker.BatchMsg{ID: m.ID, RoutingKey: m.RoutingKey, Body: m.Payload}
	}
	confirmed, err := e.pub.PublishBatch(e.ctx, batch)
	if err != nil {
		e.t.Fatalf("relayTick: publish: %v", err)
	}
	if len(confirmed) > 0 {
		if err := outbox.DeleteClaimed(e.ctx, e.pool, confirmed, "test-relay"); err != nil {
			e.t.Fatalf("relayTick: delete claimed: %v", err)
		}
	}
	return len(confirmed)
}

// waitFor polls check every 50 ms until it returns true or timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// testListenPayload returns a minimal valid payload for the listenbrainz/listen event type.
func testListenPayload() map[string]any {
	return map[string]any{
		"actor": map[string]any{
			"username": "test-user",
			"url":      "https://listenbrainz.org/user/test-user",
		},
		"listen": map[string]any{
			"listened_at": time.Now().Unix(),
		},
		"recording": map[string]any{
			"track_name":  "Test Track",
			"artist_name": "Test Artist",
		},
	}
}

// waitForCacheWarm polls the dry_run endpoint until the in-memory fanout cache
// has picked up a subscription. Avoids fragile time.Sleep calls after setupChannel.
func (e *env) waitForCacheWarm(apiKey, userID, eventType string) {
	e.t.Helper()
	waitFor(e.t, 2*time.Second, func() bool {
		resp := e.tenantDo("POST", "/v1/events?dry_run=true",
			map[string]any{"recipients": []string{userID}, "event_type": eventType, "payload": testListenPayload()},
			apiKey,
		)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var out map[string]any
		json.NewDecoder(resp.Body).Decode(&out)
		count, _ := out["delivery_count"].(float64)
		return count > 0
	})
}

// startWorker launches worker.Run in a goroutine. Concurrency and prefetch are
// both 1 — enough for tests. t.Cleanup cancels the context and drains the goroutine
// so no stale AMQP consumers leak into subsequent tests.
func (e *env) startWorker(channelType string, ad adapter.Adapter) context.CancelFunc {
	e.t.Helper()
	ctx, cancel := context.WithCancel(e.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		worker.Run(ctx, channelType, 1, 1, e.amqpURL, ad, e.deduper, e.pool)
	}()
	e.t.Cleanup(func() { cancel(); <-done })
	return cancel
}

// deleteQueues removes all delivery and ingest queues to evict stale consumer
// connections. Each queue gets its own AMQP connection so a NOT_FOUND error
// on one queue doesn't abort the rest.
func deleteQueues(amqpURL string) error {
	var queues []string
	for _, ct := range adapter.ChannelTypes() {
		s := string(ct)
		queues = append(queues,
			"deliveries."+s,
			"deliveries."+s+".retry",
			"deliveries.dead."+s,
		)
	}
	queues = append(queues, rabbitmq.QueueIngest)

	for _, q := range queues {
		conn, err := amqp.Dial(amqpURL)
		if err != nil {
			return err
		}
		ch, err := conn.Channel()
		if err != nil {
			conn.Close()
			return err
		}
		ch.QueueDelete(q, false, false, false) // ignore NOT_FOUND
		conn.Close()
	}
	return nil
}

// setupChannel seeds all routing gates (Gates 2 and 3) for one (user, tenant, channelType, eventType)
// combination directly in the DB. Gate 1 is enforced by the static registry.
// Returns the user_channel ID.
func (e *env) setupChannel(tenantID, userID, channelType, eventType, channelURL string) int64 {
	e.t.Helper()
	e.mustExec(`INSERT INTO users (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`, userID)

	chanConfig, err := json.Marshal(map[string]string{"url": channelURL})
	if err != nil {
		e.t.Fatalf("setupChannel: marshal config: %v", err)
	}

	var channelID int64
	if err := e.pool.QueryRow(e.ctx,
		`INSERT INTO user_channels (user_id, channel_type, label, config)
		 VALUES ($1, $2, 'Test', $3) RETURNING id`,
		userID, channelType, chanConfig,
	).Scan(&channelID); err != nil {
		e.t.Fatalf("setupChannel: insert user_channel: %v", err)
	}

	e.mustExec(
		`INSERT INTO user_tenant_channel_mapping
		     (user_id, tenant_id, channel_type, user_channel_id, is_enabled)
		 VALUES ($1, $2, $3, $4, true) ON CONFLICT DO NOTHING`,
		userID, tenantID, channelType, channelID,
	)
	e.mustExec(
		`INSERT INTO user_event_subscriptions
		     (user_id, tenant_id, event_type, channel_type, is_enabled)
		 VALUES ($1, $2, $3, $4, true) ON CONFLICT DO NOTHING`,
		userID, tenantID, eventType, channelType,
	)
	return channelID
}

// setupWebhookChannel is a convenience wrapper around setupChannel for webhook channels.
func (e *env) setupWebhookChannel(tenantID, userID, eventType, webhookURL string) int64 {
	return e.setupChannel(tenantID, userID, "webhook", eventType, webhookURL)
}

func (e *env) mustExec(query string, args ...any) {
	e.t.Helper()
	if _, err := e.pool.Exec(e.ctx, query, args...); err != nil {
		e.t.Fatalf("mustExec: %v\nquery: %s", err, query)
	}
}

// parseEventID extracts event_id from a 202 ingest response.
func parseEventID(t *testing.T, resp *http.Response) int64 {
	t.Helper()
	var out map[string]any
	decodeJSON(t, resp, &out)
	v, ok := out["event_id"].(float64)
	if !ok {
		t.Fatalf("parseEventID: no event_id in response: %v", out)
	}
	return int64(v)
}

// stubIntrospector implements oauth.Introspector for e2e tests.
// Register tokens with stub.grant(token, id) before making requests.
type stubIntrospector struct {
	mu     sync.RWMutex
	tokens map[string]oauth.Claims
}

func newStubIntrospector() *stubIntrospector {
	return &stubIntrospector{tokens: make(map[string]oauth.Claims)}
}

func (s *stubIntrospector) grant(token, id string) {
	s.mu.Lock()
	s.tokens[token] = oauth.Claims{ID: id}
	s.mu.Unlock()
}

func (s *stubIntrospector) Introspect(_ context.Context, token string) (oauth.Claims, error) {
	s.mu.RLock()
	claims, ok := s.tokens[token]
	s.mu.RUnlock()
	if !ok {
		return oauth.Claims{}, oauth.ErrInactive
	}
	return claims, nil
}

var _ oauth.Introspector = (*stubIntrospector)(nil)

// adapterFunc lets tests implement adapter.Adapter inline with a closure.
type adapterFunc func(context.Context, fanout.WorkerMessage) error

func (f adapterFunc) Deliver(ctx context.Context, msg fanout.WorkerMessage) error { return f(ctx, msg) }
func (adapterFunc) MaxAttempts() int                                              { return 5 }

var _ adapter.Adapter = adapterFunc(nil)
