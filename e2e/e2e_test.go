//go:build integration

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/metabrainz/synapse/internal/api"
	"github.com/metabrainz/synapse/internal/broker/rabbitmq"
	"github.com/metabrainz/synapse/internal/config"
	"github.com/metabrainz/synapse/internal/dedup"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/channels"
	"github.com/metabrainz/synapse/internal/store/outbox"
	"github.com/metabrainz/synapse/internal/store/subscriptions"
	"github.com/metabrainz/synapse/internal/store/tenants"
	"github.com/metabrainz/synapse/testutil"
)

const adminKey = "test-admin-key"

// env holds everything a test needs.
type env struct {
	ctx     context.Context
	t       *testing.T
	server  *httptest.Server
	pool    *pgxpool.Pool
	amqpURL string
}

// setup spins up real infra, runs migrations, wires the full router.
func setup(t *testing.T) *env {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	pool := testutil.NewTestPool(ctx, t)
	// cancel is registered AFTER NewTestPool (which registers pool.Close), so it
	// runs FIRST at cleanup (LIFO). This unblocks the cache listen goroutine so
	// it releases its pool connection before pool.Close() waits on it.
	t.Cleanup(cancel)

	// Truncate all data tables so tests using a shared local DB don't bleed into each other.
	if _, err := pool.Exec(ctx,
		`TRUNCATE outbox, deliveries, subscriptions, channels, event_type_definitions, events, tenants RESTART IDENTITY CASCADE`,
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

	if err := rabbitmq.Setup(amqpURL); err != nil {
		t.Fatalf("rabbitmq topology: %v", err)
	}
	if err := purgeQueues(t, amqpURL); err != nil {
		t.Fatalf("purge queues: %v", err)
	}

	rdb, err := store.NewRedis(ctx, config.RedisConfig{Addr: rdbAddr})
	if err != nil {
		t.Fatalf("redis client: %v", err)
	}
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}

	tenantRepo := tenants.New(pool)
	channelRepo := channels.New(pool)
	subRepo := subscriptions.New(pool)

	cache := fanout.NewCache(pool, subRepo)
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("fanout cache: %v", err)
	}

	fan := fanout.New(cache)
	deduper := dedup.New(rdb)

	router := api.NewRouter(
		api.Config{AdminKey: adminKey},
		pool,
		tenantRepo,
		channelRepo,
		subRepo,
		fan,
		deduper,
	)

	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)

	return &env{ctx: ctx, t: t, server: srv, pool: pool, amqpURL: amqpURL}
}

// --- HTTP helpers ---

func (e *env) do(method, path string, body any, headers map[string]string) *http.Response {
	e.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		json.NewEncoder(&buf).Encode(body)
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

func (e *env) adminDo(method, path string, body any) *http.Response {
	return e.do(method, path, body, map[string]string{"X-Admin-Key": adminKey})
}

func (e *env) tenantDo(method, path string, body any, apiKey string) *http.Response {
	return e.do(method, path, body, map[string]string{"Authorization": "Bearer " + apiKey})
}

func decodeJSON(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

// --- Fixtures ---

// createTenant provisions a tenant and returns its ID and API key.
func (e *env) createTenant(id, name string) string {
	e.t.Helper()
	resp := e.adminDo("POST", "/v1/admin/tenants", map[string]string{"id": id, "name": name})
	if resp.StatusCode != http.StatusCreated {
		e.t.Fatalf("create tenant: status %d", resp.StatusCode)
	}
	var out map[string]string
	decodeJSON(e.t, resp, &out)
	return out["api_key"]
}

// registerEventType inserts an event_type_definition directly — no API endpoint yet.
func (e *env) registerEventType(tenantID, eventType string) {
	e.t.Helper()
	_, err := e.pool.Exec(e.ctx,
		`INSERT INTO event_type_definitions (tenant_id, event_type) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		tenantID, eventType,
	)
	if err != nil {
		e.t.Fatalf("register event type: %v", err)
	}
}

// createChannel creates a channel via the API and returns its ID.
func (e *env) createChannel(apiKey, userID, chanType string) int64 {
	e.t.Helper()
	resp := e.tenantDo("POST", fmt.Sprintf("/v1/users/%s/channels", userID),
		map[string]any{"type": chanType, "config": map[string]string{"url": "https://example.com/hook"}},
		apiKey,
	)
	if resp.StatusCode != http.StatusCreated {
		e.t.Fatalf("create channel: status %d", resp.StatusCode)
	}
	var out map[string]int64
	decodeJSON(e.t, resp, &out)
	return out["id"]
}

// purgeQueues drains all delivery queues so stale messages from a prior run
// don't interfere with the current test.
func purgeQueues(t *testing.T, amqpURL string) error {
	t.Helper()
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	for _, ct := range rabbitmq.ChannelTypes {
		if _, err := ch.QueuePurge("deliveries."+ct, false); err != nil {
			return err
		}
	}
	return nil
}

// createSubscription subscribes a channel to an event type via the API.
func (e *env) createSubscription(apiKey, userID string, channelID int64, eventType string) {
	e.t.Helper()
	resp := e.tenantDo("POST", fmt.Sprintf("/v1/users/%s/channels/%d/subscriptions", userID, channelID),
		map[string]string{"event_type": eventType},
		apiKey,
	)
	if resp.StatusCode != http.StatusCreated {
		e.t.Fatalf("create subscription: status %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// --- Tests ---

func TestIngestCreatesDBRows(t *testing.T) {
	e := setup(t)

	apiKey := e.createTenant("lb", "ListenBrainz")
	e.registerEventType("lb", "listen")
	channelID := e.createChannel(apiKey, "user-1", "webhook")
	e.createSubscription(apiKey, "user-1", channelID, "listen")

	// Give cache time to warm (LISTEN/NOTIFY or initial build).
	time.Sleep(100 * time.Millisecond)

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{
			"user_id":    "user-1",
			"event_type": "listen",
			"payload":    map[string]string{"track": "Pyramid Song"},
		},
		apiKey,
	)

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}
	var out map[string]any
	decodeJSON(t, resp, &out)
	if out["delivery_count"].(float64) != 1 {
		t.Fatalf("expected delivery_count=1, got %v", out["delivery_count"])
	}

	// Verify outbox has one row.
	msgs, err := outbox.FetchPending(e.ctx, e.pool, 10)
	if err != nil {
		t.Fatalf("fetch outbox: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 outbox row, got %d", len(msgs))
	}
	if msgs[0].RoutingKey != "webhook" {
		t.Fatalf("expected routing key 'webhook', got %q", msgs[0].RoutingKey)
	}
}

func TestIngestDedup(t *testing.T) {
	e := setup(t)

	apiKey := e.createTenant("mb", "MusicBrainz")
	e.registerEventType("mb", "edit.created")
	channelID := e.createChannel(apiKey, "user-2", "webhook")
	e.createSubscription(apiKey, "user-2", channelID, "edit.created")
	time.Sleep(100 * time.Millisecond)

	payload := map[string]any{
		"user_id":         "user-2",
		"event_type":      "edit.created",
		"payload":         map[string]string{"edit_id": "42"},
		"idempotency_key": "edit-42-v1",
	}

	resp1 := e.tenantDo("POST", "/v1/events", payload, apiKey)
	if resp1.StatusCode != http.StatusAccepted {
		t.Fatalf("first ingest: expected 202, got %d", resp1.StatusCode)
	}
	resp1.Body.Close()

	resp2 := e.tenantDo("POST", "/v1/events", payload, apiKey)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("second ingest: expected 200, got %d", resp2.StatusCode)
	}
	var out map[string]any
	decodeJSON(t, resp2, &out)
	if out["deduplicated"] != true {
		t.Fatalf("expected deduplicated=true, got %v", out["deduplicated"])
	}
}

func TestDryRun(t *testing.T) {
	e := setup(t)

	apiKey := e.createTenant("bb", "BookBrainz")
	e.registerEventType("bb", "review.created")
	channelID := e.createChannel(apiKey, "user-3", "webhook")
	e.createSubscription(apiKey, "user-3", channelID, "review.created")
	time.Sleep(100 * time.Millisecond)

	resp := e.tenantDo("POST", "/v1/events?dry_run=true",
		map[string]any{
			"user_id":    "user-3",
			"event_type": "review.created",
			"payload":    map[string]string{},
		},
		apiKey,
	)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var out map[string]any
	decodeJSON(t, resp, &out)
	if out["delivery_count"].(float64) != 1 {
		t.Fatalf("expected delivery_count=1, got %v", out["delivery_count"])
	}

	// Verify nothing was written to the DB.
	msgs, err := outbox.FetchPending(e.ctx, e.pool, 10)
	if err != nil {
		t.Fatalf("fetch outbox: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("dry_run wrote %d outbox rows, expected 0", len(msgs))
	}
}

func TestAuthRequired(t *testing.T) {
	e := setup(t)

	resp := e.do("POST", "/v1/events", map[string]any{"user_id": "x", "event_type": "y"}, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestRelayPublishesAndClearsOutbox(t *testing.T) {
	e := setup(t)

	apiKey := e.createTenant("lb2", "ListenBrainz2")
	e.registerEventType("lb2", "listen")
	channelID := e.createChannel(apiKey, "user-4", "webhook")
	e.createSubscription(apiKey, "user-4", channelID, "listen")
	time.Sleep(100 * time.Millisecond)

	// Ingest an event — creates one outbox row.
	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "user-4", "event_type": "listen", "payload": map[string]string{}},
		apiKey,
	)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest: expected 202, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Run one relay tick.
	pub, err := rabbitmq.New(e.amqpURL)
	if err != nil {
		t.Fatalf("relay publisher: %v", err)
	}
	defer pub.Close()

	if err := store.WithTx(e.ctx, e.pool, func(q store.Querier) error {
		msgs, err := outbox.FetchPending(e.ctx, q, 10)
		if err != nil {
			return err
		}
		for _, m := range msgs {
			if err := pub.Publish(e.ctx, m.RoutingKey, m.Payload); err != nil {
				return err
			}
		}
		ids := make([]int64, len(msgs))
		for i, m := range msgs {
			ids[i] = m.ID
		}
		return outbox.DeleteBatch(e.ctx, q, ids)
	}); err != nil {
		t.Fatalf("relay tick: %v", err)
	}

	// Outbox should be empty now.
	remaining, err := outbox.FetchPending(e.ctx, e.pool, 10)
	if err != nil {
		t.Fatalf("fetch outbox after relay: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected empty outbox, got %d rows", len(remaining))
	}

	// Consume one message from the webhook queue and verify its shape.
	conn, err := amqp.Dial(e.amqpURL)
	if err != nil {
		t.Fatalf("amqp dial: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("amqp channel: %v", err)
	}
	defer ch.Close()

	msg, ok, err := ch.Get("deliveries.webhook", true)
	if err != nil {
		t.Fatalf("amqp get: %v", err)
	}
	if !ok {
		t.Fatal("expected one message in deliveries.webhook queue, got none")
	}

	var wm fanout.WorkerMessage
	if err := json.Unmarshal(msg.Body, &wm); err != nil {
		t.Fatalf("unmarshal worker message: %v", err)
	}
	if wm.EventType != "listen" {
		t.Fatalf("expected event_type='listen', got %q", wm.EventType)
	}
	if wm.TenantID != "lb2" {
		t.Fatalf("expected tenant_id='lb2', got %q", wm.TenantID)
	}
}
