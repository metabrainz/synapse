//go:build integration

package e2e_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/metabrainz/synapse/internal/ingest"
	"github.com/metabrainz/synapse/internal/live"
)

func (e *env) subscribe(user string) <-chan live.Nudge {
	e.t.Helper()
	sub := e.rdb.Subscribe(e.ctx, live.Channel(user))
	e.t.Cleanup(func() { sub.Close() })
	if _, err := sub.Receive(e.ctx); err != nil {
		e.t.Fatalf("subscribe: %v", err)
	}
	out := make(chan live.Nudge, 16)
	go func() {
		for m := range sub.Channel() {
			var n live.Nudge
			if json.Unmarshal([]byte(m.Payload), &n) == nil {
				out <- n
			}
		}
	}()
	return out
}

func expectNudge(t *testing.T, ch <-chan live.Nudge) {
	t.Helper()
	select {
	case n := <-ch:
		if n != (live.Nudge{Kind: live.KindCreated, Tenant: testTenantID}) {
			t.Fatalf("unexpected nudge %+v", n)
		}
	case <-time.After(time.Second):
		t.Fatal("no nudge within 1s")
	}
}

func expectNoNudge(t *testing.T, ch <-chan live.Nudge) {
	t.Helper()
	select {
	case n := <-ch:
		t.Fatalf("unexpected nudge %+v", n)
	case <-time.After(300 * time.Millisecond):
	}
}

func followBody(recipients ...string) map[string]any {
	return map[string]any{
		"recipients": recipients,
		"event_type": "follow",
		"payload":    map[string]any{"actor": map[string]any{"username": "carol", "url": "https://listenbrainz.org/user/carol"}},
	}
}

func TestIngestPublishesCreatedOnce(t *testing.T) {
	e := setup(t)
	nudges := e.subscribe("alice")

	r := e.tenantDo("POST", "/v1/events", followBody("alice", "alice"), e.apiKey)
	r.Body.Close()
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest: want 202, got %d", r.StatusCode)
	}

	expectNudge(t, nudges)
	expectNoNudge(t, nudges)
}

func TestIngestNoNudgeForNonInApp(t *testing.T) {
	e := setup(t)
	nudges := e.subscribe("alice")

	r := e.tenantDo("POST", "/v1/events", map[string]any{
		"recipients": []string{"alice"},
		"event_type": "listen",
		"payload":    testListenPayload(),
	}, e.apiKey)
	r.Body.Close()
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest: want 202, got %d", r.StatusCode)
	}

	expectNoNudge(t, nudges)
}

func TestIngestDryRunAndDuplicateNoNudge(t *testing.T) {
	e := setup(t)
	nudges := e.subscribe("alice")

	r := e.tenantDo("POST", "/v1/events?dry_run=true", followBody("alice"), e.apiKey)
	r.Body.Close()
	expectNoNudge(t, nudges)

	body := followBody("alice")
	body["idempotency_key"] = "live-dup-1"
	r = e.tenantDo("POST", "/v1/events", body, e.apiKey)
	r.Body.Close()
	expectNudge(t, nudges)

	r = e.tenantDo("POST", "/v1/events", body, e.apiKey)
	r.Body.Close()
	if r.StatusCode != http.StatusOK {
		t.Fatalf("duplicate: want 200, got %d", r.StatusCode)
	}
	expectNoNudge(t, nudges)
}

func TestConsumerBatchOneNudgePerUser(t *testing.T) {
	e := setup(t)
	nudges := e.subscribe("alice")

	payload, _ := json.Marshal(followBody("alice")["payload"])
	for range 2 {
		publishIngestEvent(t, e.amqpURL, ingest.Message{
			TenantID:   testTenantID,
			Recipients: []string{"alice"},
			EventType:  "follow",
			Payload:    payload,
		})
	}
	// Both messages must land in one batch for the dedup to apply; the 50ms drain window covers back-to-back publishes.
	e.startIngestConsumer()

	expectNudge(t, nudges)
	expectNoNudge(t, nudges)
}
