//go:build integration

package e2e_test

import (
	"encoding/json"
	"fmt"
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

func expectChanged(t *testing.T, ch <-chan live.Nudge) {
	t.Helper()
	select {
	case n := <-ch:
		if n != (live.Nudge{Kind: live.KindChanged, Tenant: testTenantID}) {
			t.Fatalf("unexpected nudge %+v", n)
		}
	case <-time.After(time.Second):
		t.Fatal("no nudge within 1s")
	}
}

func (e *env) seedFollows(user string, n int) []int64 {
	e.t.Helper()
	for range n {
		r := e.tenantDo("POST", "/v1/events", followBody(user), e.apiKey)
		r.Body.Close()
		if r.StatusCode != http.StatusAccepted {
			e.t.Fatalf("ingest: want 202, got %d", r.StatusCode)
		}
	}
	e.stub.grant("tok-"+user, user)
	_, page := e.listNotifications("tok-"+user, "")
	ids := make([]int64, len(page.Notifications))
	for i, n := range page.Notifications {
		ids[i] = n.ID
	}
	return ids
}

func TestMarkReadPublishesChanged(t *testing.T) {
	e := setup(t)
	ids := e.seedFollows("alice", 2)
	e.setPreference("tok-alice", true)
	nudges := e.subscribe("alice")

	for _, path := range []string{
		fmt.Sprintf("/v1/me/tenants/%s/notifications/%d/read", testTenantID, ids[0]),
		fmt.Sprintf("/v1/me/notifications/%d/read", ids[1]),
	} {
		r := e.userDo("POST", path, nil, "tok-alice")
		r.Body.Close()
		if r.StatusCode != http.StatusNoContent {
			t.Fatalf("%s: want 204, got %d", path, r.StatusCode)
		}
		expectChanged(t, nudges)
		expectNoNudge(t, nudges)
	}
}

func TestDismissPublishesChanged(t *testing.T) {
	e := setup(t)
	ids := e.seedFollows("alice", 2)
	e.setPreference("tok-alice", true)
	nudges := e.subscribe("alice")

	for _, path := range []string{
		fmt.Sprintf("/v1/me/tenants/%s/notifications/%d", testTenantID, ids[0]),
		fmt.Sprintf("/v1/me/notifications/%d", ids[1]),
	} {
		r := e.userDo("DELETE", path, nil, "tok-alice")
		r.Body.Close()
		if r.StatusCode != http.StatusNoContent {
			t.Fatalf("%s: want 204, got %d", path, r.StatusCode)
		}
		expectChanged(t, nudges)
		expectNoNudge(t, nudges)
	}
}

func TestReadAllPublishesPerTenant(t *testing.T) {
	e := setup(t)
	e.seedFollows("alice", 2)
	nudges := e.subscribe("alice")
	path := "/v1/me/tenants/" + testTenantID + "/notifications/read-all"

	for _, want := range []int64{2, 0} {
		r := e.userDo("POST", path, nil, "tok-alice")
		var out struct {
			Updated int64 `json:"updated"`
		}
		decodeJSON(t, r, &out)
		if out.Updated != want {
			t.Fatalf("updated: want %d, got %d", want, out.Updated)
		}
		if want > 0 {
			expectChanged(t, nudges)
		}
		expectNoNudge(t, nudges)
	}
}

func TestMutationNotFoundNoNudge(t *testing.T) {
	e := setup(t)
	e.seedFollows("alice", 1)
	bobIDs := e.seedFollows("bob", 1)
	nudges := e.subscribe("alice")
	base := "/v1/me/tenants/" + testTenantID + "/notifications"

	for _, id := range []int64{999999, bobIDs[0]} {
		for _, req := range [][2]string{
			{"POST", fmt.Sprintf("%s/%d/read", base, id)},
			{"DELETE", fmt.Sprintf("%s/%d", base, id)},
		} {
			r := e.userDo(req[0], req[1], nil, "tok-alice")
			r.Body.Close()
			if r.StatusCode != http.StatusNotFound {
				t.Fatalf("%s %s: want 404, got %d", req[0], req[1], r.StatusCode)
			}
		}
	}
	expectNoNudge(t, nudges)
}
