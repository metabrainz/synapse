//go:build integration

package e2e_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/metabrainz/synapse/internal/store/deliveries"
)

// pollDeliveryCount polls the DB until deliveries exist or 500ms elapse.
func pollDeliveryCount(e *env, eventID int64) int {
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		list, _ := deliveries.ListByEvent(e.ctx, e.pool, eventID)
		if len(list) > 0 {
			return len(list)
		}
		time.Sleep(50 * time.Millisecond)
	}
	list, _ := deliveries.ListByEvent(e.ctx, e.pool, eventID)
	return len(list)
}

// TestThreeGateFanout verifies Gates 2 and 3 must be open for a delivery.
// Gate 1 is now enforced by the static registry (webhook is allowed for listenbrainz/listen).
func TestThreeGateFanout(t *testing.T) {
	e := setup(t)

	apiKey := e.apiKey
	userID := "42"
	e.setupWebhookChannel(testTenantID, userID, "listen", "http://localhost:19999/sink")

	// Wait for the fanout cache to pick up the new subscription.
	waitFor(t, 2*time.Second, func() bool {
		resp := e.tenantDo("POST", "/v1/events?dry_run=true",
			map[string]any{"recipients": []string{userID}, "event_type": "listen", "payload": testListenPayload()},
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

	fireAndCount := func(t *testing.T) int {
		t.Helper()
		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"recipients": []string{userID},
			"event_type": "listen",
			"payload": map[string]any{
				"actor":     map[string]any{"username": "test-user", "url": "https://listenbrainz.org/user/test-user"},
				"listen":    map[string]any{"listened_at": 1779613826},
				"recording": map[string]any{"track_name": "Dope Shope", "artist_name": "Yo Yo Honey Singh"},
			},
		}, apiKey)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("expected 202, got %d", resp.StatusCode)
		}
		return pollDeliveryCount(e, parseEventID(t, resp))
	}

	t.Run("all gates open — delivery created", func(t *testing.T) {
		if n := fireAndCount(t); n != 1 {
			t.Fatalf("expected 1 delivery, got %d", n)
		}
	})

	t.Run("user mapping disabled — no delivery", func(t *testing.T) {
		e.mustExec(`UPDATE user_tenant_channel_mapping SET is_enabled = false
			WHERE user_id = $1 AND tenant_id = $2 AND channel_type = 'webhook'`, userID, testTenantID)
		defer e.mustExec(`UPDATE user_tenant_channel_mapping SET is_enabled = true
			WHERE user_id = $1 AND tenant_id = $2 AND channel_type = 'webhook'`, userID, testTenantID)

		waitFor(t, 2*time.Second, func() bool {
			resp := e.tenantDo("POST", "/v1/events?dry_run=true",
				map[string]any{"recipients": []string{userID}, "event_type": "listen", "payload": testListenPayload()},
				apiKey,
			)
			defer resp.Body.Close()
			var out map[string]any
			json.NewDecoder(resp.Body).Decode(&out)
			count, _ := out["delivery_count"].(float64)
			return count == 0
		})

		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"recipients": []string{userID}, "event_type": "listen",
			"payload": map[string]any{
				"actor":     map[string]any{"username": "test-user", "url": "https://listenbrainz.org/user/test-user"},
				"listen":    map[string]any{"listened_at": 1779613826},
				"recording": map[string]any{"track_name": "Dope Shope", "artist_name": "Yo Yo Honey Singh"},
			},
		}, apiKey)
		time.Sleep(100 * time.Millisecond)
		list, _ := deliveries.ListByEvent(e.ctx, e.pool, parseEventID(t, resp))
		if len(list) != 0 {
			t.Fatalf("expected 0 deliveries (user mapping disabled), got %d", len(list))
		}
	})

	t.Run("user subscription disabled — no delivery", func(t *testing.T) {
		e.mustExec(`UPDATE user_event_subscriptions SET is_enabled = false
			WHERE user_id = $1 AND tenant_id = $2 AND event_type = 'listen' AND channel_type = 'webhook'`,
			userID, testTenantID)
		defer e.mustExec(`UPDATE user_event_subscriptions SET is_enabled = true
			WHERE user_id = $1 AND tenant_id = $2 AND event_type = 'listen' AND channel_type = 'webhook'`,
			userID, testTenantID)

		waitFor(t, 2*time.Second, func() bool {
			resp := e.tenantDo("POST", "/v1/events?dry_run=true",
				map[string]any{"recipients": []string{userID}, "event_type": "listen", "payload": testListenPayload()},
				apiKey,
			)
			defer resp.Body.Close()
			var out map[string]any
			json.NewDecoder(resp.Body).Decode(&out)
			count, _ := out["delivery_count"].(float64)
			return count == 0
		})

		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"recipients": []string{userID}, "event_type": "listen",
			"payload": map[string]any{
				"actor":     map[string]any{"username": "test-user", "url": "https://listenbrainz.org/user/test-user"},
				"listen":    map[string]any{"listened_at": 1779613826},
				"recording": map[string]any{"track_name": "Dope Shope", "artist_name": "Yo Yo Honey Singh"},
			},
		}, apiKey)
		time.Sleep(100 * time.Millisecond)
		list, _ := deliveries.ListByEvent(e.ctx, e.pool, parseEventID(t, resp))
		if len(list) != 0 {
			t.Fatalf("expected 0 deliveries (user subscription disabled), got %d", len(list))
		}
	})
}

// TestSchemaValidation verifies the static registry rejects invalid payloads.
func TestSchemaValidation(t *testing.T) {
	e := setup(t)
	apiKey := e.apiKey // listenbrainz is in the static registry — no setup needed

	validPayload := map[string]any{
		"actor":     map[string]any{"username": "test-user", "url": "https://listenbrainz.org/user/test-user"},
		"listen":    map[string]any{"listened_at": 1779613826},
		"recording": map[string]any{"track_name": "Dope Shope", "artist_name": "Yo Yo Honey Singh"},
	}

	t.Run("valid listen payload — 202", func(t *testing.T) {
		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"recipients": []string{"1"}, "event_type": "listen", "payload": validPayload,
		}, apiKey)
		if resp.StatusCode != http.StatusAccepted {
			var body map[string]any
			decodeJSON(t, resp, &body)
			t.Fatalf("expected 202, got %d: %v", resp.StatusCode, body)
		}
		resp.Body.Close()
	})

	t.Run("missing listen.listened_at — 400", func(t *testing.T) {
		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"recipients": []string{"1"},
			"event_type": "listen",
			"payload": map[string]any{
				"actor":     map[string]any{"username": "u", "url": "https://listenbrainz.org/user/u"},
				"listen":    map[string]any{},
				"recording": map[string]any{"track_name": "Dope Shope", "artist_name": "Yo Yo Honey Singh"},
			},
		}, apiKey)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 for missing listen.listened_at, got %d", resp.StatusCode)
		}
	})

	t.Run("missing recording.track_name — 400", func(t *testing.T) {
		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"recipients": []string{"1"},
			"event_type": "listen",
			"payload": map[string]any{
				"actor":     map[string]any{"username": "u", "url": "https://listenbrainz.org/user/u"},
				"listen":    map[string]any{"listened_at": 1779613826},
				"recording": map[string]any{"artist_name": "Yo Yo Honey Singh"},
			},
		}, apiKey)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 for missing recording.track_name, got %d", resp.StatusCode)
		}
	})

	t.Run("unknown event type — 400", func(t *testing.T) {
		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"recipients": []string{"1"}, "event_type": "not_registered", "payload": testListenPayload(),
		}, apiKey)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 for unknown event type, got %d", resp.StatusCode)
		}
	})

	t.Run("full real-world listen payload — 202", func(t *testing.T) {
		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"recipients": []string{"1"},
			"event_type": "listen",
			"payload": map[string]any{
				"actor": map[string]any{
					"username": "yo-yo-fan",
					"url":      "https://listenbrainz.org/user/yo-yo-fan",
				},
				"listen": map[string]any{
					"listened_at":       1779613826,
					"recording_msid":    "cbc6f6aa-b073-48db-8a2e-6dd69a428b12",
					"music_service":     "spotify.com",
					"duration_ms":       193608,
					"submission_client": "listenbrainz-player",
				},
				"recording": map[string]any{
					"track_name":   "Dope Shope",
					"artist_name":  "Yo Yo Honey Singh, Deep Money",
					"release_name": "International Villager",
				},
			},
		}, apiKey)
		if resp.StatusCode != http.StatusAccepted {
			var body map[string]any
			decodeJSON(t, resp, &body)
			t.Fatalf("expected 202 for full payload, got %d: %v", resp.StatusCode, body)
		}
		resp.Body.Close()
	})
}
