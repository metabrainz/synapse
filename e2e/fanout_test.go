//go:build integration

package e2e_test

import (
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

// TestThreeGateFanout verifies all three routing gates must be open for a delivery.
// Closing any single gate must suppress delivery.
func TestThreeGateFanout(t *testing.T) {
	e := setup(t)

	tenantID := "lb-fanout-test"
	apiKey := e.createTenant(tenantID, "LB Fanout")
	e.registerEventType(tenantID, "listen")

	userID := "42"
	e.setupWebhookChannel(tenantID, userID, "listen", "http://localhost:19999/sink")

	// Wait for the fanout cache to pick up the new subscription.
	waitFor(t, 2*time.Second, func() bool {
		resp := e.tenantDo("POST", "/v1/events?dry_run=true",
			map[string]any{"user_id": userID, "event_type": "listen", "payload": map[string]any{}},
			apiKey,
		)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var out map[string]any
		decodeJSON(t, resp, &out)
		count, _ := out["delivery_count"].(float64)
		return count > 0
	})

	fireAndCount := func(t *testing.T) int {
		t.Helper()
		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"user_id":    userID,
			"event_type": "listen",
			"payload": map[string]any{
					"listened_at":    1779613826,
					"track_metadata": map[string]any{"track_name": "Dope Shope", "artist_name": "Yo Yo Honey Singh"},
				},
		}, apiKey)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("expected 202, got %d", resp.StatusCode)
		}
		return pollDeliveryCount(e, parseEventID(t, resp))
	}

	t.Run("all gates open — delivery created", func(t *testing.T) {
		n := fireAndCount(t)
		if n != 1 {
			t.Fatalf("expected 1 delivery, got %d", n)
		}
	})

	t.Run("admin rule disabled — no delivery", func(t *testing.T) {
		e.mustExec(`UPDATE tenant_event_channel_rules SET is_allowed = false
			WHERE tenant_id = $1 AND event_type = 'listen' AND channel_type = 'webhook'`, tenantID)
		defer e.mustExec(`UPDATE tenant_event_channel_rules SET is_allowed = true
			WHERE tenant_id = $1 AND event_type = 'listen' AND channel_type = 'webhook'`, tenantID)

		waitFor(t, 2*time.Second, func() bool {
			resp := e.tenantDo("POST", "/v1/events?dry_run=true",
				map[string]any{"user_id": userID, "event_type": "listen", "payload": map[string]any{}},
				apiKey,
			)
			defer resp.Body.Close()
			var out map[string]any
			decodeJSON(t, resp, &out)
			count, _ := out["delivery_count"].(float64)
			return count == 0
		})

		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"user_id":    userID,
			"event_type": "listen",
			"payload": map[string]any{
					"listened_at":    1779613826,
					"track_metadata": map[string]any{"track_name": "Dope Shope", "artist_name": "Yo Yo Honey Singh"},
				},
		}, apiKey)
		eventID := parseEventID(t, resp)
		time.Sleep(100 * time.Millisecond)
		list, _ := deliveries.ListByEvent(e.ctx, e.pool, eventID)
		if len(list) != 0 {
			t.Fatalf("expected 0 deliveries (admin rule disabled), got %d", len(list))
		}
	})

	t.Run("user mapping disabled — no delivery", func(t *testing.T) {
		e.mustExec(`UPDATE user_tenant_channel_mapping SET is_enabled = false
			WHERE user_id = $1 AND tenant_id = $2 AND channel_type = 'webhook'`, userID, tenantID)
		defer e.mustExec(`UPDATE user_tenant_channel_mapping SET is_enabled = true
			WHERE user_id = $1 AND tenant_id = $2 AND channel_type = 'webhook'`, userID, tenantID)

		waitFor(t, 2*time.Second, func() bool {
			resp := e.tenantDo("POST", "/v1/events?dry_run=true",
				map[string]any{"user_id": userID, "event_type": "listen", "payload": map[string]any{}},
				apiKey,
			)
			defer resp.Body.Close()
			var out map[string]any
			decodeJSON(t, resp, &out)
			count, _ := out["delivery_count"].(float64)
			return count == 0
		})

		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"user_id":    userID,
			"event_type": "listen",
			"payload": map[string]any{
					"listened_at":    1779613826,
					"track_metadata": map[string]any{"track_name": "Dope Shope", "artist_name": "Yo Yo Honey Singh"},
				},
		}, apiKey)
		eventID := parseEventID(t, resp)
		time.Sleep(100 * time.Millisecond)
		list, _ := deliveries.ListByEvent(e.ctx, e.pool, eventID)
		if len(list) != 0 {
			t.Fatalf("expected 0 deliveries (user mapping disabled), got %d", len(list))
		}
	})

	t.Run("user subscription disabled — no delivery", func(t *testing.T) {
		e.mustExec(`UPDATE user_event_subscriptions SET is_enabled = false
			WHERE user_id = $1 AND tenant_id = $2 AND event_type = 'listen' AND channel_type = 'webhook'`,
			userID, tenantID)
		defer e.mustExec(`UPDATE user_event_subscriptions SET is_enabled = true
			WHERE user_id = $1 AND tenant_id = $2 AND event_type = 'listen' AND channel_type = 'webhook'`,
			userID, tenantID)

		waitFor(t, 2*time.Second, func() bool {
			resp := e.tenantDo("POST", "/v1/events?dry_run=true",
				map[string]any{"user_id": userID, "event_type": "listen", "payload": map[string]any{}},
				apiKey,
			)
			defer resp.Body.Close()
			var out map[string]any
			decodeJSON(t, resp, &out)
			count, _ := out["delivery_count"].(float64)
			return count == 0
		})

		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"user_id":    userID,
			"event_type": "listen",
			"payload": map[string]any{
					"listened_at":    1779613826,
					"track_metadata": map[string]any{"track_name": "Dope Shope", "artist_name": "Yo Yo Honey Singh"},
				},
		}, apiKey)
		eventID := parseEventID(t, resp)
		time.Sleep(100 * time.Millisecond)
		list, _ := deliveries.ListByEvent(e.ctx, e.pool, eventID)
		if len(list) != 0 {
			t.Fatalf("expected 0 deliveries (user subscription disabled), got %d", len(list))
		}
	})
}

// TestSchemaValidation verifies the static registry rejects invalid payloads.
func TestSchemaValidation(t *testing.T) {
	e := setup(t)
	tenantID := "listenbrainz"
	apiKey := e.createTenant(tenantID, "ListenBrainz")

	e.registerEventType(tenantID, "listen")

	validPayload := map[string]any{
		"listened_at": 1779613826,
		"track_metadata": map[string]any{
			"track_name":  "Dope Shope",
			"artist_name": "Yo Yo Honey Singh",
		},
	}

	t.Run("valid listen payload — 202", func(t *testing.T) {
		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"user_id": "1", "event_type": "listen", "payload": validPayload,
		}, apiKey)
		if resp.StatusCode != http.StatusAccepted {
			var body map[string]any
			decodeJSON(t, resp, &body)
			t.Fatalf("expected 202, got %d: %v", resp.StatusCode, body)
		}
		resp.Body.Close()
	})

	t.Run("missing listened_at — 400", func(t *testing.T) {
		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"user_id":    "1",
			"event_type": "listen",
			"payload": map[string]any{
				"track_metadata": map[string]any{"track_name": "Dope Shope", "artist_name": "Yo Yo Honey Singh"},
			},
		}, apiKey)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 for missing listened_at, got %d", resp.StatusCode)
		}
	})

	t.Run("missing track_name inside track_metadata — 400", func(t *testing.T) {
		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"user_id":    "1",
			"event_type": "listen",
			"payload": map[string]any{
				"listened_at":    1779613826,
				"track_metadata": map[string]any{"artist_name": "Yo Yo Honey Singh"},
			},
		}, apiKey)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 for missing track_name, got %d", resp.StatusCode)
		}
	})

	t.Run("full real-world payload with additional_info — 202", func(t *testing.T) {
		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"user_id":    "1",
			"event_type": "listen",
			"payload": map[string]any{
				"listened_at":    1779613826,
				"recording_msid": "cbc6f6aa-b073-48db-8a2e-6dd69a428b12",
				"track_metadata": map[string]any{
					"track_name":   "Dope Shope",
					"artist_name":  "Yo Yo Honey Singh, Deep Money",
					"release_name": "International Villager",
					"additional_info": map[string]any{
						"duration_ms":    193608,
						"music_service":  "spotify.com",
						"tracknumber":    5,
						"artist_names":   []string{"Yo Yo Honey Singh", "Deep Money"},
					},
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
