//go:build integration

package e2e_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestInAppIsAlwaysOn(t *testing.T) {
	e := setup(t)
	e.stub.grant("tok-alice", "alice")

	t.Run("not in the event-type catalog", func(t *testing.T) {
		resp := e.userDo("GET", "/v1/me/tenants/"+testTenantID+"/event-types", nil, "tok-alice")
		var items []struct {
			Name            string   `json:"name"`
			AllowedChannels []string `json:"allowed_channels"`
		}
		decodeJSON(t, resp, &items)
		if len(items) == 0 {
			t.Fatal("empty catalog")
		}
		for _, it := range items {
			for _, ch := range it.AllowedChannels {
				if ch == "in_app" {
					t.Fatalf("event %q lists in_app as a channel the user can subscribe to", it.Name)
				}
			}
		}
	})

	t.Run("is not a channel: cannot be created, assigned or subscribed to", func(t *testing.T) {
		for name, resp := range map[string]*http.Response{
			"create":    e.userDo("POST", "/v1/me/channels", map[string]any{"channel_type": "in_app"}, "tok-alice"),
			"assign":    e.userDo("PUT", "/v1/me/tenants/"+testTenantID+"/channels/in_app", map[string]any{"channel_id": 1}, "tok-alice"),
			"subscribe": e.userDo("PUT", "/v1/me/tenants/"+testTenantID+"/subscriptions/follow/in_app", nil, "tok-alice"),
		} {
			resp.Body.Close()
			if resp.StatusCode != http.StatusUnprocessableEntity {
				t.Errorf("%s: want 422, got %d", name, resp.StatusCode)
			}
		}
	})

	t.Run("dry-run counts one in-app result per recipient with no setup", func(t *testing.T) {
		resp := e.tenantDo("POST", "/v1/events?dry_run=true", map[string]any{
			"recipients": []string{"never-seen-1", "never-seen-2"},
			"event_type": "follow",
			"payload":    map[string]any{"actor": map[string]any{"username": "x", "url": "https://listenbrainz.org/user/x"}},
		}, e.apiKey)
		var out map[string]any
		decodeJSON(t, resp, &out)
		if got, _ := out["delivery_count"].(float64); got != 2 {
			t.Fatalf("want delivery_count=2, got %v", out["delivery_count"])
		}
	})

	t.Run("writes a notification, not a delivery, for every recipient", func(t *testing.T) {
		e.setupWebhookChannel(testTenantID, "has-webhook", "follow", "https://example.com/hook")
		body := map[string]any{
			"recipients": []string{"has-webhook", "never-seen"},
			"event_type": "follow",
			"payload":    map[string]any{"actor": map[string]any{"username": "x", "url": "https://listenbrainz.org/user/x"}},
		}
		waitFor(t, 2*time.Second, func() bool { // subscription cache warm: 2 notifications + 1 webhook
			resp := e.tenantDo("POST", "/v1/events?dry_run=true", body, e.apiKey)
			defer resp.Body.Close()
			var out map[string]any
			json.NewDecoder(resp.Body).Decode(&out)
			count, _ := out["delivery_count"].(float64)
			return count == 3
		})

		resp := e.tenantDo("POST", "/v1/events", body, e.apiKey)
		var out map[string]any
		decodeJSON(t, resp, &out)
		if got, _ := out["delivery_count"].(float64); got != 3 {
			t.Fatalf("want delivery_count=3 (2 notifications + 1 webhook), got %v", out["delivery_count"])
		}
		eventID := int64(out["event_id"].(float64))

		rows, err := e.pool.Query(e.ctx, `SELECT user_id, tenant_id, expires_at FROM notifications WHERE event_id = $1`, eventID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		got := map[string]bool{}
		for rows.Next() {
			var user, tenant string
			var expires time.Time
			if err := rows.Scan(&user, &tenant, &expires); err != nil {
				t.Fatal(err)
			}
			if tenant != testTenantID {
				t.Errorf("notification for %s has tenant %q", user, tenant)
			}
			if d := time.Until(expires); d < 6*24*time.Hour || d > 7*24*time.Hour {
				t.Errorf("notification for %s expires in %v, want about 7 days (the tenant's setting)", user, d)
			}
			got[user] = true
		}
		if !got["has-webhook"] || !got["never-seen"] || len(got) != 2 {
			t.Fatalf("want a notification for both recipients, got %v", got)
		}

		var inAppDeliveries, webhookDeliveries int
		if err := e.pool.QueryRow(e.ctx,
			`SELECT count(*) FILTER (WHERE channel_type = 'in_app'), count(*) FILTER (WHERE channel_type = 'webhook')
			 FROM deliveries WHERE event_id = $1`, eventID).Scan(&inAppDeliveries, &webhookDeliveries); err != nil {
			t.Fatal(err)
		}
		if inAppDeliveries != 0 || webhookDeliveries != 1 {
			t.Fatalf("want 0 in-app deliveries and 1 webhook delivery, got %d and %d", inAppDeliveries, webhookDeliveries)
		}
	})
}
