//go:build integration

package e2e_test

import (
	"fmt"
	"net/http"
	"testing"
)

func TestMeChannels(t *testing.T) {
	e := setup(t)

	const token = "user-token-alice"
	const userID = "alice"
	e.stub.grant(token, userID)

	t.Run("list empty", func(t *testing.T) {
		resp := e.userDo("GET", "/v1/me/channels", nil, token)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("want 200, got %d", resp.StatusCode)
		}
		var chans []map[string]any
		decodeJSON(t, resp, &chans)
		if len(chans) != 0 {
			t.Fatalf("want empty list, got %d items", len(chans))
		}
	})

	t.Run("create channel", func(t *testing.T) {
		resp := e.userDo("POST", "/v1/me/channels", map[string]any{
			"channel_type": "webhook",
			"label":        "my webhook",
			"config":       map[string]string{"url": "http://example.com/hook"},
		}, token)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("want 201, got %d", resp.StatusCode)
		}
		var out map[string]any
		decodeJSON(t, resp, &out)
		if _, ok := out["id"]; !ok {
			t.Fatalf("want id in response, got %v", out)
		}
	})

	t.Run("list after create", func(t *testing.T) {
		resp := e.userDo("GET", "/v1/me/channels", nil, token)
		var chans []map[string]any
		decodeJSON(t, resp, &chans)
		if len(chans) != 1 {
			t.Fatalf("want 1 channel, got %d", len(chans))
		}
		if chans[0]["channel_type"] != "webhook" {
			t.Fatalf("want channel_type=webhook, got %v", chans[0]["channel_type"])
		}
	})

	t.Run("create missing channel_type — 400", func(t *testing.T) {
		resp := e.userDo("POST", "/v1/me/channels", map[string]any{
			"label": "no type",
		}, token)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("want 400, got %d", resp.StatusCode)
		}
	})

	t.Run("delete channel", func(t *testing.T) {
		cr := e.userDo("POST", "/v1/me/channels", map[string]any{
			"channel_type": "webhook",
			"label":        "to delete",
			"config":       map[string]string{"url": "http://example.com/del"},
		}, token)
		var out map[string]any
		decodeJSON(t, cr, &out)
		id := int64(out["id"].(float64))

		del := e.userDo("DELETE", "/v1/me/channels/"+fmt.Sprintf("%d", id), nil, token)
		defer del.Body.Close()
		if del.StatusCode != http.StatusNoContent {
			t.Fatalf("want 204, got %d", del.StatusCode)
		}

		// Verify deletion.
		list := e.userDo("GET", "/v1/me/channels", nil, token)
		var remaining []map[string]any
		decodeJSON(t, list, &remaining)
		// The "create channel" subtest already ran and added 1 channel;
		// after this delete we should be back to 1 (the one from "create channel" subtest).
		// But subtests share state, so just verify the deleted channel's ID is not present.
		for _, ch := range remaining {
			if gotID := int64(ch["id"].(float64)); gotID == id {
				t.Fatalf("channel %d still present after delete", id)
			}
		}
	})

	t.Run("unauthorized without token — 401", func(t *testing.T) {
		resp := e.do("GET", "/v1/me/channels", nil, nil)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("want 401, got %d", resp.StatusCode)
		}
	})

	t.Run("user isolation — bob cannot see alice channels", func(t *testing.T) {
		const bobToken = "user-token-alice-isolation-bob"
		e.stub.grant(bobToken, "bob-isolation")

		resp := e.userDo("GET", "/v1/me/channels", nil, bobToken)
		var chans []map[string]any
		decodeJSON(t, resp, &chans)
		if len(chans) != 0 {
			t.Fatalf("bob should see 0 channels (not alice's), got %d", len(chans))
		}
	})
}

func TestMeTenantChannels(t *testing.T) {
	e := setup(t)

	const token = "user-token-bob"
	const userID = "bob"
	e.stub.grant(token, userID)

	tenantID := "lb-me-test"
	e.createTenant(tenantID, "LB Me Test")
	e.registerEventType(tenantID, "listen")
	e.registerChannelRule(tenantID, "listen", "webhook")

	// Create a user channel first.
	cr := e.userDo("POST", "/v1/me/channels", map[string]any{
		"channel_type": "webhook",
		"label":        "bob hook",
		"config":       map[string]string{"url": "http://localhost:19999/sink"},
	}, token)
	var out map[string]any
	decodeJSON(t, cr, &out)
	channelID := int64(out["id"].(float64))

	t.Run("assign channel to tenant", func(t *testing.T) {
		resp := e.userDo("PUT",
			"/v1/me/tenants/"+tenantID+"/channels/webhook",
			map[string]any{"channel_id": channelID},
			token,
		)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("want 204, got %d", resp.StatusCode)
		}
	})

	t.Run("list tenant channels — mapping present", func(t *testing.T) {
		resp := e.userDo("GET", "/v1/me/tenants/"+tenantID+"/channels", nil, token)
		var mappings []map[string]any
		decodeJSON(t, resp, &mappings)
		if len(mappings) != 1 {
			t.Fatalf("want 1 mapping, got %d", len(mappings))
		}
		if mappings[0]["channel_type"] != "webhook" {
			t.Fatalf("unexpected mapping: %v", mappings[0])
		}
	})

	t.Run("reassign replaces existing (radio-button)", func(t *testing.T) {
		cr2 := e.userDo("POST", "/v1/me/channels", map[string]any{
			"channel_type": "webhook",
			"label":        "bob hook 2",
			"config":       map[string]string{"url": "http://localhost:19999/sink2"},
		}, token)
		var out2 map[string]any
		decodeJSON(t, cr2, &out2)
		channelID2 := int64(out2["id"].(float64))

		resp := e.userDo("PUT",
			"/v1/me/tenants/"+tenantID+"/channels/webhook",
			map[string]any{"channel_id": channelID2},
			token,
		)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("want 204, got %d", resp.StatusCode)
		}

		list := e.userDo("GET", "/v1/me/tenants/"+tenantID+"/channels", nil, token)
		var mappings []map[string]any
		decodeJSON(t, list, &mappings)
		if len(mappings) != 1 {
			t.Fatalf("want 1 mapping after reassign, got %d", len(mappings))
		}
		gotID := int64(mappings[0]["user_channel_id"].(float64))
		if gotID != channelID2 {
			t.Fatalf("want user_channel_id=%d after reassign, got %d", channelID2, gotID)
		}
	})

	t.Run("remove tenant channel assignment", func(t *testing.T) {
		resp := e.userDo("DELETE",
			"/v1/me/tenants/"+tenantID+"/channels/webhook",
			nil, token,
		)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("want 204, got %d", resp.StatusCode)
		}

		list := e.userDo("GET", "/v1/me/tenants/"+tenantID+"/channels", nil, token)
		var mappings []map[string]any
		decodeJSON(t, list, &mappings)
		if len(mappings) != 0 {
			t.Fatalf("want 0 mappings after removal, got %d", len(mappings))
		}
	})
}

func TestMeSubscriptions(t *testing.T) {
	e := setup(t)

	const token = "user-token-carol"
	const userID = "carol"
	e.stub.grant(token, userID)

	tenantID := "lb-sub-test"
	e.createTenant(tenantID, "LB Sub Test")
	e.registerEventType(tenantID, "listen")

	t.Run("list empty subscriptions", func(t *testing.T) {
		resp := e.userDo("GET", "/v1/me/tenants/"+tenantID+"/subscriptions", nil, token)
		var subs []map[string]any
		decodeJSON(t, resp, &subs)
		if len(subs) != 0 {
			t.Fatalf("want 0 subs, got %d", len(subs))
		}
	})

	t.Run("subscribe", func(t *testing.T) {
		resp := e.userDo("PUT",
			"/v1/me/tenants/"+tenantID+"/subscriptions/listen/webhook",
			nil, token,
		)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("want 204, got %d", resp.StatusCode)
		}
	})

	t.Run("list after subscribe", func(t *testing.T) {
		resp := e.userDo("GET", "/v1/me/tenants/"+tenantID+"/subscriptions", nil, token)
		var subs []map[string]any
		decodeJSON(t, resp, &subs)
		if len(subs) != 1 {
			t.Fatalf("want 1 sub, got %d", len(subs))
		}
		if subs[0]["event_type"] != "listen" || subs[0]["channel_type"] != "webhook" {
			t.Fatalf("unexpected sub: %v", subs[0])
		}
		if subs[0]["is_enabled"] != true {
			t.Fatalf("want is_enabled=true, got %v", subs[0]["is_enabled"])
		}
	})

	t.Run("subscribe is idempotent", func(t *testing.T) {
		resp := e.userDo("PUT",
			"/v1/me/tenants/"+tenantID+"/subscriptions/listen/webhook",
			nil, token,
		)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("want 204 on idempotent subscribe, got %d", resp.StatusCode)
		}
	})

	t.Run("unsubscribe", func(t *testing.T) {
		resp := e.userDo("DELETE",
			"/v1/me/tenants/"+tenantID+"/subscriptions/listen/webhook",
			nil, token,
		)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("want 204, got %d", resp.StatusCode)
		}

		list := e.userDo("GET", "/v1/me/tenants/"+tenantID+"/subscriptions", nil, token)
		var subs []map[string]any
		decodeJSON(t, list, &subs)
		if len(subs) != 0 {
			t.Fatalf("want 0 subs after unsubscribe, got %d", len(subs))
		}
	})
}

func TestMeFullFlow(t *testing.T) {
	e := setup(t)

	const token = "user-token-dave"
	const userID = "dave"
	e.stub.grant(token, userID)

	tenantID := "lb-me-flow"
	apiKey := e.createTenant(tenantID, "LB Me Flow")
	e.registerEventType(tenantID, "listen")
	e.registerChannelRule(tenantID, "listen", "webhook") // Gate 1

	// Gates 2 + 3 via /v1/me/* APIs.
	cr := e.userDo("POST", "/v1/me/channels", map[string]any{
		"channel_type": "webhook",
		"label":        "dave hook",
		"config":       map[string]string{"url": "http://localhost:19999/sink"},
	}, token)
	var out map[string]any
	decodeJSON(t, cr, &out)
	channelID := int64(out["id"].(float64))

	assignResp := e.userDo("PUT",
		"/v1/me/tenants/"+tenantID+"/channels/webhook",
		map[string]any{"channel_id": channelID},
		token,
	)
	assignResp.Body.Close()
	if assignResp.StatusCode != http.StatusNoContent {
		t.Fatalf("assign: want 204, got %d", assignResp.StatusCode)
	}

	subResp := e.userDo("PUT",
		"/v1/me/tenants/"+tenantID+"/subscriptions/listen/webhook",
		nil, token,
	)
	subResp.Body.Close()
	if subResp.StatusCode != http.StatusNoContent {
		t.Fatalf("subscribe: want 204, got %d", subResp.StatusCode)
	}

	// Wait for fanout cache to pick up the new subscription.
	e.waitForCacheWarm(apiKey, userID, "listen")

	evResp := e.tenantDo("POST", "/v1/events", map[string]any{
		"user_id":    userID,
		"event_type": "listen",
		"payload": map[string]any{
			"listened_at":    1779613826,
			"track_metadata": map[string]any{"track_name": "Dope Shope", "artist_name": "Yo Yo Honey Singh"},
		},
	}, apiKey)
	defer evResp.Body.Close()
	if evResp.StatusCode != http.StatusAccepted {
		t.Fatalf("fire event: want 202, got %d", evResp.StatusCode)
	}
	n := pollDeliveryCount(e, parseEventID(t, evResp))
	if n != 1 {
		t.Fatalf("want 1 delivery, got %d", n)
	}
}
