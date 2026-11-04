//go:build integration

package e2e_test

import (
	"fmt"
	"net/http"
	"testing"
)

type notificationPage struct {
	Notifications []struct {
		ID        int64  `json:"id"`
		TenantID  string `json:"tenant_id"`
		EventType string `json:"event_type"`
	} `json:"notifications"`
	NextCursor *int64 `json:"next_cursor"`
}

func (e *env) listNotifications(token, query string) (int, notificationPage) {
	e.t.Helper()
	resp := e.userDo("GET", "/v1/me/tenants/"+testTenantID+"/notifications"+query, nil, token)
	var page notificationPage
	status := resp.StatusCode
	if status == http.StatusOK {
		decodeJSON(e.t, resp, &page)
	} else {
		resp.Body.Close()
	}
	return status, page
}

func (e *env) unreadCount(token string) int {
	e.t.Helper()
	resp := e.userDo("GET", "/v1/me/tenants/"+testTenantID+"/notifications/unread-count", nil, token)
	var out struct {
		Count int `json:"count"`
	}
	decodeJSON(e.t, resp, &out)
	return out.Count
}

func TestInAppNotifications(t *testing.T) {
	e := setup(t)

	const alice, bob = "alice", "bob"
	e.stub.grant("tok-alice", alice)
	e.stub.grant("tok-bob", bob)
	base := "/v1/me/tenants/" + testTenantID

	// Alice has never authenticated, so she has no users row yet: in-app must not need one.
	const n = 3
	for range n {
		r := e.tenantDo("POST", "/v1/events", map[string]any{
			"recipients": []string{alice},
			"event_type": "follow", // listen is deliberately not in-app: too high-volume for an inbox
			"payload":    map[string]any{"actor": map[string]any{"username": "carol", "url": "https://listenbrainz.org/user/carol"}},
		}, e.apiKey)
		r.Body.Close()
		if r.StatusCode != http.StatusAccepted {
			t.Fatalf("ingest: want 202, got %d", r.StatusCode)
		}
	}

	if _, page := e.listNotifications("tok-alice", ""); len(page.Notifications) != n {
		t.Fatalf("want %d notifications visible right after ingest, got %d", n, len(page.Notifications))
	}

	var outbox int
	e.pool.QueryRow(e.ctx, `SELECT count(*) FROM outbox`).Scan(&outbox)
	if outbox != 0 {
		t.Fatalf("in-app must bypass the outbox, found %d outbox rows", outbox)
	}

	t.Run("pagination is newest-first with cursor", func(t *testing.T) {
		_, p1 := e.listNotifications("tok-alice", "?limit=2")
		if len(p1.Notifications) != 2 || p1.NextCursor == nil {
			t.Fatalf("page 1: want 2 items + cursor, got %d items, cursor=%v", len(p1.Notifications), p1.NextCursor)
		}
		if p1.Notifications[0].ID <= p1.Notifications[1].ID {
			t.Fatalf("want descending ids, got %d then %d", p1.Notifications[0].ID, p1.Notifications[1].ID)
		}
		_, p2 := e.listNotifications("tok-alice", fmt.Sprintf("?limit=2&before_id=%d", *p1.NextCursor))
		if len(p2.Notifications) != 1 || p2.NextCursor != nil {
			t.Fatalf("page 2: want 1 item and no cursor, got %d items, cursor=%v", len(p2.Notifications), p2.NextCursor)
		}
	})

	t.Run("read state", func(t *testing.T) {
		if got := e.unreadCount("tok-alice"); got != n {
			t.Fatalf("unread: want %d, got %d", n, got)
		}
		_, page := e.listNotifications("tok-alice", "")
		id := page.Notifications[0].ID

		for range 2 {
			r := e.userDo("POST", fmt.Sprintf("%s/notifications/%d/read", base, id), nil, "tok-alice")
			r.Body.Close()
			if r.StatusCode != http.StatusNoContent {
				t.Fatalf("mark read: want 204, got %d", r.StatusCode)
			}
		}
		if got := e.unreadCount("tok-alice"); got != n-1 {
			t.Fatalf("unread after one read: want %d, got %d", n-1, got)
		}
		_, unread := e.listNotifications("tok-alice", "?unread=true")
		if len(unread.Notifications) != n-1 {
			t.Fatalf("unread filter: want %d, got %d", n-1, len(unread.Notifications))
		}
	})

	t.Run("event_type filter", func(t *testing.T) {
		_, page := e.listNotifications("tok-alice", "?event_type=follow")
		if len(page.Notifications) == 0 {
			t.Fatal("want follow notifications, got none")
		}
		_, page = e.listNotifications("tok-alice", "?event_type=thanks")
		if len(page.Notifications) != 0 {
			t.Fatalf("want 0 thanks notifications, got %d", len(page.Notifications))
		}
		if status, _ := e.listNotifications("tok-alice", "?event_type=bogus"); status != http.StatusBadRequest {
			t.Fatalf("unknown event_type: want 400, got %d", status)
		}
	})

	t.Run("isolated per user", func(t *testing.T) {
		_, page := e.listNotifications("tok-bob", "")
		if len(page.Notifications) != 0 {
			t.Fatalf("bob sees %d of alice's notifications", len(page.Notifications))
		}
		_, alicePage := e.listNotifications("tok-alice", "")
		r := e.userDo("DELETE", fmt.Sprintf("%s/notifications/%d", base, alicePage.Notifications[0].ID), nil, "tok-bob")
		r.Body.Close()
		if r.StatusCode != http.StatusNotFound {
			t.Fatalf("bob dismissing alice's notification: want 404, got %d", r.StatusCode)
		}
	})

	t.Run("read-all then dismiss", func(t *testing.T) {
		r := e.userDo("POST", base+"/notifications/read-all", nil, "tok-alice")
		var out struct {
			Updated int `json:"updated"`
		}
		decodeJSON(t, r, &out)
		if out.Updated != n-1 {
			t.Fatalf("read-all updated: want %d, got %d", n-1, out.Updated)
		}
		if got := e.unreadCount("tok-alice"); got != 0 {
			t.Fatalf("unread after read-all: want 0, got %d", got)
		}

		_, page := e.listNotifications("tok-alice", "")
		id := page.Notifications[0].ID
		for i, want := range []int{http.StatusNoContent, http.StatusNotFound} {
			r := e.userDo("DELETE", fmt.Sprintf("%s/notifications/%d", base, id), nil, "tok-alice")
			r.Body.Close()
			if r.StatusCode != want {
				t.Fatalf("dismiss attempt %d: want %d, got %d", i+1, want, r.StatusCode)
			}
		}
		_, after := e.listNotifications("tok-alice", "")
		if len(after.Notifications) != n-1 {
			t.Fatalf("after dismiss: want %d, got %d", n-1, len(after.Notifications))
		}
	})

	t.Run("expired notifications are hidden even before cleanup deletes them", func(t *testing.T) {
		_, page := e.listNotifications("tok-alice", "")
		if len(page.Notifications) == 0 {
			t.Fatal("setup: want a visible notification")
		}
		id := page.Notifications[0].ID
		before := e.unreadCount("tok-alice")
		if _, err := e.pool.Exec(e.ctx, `UPDATE notifications SET expires_at = now() - interval '1 second', read_at = NULL WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}

		_, after := e.listNotifications("tok-alice", "")
		if len(after.Notifications) != len(page.Notifications)-1 {
			t.Fatalf("list: want %d after expiry, got %d", len(page.Notifications)-1, len(after.Notifications))
		}
		if got := e.unreadCount("tok-alice"); got != before {
			t.Fatalf("unread count: expired unread notification still counted (was %d, now %d)", before, got)
		}
		for name, r := range map[string]*http.Response{
			"read":    e.userDo("POST", fmt.Sprintf("%s/notifications/%d/read", base, id), nil, "tok-alice"),
			"dismiss": e.userDo("DELETE", fmt.Sprintf("%s/notifications/%d", base, id), nil, "tok-alice"),
		} {
			r.Body.Close()
			if r.StatusCode != http.StatusNotFound {
				t.Errorf("%s of an expired notification: want 404, got %d", name, r.StatusCode)
			}
		}
	})

	t.Run("unknown tenant", func(t *testing.T) {
		r := e.userDo("GET", "/v1/me/tenants/nope/notifications", nil, "tok-alice")
		r.Body.Close()
		if r.StatusCode != http.StatusNotFound {
			t.Fatalf("want 404, got %d", r.StatusCode)
		}
	})
}

func (e *env) setPreference(token string, on bool) int {
	e.t.Helper()
	r := e.userDo("PATCH", "/v1/me/preferences", map[string]any{"cross_tenant_notifications": on}, token)
	r.Body.Close()
	return r.StatusCode
}

// seedTwoTenants creates two listenbrainz notifications and one from another tenant,
// and returns the id of the latter.
func (e *env) seedTwoTenants() int64 {
	e.t.Helper()
	for range 2 {
		r := e.tenantDo("POST", "/v1/events", map[string]any{
			"recipients": []string{"alice"},
			"event_type": "follow",
			"payload":    map[string]any{"actor": map[string]any{"username": "carol", "url": "https://listenbrainz.org/user/carol"}},
		}, e.apiKey)
		r.Body.Close()
	}
	e.mustExec(`INSERT INTO events (id, tenant_id, event_type, payload) VALUES (900001, 'critiquebrainz', 'follow', '{}')`)
	var id int64
	if err := e.pool.QueryRow(e.ctx,
		`INSERT INTO notifications (event_id, user_id, tenant_id, expires_at)
		 VALUES (900001, 'alice', 'critiquebrainz', now() + interval '1 day') RETURNING id`).Scan(&id); err != nil {
		e.t.Fatal(err)
	}
	return id
}

func TestInAppTenantRoutesIgnoreThePreference(t *testing.T) {
	e := setup(t)
	e.stub.grant("tok-alice", "alice")
	otherID := e.seedTwoTenants()
	if got := e.setPreference("tok-alice", true); got != http.StatusOK {
		t.Fatalf("enable preference: want 200, got %d", got)
	}
	base := "/v1/me/tenants/" + testTenantID + "/notifications"

	_, page := e.listNotifications("tok-alice", "")
	if len(page.Notifications) != 2 {
		t.Fatalf("a tenant list must stay scoped to its tenant, got %d notifications", len(page.Notifications))
	}
	if got := e.unreadCount("tok-alice"); got != 2 {
		t.Fatalf("a tenant unread count must stay scoped, want 2, got %d", got)
	}
	r := e.userDo("POST", fmt.Sprintf("%s/%d/read", base, otherID), nil, "tok-alice")
	r.Body.Close()
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("another tenant's notification through a tenant route: want 404, got %d", r.StatusCode)
	}
	ra := e.userDo("POST", base+"/read-all", nil, "tok-alice")
	var out struct {
		Updated int `json:"updated"`
	}
	decodeJSON(t, ra, &out)
	if out.Updated != 2 {
		t.Fatalf("a tenant's read-all must touch only its own notifications, want 2 updated, got %d", out.Updated)
	}
	var otherUnread bool
	e.pool.QueryRow(e.ctx, `SELECT read_at IS NULL FROM notifications WHERE id = $1`, otherID).Scan(&otherUnread)
	if !otherUnread {
		t.Fatal("a tenant's read-all marked another tenant's notification read")
	}
}

func TestInAppGlobalRoutes(t *testing.T) {
	e := setup(t)
	e.stub.grant("tok-alice", "alice")
	otherID := e.seedTwoTenants()
	all := "/v1/me/notifications"

	t.Run("forbidden until the preference is on", func(t *testing.T) {
		for name, r := range map[string]*http.Response{
			"list":      e.userDo("GET", all, nil, "tok-alice"),
			"counts":    e.userDo("GET", all+"/unread-counts", nil, "tok-alice"),
			"read-all":  e.userDo("POST", all+"/read-all", nil, "tok-alice"),
			"read":      e.userDo("POST", fmt.Sprintf("%s/%d/read", all, otherID), nil, "tok-alice"),
			"dismissal": e.userDo("DELETE", fmt.Sprintf("%s/%d", all, otherID), nil, "tok-alice"),
		} {
			r.Body.Close()
			if r.StatusCode != http.StatusForbidden {
				t.Errorf("%s: want 403, got %d", name, r.StatusCode)
			}
		}
	})

	if got := e.setPreference("tok-alice", true); got != http.StatusOK {
		t.Fatalf("enable preference: want 200, got %d", got)
	}

	t.Run("list and counts span tenants", func(t *testing.T) {
		r := e.userDo("GET", all, nil, "tok-alice")
		var page notificationPage
		decodeJSON(t, r, &page)
		tenants := map[string]int{}
		for _, n := range page.Notifications {
			tenants[n.TenantID]++
		}
		if tenants[testTenantID] != 2 || tenants["critiquebrainz"] != 1 {
			t.Fatalf("want 2 from %s and 1 from critiquebrainz, got %v", testTenantID, tenants)
		}
		r = e.userDo("GET", all+"?tenant_id="+testTenantID, nil, "tok-alice")
		decodeJSON(t, r, &page)
		if len(page.Notifications) != 2 {
			t.Fatalf("tenant_id filter: want only the 2 %s notifications, got %d", testTenantID, len(page.Notifications))
		}
		r = e.userDo("GET", all+"/unread-counts", nil, "tok-alice")
		var counts struct {
			Count    int            `json:"count"`
			ByTenant map[string]int `json:"by_tenant"`
		}
		decodeJSON(t, r, &counts)
		if counts.Count != 3 || counts.ByTenant[testTenantID] != 2 || counts.ByTenant["critiquebrainz"] != 1 {
			t.Fatalf("unread counts: want 3 (2 + 1), got %+v", counts)
		}
	})

	t.Run("mutations reach any tenant", func(t *testing.T) {
		r := e.userDo("POST", fmt.Sprintf("%s/%d/read", all, otherID), nil, "tok-alice")
		r.Body.Close()
		if r.StatusCode != http.StatusNoContent {
			t.Fatalf("mark another tenant's notification read: want 204, got %d", r.StatusCode)
		}
		ra := e.userDo("POST", all+"/read-all", nil, "tok-alice")
		var out struct {
			Updated int `json:"updated"`
		}
		decodeJSON(t, ra, &out)
		if out.Updated != 2 {
			t.Fatalf("global read-all: want the 2 remaining unread, got %d", out.Updated)
		}
		rd := e.userDo("DELETE", fmt.Sprintf("%s/%d", all, otherID), nil, "tok-alice")
		rd.Body.Close()
		if rd.StatusCode != http.StatusNoContent {
			t.Fatalf("dismiss: want 204, got %d", rd.StatusCode)
		}
		r2 := e.userDo("GET", all, nil, "tok-alice")
		var page notificationPage
		decodeJSON(t, r2, &page)
		for _, n := range page.Notifications {
			if n.TenantID == "critiquebrainz" {
				t.Fatalf("a dismissed notification must disappear, still listed: %+v", n)
			}
		}
	})

	t.Run("forbidden again once the preference is turned off", func(t *testing.T) {
		if got := e.setPreference("tok-alice", false); got != http.StatusOK {
			t.Fatalf("disable preference: want 200, got %d", got)
		}
		r := e.userDo("GET", all, nil, "tok-alice")
		r.Body.Close()
		if r.StatusCode != http.StatusForbidden {
			t.Fatalf("want 403, got %d", r.StatusCode)
		}
	})
}
