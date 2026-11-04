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
