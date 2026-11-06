//go:build integration

package e2e_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/metabrainz/synapse/internal/live"
)

const lbOrigin = "https://listenbrainz.org"

// ticket mints a stream ticket for user over the OAuth route.
func (e *env) ticket(user string) string {
	e.t.Helper()
	e.stub.grant("tok-"+user, user)
	r := e.userDo("POST", "/v1/me/notifications/stream-ticket", nil, "tok-"+user)
	var out struct {
		Ticket string `json:"ticket"`
	}
	decodeJSON(e.t, r, &out)
	if r.StatusCode != http.StatusOK {
		e.t.Fatalf("stream ticket: want 200, got %d", r.StatusCode)
	}
	return out.Ticket
}

func tenantStream(ticket string) string {
	return "/v1/stream/tenants/" + testTenantID + "/notifications?ticket=" + ticket
}

func (e *env) openStream(path, origin string) (*http.Response, *bufio.Reader) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(e.ctx, 10*time.Second)
	e.t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, "GET", e.server.URL+path, nil)
	if err != nil {
		e.t.Fatalf("build request: %v", err)
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("open stream: %v", err)
	}
	e.t.Cleanup(func() { resp.Body.Close() })
	return resp, bufio.NewReader(resp.Body)
}

// openOK opens a stream, asserts 200 and consumes the leading retry frame.
func (e *env) openOK(path string) *bufio.Reader {
	e.t.Helper()
	resp, rd := e.openStream(path, "")
	if resp.StatusCode != http.StatusOK {
		e.t.Fatalf("open %s: want 200, got %d", path, resp.StatusCode)
	}
	if frame := readFrame(e.t, rd); frame != "retry: 5000\n" {
		e.t.Fatalf("first frame: want retry, got %q", frame)
	}
	return rd
}

// readFrame returns one SSE frame without its terminating blank line.
func readFrame(t *testing.T, rd *bufio.Reader) string {
	t.Helper()
	var b strings.Builder
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			t.Fatalf("read stream: %v (partial %q)", err, b.String())
		}
		if line == "\n" {
			return b.String()
		}
		b.WriteString(line)
	}
}

func nextEvent(t *testing.T, rd *bufio.Reader) (name, data string) {
	t.Helper()
	for line := range strings.Lines(readFrame(t, rd)) {
		k, v, _ := strings.Cut(strings.TrimSuffix(line, "\n"), ": ")
		switch k {
		case "event":
			name = v
		case "data":
			data = v
		}
	}
	return name, data
}

func nextInbox(t *testing.T, rd *bufio.Reader) live.Nudge {
	t.Helper()
	for {
		name, data := nextEvent(t, rd)
		if name == "ping" {
			continue
		}
		if name != "inbox" {
			t.Fatalf("want inbox event, got %q", name)
		}
		var n live.Nudge
		if err := json.Unmarshal([]byte(data), &n); err != nil {
			t.Fatalf("decode nudge %q: %v", data, err)
		}
		return n
	}
}

func TestStreamHeadersRetryPing(t *testing.T) {
	e := setup(t)
	resp, rd := e.openStream(tenantStream(e.ticket("alice")), "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	for k, want := range map[string]string{
		"Content-Type":      "text/event-stream",
		"Cache-Control":     "no-store",
		"X-Accel-Buffering": "no",
	} {
		if got := resp.Header.Get(k); got != want {
			t.Errorf("%s: want %q, got %q", k, want, got)
		}
	}
	if frame := readFrame(t, rd); frame != "retry: 5000\n" {
		t.Fatalf("first frame: want retry, got %q", frame)
	}
	if name, data := nextEvent(t, rd); name != "ping" || data != "{}" {
		t.Fatalf("want ping {}, got %q %q", name, data)
	}
}

func TestStreamSurvivesWriteTimeout(t *testing.T) {
	e := setup(t)
	rd := e.openOK(tenantStream(e.ticket("alice")))

	pings := 0
	for start := time.Now(); time.Since(start) < 2500*time.Millisecond; {
		if name, _ := nextEvent(t, rd); name == "ping" {
			pings++
		}
	}
	if pings < 10 {
		t.Fatalf("want a ping every 200ms past the 1s write timeout, got %d", pings)
	}
}

func TestStreamExpires(t *testing.T) {
	e := setup(t)
	resp, _ := e.openStream(tenantStream(e.ticket("alice")), "")
	start := time.Now()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if d := time.Since(start); d < 3*time.Second || d > 4500*time.Millisecond {
		t.Fatalf("want the stream to end after its 3s lifetime plus up to 600ms jitter, ended after %s", d)
	}
}

func TestStreamTenantFilter(t *testing.T) {
	e := setup(t)
	e.stub.grant("tok-alice", "alice")
	if got := e.setPreference("tok-alice", true); got != http.StatusOK {
		t.Fatalf("enable preference: want 200, got %d", got)
	}
	tenantRd := e.openOK(tenantStream(e.ticket("alice")))
	globalRd := e.openOK("/v1/stream/notifications?ticket=" + e.ticket("alice"))

	pub := live.NewPublisher(e.rdb)
	other := live.Nudge{Kind: live.KindCreated, Tenant: "bookbrainz"}
	mine := live.Nudge{Kind: live.KindCreated, Tenant: testTenantID}
	pub.Publish(e.ctx, "alice", other)
	pub.Publish(e.ctx, "alice", mine)

	if n := nextInbox(t, tenantRd); n != mine {
		t.Fatalf("tenant stream: want %+v, got %+v", mine, n)
	}
	for _, want := range []live.Nudge{other, mine} {
		if n := nextInbox(t, globalRd); n != want {
			t.Fatalf("global stream: want %+v, got %+v", want, n)
		}
	}
}

func TestStreamAuthErrors(t *testing.T) {
	e := setup(t)

	reused := e.ticket("alice")
	e.openOK(tenantStream(reused))

	for _, c := range []struct {
		name string
		path string
		want int
	}{
		{"no ticket", tenantStream(""), http.StatusUnauthorized},
		{"reused ticket", tenantStream(reused), http.StatusUnauthorized},
		{"unknown tenant", "/v1/stream/tenants/nope/notifications?ticket=" + e.ticket("alice"), http.StatusNotFound},
		{"global, preference off", "/v1/stream/notifications?ticket=" + e.ticket("alice"), http.StatusForbidden},
	} {
		resp, _ := e.openStream(c.path, "")
		resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%s: want %d, got %d", c.name, c.want, resp.StatusCode)
		}
	}
}

func TestStreamCORS(t *testing.T) {
	e := setup(t)

	resp, _ := e.openStream(tenantStream(e.ticket("alice")), lbOrigin)
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != lbOrigin {
		t.Fatalf("allowed origin: want %q, got %q", lbOrigin, got)
	}
	if got := resp.Header.Get("Vary"); got != "Origin" {
		t.Fatalf("Vary: want Origin, got %q", got)
	}
	if got := resp.Header.Get("Access-Control-Allow-Credentials"); got != "" {
		t.Fatalf("credentials must not be allowed, got %q", got)
	}

	resp, _ = e.openStream(tenantStream(e.ticket("alice")), "https://evil.example")
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("disallowed origin: want no header, got %q", got)
	}
}

func TestStreamHubClosed(t *testing.T) {
	e := setup(t)
	e.hub.Close()

	resp, rd := e.openStream(tenantStream(e.ticket("alice")), "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("want EOF, got %v", err)
	}
	if strings.Contains(string(body), "event: inbox") {
		t.Fatalf("closed hub must not deliver, got %q", body)
	}
}

// The spec's definition of done: a real ingest and a real mark-read reach an open stream.
func TestStreamEndToEnd(t *testing.T) {
	e := setup(t)
	rd := e.openOK(tenantStream(e.ticket("alice")))

	r := e.tenantDo("POST", "/v1/events", followBody("alice"), e.apiKey)
	r.Body.Close()
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest: want 202, got %d", r.StatusCode)
	}
	if n := nextInbox(t, rd); n != (live.Nudge{Kind: live.KindCreated, Tenant: testTenantID}) {
		t.Fatalf("want created, got %+v", n)
	}

	_, page := e.listNotifications("tok-alice", "")
	if len(page.Notifications) != 1 {
		t.Fatalf("want 1 notification, got %d", len(page.Notifications))
	}
	r = e.userDo("POST", fmt.Sprintf("/v1/me/tenants/%s/notifications/%d/read", testTenantID, page.Notifications[0].ID), nil, "tok-alice")
	r.Body.Close()
	if r.StatusCode != http.StatusNoContent {
		t.Fatalf("mark read: want 204, got %d", r.StatusCode)
	}
	if n := nextInbox(t, rd); n != (live.Nudge{Kind: live.KindChanged, Tenant: testTenantID}) {
		t.Fatalf("want changed, got %+v", n)
	}
}
