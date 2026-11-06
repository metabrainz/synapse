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

const (
	tenantStreamPath = "/v1/stream/tenants/" + testTenantID + "/notifications"
	globalStreamPath = "/v1/stream/notifications"
)

// token grants an OAuth token for user and returns it.
func (e *env) token(user string) string {
	e.stub.grant("tok-"+user, user)
	return "tok-" + user
}

func (e *env) openStream(path, origin, token string) (*http.Response, *bufio.Reader) {
	e.t.Helper()
	return e.streamRequest("GET", path, origin, token)
}

func (e *env) streamRequest(method, path, origin, token string) (*http.Response, *bufio.Reader) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(e.ctx, 10*time.Second)
	e.t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, method, e.server.URL+path, nil)
	if err != nil {
		e.t.Fatalf("build request: %v", err)
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("open stream: %v", err)
	}
	e.t.Cleanup(func() { resp.Body.Close() })
	return resp, bufio.NewReader(resp.Body)
}

// openOK opens a stream, asserts 200 and consumes the leading retry frame.
func (e *env) openOK(path, token string) *bufio.Reader {
	e.t.Helper()
	resp, rd := e.openStream(path, "", token)
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
	resp, rd := e.openStream(tenantStreamPath, "", e.token("alice"))
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
	rd := e.openOK(tenantStreamPath, e.token("alice"))

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
	resp, _ := e.openStream(tenantStreamPath, "", e.token("alice"))
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
	tenantRd := e.openOK(tenantStreamPath, e.token("alice"))
	globalRd := e.openOK(globalStreamPath, e.token("alice"))

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

	for _, c := range []struct {
		name  string
		path  string
		token string
		want  int
	}{
		{"no token", tenantStreamPath, "", http.StatusUnauthorized},
		{"inactive token", tenantStreamPath, "tok-nobody", http.StatusUnauthorized},
		{"unknown tenant", "/v1/stream/tenants/nope/notifications", e.token("alice"), http.StatusNotFound},
		{"global, preference off", globalStreamPath, e.token("alice"), http.StatusForbidden},
	} {
		resp, _ := e.openStream(c.path, "", c.token)
		resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%s: want %d, got %d", c.name, c.want, resp.StatusCode)
		}
	}
}

// The page must be able to read a 401 to know it should refresh its token.
func TestStreamUnauthorizedHasCORS(t *testing.T) {
	e := setup(t)
	resp, _ := e.openStream(tenantStreamPath, lbOrigin, "")
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != lbOrigin {
		t.Fatalf("Access-Control-Allow-Origin: want %q, got %q", lbOrigin, got)
	}
}

func TestStreamIntrospectionDown(t *testing.T) {
	e := setup(t)
	resp, _ := e.openStream(tenantStreamPath, lbOrigin, "tok-down")
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != lbOrigin {
		t.Fatalf("Access-Control-Allow-Origin: want %q, got %q", lbOrigin, got)
	}
}

func TestStreamCORSPreflight(t *testing.T) {
	e := setup(t)
	preflight := func(path, origin string) *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(e.ctx, "OPTIONS", e.server.URL+path, nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set("Origin", origin)
		req.Header.Set("Access-Control-Request-Method", "GET")
		req.Header.Set("Access-Control-Request-Headers", "authorization")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("preflight: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("preflight %s from %s: want 204, got %d", path, origin, resp.StatusCode)
		}
		return resp
	}

	resp := preflight(tenantStreamPath, lbOrigin)
	for k, want := range map[string]string{
		"Access-Control-Allow-Origin":  lbOrigin,
		"Access-Control-Allow-Methods": "GET",
		"Access-Control-Allow-Headers": "Authorization",
		"Access-Control-Max-Age":       "600",
		"Vary":                         "Origin",
	} {
		if got := resp.Header.Get(k); got != want {
			t.Errorf("%s: want %q, got %q", k, want, got)
		}
	}

	if got := preflight(globalStreamPath, lbOrigin).Header.Get("Access-Control-Allow-Origin"); got != lbOrigin {
		t.Errorf("global preflight: want %q, got %q", lbOrigin, got)
	}

	resp = preflight(tenantStreamPath, "https://evil.example")
	for _, k := range []string{"Access-Control-Allow-Origin", "Access-Control-Allow-Headers"} {
		if got := resp.Header.Get(k); got != "" {
			t.Errorf("disallowed origin: want no %s, got %q", k, got)
		}
	}
}

func TestStreamCORS(t *testing.T) {
	e := setup(t)

	resp, _ := e.openStream(tenantStreamPath, lbOrigin, e.token("alice"))
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != lbOrigin {
		t.Fatalf("allowed origin: want %q, got %q", lbOrigin, got)
	}
	if got := resp.Header.Get("Vary"); got != "Origin" {
		t.Fatalf("Vary: want Origin, got %q", got)
	}
	if got := resp.Header.Get("Access-Control-Allow-Credentials"); got != "" {
		t.Fatalf("credentials must not be allowed, got %q", got)
	}

	resp, _ = e.openStream(tenantStreamPath, "https://evil.example", e.token("alice"))
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("disallowed origin: want no header, got %q", got)
	}
}

func TestStreamHubClosed(t *testing.T) {
	e := setup(t)
	e.hub.Close()

	resp, rd := e.openStream(tenantStreamPath, "", e.token("alice"))
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
	rd := e.openOK(tenantStreamPath, e.token("alice"))

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
