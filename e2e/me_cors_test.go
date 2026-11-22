//go:build integration

package e2e_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/metabrainz/synapse/internal/api"
	"github.com/metabrainz/synapse/internal/oauth"
	"github.com/metabrainz/synapse/internal/store"
)

const (
	evilOrigin      = "https://evil.example"
	unreadCountPath = "/v1/me/tenants/" + testTenantID + "/notifications/unread-count"
)

func (e *env) meRequest(method, path, origin, token string) *http.Response {
	e.t.Helper()
	h := map[string]string{"Origin": origin}
	if token != "" {
		h["Authorization"] = "Bearer " + token
	}
	if method == http.MethodOptions {
		h["Access-Control-Request-Method"] = "POST"
		h["Access-Control-Request-Headers"] = "authorization,content-type"
	}
	resp := e.do(method, path, nil, h)
	resp.Body.Close()
	return resp
}

func wantStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		t.Fatalf("%s %s: want %d, got %d", resp.Request.Method, resp.Request.URL.Path, want, resp.StatusCode)
	}
}

func wantHeader(t *testing.T, resp *http.Response, key, want string) {
	t.Helper()
	if got := resp.Header.Get(key); got != want {
		t.Errorf("%s %s from %s: %s: want %q, got %q", resp.Request.Method, resp.Request.URL.Path, resp.Request.Header.Get("Origin"), key, want, got)
	}
}

func TestMeCORS(t *testing.T) {
	e := setup(t)
	tok := e.token("user-a")

	resp := e.meRequest("GET", unreadCountPath, lbOrigin, tok)
	wantStatus(t, resp, http.StatusOK)
	wantHeader(t, resp, "Access-Control-Allow-Origin", lbOrigin)
	wantHeader(t, resp, "Access-Control-Allow-Credentials", "")
	if !strings.Contains(strings.Join(resp.Header.Values("Vary"), ","), "Origin") {
		t.Errorf("Vary: want Origin, got %q", resp.Header.Values("Vary"))
	}

	resp = e.meRequest("GET", unreadCountPath, evilOrigin, tok)
	wantStatus(t, resp, http.StatusOK)
	wantHeader(t, resp, "Access-Control-Allow-Origin", "")
}

// The page must be able to read a 401 or 403 to know it should refresh or hide the bell.
func TestMeUnauthorizedHasCORS(t *testing.T) {
	e := setup(t)
	resp := e.meRequest("GET", unreadCountPath, lbOrigin, "")
	wantStatus(t, resp, http.StatusUnauthorized)
	wantHeader(t, resp, "Access-Control-Allow-Origin", lbOrigin)

	e.stub.grantClaims("tok-inbox", oauth.Claims{ID: "user-a", Inbox: true})
	resp = e.meRequest("GET", "/v1/me/channels", lbOrigin, "tok-inbox")
	wantStatus(t, resp, http.StatusForbidden)
	wantHeader(t, resp, "Access-Control-Allow-Origin", lbOrigin)
}

func TestMeCORSPreflight(t *testing.T) {
	e := setup(t)

	resp := e.meRequest("OPTIONS", "/v1/me/tenants/"+testTenantID+"/notifications/read-all", lbOrigin, "")
	wantStatus(t, resp, http.StatusNoContent)
	for k, want := range map[string]string{
		"Access-Control-Allow-Origin":      lbOrigin,
		"Access-Control-Allow-Methods":     "POST",
		"Access-Control-Allow-Headers":     "authorization,content-type",
		"Access-Control-Max-Age":           "600",
		"Access-Control-Allow-Credentials": "",
	} {
		wantHeader(t, resp, k, want)
	}

	for _, path := range []string{"/v1/me/channels", "/v1/me/notifications"} {
		resp := e.meRequest("OPTIONS", path, lbOrigin, "")
		wantStatus(t, resp, http.StatusNoContent)
		wantHeader(t, resp, "Access-Control-Allow-Origin", lbOrigin)
	}

	resp = e.meRequest("OPTIONS", "/v1/me/channels", evilOrigin, "")
	wantStatus(t, resp, http.StatusNoContent)
	for _, k := range []string{"Access-Control-Allow-Origin", "Access-Control-Allow-Methods", "Access-Control-Allow-Headers"} {
		wantHeader(t, resp, k, "")
	}
}

func TestMeCORSTenantScoped(t *testing.T) {
	const bbOrigin = "https://bookbrainz.org"
	// The registry knows only testTenantID; "bookbrainz" exists only in the origins config.
	e := setup(t, func(c *api.Config, _ *store.Redis) {
		c.StreamOrigins = map[string][]string{testTenantID: {lbOrigin}, "bookbrainz": {bbOrigin}}
	})
	tok := e.token("user-a")

	resp := e.meRequest("GET", unreadCountPath, bbOrigin, tok)
	wantStatus(t, resp, http.StatusOK)
	wantHeader(t, resp, "Access-Control-Allow-Origin", "")

	// A percent-encoded tenant is still that tenant.
	resp = e.meRequest("GET", "/v1/me/tenants/listen%62rainz/notifications/unread-count", bbOrigin, tok)
	wantHeader(t, resp, "Access-Control-Allow-Origin", "")

	resp = e.meRequest("GET", "/v1/me/channels", bbOrigin, tok)
	wantStatus(t, resp, http.StatusOK)
	wantHeader(t, resp, "Access-Control-Allow-Origin", bbOrigin)
}
