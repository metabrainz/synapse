//go:build integration

package e2e_test

import (
	"net/http"
	"testing"

	"github.com/metabrainz/synapse/internal/oauth"
)

func TestScopesThroughRouter(t *testing.T) {
	e := setup(t)
	e.stub.grantClaims("tok-read", oauth.Claims{ID: "reader", Read: true})

	resp := e.userDo("GET", "/v1/me/tenants/"+testTenantID+"/notifications/unread-count", nil, "tok-read")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("unread-count with read scope: want 200, got %d", resp.StatusCode)
	}
	resp = e.userDo("GET", "/v1/me/channels", nil, "tok-read")
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("channels with read scope: want 403, got %d", resp.StatusCode)
	}
	e.openOK(tenantStreamPath, "tok-read")
}
