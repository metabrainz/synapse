//go:build integration

package e2e_test

import (
	"net/http"
	"testing"

	"github.com/metabrainz/synapse/internal/oauth"
)

func TestScopesThroughRouter(t *testing.T) {
	e := setup(t)
	e.stub.grantClaims("tok-inbox", oauth.Claims{ID: "reader", Inbox: true})

	resp := e.userDo("GET", "/v1/me/tenants/"+testTenantID+"/notifications/unread-count", nil, "tok-inbox")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("unread-count with inbox scope: want 200, got %d", resp.StatusCode)
	}
	resp = e.userDo("GET", "/v1/me/channels", nil, "tok-inbox")
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("channels with inbox scope: want 403, got %d", resp.StatusCode)
	}
	e.openOK(tenantStreamPath, "tok-inbox")
}
