package api

import (
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/humatest"
	"github.com/metabrainz/synapse/internal/api/middleware"
)

// The scope each /v1/me operation documents must be the one the auth middleware enforces.
func TestDocumentedScopesMatchEnforced(t *testing.T) {
	_, api := humatest.New(t)
	registerRoutes(api, nil, nil, nil, nil, nil, nil, nil, nil, nil)

	checked := 0
	for path, item := range api.OpenAPI().Paths {
		if !strings.HasPrefix(path, "/v1/me/") {
			continue
		}
		for _, op := range []*huma.Operation{item.Get, item.Post, item.Put, item.Patch, item.Delete} {
			if op == nil {
				continue
			}
			checked++
			want := middleware.RequiredScope(path)
			if len(op.Security) != 1 || len(op.Security[0]["UserOAuth"]) != 1 || op.Security[0]["UserOAuth"][0] != want {
				t.Errorf("%s %s documents %v, middleware enforces %q", op.Method, path, op.Security, want)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no /v1/me operations registered")
	}
}
