package api

import (
	"net/http"
	"slices"
	"strings"

	"github.com/rs/cors"

	"github.com/metabrainz/synapse/internal/eventtype"
)

// corsOrigins maps a tenant ID to the browser origins allowed to call it.
type corsOrigins map[string][]string

// middleware allows a request's Origin when it belongs to tenantOf(r), and
// answers the preflight. It runs before auth so every response, 401 and 403
// included, is readable by an allowed page.
//
// An unknown tenant, or none, allows any tenant's origins; a request for an
// unknown tenant only gets a 404.
func (o corsOrigins) middleware(reg *eventtype.Registry, tenantOf func(*http.Request) string, methods, headers []string) func(http.Handler) http.Handler {
	return cors.New(cors.Options{
		AllowOriginVaryRequestFunc: func(r *http.Request, origin string) (bool, []string) {
			tenant := tenantOf(r)
			if !reg.HasTenant(tenant) {
				tenant = ""
			}
			for id, origins := range o {
				if (tenant == "" || id == tenant) && slices.Contains(origins, origin) {
					return true, nil
				}
			}
			return false, nil
		},
		AllowedMethods: methods,
		AllowedHeaders: headers,
		MaxAge:         600,
	}).Handler
}

// tenantAfter returns a tenantOf that reads the path segment after prefix.
// It reads the decoded path, the tenant huma passes to the /v1/me handlers. On
// the escaped path, /v1/me/tenants/listen%62rainz/… would be an unknown tenant
// and open listenbrainz's routes to every tenant's origins.
func tenantAfter(prefix string) func(*http.Request) string {
	return func(r *http.Request) string {
		rest, ok := strings.CutPrefix(r.URL.Path, prefix)
		if !ok {
			return ""
		}
		tenant, _, _ := strings.Cut(rest, "/")
		return tenant
	}
}
