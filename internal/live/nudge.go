package live

import "github.com/metabrainz/synapse/internal/store"

type Kind string

const (
	KindCreated Kind = "created"
	KindChanged Kind = "changed"
)

// Nudge only tells a client to refetch; it never carries notification content.
type Nudge struct {
	Kind   Kind   `json:"kind"`
	Tenant string `json:"tenant"`
}

// Channel is the user's pub/sub channel inside a Redis namespace.
func Channel(ns store.Namespace, userID string) string {
	return ns.Name("synapse:inbox:" + userID)
}
