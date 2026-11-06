package live

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

func Channel(userID string) string {
	return "synapse:inbox:" + userID
}
