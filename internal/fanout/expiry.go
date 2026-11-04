package fanout

import "time"

const DefaultInAppRetention = 30 * 24 * time.Hour

// Expiry is frozen into notifications.expires_at at fan-out.
type Expiry struct {
	Tenant map[string]time.Duration
}

func NewExpiry(tenantDays map[string]int) Expiry {
	e := Expiry{Tenant: make(map[string]time.Duration, len(tenantDays))}
	for id, d := range tenantDays {
		e.Tenant[id] = time.Duration(d) * 24 * time.Hour
	}
	return e
}

func (e Expiry) For(tenantID string) time.Duration {
	if d := e.Tenant[tenantID]; d > 0 {
		return d
	}
	return DefaultInAppRetention
}
