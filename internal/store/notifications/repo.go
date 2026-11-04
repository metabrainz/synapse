// Package notifications stores a user's inbox entries. The payload stays in events.
package notifications

import (
	"context"
	"time"

	"github.com/metabrainz/synapse/internal/store"
)

type Entry struct {
	EventID   int64
	UserID    string
	TenantID  string
	ExpiresAt time.Time
}

// InsertBatch skips a repeated (event, user) pair instead of failing the batch.
func InsertBatch(ctx context.Context, q store.Querier, batch []Entry) error {
	n := len(batch)
	eventIDs := make([]int64, n)
	userIDs := make([]string, n)
	tenantIDs := make([]string, n)
	expiresAt := make([]time.Time, n)
	for i, nt := range batch {
		eventIDs[i] = nt.EventID
		userIDs[i] = nt.UserID
		tenantIDs[i] = nt.TenantID
		expiresAt[i] = nt.ExpiresAt
	}
	_, err := q.Exec(ctx,
		`INSERT INTO notifications (event_id, user_id, tenant_id, expires_at)
		 SELECT unnest($1::bigint[]), unnest($2::text[]), unnest($3::text[]), unnest($4::timestamptz[])
		 ON CONFLICT (event_id, user_id) DO NOTHING`,
		eventIDs, userIDs, tenantIDs, expiresAt,
	)
	return err
}
