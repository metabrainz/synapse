//go:build integration

package e2e_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/metabrainz/synapse/internal/ingest"
)

// The RabbitMQ ingest path creates notifications too, and a redelivered message
// with the same idempotency key creates no second one.
func TestInAppNotificationsFromAMQPIngest(t *testing.T) {
	e := setup(t)
	e.stub.grant("tok-alice", "alice")
	e.startIngestConsumer()

	payload, _ := json.Marshal(map[string]any{"actor": map[string]any{"username": "carol", "url": "https://listenbrainz.org/user/carol"}})
	key := "amqp-follow-1"
	msg := ingest.Message{
		TenantID:       testTenantID,
		Recipients:     []string{"alice", "bob"},
		EventType:      "follow",
		Payload:        json.RawMessage(payload),
		IdempotencyKey: &key,
	}
	publishIngestEvent(t, e.amqpURL, msg)
	waitFor(t, 5*time.Second, func() bool {
		_, page := e.listNotifications("tok-alice", "")
		return len(page.Notifications) == 1
	})

	publishIngestEvent(t, e.amqpURL, msg)
	time.Sleep(time.Second)

	var notifications, events int
	e.pool.QueryRow(e.ctx, `SELECT count(*) FROM notifications`).Scan(&notifications)
	e.pool.QueryRow(e.ctx, `SELECT count(*) FROM events`).Scan(&events)
	if notifications != 2 || events != 1 {
		t.Fatalf("want 2 notifications (alice, bob) from one event after a duplicate message, got %d from %d events", notifications, events)
	}
}
