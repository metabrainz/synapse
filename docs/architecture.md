# Synapse Architecture

Synapse is an internal event fan-out system. Internal services publish events; Synapse fans them out to subscriber channels (webhooks, and more to come).

---

## Data flow

```
Internal service
      │
      │  publish (tenant_id, user_id, event_type, payload)
      ▼
RabbitMQ  events.ingest  queue
      │
      │  consume (batch)
      ▼
cmd/ingest  ──── one transaction ────▶  Postgres
                                        ├── events
                                        ├── deliveries       (one row per subscription)
                                        └── outbox           (one row per delivery)
                                               │
                                               │  poll FOR UPDATE SKIP LOCKED
                                               ▼
                                          cmd/relay  ── publish + confirm ──▶  RabbitMQ deliveries.{type}
                                                                                        │
                                                                                        │  consume
                                                                                        ▼
                                                                                   cmd/worker ──▶  Channel adapter (webhook / email / …)
```

---

## Services

### `cmd/api` — Management plane

REST API for tenant, channel, subscription, and event-type CRUD. Also accepts direct event ingestion via `POST /v1/events` for low-volume use cases.

- Tenant-authenticated routes rate-limited per tenant via a Redis token bucket.
- Auth results are cached in-memory for 30s to keep auth off the hot DB path.

### `cmd/ingest` — Ingest consumer

Bridges the RabbitMQ ingest queue to the Postgres outbox. This is the high-throughput write path.

- Collects up to `batch_size` messages per transaction.
- Each transaction: `events.InsertBatch` → `deliveries.InsertBatch` → `outbox.InsertBatch` — three tables in one commit.
- Subscription lookup uses an in-memory cache (see below), not a live DB query per event.

### `cmd/relay` — Outbox relay

Bridges the Postgres outbox to RabbitMQ. Exists because you cannot atomically commit to Postgres and publish to RabbitMQ in one transaction.

- Claims rows with `FOR UPDATE SKIP LOCKED` — multiple workers claim disjoint batches without coordination.
- Each worker holds its own AMQP channel in confirm mode so publish round-trips are independent.
- Deletes outbox rows only after the broker confirms receipt (publisher confirms).

### `cmd/worker` — Delivery worker

Consumes from the RabbitMQ `deliveries.{type}` queue and dispatches to the target system via the channel's adapter. Each channel type has its own queue, its own adapter implementation, and its own pool of worker goroutines. Adding a new connector means adding an adapter sub-package and one line in the registry — no other changes needed.

- On success: acks the message, marks delivery `DELIVERED` in Postgres.
- On failure: schedules a retry via the retry exchange (exponential backoff); marks `DEAD` after `max_attempts`.
- Redis dedup prevents double-processing on AMQP redelivery.

### `cmd/cleanup` — Housekeeping job

Runs periodically (or once via cron) to prune stale data:

- Marks `PENDING`/`RETRYING` deliveries stuck beyond their age threshold as `DEAD`.
- Deletes events older than the retention window (cascades to deliveries).
- Resets `PUBLISHING` outbox rows stuck longer than 5 minutes back to `PENDING`.

---

## Subscription cache

The ingest consumer resolves subscriptions against an **in-memory cache** rather than querying Postgres per event. This keeps the DB out of the hot ingest path.

- On startup: full table scan populates the cache (`subscriptions.ListAllForCache`).
- On subscription change: Postgres sends a `NOTIFY subscription_changes`. The cache rebuilds immediately on receipt.
- Periodic rebuild every 30 s as a safety net in case a notification is missed.
- The LISTEN connection uses a **direct Postgres DSN** (bypassing PgBouncer), because PgBouncer transaction mode drops session-level `LISTEN` state between checkouts.

---

## RabbitMQ topology

```
Exchange: deliveries        (topic) — main dispatch
Exchange: deliveries.retry  (topic) — TTL holding area (per-message expiration)
Exchange: deliveries.dead   (topic) — permanent failures

Per channel type (e.g. "webhook", "email", …):
  Queue: deliveries.{type}        → main queue   (DLX: deliveries.dead)
  Queue: deliveries.{type}.retry  → retry queue  (DLX: deliveries, routes back on TTL)
  Queue: deliveries.dead.{type}   → dead-letter  (manual inspection)

Exchange: events.ingest  (direct) — ingest queue
Queue:    events.ingest
```

The retry loop is entirely within RabbitMQ: the worker publishes to `deliveries.retry` with a per-message TTL; when that TTL expires, RabbitMQ routes the message back to `deliveries.{type}` via the DLX. New channel types get their own queue set automatically at startup — adding a connector requires no topology changes.