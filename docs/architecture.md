# Synapse Architecture

Synapse is an internal event fan-out and notification system for MetaBrainz. Internal services publish events; Synapse fans them out to subscriber channels — webhooks, Telegram, and more — with at-least-once delivery guarantees.

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
                                                                                   cmd/worker ──▶  Channel adapter (webhook / telegram / …)
```

---

## Services

### `cmd/api` — Management plane

Serves two authentication surfaces:

**Surface A — tenant API key** (`/v1/events`, `/v1/events/{id}/deliveries`): internal services publish events and query delivery status. Auth is an O(1) map lookup against the static registry loaded at startup — no DB or cache involved. Routes are rate-limited per tenant via a Redis token bucket.

**Surface B — MetaBrainz OAuth token** (`/v1/me/`*): end-users manage their own channels and subscriptions. Each request introspects the bearer token against the MB OAuth endpoint; results are cached in Redis (TTL = min(token expiry, 15 min)) to keep the MB endpoint off the hot path.

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

## Static registry

Tenant configuration (API keys, allowed event types, and which channel types each event type may fan out to) is defined as Go code and loaded at startup into an in-memory map. There is no DB lookup for tenant auth or event-type validation — resolution is an O(1) map access.

Adding a new tenant or event type requires a code change and a redeploy. This is intentional: tenants are internal services, not external customers.

---

## Three-gate fanout

When the ingest consumer processes an event, three gates decide which deliveries to create:

1. **Static registry** (`schema.Registry.IsAllowed`) — is this `(tenant_id, event_type, channel_type)` combination declared in config? Rejects unknown combos at zero DB cost.
2. **Tenant-channel mapping** (`user_tenant_channel_mapping`) — has the user assigned a channel of that type for this tenant?
3. **Event subscription** (`user_event_subscriptions`) — has the user subscribed to this event type on that channel type?

A delivery row is created only when all three gates pass. The subscription cache (below) holds gates 2 and 3 in memory.

---

## Subscription cache

The ingest consumer resolves user subscriptions against an **in-memory cache** rather than querying Postgres per event. This keeps the DB out of the hot ingest path.

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

---

## Channel adapters

Each channel type is implemented as an adapter sub-package under `internal/adapter/`. All adapters satisfy the same `Adapter` interface (`Deliver`, `MaxAttempts`). Optional interfaces extend behaviour:

- **`Starter`** — called once at startup by `adapter.Build`. Used for credential validation and webhook registration (e.g. Telegram calls `setWebhook`).
- **`RouteProvider`** — mounts adapter-owned HTTP routes into the API server at startup. Used for inbound webhooks and user-facing connect flows.

Adding a new channel type means creating a new sub-package and registering it in `adapter.Build` — no changes to the fanout, relay, or ingest layers.

### Telegram

The Telegram adapter (`internal/adapter/telegram`) delivers messages via the Telegram Bot API.

**Connect flow** — users connect their Telegram account without manually finding a chat ID:

```
User clicks "Connect Telegram" in UI
      │
      │  GET /v1/me/channels/telegram/connect
      ▼
API generates a one-time token → stores in Redis (5 min TTL)
      │
      │  returns deep link: https://t.me/BotName?start=<token>
      ▼
User opens link → taps Start in Telegram bot
      │
      │  POST /internal/telegram/webhook  (Telegram → Synapse)
      ▼
Synapse looks up token → creates user_channel row → stores channel_id in Redis
      │
      │  UI polls GET /v1/me/channels/telegram/connect/<token>
      ▼
UI shows channel as connected
```

**Message formatting** — each event type defines a message template in `internal/adapter/telegram/templates.go`. Unknown event types fall back to a plain JSON dump. Templates are compiled at startup; a bad template panics immediately rather than at delivery time.

**Webhook verification** — inbound webhook requests are validated against `X-Telegram-Bot-Api-Secret-Token`. The secret is set when registering the webhook via `setWebhook` at startup.