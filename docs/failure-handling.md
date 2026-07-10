# Failure Handling

How Synapse behaves when its dependencies degrade or go down.

---

## Postgres goes down


| Service     | What happens                                                                                                                                                                                                              |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **api**     | Requests that need the DB return 503. Health check (`/health/ready`) starts failing immediately.                                                                                                                          |
| **ingest**  | `InsertBatch` fails → entire AMQP batch is nacked with requeue. RabbitMQ holds the messages durably. No events are lost — they wait in the queue until Postgres recovers.                                                 |
| **relay**   | `ClaimBatch` fails → tick returns an error and is logged. Outbox rows stay `PENDING`. On publish failure mid-batch, claimed rows remain `PUBLISHING` until relay startup `ResetStuck` (5 min) or the cleanup job resets them. |
| **worker**  | Delivery still proceeds (adapter call is independent of Postgres). Writing the delivery status update fails and is logged, but the message is acked. Status rows may remain stale until a successful redelivery or manual reconciliation — cleanup marks stuck `PENDING`/`RETRYING` rows `DEAD`, it does not backfill `DELIVERED`. |
| **cleanup** | Fails with an error log. Stale data accumulates until the next successful run. No data is lost.                                                                                                                           |


**Recovery**: automatic. All services retry on the next operation. Queued AMQP messages are delivered once Postgres is available.

---

## RabbitMQ goes down


| Service    | What happens                                                                                                                                                                         |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **api**    | Direct ingest (`POST /v1/events`) still writes to Postgres atomically. The outbox row sits there until the relay reconnects and publishes it. No events lost.                        |
| **ingest** | Consumer connection drops → `ConsumeBatchQueue` returns an error → `errgroup` cancels all workers → service exits. Systemd / Docker restarts it. Messages stay in the durable queue. |
| **relay**  | `PublishBatch` fails → relay reconnects and returns an error. Claimed rows stay `PUBLISHING` until `ResetStuck` resets them to `PENDING` (relay startup + cleanup). Only broker-confirmed rows are deleted. |
| **worker** | Consumer connection drops → service exits (same as ingest). Messages stay in the durable queue. No messages are lost because RabbitMQ queues are declared durable.                   |


**Recovery**: ingest and worker restart and reconnect. Relay reconnects inline on the next tick. All messages that were in the queues are still there.

**Important**: if the relay or worker crashes while holding unacked messages, RabbitMQ automatically requeues them after the connection closes. A redelivered message may result in a duplicate notification, which is harmless for this system.

---

## Redis goes down

Redis is used for rate limiting, OAuth token caching, and Telegram channel linking. All fail open.

| Feature                   | Behaviour when Redis is down                                                                                                                                                                                               |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Rate limiting**         | `Allow()` returns an error → middleware lets the request through. Traffic is unthrottled until Redis recovers.                                                                                                             |
| **OAuth token cache**     | Cache miss on every request → every Surface B call hits the MB introspection endpoint directly. MB becomes the bottleneck. Fails open: requests still succeed as long as MB is reachable.                                  |
| **Telegram rate limiting** | Per-chat and global token buckets fail open → Telegram API may reject with 429, triggering the normal retry flow.                                                                                                         |

**Idempotency** is handled by the PostgreSQL unique constraint on `(tenant_id, idempotency_key)`. Duplicate requests hit the constraint and return a `200` response with the original event ID.

**Tenant auth is unaffected**: Surface A auth is an O(1) lookup against the static in-memory registry — no Redis involved.

**Recovery**: automatic. All features resume normal behaviour as soon as Redis is reachable again.

---

## High lag / backpressure

### Ingest queue backing up

Messages accumulate in the `events.ingest` queue. RabbitMQ stores them durably on disk. No data loss — ingest workers drain the backlog in batches once pressure eases. Lag shows up as increased event-to-delivery latency.

Knobs: increase `SYNAPSE_INGEST_WORKERS` or `SYNAPSE_INGEST_BATCH_SIZE` to drain faster.

### Outbox table growing

The relay polls at `outbox_poll_ms` (default 100 ms) and processes up to `batch_size` rows per tick. If the ingest rate exceeds relay throughput, the outbox grows. Rows are claimed in `created_at ASC` order, so older events are always prioritised. No data loss — the relay catches up once the burst subsides.

Knobs: increase `SYNAPSE_RELAY_WORKERS` or `SYNAPSE_RELAY_BATCH_SIZE`.

### Delivery queue backing up

Messages accumulate in `deliveries.{type}`. The prefetch setting limits how many unacked messages each worker goroutine holds at once, providing natural backpressure — RabbitMQ stops pushing to a worker that hasn't acked recent messages. No data loss.

Knobs: increase `SYNAPSE_WORKER_WEBHOOK_CONCURRENCY` or `prefetch`.

### Slow or unresponsive downstream (adapter target)

Slow targets (e.g. a webhook endpoint that takes 9 s to respond) reduce worker throughput by tying up goroutines for the full adapter timeout (10 s for webhooks). The delivery queue backs up during the outage. Once the target recovers, in-flight messages that timed out will be retried via the retry exchange with exponential back-off.

---

## PgBouncer

PgBouncer in transaction mode drops session-level state (including `LISTEN` subscriptions) between checkouts. Synapse handles this in two ways:

- **Pool**: `QueryExecModeSimpleProtocol` is set when `pgbouncer: true` — this avoids prepared statements, which PgBouncer transaction mode also doesn't support.
- **LISTEN/NOTIFY**: the subscription cache opens a **direct raw connection** to Postgres (via `direct_dsn`), bypassing PgBouncer entirely. This connection is long-lived and dedicated to `LISTEN subscription_changes`.

If `direct_dsn` is not set and PgBouncer is in use, the LISTEN connection will silently lose notifications on checkout boundaries. Set `SYNAPSE_PG_DIRECT_DSN` explicitly when running behind PgBouncer.