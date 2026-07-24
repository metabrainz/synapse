# Synapse

Synapse is an internal event fan-out and notification system for MetaBrainz. Internal services publish events; Synapse fans them out to subscriber channels — webhooks, Telegram, and more — with at-least-once delivery guarantees.

## Services

| Binary | Role |
|--------|------|
| `cmd/api` | HTTP management plane — tenant, channel, subscription CRUD + direct event ingestion |
| `cmd/ingest` | Consumes events from RabbitMQ and writes them atomically to Postgres |
| `cmd/relay` | Polls the Postgres outbox and publishes rows to RabbitMQ with publisher confirms |
| `cmd/worker` | Consumes from RabbitMQ and delivers to subscriber channels (webhook, Telegram, …) |
| `cmd/cleanup` | Periodic housekeeping — prunes old events, reconciles stuck deliveries, manages partitions |
| `cmd/migrate` | Runs database migrations (up, down, force) |

## Quick start

```bash
# Start infrastructure
make infra

# Apply migrations
make migrate

# Start all services (separate terminals)
make api
make ingest
make relay
make worker
```

## Setup

```bash
cp config.example.yaml config.yaml
```

Edit `config.yaml` with your Postgres, RabbitMQ, Redis, and OAuth credentials.

## Infrastructure

| Dependency | Purpose |
|-----------|---------|
| PostgreSQL 16+ | Event storage, delivery tracking, transactional outbox |
| RabbitMQ 3+ | Message routing between services |
| Redis 7+ | Rate limiting, OAuth token cache, Telegram connect flow |

## Testing

```bash
# Lint and vet (no modifications)
make check

# Integration tests (requires Docker)
make test-integration
```

Integration tests spin up Postgres, Redis, and RabbitMQ containers, run all migrations, and exercise the full pipeline end-to-end.

## Cleanup job

The cleanup binary handles periodic maintenance. Run it on a daily cron schedule.

```bash
go run ./cmd/cleanup -config config.yaml \
  --event-age 2160h \    # 90 days retention (default)
  --pending-age 1h \     # mark stuck PENDING deliveries as DEAD
  --retry-age 3h         # mark stuck RETRYING deliveries as DEAD
```

It also creates future monthly partitions for the deliveries table and drops partitions older than the retention window.

## Docs

| Document | Contents |
|----------|----------|
| [Architecture](docs/architecture.md) | Data flow, outbox pattern, subscription cache |
| [Worker](docs/worker.md) | Worker pipeline, retry logic, rate limiting |
| [Relay](docs/relay.md) | Outbox relay, publisher confirms, crash recovery |
| [Failure handling](docs/failure-handling.md) | What happens when each dependency goes down |
| [Infrastructure primer](docs/infra.md) | RabbitMQ and Redis fundamentals |
