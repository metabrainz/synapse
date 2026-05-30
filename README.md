# Synapse

Synapse is an internal event fan-out and notification system for MetaBrainz. Internal services publish events; Synapse fans them out to subscriber channels — webhooks, Telegram, and more — with at-least-once delivery guarantees.

## Services

| Binary | Role |
|--------|------|
| `cmd/api` | HTTP management plane — tenant, channel, subscription CRUD + direct event ingestion |
| `cmd/ingest` | Consumes events from RabbitMQ and writes them atomically to Postgres |
| `cmd/relay` | Polls the Postgres outbox and publishes rows to RabbitMQ with publisher confirms |
| `cmd/worker` | Consumes from RabbitMQ and delivers to subscriber channels (webhook, Telegram, …) |
| `cmd/cleanup` | Periodic housekeeping — prunes old events, reconciles stuck deliveries |
| `cmd/migrate` | Runs database migrations |

## Quick start

```bash
# Start infrastructure
docker compose up -d

# Apply migrations
go run ./cmd/migrate up

# Start all services (separate terminals)
go run ./cmd/api
go run ./cmd/ingest
go run ./cmd/relay
go run ./cmd/worker
```

Copy `config.example.yaml` to `config.yaml` and adjust as needed.

## Further reading

- [Architecture](docs/architecture.md) — data flow, outbox pattern, subscription cache
- [Failure handling](docs/failure-handling.md) — retries, dead-letter queues, crash recovery
