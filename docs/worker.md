# How Workers Work

> For RabbitMQ fundamentals (exchanges, queues, Ack/Nack, DLX, at-least-once delivery) see [infra.md](infra.md).

## The Pipeline

An ingest request writes an event + delivery records + outbox rows **atomically** in one transaction. A separate relay process reads the outbox and publishes each row to RabbitMQ. Workers pick up those messages and call the actual delivery adapter (webhook, email, telegram, …).

```mermaid
flowchart LR
    A[Ingest API] -->|TX: event + deliveries + outbox| B[(Postgres)]
    B -->|Outbox relay| C[RabbitMQ\ndeliveries.type]
    C -->|prefetch N| D[Worker goroutines]
    D --> E[Adapter.Deliver]
```

Each channel type (`webhook`, `telegram`, …) gets its own RabbitMQ queue and its own pool of worker goroutines. Concurrency is controlled by two knobs: the number of goroutines and the AMQP prefetch count (how many unacked messages RabbitMQ hands to each goroutine at once).

## RabbitMQ Topology

Three queues per channel type:

```mermaid
flowchart TD
    Main["deliveries.&lt;type&gt;\n(main queue)"]
    Retry["deliveries.&lt;type&gt;.retry\n(TTL queue)"]
    Dead["deliveries.&lt;type&gt;.dead\n(DLQ)"]

    Main -->|consume| W[Worker]
    W -->|retriable: PublishRetry with TTL ms| Retry
    Retry -->|TTL expires → DLX| Main
    W -->|exhausted: Nack| Dead
```

**Retry flow:** worker publishes the message to the retry exchange with a TTL (the backoff delay). When the TTL expires, RabbitMQ's DLX routes it back to the main queue. The original message is Acked — the retry copy is now in-flight.

**Dead flow:** worker returns an error → `Reject(requeue=false)` → DLX routes to dead queue.

## Per-Message Handler Logic

For each message, the worker runs these steps in order:

```mermaid
flowchart TD
    A([Message received]) --> B{Rate limited?}
    B -->|yes| C[PublishRetry with delay]
    C --> C2([Ack original])

    B -->|no| E[Adapter.Deliver]

    E -->|success| F[UpdateStatus DELIVERED]
    F --> F2([Ack])

    E -->|failure| G{Attempts exhausted?}
    G -->|yes| H["UpdateStatus DEAD\n+ Sentry"]
    H --> H2([Nack → DLQ])

    G -->|no| I[PublishRetry\nexponential backoff]
    I -->|publish ok| J[UpdateStatus RETRYING]
    J --> J2([Ack original])

    I -->|publish failed| K([Nack → redelivery])
```

### 1. Rate limit check
If the adapter implements `RateLimiter`, the rate limit is checked first. If rate-limited, the message is re-queued via `PublishRetry` with the adapter-specified delay and the original is Acked.

### 2. Deliver
Calls `adapter.Deliver(ctx, msg)`. The `WorkerMessage` carries a snapshot of all config at fan-out time: channel config, payload, attempt counter, max attempts. Config changes after fan-out do not affect in-flight messages.

### 3. Success path
Update delivery status to `DELIVERED` in Postgres. Ack the message.

### 4. Retriable failure
Bump the attempt counter in the message, marshal it, publish to the retry exchange with exponential backoff (30s → 60s → 120s → … capped at 30 min). If the adapter signals a specific `retry_after`, use that if it's longer than the computed backoff. Update status to `RETRYING`. Ack the original.

### 5. Exhausted retries
`attempt >= maxAttempts` → capture to Sentry, update status to `DEAD`, return error → Nack → DLQ.

### At-least-once delivery

Workers accept at-least-once delivery semantics. If a worker crashes or a message is redelivered by RabbitMQ, the same notification may be sent more than once. For a notification system, this is acceptable — a user receiving a duplicate notification is harmless compared to missing one entirely.

### Panic safety
If the handler panics (nil pointer in an adapter, bad JSON, etc.), `consumer.go` wraps the call in a `recover()`. The message is Rejected to DLQ, the panic is logged, and the goroutine continues processing the next message — the pool does not drain.

## Summary Table

| Outcome | Original message | Next step |
|---|---|---|
| Success | Ack | Done |
| Rate limited | Ack | Re-queued with delay |
| Retriable failure | Ack (after retry published) | Retry queue → back to main |
| PublishRetry failed | Nack | RabbitMQ redelivers |
| Exhausted retries | Nack | Dead queue |
