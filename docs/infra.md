# Infrastructure Primer

Shared reference for the infrastructure Synapse depends on.

---

## RabbitMQ

RabbitMQ is a message broker — producers publish messages, consumers read them asynchronously.

**Exchange** — receives published messages and routes them to queues based on routing rules. The routing key on each message determines which queue gets it.

**Queue** — holds messages until a consumer reads them. Messages are persistent by default and survive broker restarts. A service typically has a main queue, a retry queue (TTL-based), and a dead-letter queue per logical message type.

**Ack / Nack** — a consumer must explicitly acknowledge each message. Ack = "done, remove it." Nack = "failed, do something else with it." Until one is sent, RabbitMQ considers the message in-flight and will redeliver it if the connection drops.

**Prefetch** — how many unacked messages RabbitMQ delivers to a consumer at once. Acts as a concurrency cap — a goroutine won't receive a new message until it acks/nacks the current one.

**DLX (Dead Letter Exchange)** — a fallback exchange attached to a queue. When a message is rejected (Nack) or its TTL expires, RabbitMQ automatically republishes it to the DLX instead of dropping it. The DLX routes it onward to another queue. Two common uses:

- **Retry:** publish to a TTL queue whose DLX points back to the main queue — messages are held for the TTL duration, then re-queued automatically.
- **Dead-lettering:** Nack on the main queue routes to a dead queue for manual inspection.

**At-least-once delivery** — RabbitMQ guarantees a message will be delivered at least once, but may redeliver it (e.g. if a consumer crashes before acking). Consumers must be idempotent or deduplicate.

**Publisher confirms** — a channel-level protocol where the broker sends an explicit Ack or Nack for every published message. Without confirms, `Publish` is fire-and-forget: the call returns as soon as the message leaves the socket, with no guarantee the broker persisted it. With confirms enabled, the publisher knows whether the broker received each message and can retry or surface errors for any that were NACKed. To avoid a per-message round-trip, publish the entire batch first (Phase A), then drain the confirms channel in one pass (Phase B).

---

## Redis

Redis is an in-memory key-value store used for short-lived state that must be fast and shared across process replicas.

**SETNX (Set if Not Exists)** — sets a key only if it does not already exist, returning whether the write succeeded. Atomic by nature — no two clients can both get a `true` result for the same key. Commonly used for distributed locks and exactly-once semantics.

**TTL (Time To Live)** — keys expire and are deleted automatically after a set duration. Keeps Redis memory bounded without a background cleanup job. A key without a TTL lives forever — always set one on short-lived or cache entries.

**Token bucket** — a rate limiting algorithm where a bucket holds up to `burst` tokens and refills at `rate` tokens/second. Each request consumes one token; requests that arrive to an empty bucket are rejected. Storing the bucket in Redis makes the limit consistent across multiple process replicas without inter-process coordination.

**Lua scripts** — Redis executes a Lua script atomically: the entire script runs as a single unit with no other client command interleaving. This solves the read-modify-write race: without atomicity, two clients could both read the same value, both decide to act on it, and both write back — double-spending a token, double-granting a lock. A Lua script collapses the read and write into one atomic operation. Because Redis is single-threaded, a Lua script — regardless of how many commands it contains — adds only one unit of latency to the queue of waiting clients. The script completes in microseconds; the atomicity is free.
