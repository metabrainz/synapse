# Tenant Integration

A tenant is a MetaBrainz service that publishes events to Synapse (ListenBrainz today). The tenant picks the recipients and renders notifications; Synapse stores them, filters external deliveries by each user's subscriptions, and serves the inbox.

## 1. Register the tenant

Tenants are defined in code and config; there is no admin API.

**Event types** — `internal/eventtype/<tenant>/`, one `.go` and one `.json` (JSON Schema, draft-07) per event type:

```go
func (followEvent) EventName() string { return "follow" }
func (followEvent) AllowedChannels() []string {
	return []string{eventspec.ChannelWebhook, eventspec.ChannelTelegram, eventspec.ChannelEmail, eventspec.ChannelInApp}
}
func (followEvent) SchemaVersion() string { return "1" }
func (followEvent) Schema() []byte        { return followSchema }
func (followEvent) Telegram() string      { return `{{str .Payload "actor" "username"}} started following you` }
```

- Include `ChannelInApp` for events that belong in the inbox.
- An event that allows Telegram must implement `Telegram()`; startup panics otherwise.
- List every event in the tenant's `All` slice and add the tenant to `KnownTenants` in `internal/eventtype/registry.go`.

**Config** — `tenants.<id>` in `config.yaml`:

| Key | Purpose |
|---|---|
| `api_key` | Bearer token for `/v1/events`. Keep it in the secret store. |
| `email_from` | Sender address; email deliveries fail without it. |
| `notification_settings_url` | Settings link in emails. |
| `in_app_retention_days` | Inbox lifetime of a notification. Default 30. |
| `stream_origins` | Browser origins allowed to open the live stream. Exact match, one per environment. |

## 2. Publish events

```json
{
  "event_type": "follow",
  "recipients": ["1234", "5678"],
  "payload": {"actor": {"username": "example_user", "url": "https://listenbrainz.org/user/example_user"}},
  "idempotency_key": "follow:example_user:1234"
}
```

- `recipients`: MetaBrainz user IDs (OAuth `sub`), 1–5000, duplicates removed. Split larger audiences into several events.
- `payload`: validated against the schema, stored and returned to your UI unchanged. Put everything the UI needs to render in it.
- `idempotency_key`: optional. A repeat within the event retention window (`--event-age`, 90 days by default) returns the original event. Derive it from the event, not per attempt.

**HTTP:** `POST /v1/events` with `Authorization: Bearer <api_key>`.

| Status | Meaning |
|---|---|
| 202 | Stored: `{"event_id", "delivery_count"}` |
| 200 | Duplicate idempotency key (`"deduplicated": true`) or dry run |
| 400 | Unknown event type, bad recipients, or schema failure |
| 401 | Bad API key |
| 429 | Rate limited |

`?dry_run=true` returns `delivery_count` without writing. `GET /v1/events/{id}/deliveries` shows each result; in-app ones are `DELIVERED`. OpenAPI: `GET /openapi.json`, Swagger UI: `GET /docs`.

**AMQP:** the same JSON plus `"tenant_id"`, published to exchange `events.ingest` with routing key `event`. Invalid messages are logged and dropped, so try new event types over HTTP with `dry_run` first.

## 3. Inbox

All inbox routes take the user's OAuth token; call them from your backend. Full list in [Reading notifications](architecture.md#reading-notifications).

| UI | Route |
|---|---|
| Badge | `GET /v1/me/tenants/{tenant}/notifications/unread-count` |
| List | `GET /v1/me/tenants/{tenant}/notifications?limit=25`, then `before_id=<next_cursor>` |
| Open one | `POST /v1/me/tenants/{tenant}/notifications/{id}/read` |
| Mark all read | `POST /v1/me/tenants/{tenant}/notifications/read-all` |
| Remove | `DELETE /v1/me/tenants/{tenant}/notifications/{id}` |

A notification has `id`, `event_type`, `payload`, `created_at`, `expires_at`, and `read_at` once read. Skip unknown `event_type`s so a new type doesn't break an older frontend.

External channels are opt-in per user: `/v1/me/channels` to link one, `PUT /v1/me/tenants/{tenant}/channels/{channel_type}` to assign it, `PUT /v1/me/tenants/{tenant}/subscriptions/{event_type}/{channel_type}` to subscribe. In-app needs none of this.

## 4. Live updates

Live updates let the badge change without the page polling. Synapse never sends notifications over the stream; it only says "something changed", and your page refetches the count the normal way.

```
browser ── 1. open stream, Authorization: Bearer <MusicBrainz token> ──▶ Synapse
browser ◀─ 2. "inbox" event ─────────────────────────────────────────── Synapse
browser ── 3. refetch the count ──▶ your backend ──▶ Synapse
```

### What you build

**Backend:** nothing. The browser uses the MusicBrainz token it already has.

**Frontend:** open the stream with `fetch` and refetch when told to. `EventSource` can't send the `Authorization` header, so it can't be used.

```
GET <synapse>/v1/stream/tenants/{tenant}/notifications
Authorization: Bearer <MusicBrainz token>
```

Your page's origin must be listed in `stream_origins`. The stream sends two kinds of events:

| Event | Meaning | What to do |
|---|---|---|
| `inbox` | A notification arrived, or the user read or dismissed one somewhere | Refetch the unread count |
| `ping` | Every 25 s, to show the stream is alive | Nothing |

### Reconnecting

The stream closes regularly: after about 15 minutes, when Synapse restarts, or when the network drops. Reconnect every time:

- **401:** the token expired. Refresh it (`refreshMBToken` in ListenBrainz), then reconnect.
- **Anything else:** wait a random time and reconnect.

Since anything can happen while the stream is closed, refetch the count every time it opens.

### Copy this

```js
function connectInbox({ synapse, tenant, getToken, refreshToken, refetch }) {
  let ctrl, watchdog, delay = 5000;

  // Wait a random time, then reconnect. The random wait stops every user
  // reconnecting at the same moment.
  const retry = () => {
    ctrl?.abort();
    clearTimeout(watchdog);
    setTimeout(open, Math.random() * delay);
    delay = Math.min(delay * 2, 60000);
  };

  // No event for 60 s means the connection is dead, e.g. after the laptop slept.
  const alive = () => {
    clearTimeout(watchdog);
    watchdog = setTimeout(retry, 60000);
  };

  async function open() {
    ctrl = new AbortController();
    try {
      const res = await fetch(`${synapse}/v1/stream/tenants/${tenant}/notifications`, {
        headers: { Authorization: `Bearer ${getToken()}` },
        signal: ctrl.signal,
      });
      if (res.status === 401) {
        await refreshToken();
        return retry();
      }
      if (!res.ok) return retry();

      delay = 5000;
      alive();
      refetch();

      const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
      let buf = "";
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += value;
        let end;
        while ((end = buf.indexOf("\n\n")) >= 0) {
          const event = /^event: (.*)$/m.exec(buf.slice(0, end))?.[1];
          buf = buf.slice(end + 2);
          alive();
          // Wait up to 3 s so users notified together don't all refetch at once.
          if (event === "inbox") setTimeout(refetch, Math.random() * 3000);
        }
      }
    } catch (err) {
      if (err.name === "AbortError") return;
    }
    retry();
  }

  open();
}
```

### Good to know

- **Several `inbox` events in a row mean the same as one.** Some may be dropped. Never count them; always refetch.
- **Hidden tabs:** skip the refetch and do it on `visibilitychange`. Refetch the list only if it is open.
- **Sound:** play it when the refetched count is higher than the last one, not on the first count after page load.
- **If live updates are down**, nothing is lost. The inbox stays correct and catches up on the next refetch.
- **Proxy:** serve the stream over HTTP/2 with response buffering off. Browsers allow only six HTTP/1.1 connections per site, and a few open tabs use them up.
- **All tenants in one inbox:** use `/v1/me/notifications…` and `/v1/stream/notifications`. Both need the user's `cross_tenant_notifications` preference.

| Status | Cause | What to do |
|---|---|---|
| 401 | Token missing or expired | Refresh the token, reconnect |
| 403 | Global stream while `cross_tenant_notifications` is off | Use the tenant stream |
| 404 | Unknown tenant | Fix the URL |
| 429 | The user has more than 32 streams open | Back off |
| 503 | Live updates are down | Back off; the inbox still works |
