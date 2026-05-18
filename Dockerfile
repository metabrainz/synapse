FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /bin/api      ./cmd/api      && \
    CGO_ENABLED=0 go build -o /bin/relay    ./cmd/relay    && \
    CGO_ENABLED=0 go build -o /bin/worker   ./cmd/worker   && \
    CGO_ENABLED=0 go build -o /bin/ingest   ./cmd/ingest   && \
    CGO_ENABLED=0 go build -o /bin/migrate  ./cmd/migrate  && \
    CGO_ENABLED=0 go build -o /bin/cleanup  ./cmd/cleanup  && \
    CGO_ENABLED=0 go build -o /bin/loadtest ./loadtest

# ─── runtime ────────────────────────────────────────────────────────────────
FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /bin/api      /bin/api
COPY --from=builder /bin/relay    /bin/relay
COPY --from=builder /bin/worker   /bin/worker
COPY --from=builder /bin/ingest   /bin/ingest
COPY --from=builder /bin/migrate  /bin/migrate
COPY --from=builder /bin/cleanup  /bin/cleanup
COPY --from=builder /bin/loadtest /bin/loadtest

COPY migrations /migrations

ENTRYPOINT ["/bin/api"]
