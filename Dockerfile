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
    CGO_ENABLED=0 go build -o /bin/cleanup  ./cmd/cleanup

# ─── runtime ────────────────────────────────────────────────────────────────
FROM metabrainz/consul-template-base:noble-1.0.2-v0.3-2

ARG GIT_COMMIT_SHA
LABEL org.label-schema.vcs-url="https://github.com/metabrainz/synapse.git" \
      org.label-schema.vcs-ref="" \
      org.label-schema.schema-version="1.0.0-rc1" \
      org.label-schema.vendor="MetaBrainz Foundation" \
      org.label-schema.name="Synapse" \
      org.metabrainz.based-on-image="metabrainz/consul-template-base:noble-1.0.2-v0.3-2"
LABEL org.label-schema.vcs-ref=$GIT_COMMIT_SHA
ENV GIT_SHA=${GIT_COMMIT_SHA}

COPY --from=builder /bin/api      /bin/api
COPY --from=builder /bin/relay    /bin/relay
COPY --from=builder /bin/worker   /bin/worker
COPY --from=builder /bin/ingest   /bin/ingest
COPY --from=builder /bin/migrate  /bin/migrate
COPY --from=builder /bin/cleanup  /bin/cleanup

COPY migrations /migrations

RUN mkdir -p /etc/synapse
COPY deploy/config.yaml.ctmpl         /etc/synapse/config.yaml.ctmpl
COPY deploy/consul-template-api.conf  /etc/synapse/consul-template-api.conf
COPY deploy/consul-template-worker.conf /etc/synapse/consul-template-worker.conf
COPY deploy/consul-template-relay.conf  /etc/synapse/consul-template-relay.conf
COPY deploy/consul-template-ingest.conf /etc/synapse/consul-template-ingest.conf
COPY deploy/run-synapse-command        /usr/local/bin/run-synapse-command

ENTRYPOINT ["consul-template"]
CMD ["-config", "/etc/synapse/consul-template-api.conf"]
