.PHONY: all build migrate api relay worker ingest cleanup \
        infra infra-down \
        test-integration test-integration-down \
        check vet fmt tidy clean

all: tidy fmt vet build test-integration


build:
	go build -o bin/api     ./cmd/api
	go build -o bin/worker  ./cmd/worker
	go build -o bin/relay   ./cmd/relay
	go build -o bin/ingest  ./cmd/ingest
	go build -o bin/cleanup ./cmd/cleanup
	go build -o bin/migrate ./cmd/migrate

infra:
	docker compose up postgres redis rabbitmq -d --wait

infra-down:
	docker compose stop postgres redis rabbitmq

migrate:
	go run ./cmd/migrate up

api:
	go run ./cmd/api -config config.yaml

relay:
	go run ./cmd/relay -config config.yaml

worker:
	go run ./cmd/worker -config config.yaml

ingest:
	go run ./cmd/ingest -config config.yaml

cleanup:
	go run ./cmd/cleanup

# Start infra in the background, run tests with direct log streaming, always clean up.
test-integration:
	@docker compose -f docker-compose.test.yml up -d --wait postgres redis rabbitmq; \
	  docker compose -f docker-compose.test.yml run --rm --build test; \
	  STATUS=$$?; \
	  docker compose -f docker-compose.test.yml down -v; \
	  exit $$STATUS

# Manual cleanup if a run was cancelled before down could run.
test-integration-down:
	docker compose -f docker-compose.test.yml down -v

# Read-only checks — used by CI and runnable locally before pushing.
# Fails if fmt or tidy would produce any diff; never modifies files.
check:
	@test -z "$$(gofmt -l .)" || (echo "Run 'make fmt' to fix formatting:" && gofmt -l . && exit 1)
	@go mod tidy && git diff --exit-code go.mod go.sum || (echo "Run 'make tidy' to fix go.mod/go.sum" && exit 1)
	go vet ./...
	go vet -tags integration ./e2e/...

vet:
	go vet ./...
	go vet -tags integration ./e2e/...

fmt:
	gofmt -w .

tidy:
	go mod tidy

clean:
	rm -rf bin/
