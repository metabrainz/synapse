.PHONY: build migrate api relay worker ingest cleanup \
        infra infra-down \
        test test-integration test-integration-down fmt tidy clean

# ── Integration test connection strings (docker-compose.test.yml ports) ───────
TEST_PG_DSN       := postgres://synapse:synapse@localhost:5433/synapse_test?sslmode=disable
TEST_REDIS_ADDR   := localhost:6380
TEST_RABBITMQ_URL := amqp://guest:guest@localhost:5673/

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

test:
	go test ./... -short -count=1 -timeout 60s

# Spin up isolated test containers, run integration tests against them, tear down.
# Containers are always removed (docker compose down -v) even when tests fail.
test-integration:
	docker compose -f docker-compose.test.yml up -d --wait
	@trap 'docker compose -f docker-compose.test.yml down -v' EXIT; \
	  SYNAPSE_TEST_PG_DSN="$(TEST_PG_DSN)" \
	  SYNAPSE_TEST_REDIS_ADDR="$(TEST_REDIS_ADDR)" \
	  SYNAPSE_TEST_RABBITMQ_URL="$(TEST_RABBITMQ_URL)" \
	  go test -tags integration -count=1 -timeout 300s -v ./e2e/...

# Manually remove test containers and volumes (useful after a cancelled run).
test-integration-down:
	docker compose -f docker-compose.test.yml down -v

fmt:
	gofmt -w .

tidy:
	go mod tidy

clean:
	rm -rf bin/
