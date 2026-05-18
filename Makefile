.PHONY: build migrate api relay worker ingest cleanup \
        infra infra-down loadtest \
        test test-integration fmt tidy clean

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

loadtest:
	go run ./loadtest/ \
		-api http://localhost:8080 \
		-admin-key local-dev-key \
		-local

test:
	go test ./... -short -count=1 -timeout 60s

test-integration:
	go test ./... -count=1 -timeout 300s -tags integration

fmt:
	gofmt -w .

tidy:
	go mod tidy

clean:
	rm -rf bin/
