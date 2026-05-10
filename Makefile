.PHONY: build test fmt tidy migrate-up migrate-down docker-up docker-down docker-reset clean

build:
	go build -o bin/api     ./cmd/api
	go build -o bin/worker  ./cmd/worker
	go build -o bin/relay   ./cmd/relay
	go build -o bin/cleanup ./cmd/cleanup
	go build -o bin/migrate ./cmd/migrate

migrate-up:
	go run ./cmd/migrate up

migrate-down:
	go run ./cmd/migrate down

test:
	go test ./... -short -count=1 -timeout 60s

test-integration:
	go test ./... -count=1 -timeout 300s -tags integration

docker-up:
	docker compose up -d --wait

docker-down:
	docker compose down

docker-reset:
	docker compose down -v && docker compose up -d --wait

fmt:
	gofmt -w .

tidy:
	go mod tidy

clean:
	rm -rf bin/
