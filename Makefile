.PHONY: build test fmt tidy docker-up docker-down docker-reset clean

build:
	go build -o bin/api     ./cmd/api
	go build -o bin/worker  ./cmd/worker
	go build -o bin/relay   ./cmd/relay
	go build -o bin/cleanup ./cmd/cleanup

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
