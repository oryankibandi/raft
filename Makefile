build:
	@go build -o bin/raft cmd/main.go

run:
	@go run cmd/main.go

run-bin:
	./bin/raft
