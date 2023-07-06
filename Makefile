.PHONY: code_gen format lint test vendor sqlc

format:
	goimports -w .

lint:
	golangci-lint run

code_gen: sqlc
	go mod tidy

sqlc:
	cd internal/queries && sqlc generate

vendor:
	go mod vendor

