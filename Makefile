.PHONY: code_gen format lint test vendor sqlc

code_gen: sqlc

format:
	goimports -w .

lint:
	golangci-lint run

sqlc:
	cd internal/queries && sqlc generate

vendor:
	go mod vendor

