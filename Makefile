.PHONY: code_gen format go_mod_tidy lint test vendor sqlc

format:
	goimports -w .

lint:
	golangci-lint run

code_gen: go_mod_tidy sqlc

go_mod_tidy:
	go mod tidy

sqlc:
	cd internal/queries && sqlc generate

vendor:
	go mod vendor

