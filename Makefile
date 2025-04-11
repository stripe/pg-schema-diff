.PHONY: code_gen format go_lint go_lint_fix go_mod_tidy lint sqlc sql_lint sql_lint_fix vendor sql_whitespace_fix

code_gen: go_mod_tidy sqlc

go_lint:
	golangci-lint run

go_lint_fix:
	golangci-lint run --fix

go_mod_tidy:
	go mod tidy

lint: go_lint sql_lint

lint_fix: go_lint_fix sql_lint_fix

sqlc:
	cd internal/queries && sqlc generate

sql_lint:
	sqlfluff lint

sql_lint_fix:
	sqlfluff fix

vendor:
	go mod vendor

sql_whitespace_dry_run:
	go run ./scripts/acceptance_test_sql_linter/main.go

sql_whitespace_fix:
	go run ./scripts/acceptance_test_sql_linter/main.go --fix

