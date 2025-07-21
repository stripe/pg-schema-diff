# Sort targets alphabetically.
.PHONY: code_gen format go_lint go_lint_fix go_mod_tidy lint multiline_sql_strings_lint_fix sqlc sql_lint sql_lint_fix vendor

code_gen: go_mod_tidy sqlc

go_lint:
	golangci-lint run

go_lint_fix:
	golangci-lint run --fix

go_mod_tidy:
	go mod tidy

lint: go_lint multiline_sql_strings_lint_fix sql_lint

lint_fix: go_lint_fix multiline_sql_strings_lint_fix sql_lint_fix

multiline_sql_strings_lint_fix:
	go run ./scripts/lint/multiline_sql_strings_lint.go --fix

sqlc:
	cd internal/queries && sqlc generate

sql_lint:
	sqlfluff lint

sql_lint_fix:
	sqlfluff fix

vendor:
	go mod vendor