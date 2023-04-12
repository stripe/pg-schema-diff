# Expected workflow

1. Add a SQL query/statement in `queries.sql`, following the other examples.
2. Run `make sqlc` using the same version of sqlc as in `build/Dockerfile.codegen`

`sqlc` decides what the return-type of the generated method should be based on the `:exec` suffix (documentation [here](https://docs.sqlc.dev/en/latest/reference/query-annotations.html)):
  - `:exec` will only tell you whether the query succeeded: `error`
  - `:execrows` will tell you how many rows were affected: `(int64, error)`
  - `:one` will give you back a single struct: `(Author, error)`
  - `:many` will give you back a slice of structs: `([]Author, error)`


It is configured by the `sqlc.yaml` file. You can read docs about the various
options it supports
[here](https://docs.sqlc.dev/en/latest/reference/config.html).
