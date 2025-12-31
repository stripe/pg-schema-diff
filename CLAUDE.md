# pg-schema-diff

A declarative schema migration tool for PostgreSQL that computes the difference between two database schemas and generates minimal, optimized SQL to migrate from one to the other with zero-downtime where possible.

## Project Overview

**Problem Solved**: Developers declare their desired database schema in DDL files, and pg-schema-diff automatically generates safe, optimized migration SQL that minimizes downtime and locks.

**Key Features**:
- Computes diffs between schemas (DDL files, databases, or directories)
- Generates SQL using native Postgres online operations (concurrent index builds, online constraint validation)
- Provides hazard warnings for dangerous operations
- Validates migration plans against temporary databases before execution

## Directory Structure

```
cmd/pg-schema-diff/     # CLI entry point (Cobra-based)
├── plan_cmd.go         # 'plan' subcommand - generates migration SQL
├── apply_cmd.go        # 'apply' subcommand - applies migrations
├── flags.go            # Flag parsing and DB connection handling

pkg/                    # Public API packages
├── diff/               # Core diffing and plan generation (main library interface)
├── tempdb/             # Temporary database factory for plan validation
├── log/                # Logging interface
├── schema/             # Public schema API wrapper
├── sqldb/              # Database queryable interface

internal/               # Internal implementation
├── schema/             # Complete schema representation types (schema.go is 46KB)
├── queries/            # SQL queries via sqlc for schema introspection
├── migration_acceptance_tests/  # Comprehensive test suite (24 test files)
├── pgengine/           # Postgres engine management for tests
├── pgdump/             # pg_dump integration
├── graph/              # Dependency graph for statement ordering
```

## Key Packages

### pkg/diff/ - Core Diffing Engine
- `plan_generator.go`: Orchestrates plan generation and validation
- `sql_generator.go`: Generates SQL statements for all object types (2,700+ lines)
- `sql_graph.go`: Dependency graph for correct statement ordering
- `schema_source.go`: Schema sources (DDL files, database, directories)

### internal/schema/ - Schema Representation
Core types in `schema.go`:
- `Schema`: Top-level container for all database objects
- `Table`: Tables with columns, constraints, policies, triggers
- `Index`: Index definitions including partial indexes and expressions
- `Column`, `ForeignKeyConstraint`, `CheckConstraint`, `View`, `Function`, etc.

### internal/queries/ - Database Queries
Uses **sqlc** for type-safe SQL queries. To modify:
1. Edit `queries.sql`
2. Run `make sqlc` to regenerate `queries.sql.go`

## Development Commands

```bash
# Run all tests (requires Docker or local Postgres)
go test -v -race ./... -timeout 30m

# Run specific acceptance tests
go test -v ./internal/migration_acceptance_tests/... -run TestIndexAcceptance

# Lint
make lint

# Fix lint issues
make lint_fix

# Regenerate sqlc code
make sqlc

# Tidy dependencies
make go_mod_tidy
```

## Testing

### Acceptance Tests
Located in `internal/migration_acceptance_tests/`. Each test file covers specific features:
- `index_cases_test.go`: Index operations
- `table_cases_test.go`: Table operations
- `column_cases_test.go`: Column operations
- `check_constraint_cases_test.go`, `foreign_key_constraint_cases_test.go`: Constraints
- `view_cases_test.go`, `function_cases_test.go`, `trigger_cases_test.go`, etc.

Test case structure:
```go
acceptanceTestCase{
    name:                 "test name",
    oldSchemaDDL:        []string{"CREATE TABLE ..."},
    newSchemaDDL:        []string{"CREATE TABLE ... (modified)"},
    expectedHazardTypes: []diff.MigrationHazardType{...},
    expectedPlanDDL:     []string{"ALTER TABLE ..."},  // optional: assert exact DDL
    expectEmptyPlan:     false,                         // optional: assert no changes
    planOpts:            []diff.PlanOpt{...},           // optional: custom plan options
}
```

### Running Tests with Docker
```bash
docker build -f build/Dockerfile.test --build-arg PG_MAJOR=15 -t pg-schema-diff-test .
docker run pg-schema-diff-test
```

## Key Concepts

### Migration Hazards
Operations are flagged with hazard types:
- `MigrationHazardTypeAcquiresAccessExclusiveLock`: Full table lock
- `MigrationHazardTypeDeletesData`: Potential data loss
- `MigrationHazardTypeIndexBuild`: Performance impact during build
- `MigrationHazardTypeIndexDropped`: Query performance may degrade
- `MigrationHazardTypeCorrectness`: Potential correctness issues

### Plan and Statements
```go
type Plan struct {
    Statements        []Statement
    CurrentSchemaHash string  // For validation before applying
}

type Statement struct {
    DDL         string           // SQL to execute
    Timeout     time.Duration    // statement_timeout
    LockTimeout time.Duration    // lock_timeout
    Hazards     []MigrationHazard
}
```

### Online Migration Techniques
- **Concurrent Index Building**: `CREATE INDEX CONCURRENTLY`
- **Online Index Replacement**: Rename old, build new concurrently, drop old
- **Online NOT NULL**: Uses check constraints temporarily
- **Online Constraint Validation**: Add as `NOT VALID`, validate separately

## CLI Usage

```bash
# Generate migration plan (from database to DDL files)
pg-schema-diff plan \
    --from-dsn "postgres://user:pass@localhost:5432/mydb" \
    --to-dir ./schema

# Generate plan between two databases
pg-schema-diff plan \
    --from-dsn "postgres://..." \
    --to-dsn "postgres://..."

# Apply migration (requires hazard approval)
pg-schema-diff apply \
    --from-dsn "postgres://user:pass@localhost:5432/mydb" \
    --to-dir ./schema \
    --allow-hazards INDEX_BUILD,ACQUIRES_ACCESS_EXCLUSIVE_LOCK

# Output formats: sql (default), json, pretty
pg-schema-diff plan --from-dsn "..." --to-dir ./schema --output-format json
```

## Library Usage

```go
import (
    "github.com/stripe/pg-schema-diff/pkg/diff"
    "github.com/stripe/pg-schema-diff/pkg/tempdb"
)

// Create temp database factory for plan validation
tempDbFactory, _ := tempdb.NewOnInstanceFactory(ctx, func(ctx context.Context, dbName string) (*sql.DB, error) {
    return sql.Open("postgres", fmt.Sprintf(".../%s", dbName))
})

// Define schema sources
currentSchema := diff.DBSchemaSource(db)  // db is *sql.DB or sqldb.Queryable
targetSchema, _ := diff.DirSchemaSource([]string{"./schema"})  // returns (SchemaSource, error)

// Generate plan
plan, _ := diff.Generate(ctx, currentSchema, targetSchema,
    diff.WithTempDbFactory(tempDbFactory),
)

// Apply statements (set timeouts before each statement)
for _, stmt := range plan.Statements {
    conn.ExecContext(ctx, fmt.Sprintf("SET SESSION statement_timeout = %d", stmt.Timeout.Milliseconds()))
    conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_timeout = %d", stmt.LockTimeout.Milliseconds()))
    conn.ExecContext(ctx, stmt.ToSQL())
}
```

## Code Patterns

### Adding New Schema Object Support
1. Add type to `internal/schema/schema.go`
2. Add query to `internal/queries/queries.sql`, run `make sqlc`
3. Update schema fetching logic, schema structs, and tests in `internal/schema`
4. Add diffing logic in `pkg/diff/diff.go`
5. Add SQL generation logic in `pkg/diff/x_sql_generator.go`
6. Add acceptance tests in `internal/migration_acceptance_tests/`

### Error Handling
Use `fmt.Errorf` with `%w` for error wrapping. Functions return `error` as last return value.

### Testing Conventions
- Use `testify/assert` and `testify/require`
- Acceptance tests use shared Postgres via `pgengine`
- Test cases are typically table-driven
