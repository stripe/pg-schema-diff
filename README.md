# pg-schema-diff
[![run_tests](https://github.com/stripe/pg-schema-diff/actions/workflows/run-tests.yaml/badge.svg)](https://github.com/stripe/pg-schema-diff/actions/workflows/run-tests.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/stripe/pg-schema-diff)](https://goreportcard.com/report/github.com/stripe/pg-schema-diff)
[![Go Reference](https://pkg.go.dev/badge/github.com/stripe/pg-schema-diff.svg)](https://pkg.go.dev/github.com/stripe/pg-schema-diff)
![GitHub Release](https://img.shields.io/github/v/release/stripe/pg-schema-diff?include_prereleases)

Computes the diff(erences) between Postgres database schemas and generates the SQL required to get your database schema from point A to B with 
minimal downtime & locks. This enables you to take your database and migrate it to any desired schema defined in plain DDL.

The tooling attempts to use native postgres migration operations to perform online migrations and avoid locking wherever possible. Not all migrations will
be lock-free and some might require downtime, but the hazards system will warn you ahead of time when that's the case.
Stateful online migration techniques, like shadow tables, aren't yet supported.

### Online index Replacement
Your project's diff:
```
$ git diff
diff --git a/schema/schema.sql b/schema/schema.sql
index cc3a14b..cf4b32d 100644
--- a/schema/schema.sql
+++ b/schema/schema.sql
@@ -2,5 +2,5 @@ CREATE TABLE foobar(
  	created_at timestamp,
 	message text
 );
-CREATE INDEX message_idx ON foobar(message);
+CREATE INDEX message_idx ON foobar(message, created_at);
```
An example generated plan (*queries using `message_idx` will always have an index backing them, even while the new index is being built*):
```
/*
Statement 0
*/
SET SESSION statement_timeout = 3000;
SET SESSION lock_timeout = 3000;
ALTER INDEX "public"."message_idx" RENAME TO "pgschemadiff_tmpidx_message_idx_cWw18w4cQ_i7MKzfy7ek9g";

/*
Statement 1
  - INDEX_BUILD: This might affect database performance. Concurrent index builds require a non-trivial amount of CPU, potentially affecting database performance. They also can take a while but do not lock out writes.
*/
SET SESSION statement_timeout = 1200000;
SET SESSION lock_timeout = 3000;
CREATE INDEX CONCURRENTLY message_idx ON public.foobar USING btree (message, created_at);

/*
Statement 2
  - INDEX_DROPPED: Dropping this index means queries that use this index might perform worse because they will no longer will be able to leverage it.
*/
SET SESSION statement_timeout = 1200000;
SET SESSION lock_timeout = 3000;
DROP INDEX CONCURRENTLY "public"."pgschemadiff_tmpidx_message_idx_cWw18w4cQ_i7MKzfy7ek9g";
```
### Online `NOT NULL` constraint creation
Your project's diff:
```
diff --git a/schema/schema.sql b/schema/schema.sql
index cc3a14b..5a1cec2 100644
--- a/schema/schema.sql
+++ b/schema/schema.sql
@@ -1,5 +1,5 @@
 CREATE TABLE foobar(
- 	created_at timestamp,
+ 	created_at timestamp NOT NULL,
 	message text
 );
 CREATE INDEX message_idx ON foobar(message);
```
An example generated plan (*leverages check constraints to eliminate the need for a long-lived access-exclusive lock on the table*):
```
/*
Statement 0
*/
SET SESSION statement_timeout = 3000;
SET SESSION lock_timeout = 3000;
ALTER TABLE "public"."foobar" ADD CONSTRAINT "pgschemadiff_tmpnn_GimngG1rRkODhKvgjhGfNA" CHECK("created_at" IS NOT NULL) NOT VALID;

/*
Statement 1
*/
SET SESSION statement_timeout = 3000;
SET SESSION lock_timeout = 3000;
ALTER TABLE "public"."foobar" VALIDATE CONSTRAINT "pgschemadiff_tmpnn_GimngG1rRkODhKvgjhGfNA";

/*
Statement 2
*/
SET SESSION statement_timeout = 3000;
SET SESSION lock_timeout = 3000;
ALTER TABLE "public"."foobar" ALTER COLUMN "created_at" SET NOT NULL;

/*
Statement 3
*/
SET SESSION statement_timeout = 3000;
SET SESSION lock_timeout = 3000;
ALTER TABLE "public"."foobar" DROP CONSTRAINT "pgschemadiff_tmpnn_GimngG1rRkODhKvgjhGfNA";
```

# Key features
* Declarative schema migrations
* The use of postgres native operations for zero-downtime migrations wherever possible:
  * Concurrent index builds
  * Online index replacement: If some index is changed, the new version will be built before the old version is dropped, preventing a window where no index is backing queries
  * Online constraint builds: Constraints (check, foreign key) are added as `INVALID` before being validated, eliminating the need
	for a long access-exclusive lock on the table
  * Online `NOT NULL` constraint creation using check constraints to eliminate the need for an access-exclusive lock on the table
  * Prioritized index builds: Building new indexes is always prioritized over deleting old indexes
* A comprehensive set of features to ensure the safety of planned migrations:
  * Operators warned of dangerous operations.
  * Migration plans are validated first against a temporary database exactly as they would be performed against the real database.
* Strong support of partitions
* Data-preserving table removal with explicit, separately ordered cleanup plans
# Install
```bash
go get -u github.com/stripe/pg-schema-diff@latest
```
# Using Library
Docs to use the library can be found [here](https://pkg.go.dev/github.com/stripe/pg-schema-diff).

## 1. Generating plan
```go
// The tempDbFactory is used in plan generation to extract the new schema and validate the plan
tempDbFactory, err := tempdb.NewOnInstanceFactory(ctx, connPool.Config())
if err != nil {
	panic("Generating the TempDbFactory failed")
}
defer tempDbFactory.Close()
// Generate the migration plan
plan, err := diff.Generate(ctx, diff.DBSchemaSource(connPool), diff.DDLSchemaSource(ddl),
	diff.WithTempDbFactory(tempDbFactory),
)
if err != nil {
	panic("Generating the plan failed")
}	
```

Removed and recreated tables are not dropped by the ordinary migration. They are
moved into uniquely named, marked archival schemas using the default
`pgschemadiff_archive` prefix. The returned plan has this JSON shape:

```json
{
  "statements": [],
  "cleanup_statements": [
    {"ddl": "DROP TABLE ... RESTRICT", "hazards": []}
  ],
  "current_schema_hash": "pg-schema-diff:snapshot:v1:sha256:..."
}
```

`cleanup_statements` is omitted when empty. A custom prefix replaces the default:

```go
plan, err := diff.Generate(
	ctx,
	diff.DBSchemaSource(connPool),
	diff.DDLSchemaSource(ddl),
	diff.WithTempDbFactory(tempDbFactory),
	diff.WithSchemaPartialArchivalPrefix("deleted_archive"),
)
```

Include and exclude schema options still accumulate. Prefix-like schemas are not
trusted by name alone: existing archival schemas must have valid markers and
matching catalog identities and cleanup digests. Target schemas may not use the
selected reserved archival naming grammar.

## 2. Applying plan
We leave plan application up to the user. For example, you might want to take out a session-level advisory lock if you are 
concerned about concurrent migrations on your database. You might also want a second user to approve the plan
before applying it.

Example apply:
```go
for _, stmt := range plan.Statements {
	if _, err := conn.ExecContext(ctx, stmt.ToSQL()); err != nil {
		panic(fmt.Sprintf("executing migration statement. the database maybe be in a dirty state: %s: %s", stmt, err))
	}
}
```

Apply only `plan.Statements` for the ordinary migration. This is also the
compatibility behavior for existing plan consumers: removed rows remain retained
and `CleanupStatements` is not applied automatically. Do not concatenate the two
lists. Cleanup is an optional, destructive, later operation with its own hazards:

```go
for _, stmt := range plan.CleanupStatements {
	// Require separate review and scheduling before executing destructive cleanup.
	if _, err := conn.ExecContext(ctx, stmt.ToSQL()); err != nil {
		panic(fmt.Sprintf("executing cleanup statement: %s", err))
	}
}
```

Ordinary archival statements can report `AUTHZ_UPDATE`, lock, and `CORRECTNESS`
hazards. Physical table deletion and its `DELETES_DATA` hazard are confined to
`CleanupStatements`; standalone destructive changes such as dropping a column or
sequence may still report `DELETES_DATA` in the ordinary list.

Before applying a stored plan, compare `plan.CurrentSchemaHash` with
`schema.GetSchemaHash`. Both use the versioned
`pg-schema-diff:snapshot:v1:sha256:` contract. Plans generated with a custom
archival prefix must use the matching public helper:

```go
currentHash, err := schema.GetSchemaHashWithArchivalPrefix(
	ctx,
	connPool,
	"deleted_archive",
)
if err != nil || currentHash != plan.CurrentSchemaHash {
	panic("the source schema changed after plan generation")
}
```

The v1 hash intentionally replaces the previous modeled-schema hash and is not
compatible with stored legacy hash values. Adding `CleanupStatements` to `Plan`
also means external unkeyed `diff.Plan{...}` literals no longer compile; use keyed
fields.

Archival supports ordinary tables, complete declarative partition trees, and
detached declarative subtrees. It preserves supported table-local objects and
isolates supported ACL, foreign-key, publication, and dependency boundaries.
Traditional inheritance, foreign-table partitions, extension-owned removed
tables, untrackable persisting routines, and dependencies that cannot be safely
round-tripped fail closed. `WithDoNotValidatePlan` disables temporary-database
execution only; source safety checks still run and never restore destructive table
deletion.

# Supported Postgres versions
Supported: 14, 15, 16, 17
Unsupported: <= 13  are not supported. Use at your own risk.

# Unsupported migrations
An abridged list of unsupported migrations:
- Types (Only enums are currently supported)
- Renaming. The diffing library relies on names to identify the old and new versions of a table, index, etc. If you rename
an object, it will be treated as a drop and an add

# Contributing
This project is in its early stages. We appreciate all the feature/bug requests we receive, but we have limited cycles
to review direct code contributions at this time. See [Contributing](CONTRIBUTING.md) to learn more.
