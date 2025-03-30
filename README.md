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
The generated plan (*queries using `message_idx` will always have an index backing them, even while the new index is being built*):
```
$ pg-schema-diff plan --dsn "postgres://postgres:postgres@localhost:5432/postgres" --schema-dir ./schema
################################ Generated plan ################################
1. ALTER INDEX "message_idx" RENAME TO "pgschemadiff_tmpidx_message_idx_IiaKzkvPQtyA7ob9piVqiQ";
        -- Statement Timeout: 3s

2. CREATE INDEX CONCURRENTLY message_idx ON public.foobar USING btree (message, created_at);
        -- Statement Timeout: 20m0s
        -- Lock Timeout: 3s
        -- Hazard INDEX_BUILD: This might affect database performance. Concurrent index builds require a non-trivial amount of CPU, potentially affecting database performance. They also can take a while but do not lock out writes.

3. DROP INDEX CONCURRENTLY "pgschemadiff_tmpidx_message_idx_IiaKzkvPQtyA7ob9piVqiQ";
        -- Statement Timeout: 20m0s
        -- Lock Timeout: 3s
        -- Hazard INDEX_DROPPED: Dropping this index means queries that use this index might perform worse because they will no longer will be able to leverage it.
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
The generated plan (*leverages check constraints to eliminate the need for a long-lived access-exclusive lock on the table*):
```
$ pg-schema-diff plan --dsn "postgres://postgres:postgres@localhost:5432/postgres" --schema-dir ./schema
################################ Generated plan ################################
1. ALTER TABLE "public"."foobar" ADD CONSTRAINT "pgschemadiff_tmpnn_BCOxMXqAQwaXlKPCRXoMMg" CHECK("created_at" IS NOT NULL) NOT VALID;
        -- Statement Timeout: 3s

2. ALTER TABLE "public"."foobar" VALIDATE CONSTRAINT "pgschemadiff_tmpnn_BCOxMXqAQwaXlKPCRXoMMg";
        -- Statement Timeout: 3s

3. ALTER TABLE "public"."foobar" ALTER COLUMN "created_at" SET NOT NULL;
        -- Statement Timeout: 3s

4. ALTER TABLE "public"."foobar" DROP CONSTRAINT "pgschemadiff_tmpnn_BCOxMXqAQwaXlKPCRXoMMg";
        -- Statement Timeout: 3s
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
# Install
## CLI
```bash
go install github.com/stripe/pg-schema-diff/cmd/pg-schema-diff@latest
```

## Library
```bash
go get -u github.com/stripe/pg-schema-diff@latest
```
# Using CLI
## 1. Apply schema to fresh database
Create a directory to hold your schema files. Then, generate sql files and place them into a schema dir.
```bash
mkdir schema
echo "CREATE TABLE foobar (id int);" > schema/foobar.sql
echo "CREATE TABLE bar (id varchar(255), message TEXT NOT NULL);" > schema/bar.sql
```

Apply the schema to a fresh database. [The connection string spec can be found here](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING).
Setting the `PGPASSWORD` env var will override any password set in the connection string and is recommended.
```bash
pg-schema-diff apply --from-dsn "postgres://postgres:postgres@localhost:5432/postgres" --to-dir schema 
```

## 2. Updating schema
Update the SQL file(s)
```bash
echo "CREATE INDEX message_idx ON bar(message)" >> schema/bar.sql
```

Apply the schema. Any hazards in the generated plan must be approved
```bash
pg-schema-diff apply --from-dsn "postgres://postgres:postgres@localhost:5432/postgres" --to-dir schema --allow-hazards INDEX_BUILD
```

# Using Library
Docs to use the library can be found [here](https://pkg.go.dev/github.com/stripe/pg-schema-diff). Check out [the CLI](https://github.com/stripe/pg-schema-diff/tree/main/cmd/pg-schema-diff)
for an example implementation with the library

## 1. Generating plan
```go
// The tempDbFactory is used in plan generation to extract the new schema and validate the plan
tempDbFactory, err := tempdb.NewOnInstanceFactory(ctx, func(ctx context.Context, dbName string) (*sql.DB, error) {
	copiedConfig := connConfig.Copy()
	copiedConfig.Database = dbName
	return openDbWithPgxConfig(copiedConfig)
})
if err != nil {
	panic("Generating the TempDbFactory failed")
}
defer tempDbFactory.Close()
// Generate the migration plan
plan, err := diff.Generate(ctx, diff.DBSchemaSource(connPool), diff.DDLSchemaSource(ddl),
	diff.WithTempDbFactory(tempDbFactory),
	diff.WithDataPackNewTables(),
)
if err != nil {
	panic("Generating the plan failed")
}	
```

## 2. Applying plan
We leave plan application up to the user. For example, you might want to take out a session-level advisory lock if you are 
concerned about concurrent migrations on your database. You might also want a second user to approve the plan
before applying it.

Example apply:
```go
for _, stmt := range plan.Statements {
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION statement_timeout = %d", stmt.Timeout.Milliseconds())); err != nil {
		panic(fmt.Sprintf("setting statement timeout: %s", err))
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_timeout = %d", stmt.LockTimeout.Milliseconds())); err != nil {
		panic(fmt.Sprintf("setting lock timeout: %s", err))
	}
	if _, err := conn.ExecContext(ctx, stmt.ToSQL()); err != nil {
		panic(fmt.Sprintf("executing migration statement. the database maybe be in a dirty state: %s: %s", stmt, err))
	}
}
```

# Supported Postgres versions
Supported: 14, 15, 16, 17  
Unsupported: <= 13  are not supported. Use at your own risk.

# Unsupported migrations
An abridged list of unsupported migrations:
- Views (Planned)
- Privileges (Planned)
- Types (Only enums are currently supported)
- Renaming. The diffing library relies on names to identify the old and new versions of a table, index, etc. If you rename
an object, it will be treated as a drop and an add

# Contributing
This project is in its early stages. We appreciate all the feature/bug requests we receive, but we have limited cycles
to review direct code contributions at this time. See [Contributing](CONTRIBUTING.md) to learn more.
