# pg-schema-diff
Computes the diff(erences) between Postgres database schemas and generates the SQL required to get your database schema from point A to B with 
minimal downtime & locks. This enables you to take your database and migrate it to any desired schema defined in plain DDL.

The tooling attempts to use native postgres migration operations to perform online migrations and avoid locking wherever possible. Not all migrations will
be lock-free and some might require downtime, but the hazards system will warn you ahead of time when that's the case.
Stateful online migration techniques, like shadow tables, aren't yet supported.

Your project's diff:
```
$ git diff
diff --git a/schema/schema.sql b/schema/schema.sql
index 062816a..08a6a40 100644
--- a/schema/schema.sql
+++ b/schema/schema.sql
@@ -4,4 +4,4 @@ CREATE TABLE foobar(
 	sender text,
 	created_at timestamp
 );
-CREATE INDEX message_idx ON foobar(message);
+CREATE INDEX message_idx ON foobar(message, created_at);
```
The generated plan (*online index replacement, i.e., queries using `message_idx` will always have an index backing them*):
```
$ pg-schema-diff plan --dsn "postgres://postgres:postgres@localhost:5432/postgres" --schema-dir ./schema
################################ Generated plan ################################
1. ALTER INDEX "message_idx" RENAME TO "message_idx_0ef03fcb-2368-4d57-9c81-f44cb58e7325";
	-- Statement Timeout: 3s

2. CREATE INDEX CONCURRENTLY message_idx ON public.foobar USING btree (message, created_at);
	-- Statement Timeout: 20m0s
	-- Lock Timeout: 3s
	-- Hazard INDEX_BUILD: This might affect database performance. Concurrent index builds require a non-trivial amount of CPU, potentially affecting database performance. They also can take a while but do not lock out writes.

3. DROP INDEX CONCURRENTLY "message_idx_0ef03fcb-2368-4d57-9c81-f44cb58e7325";
	-- Statement Timeout: 20m0s
	-- Lock Timeout: 3s
	-- Hazard INDEX_DROPPED: Dropping this index means queries that use this index might perform worse because they will no longer will be able to leverage it.
```

# Key features
* Declarative schema migrations
* The use of postgres native operations for zero-downtime migrations wherever possible:
  * Concurrent index builds
  * Online index replacement: If some index is changed, the new version will be built before the old version is dropped, preventing a window where no index is backing queries
  * Online check constraint builds: Check constraints are added as `INVALID` before being validated, eliminating the need
	for a long access-exclusive lock on the table
  * Prioritized index builds: Building new indexes is always prioritized over deleting old indexes
* A comprehensive set of features to ensure the safety of planned migrations:
  * Operators warned of dangerous operations.
  * Migration plans are validated first against a temporary database exactly as they would be performed against the real database.
* Strong support of partitions
# Install
## CLI
```bash
go install github.com/stripe/pg-schema-diff/cmd/pg-schema-diff
```

## Library
```bash
go get -u github.com/stripe/pg-schema-diff
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
pg-schema-diff apply --dsn "postgres://postgres:postgres@localhost:5432/postgres" --schema-dir schema 
```

## 2. Updating schema
Update the SQL file(s)
```bash
echo "CREATE INDEX message_idx ON bar(message)" >> schema/bar.sql
```

Apply the schema. Any hazards in the generated plan must be approved
```bash
pg-schema-diff apply --dsn "postgres://postgres:postgres@localhost:5432/postgres" --schema-dir schema --allow-hazards INDEX_BUILD
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
plan, err := diff.GeneratePlan(ctx, conn, tempDbFactory, ddl,
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
- 14 (tested with 14.7)
- 15 (tested with 15.2)

Postgres v13 and below are not supported. Use at your own risk.

# Unsupported migrations
Note, the library only currently supports diffing the *public* schema. Support for diffing other schemas is on the roadmap

*Unsupported*:
- (On roadmap) Diffing schemas other than "public"
- (On roadmap) Adding and remove partitions from an existing partitioned table
- (On roadmap) Check constraints localized to specific partitions
- Partitioned partitions (partitioned tables are supported but not partitioned partitions)
- Materialized views
- Renaming. The diffing library relies on names to identify the old and new versions of a table, index, etc. If you rename
an object, it will be treated as a drop and an add

# Contributing
This project is in its early stages. We appreciate all the feature/bug requests we receive, but we have limited cycles
to review direct code contributions at this time. See [Contributing](CONTRIBUTING.md) to learn more.
