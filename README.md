# pg-schema-diff

Diffs Postgres database schemas and generates the SQL required to get your database schema from point A to B. This 
enables you to take your database and migrate it to any desired schema defined with plain DDL.

The tooling attempts to use native postgres migration operations and avoid locking wherever possible. Not all migrations will
be lock-free and some might require downtime, but the hazards system will warn you ahead of time when that's the case.
Stateful online migration techniques, like shadow tables, aren't yet supported.

```
pg-schema-diff plan --dsn "postgres://postgres:postgres@localhost:5432/postgres" --schema-dir schema

################################ Generated plan ################################
1. ALTER TABLE "foobar" ADD COLUMN "fizz" character varying(255) COLLATE "pg_catalog"."default";
        -- Timeout: 3s

2. CREATE INDEX CONCURRENTLY fizz_idx ON public.foobar USING btree (fizz);
        -- Timeout: 20m0s
        -- Hazard INDEX_BUILD: This might affect database performance. Concurrent index builds require a non-trivial amount of CPU, potentially affecting database performance. They also can take a while but do not lock out writes.
```

# Key features
*Broad support for diffing & applying arbitrary postgres schemas defined in declarative DDL:*
- Tables
- Columns
- Check Constraints
- Indexes
- Partitions
- Functions/Triggers  (functions created by extensions are ignored)
- Sequences
- Extensions

*A comprehensive set of features to ensure the safety of planned migrations:*
- Dangerous operations are flagged as hazards and must be approved before a migration can be applied.
	- Data deletion hazards identify operations which will in some way delete or alter data.
	- Downtime/locking hazards identify operations which will impede or stop other queries.
	- Performance hazards identify operations which are resource intensive and might slow other queries.
- Migration plans are validated first against a temporary database exactly as they would be performed against the real database.
- The library is tested against an extensive suite of unit and acceptance tests.

*The use of postgres native operations for zero-downtime migrations wherever possible:*
- Concurrent index builds
- Online index replacement

# Install
## CLI
```bash
go install github.com/stripe/pg-schema-diff/cmd/pg-schema-diff
```

## Library
```bash
go get -u github.com/stripe/pg-schema-diff
````
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
We leave plan application up to the user. For example, Users might want to take out a session-level advisory lock if they are 
concerned about concurrent migrations on their database. They might also want a second user to approve the plan
before applying it.

Example apply:
```go
for _, stmt := range plan.Statements {
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION statement_timeout = %d", stmt.Timeout.Milliseconds())); err != nil {
		panic(fmt.Sprintf("setting statement timeout: %s", err))
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
- (On roadmap) Foreign key constraints
- (On roadmap) Diffing schemas other than "public"
- (On roadmap) Unique constraints (unique indexes are supported but not unique constraints)
- (On roadmap) Adding and remove partitions from an existing partitioned table
- (On roadmap) Check constraints localized to specific partitions
- Partitioned partitions (partitioned tables are supported but not partitioned partitions)
- Materialized views
- Renaming. The diffing library relies on names to identify the old and new versions of a table, index, etc. If you rename
an object, it will be treated as a drop and an add

# Contributing
This project is in its early stages. We appreciate all the feature/bug requests we receive, but we have limited cycles
to review direct code contributions at this time. See [Contributing](CONTRIBUTING.md) to learn more.
