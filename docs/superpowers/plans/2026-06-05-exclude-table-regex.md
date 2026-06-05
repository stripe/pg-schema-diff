# `--exclude-table` Regex Flag Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a repeatable `--exclude-table <pattern>` flag (and corresponding library options) that excludes tables matching a Go regexp from schema introspection in `plan`, `apply`, and `dump`.

**Architecture:** Mirrors the existing `--exclude-schema` plumbing: CLI flag → `diff.WithExcludeTablePatterns` (PlanOpt) → `schema.WithExcludeTables` (GetSchemaOpt) → a table filter applied inside `GetSchema`. The filter is applied once, centrally, at the end of `schemaFetcher.getSchema()` (rather than inside each fetch function) so that partitions of excluded parents — and the indexes/triggers/FKs owned by those partitions — are excluded correctly via a fixed-point computation over the actual table list. Patterns are fully anchored (`^(?:pat)$`) and matched against both the bare table name and the `schema.table` qualified name (unescaped identifiers).

**Tech Stack:** Go 1.25, cobra (CLI), testify (tests), pgengine (DB-backed tests; requires a local Postgres — same requirement as the rest of the test suite).

**Spec:** `docs/superpowers/specs/2026-06-04-exclude-table-regex-design.md`

**Branch:** `exclude-table-regex` (already created; spec committed)

**Note on spec deviation (approved direction, refined):** The spec said "filter inside each fetch function". During planning we found that approach mishandles partitions of excluded parents: an index on partition `events_p1` (whose parent `tmp_events` matches the pattern, but whose own name doesn't) would be kept while its table is dropped. Filtering once at the end of `getSchema()` with a fixed-point excluded-table set handles this correctly and is simpler. External behavior is otherwise exactly as specced.

**Reading list for the implementer:**
- `internal/schema/filters.go` — `nameFilter` type and `filterSliceByName` (the building blocks we reuse)
- `internal/schema/schema.go:549-660` — `GetSchemaOpt`, `getSchemaOptions`, `GetSchema`, `buildNameFilter`
- `internal/schema/schema.go:661-842` — `schemaFetcher` and `getSchema()` (where the new filter is applied)
- `pkg/diff/plan_generator.go:81-97` — `WithExcludeSchemas` (the pattern `WithExcludeTablePatterns` copies)
- `cmd/pg-schema-diff/plan_cmd.go:219-245,317-366` — flag creation and `parsePlanOptions`
- `cmd/pg-schema-diff/dump_cmd.go` — dump command flags
- `internal/migration_acceptance_tests/acceptance_test.go` — acceptance test harness (note: `expectedDBSchemaDDL` must include excluded tables that remain in the old DB, since the harness compares pg_dump output after applying the plan)

**Conventions:** `nameFilter` returns `true` = keep, `false` = exclude. All test commands below are run from the repo root `/workspace/c/pg-schema-diff`. DB-backed tests need a running Postgres reachable by `internal/pgengine`; if the environment cannot run them, note that explicitly rather than claiming they passed.

---

### Task 1: Regex table filter + identifier unescaping (`internal/schema/filters.go`)

**Files:**
- Modify: `internal/schema/filters.go`
- Test: `internal/schema/filters_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/schema/filters_test.go` (it already imports `testing` and `testify/assert`; add `require` to the import block):

```go
func TestUnescapeIdentifier(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    string
		expected string
	}{
		{name: "quoted", input: `"foobar"`, expected: "foobar"},
		{name: "quoted with inner quotes", input: `"foo""bar"`, expected: `foo"bar`},
		{name: "unquoted", input: "foobar", expected: "foobar"},
		{name: "empty", input: "", expected: ""},
		{name: "single quote char", input: `"`, expected: `"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, unescapeIdentifier(tc.input))
		})
	}
}

func TestBuildExcludeTablesFilter(t *testing.T) {
	for _, tc := range []struct {
		name     string
		patterns []string
		input    SchemaQualifiedName
		// expectedKeep is whether the filter should keep (true) or exclude (false) the input.
		expectedKeep bool
	}{
		{
			name:         "bare name match",
			patterns:     []string{"tmp_.*"},
			input:        SchemaQualifiedName{SchemaName: "public", EscapedName: `"tmp_foo"`},
			expectedKeep: false,
		},
		{
			name:         "bare name match in non-public schema",
			patterns:     []string{"tmp_.*"},
			input:        SchemaQualifiedName{SchemaName: "schema_1", EscapedName: `"tmp_foo"`},
			expectedKeep: false,
		},
		{
			name:         "no match",
			patterns:     []string{"tmp_.*"},
			input:        SchemaQualifiedName{SchemaName: "public", EscapedName: `"foobar"`},
			expectedKeep: true,
		},
		{
			name:         "anchored: no substring match",
			patterns:     []string{"tmp_.*"},
			input:        SchemaQualifiedName{SchemaName: "public", EscapedName: `"my_tmp_foo"`},
			expectedKeep: true,
		},
		{
			name:         "anchored: plain name does not match as prefix",
			patterns:     []string{"users"},
			input:        SchemaQualifiedName{SchemaName: "public", EscapedName: `"users_audit"`},
			expectedKeep: true,
		},
		{
			name:         "qualified name match",
			patterns:     []string{`schema_1\.tmp_.*`},
			input:        SchemaQualifiedName{SchemaName: "schema_1", EscapedName: `"tmp_foo"`},
			expectedKeep: false,
		},
		{
			name:         "qualified pattern does not match other schemas",
			patterns:     []string{`schema_1\.tmp_.*`},
			input:        SchemaQualifiedName{SchemaName: "public", EscapedName: `"tmp_foo"`},
			expectedKeep: true,
		},
		{
			name:         "multiple patterns",
			patterns:     []string{"foo", "bar"},
			input:        SchemaQualifiedName{SchemaName: "public", EscapedName: `"bar"`},
			expectedKeep: false,
		},
		{
			name:         "identifier with special characters",
			patterns:     []string{"some table"},
			input:        SchemaQualifiedName{SchemaName: "public", EscapedName: `"some table"`},
			expectedKeep: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter, err := buildExcludeTablesFilter(tc.patterns)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedKeep, filter(tc.input))
		})
	}
}

func TestBuildExcludeTablesFilterInvalidPattern(t *testing.T) {
	_, err := buildExcludeTablesFilter([]string{"["})
	require.ErrorContains(t, err, "compiling exclude table pattern")
}

func TestBuildExcludeTablesFilterEmpty(t *testing.T) {
	filter, err := buildExcludeTablesFilter(nil)
	require.NoError(t, err)
	require.Nil(t, filter)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/schema/ -run 'TestUnescapeIdentifier|TestBuildExcludeTablesFilter' -v`
Expected: FAIL to compile — `undefined: unescapeIdentifier`, `undefined: buildExcludeTablesFilter`

- [ ] **Step 3: Write the implementation**

Add to `internal/schema/filters.go` (the file currently has no imports; add an import block):

```go
import (
	"fmt"
	"regexp"
	"strings"
)
```

Append:

```go
// unescapeIdentifier converts an escaped identifier (as produced by EscapeIdentifier) back to its raw form.
// Identifiers that are not wrapped in double quotes are returned as-is.
func unescapeIdentifier(escaped string) string {
	if len(escaped) >= 2 && strings.HasPrefix(escaped, `"`) && strings.HasSuffix(escaped, `"`) {
		return strings.ReplaceAll(escaped[1:len(escaped)-1], `""`, `"`)
	}
	return escaped
}

// buildExcludeTablesFilter builds a nameFilter that excludes (returns false for) any table whose unescaped name or
// unescaped schema-qualified name (e.g., "public.foobar") fully matches any of the given regex patterns. Patterns
// are anchored, i.e., wrapped in ^(?:...)$, so "users" matches only a table named exactly "users". Returns nil if no
// patterns are provided.
func buildExcludeTablesFilter(patterns []string) (nameFilter, error) {
	if len(patterns) == 0 {
		return nil, nil
	}

	var regexes []*regexp.Regexp
	for _, pattern := range patterns {
		regex, err := regexp.Compile(fmt.Sprintf("^(?:%s)$", pattern))
		if err != nil {
			return nil, fmt.Errorf("compiling exclude table pattern %q: %w", pattern, err)
		}
		regexes = append(regexes, regex)
	}

	return func(table SchemaQualifiedName) bool {
		name := unescapeIdentifier(table.EscapedName)
		qualifiedName := fmt.Sprintf("%s.%s", table.SchemaName, name)
		for _, regex := range regexes {
			if regex.MatchString(name) || regex.MatchString(qualifiedName) {
				return false
			}
		}
		return true
	}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/schema/ -run 'TestUnescapeIdentifier|TestBuildExcludeTablesFilter' -v`
Expected: PASS (all subtests)

- [ ] **Step 5: Commit**

```bash
git add internal/schema/filters.go internal/schema/filters_test.go
git commit -m "Add anchored regex table filter and identifier unescaping"
```

---

### Task 2: `excludeTables` schema prune function (`internal/schema/filters.go`)

Removes excluded tables from an assembled `Schema`, plus partitions of excluded tables (transitively) and objects owned by excluded tables (indexes, FK constraints, triggers). Check constraints, policies, and privileges live on the `Table` struct and drop with it.

**Files:**
- Modify: `internal/schema/filters.go`
- Test: `internal/schema/filters_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/schema/filters_test.go`:

```go
func TestExcludeTables(t *testing.T) {
	fooTable := SchemaQualifiedName{SchemaName: "public", EscapedName: `"foo"`}
	tmpTable := SchemaQualifiedName{SchemaName: "public", EscapedName: `"tmp_bar"`}
	partitionedTable := SchemaQualifiedName{SchemaName: "public", EscapedName: `"tmp_events"`}
	// The partition and sub-partition names do not match the exclude pattern; they must be excluded because their
	// (transitive) parent is excluded.
	partition := SchemaQualifiedName{SchemaName: "public", EscapedName: `"events_p1"`}
	subPartition := SchemaQualifiedName{SchemaName: "public", EscapedName: `"events_p1_sub"`}

	input := Schema{
		Tables: []Table{
			{SchemaQualifiedName: fooTable},
			{SchemaQualifiedName: tmpTable},
			{SchemaQualifiedName: partitionedTable, PartitionKeyDef: "RANGE (id)"},
			{SchemaQualifiedName: partition, ParentTable: &partitionedTable, PartitionKeyDef: "RANGE (id)"},
			{SchemaQualifiedName: subPartition, ParentTable: &partition},
		},
		Indexes: []Index{
			{Name: "foo_idx", OwningRelName: fooTable},
			{Name: "tmp_bar_idx", OwningRelName: tmpTable},
			{Name: "events_p1_idx", OwningRelName: partition},
		},
		ForeignKeyConstraints: []ForeignKeyConstraint{
			{EscapedName: `"foo_fk"`, OwningTable: fooTable, ForeignTable: tmpTable},
			{EscapedName: `"tmp_bar_fk"`, OwningTable: tmpTable, ForeignTable: fooTable},
		},
		Triggers: []Trigger{
			{EscapedName: `"foo_trigger"`, OwningTable: fooTable},
			{EscapedName: `"tmp_bar_trigger"`, OwningTable: tmpTable},
		},
	}

	filter, err := buildExcludeTablesFilter([]string{"tmp_.*"})
	require.NoError(t, err)
	output := excludeTables(input, filter)

	var tableNames []string
	for _, table := range output.Tables {
		tableNames = append(tableNames, table.GetFQEscapedName())
	}
	assert.ElementsMatch(t, []string{fooTable.GetFQEscapedName()}, tableNames)

	var indexNames []string
	for _, idx := range output.Indexes {
		indexNames = append(indexNames, idx.Name)
	}
	assert.ElementsMatch(t, []string{"foo_idx"}, indexNames)

	// The FK owned by the kept table is kept even though it references an excluded table. This is consistent with
	// how cross-schema FKs behave with WithExcludeSchemas.
	var fkNames []string
	for _, fk := range output.ForeignKeyConstraints {
		fkNames = append(fkNames, fk.EscapedName)
	}
	assert.ElementsMatch(t, []string{`"foo_fk"`}, fkNames)

	var triggerNames []string
	for _, trigger := range output.Triggers {
		triggerNames = append(triggerNames, trigger.EscapedName)
	}
	assert.ElementsMatch(t, []string{`"foo_trigger"`}, triggerNames)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/schema/ -run TestExcludeTables -v`
Expected: FAIL to compile — `undefined: excludeTables`

- [ ] **Step 3: Write the implementation**

Append to `internal/schema/filters.go`:

```go
// excludeTables removes tables for which keepTable returns false from the schema, along with partitions of excluded
// tables (transitively) and any objects owned by excluded tables (indexes, foreign key constraints, triggers). Check
// constraints, policies, and privileges are stored on the Table struct, so they are removed with their table.
//
// Foreign keys owned by kept tables that reference an excluded table are kept, consistent with how cross-schema
// foreign keys behave with WithExcludeSchemas (see the nameFilter docstring on schemaFetcher about dependency
// validation).
func excludeTables(s Schema, keepTable nameFilter) Schema {
	excludedTables := make(map[string]bool)
	// Iterate until a fixed point is reached to handle multi-level partitioning, where a partition's parent is
	// itself a partition of an excluded table.
	for {
		changed := false
		for _, table := range s.Tables {
			fqName := table.GetFQEscapedName()
			if excludedTables[fqName] {
				continue
			}
			parentIsExcluded := table.ParentTable != nil && excludedTables[table.ParentTable.GetFQEscapedName()]
			if parentIsExcluded || !keepTable(table.SchemaQualifiedName) {
				excludedTables[fqName] = true
				changed = true
			}
		}
		if !changed {
			break
		}
	}

	keepOwningRel := func(owningRel SchemaQualifiedName) bool {
		return !excludedTables[owningRel.GetFQEscapedName()]
	}
	s.Tables = filterSliceByName(s.Tables, func(t Table) SchemaQualifiedName { return t.SchemaQualifiedName }, keepOwningRel)
	s.Indexes = filterSliceByName(s.Indexes, func(idx Index) SchemaQualifiedName { return idx.OwningRelName }, keepOwningRel)
	s.ForeignKeyConstraints = filterSliceByName(s.ForeignKeyConstraints, func(fk ForeignKeyConstraint) SchemaQualifiedName { return fk.OwningTable }, keepOwningRel)
	s.Triggers = filterSliceByName(s.Triggers, func(t Trigger) SchemaQualifiedName { return t.OwningTable }, keepOwningRel)
	return s
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/schema/ -run TestExcludeTables -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/schema/filters.go internal/schema/filters_test.go
git commit -m "Add excludeTables schema pruning for table exclusion"
```

---

### Task 3: `WithExcludeTables` GetSchemaOpt wired into `GetSchema` (`internal/schema/schema.go`)

**Files:**
- Modify: `internal/schema/schema.go` (options at ~549-576, `GetSchema` at ~578-607, `schemaFetcher` struct at ~661-679, `getSchema()` return at ~828-842)
- Test: `internal/schema/schema_test.go` (DB-backed — requires Postgres)

- [ ] **Step 1: Write the failing test**

Append to `internal/schema/schema_test.go` (file already imports `context`, `database/sql`, `testing`, `require`, `assert`, `pgengine`):

```go
func TestGetSchemaWithExcludeTables(t *testing.T) {
	engine, err := pgengine.StartEngine()
	require.NoError(t, err)
	defer engine.Close()

	db, err := engine.CreateDatabase()
	require.NoError(t, err)
	defer db.DropDB()

	connPool, err := sql.Open("pgx", db.GetDSN())
	require.NoError(t, err)
	defer connPool.Close()

	_, err = connPool.Exec(`
		CREATE SCHEMA schema_1;

		CREATE TABLE foo (id INT PRIMARY KEY);
		CREATE INDEX foo_idx ON foo(id);

		-- Excluded by the bare pattern, along with its index and foreign key
		CREATE TABLE tmp_bar (id INT PRIMARY KEY, foo_id INT REFERENCES foo(id));
		CREATE INDEX tmp_bar_idx ON tmp_bar(foo_id);

		-- Excluded by the bare pattern, which matches tables in all schemas
		CREATE TABLE schema_1.tmp_bar (id INT);

		-- Excluded by the schema-qualified pattern
		CREATE TABLE schema_1.qualified_excluded (id INT);
		-- Kept: the schema-qualified pattern only matches schema_1
		CREATE TABLE qualified_excluded (id INT);

		-- Kept: patterns are anchored, so tmp_.* must match the entire name
		CREATE TABLE my_tmp_bar (id INT);

		-- The partition is excluded because its parent is excluded, even though its own name does not match
		CREATE TABLE tmp_events (id INT) PARTITION BY RANGE (id);
		CREATE TABLE events_p1 PARTITION OF tmp_events FOR VALUES FROM (0) TO (100);
	`)
	require.NoError(t, err)

	fetchedSchema, err := GetSchema(context.Background(), connPool, WithExcludeTables(`tmp_.*`, `schema_1\.qualified_excluded`))
	require.NoError(t, err)

	var tableNames []string
	for _, table := range fetchedSchema.Tables {
		tableNames = append(tableNames, table.GetFQEscapedName())
	}
	assert.ElementsMatch(t, []string{`"public"."foo"`, `"public"."qualified_excluded"`, `"public"."my_tmp_bar"`}, tableNames)

	var indexNames []string
	for _, idx := range fetchedSchema.Indexes {
		indexNames = append(indexNames, idx.Name)
	}
	assert.ElementsMatch(t, []string{"foo_pkey", "foo_idx"}, indexNames)

	assert.Empty(t, fetchedSchema.ForeignKeyConstraints)

	// Invalid patterns error out before any introspection happens.
	_, err = GetSchema(context.Background(), connPool, WithExcludeTables(`[`))
	require.ErrorContains(t, err, "compiling exclude table pattern")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/schema/ -run TestGetSchemaWithExcludeTables -v`
Expected: FAIL to compile — `undefined: WithExcludeTables`

- [ ] **Step 3: Write the implementation**

In `internal/schema/schema.go`, after `WithExcludeSchemas` (line ~567), add:

```go
// WithExcludeTables filters the schema to exclude tables whose unescaped name or schema-qualified name (e.g.,
// "public.foobar") fully matches any of the given regex patterns (patterns are anchored, i.e., evaluated as
// ^(?:pattern)$). Objects owned by an excluded table (indexes, constraints, triggers, policies, privileges) and
// partitions of an excluded table are also excluded. This unions with any patterns already provided via
// WithExcludeTables. If empty, then no tables are excluded.
func WithExcludeTables(patterns ...string) GetSchemaOpt {
	return func(o *getSchemaOptions) {
		o.excludeTablePatterns = append(o.excludeTablePatterns, patterns...)
	}
}
```

In the `getSchemaOptions` struct (line ~569), add a field after `excludeSchemas`:

```go
	// excludeTablePatterns is a list of anchored regex patterns of tables to exclude from the schema, matched
	// against both the bare table name and the schema-qualified name.
	excludeTablePatterns []string
```

In `GetSchema` (line ~578), after the `nameFilter` is built, build the table filter and pass it to the fetcher:

```go
	nameFilter, err := buildNameFilter(options)
	if err != nil {
		return Schema{}, fmt.Errorf("building name filter: %w", err)
	}

	excludeTablesFilter, err := buildExcludeTablesFilter(options.excludeTablePatterns)
	if err != nil {
		return Schema{}, fmt.Errorf("building exclude tables filter: %w", err)
	}

	return (&schemaFetcher{
		q:                      queries.New(db),
		goroutineRunnerFactory: goroutineRunnerFactory,
		nameFilter:             nameFilter,
		excludeTablesFilter:    excludeTablesFilter,
	}).getSchema(ctx)
```

In the `schemaFetcher` struct (line ~661), add a field after `nameFilter`:

```go
	// excludeTablesFilter excludes tables (and the objects they own) from the fetched schema. It is applied to the
	// assembled schema at the end of getSchema, rather than per-fetch, so that partitions of excluded tables and
	// the objects owned by those partitions are excluded as well. Nil if no table exclusions were requested.
	//
	// The same dependency-validation caveat as nameFilter applies: e.g., a foreign key on a kept table that
	// references an excluded table is kept, and plans involving it may be invalid.
	excludeTablesFilter nameFilter
```

At the end of `getSchema()` (line ~828), change the return to apply the filter:

```go
	fetchedSchema := Schema{
		NamedSchemas:          schemas,
		Extensions:            extensions,
		Enums:                 enums,
		Tables:                tables,
		Indexes:               indexes,
		ForeignKeyConstraints: fkCons,
		Sequences:             sequences,
		Functions:             functions,
		Procedures:            procedures,
		Triggers:              triggers,
		Views:                 views,
		MaterializedViews:     materializedViews,
	}

	if s.excludeTablesFilter != nil {
		fetchedSchema = excludeTables(fetchedSchema, s.excludeTablesFilter)
	}

	return fetchedSchema, nil
```

- [ ] **Step 4: Run tests to verify they pass (plus no regressions in the package)**

Run: `go test ./internal/schema/ -v -race -timeout 10m`
Expected: PASS, including `TestGetSchemaWithExcludeTables` and the pre-existing `TestSchemaTestSuite`/`TestGetSchema` style tests. (DB-backed — requires Postgres. If the environment cannot start Postgres, run `go vet ./internal/schema/` + `go build ./...` and flag the untested state to the user instead of claiming success.)

- [ ] **Step 5: Commit**

```bash
git add internal/schema/schema.go internal/schema/schema_test.go
git commit -m "Add WithExcludeTables GetSchemaOpt for regex-based table exclusion"
```

---

### Task 4: Public API — `diff.WithExcludeTablePatterns` and `pkg/schema.WithExcludeTables`

**Files:**
- Modify: `pkg/diff/plan_generator.go` (after `WithExcludeSchemas`, line ~91)
- Modify: `pkg/schema/schema.go` (the `var` block, line ~13)

- [ ] **Step 1: Add the PlanOpt**

In `pkg/diff/plan_generator.go`, after `WithExcludeSchemas` (line ~91), add:

```go
// WithExcludeTablePatterns excludes tables whose name or schema-qualified name (e.g., "public.foobar") fully matches
// any of the given regex patterns (patterns are anchored, i.e., evaluated as ^(?:pattern)$). Objects owned by an
// excluded table (indexes, constraints, triggers, policies, privileges) and partitions of an excluded table are also
// excluded. The exclusion applies to both the current and target schemas.
func WithExcludeTablePatterns(patterns ...string) PlanOpt {
	return func(opts *planOptions) {
		opts.getSchemaOpts = append(opts.getSchemaOpts, schema.WithExcludeTables(patterns...))
	}
}
```

(`schema` here is the existing import of `internal/schema` in that file — same one `WithExcludeSchemas` uses.)

- [ ] **Step 2: Export from pkg/schema**

In `pkg/schema/schema.go`, extend the `var` block:

```go
var (
	WithIncludeSchemas = internalschema.WithIncludeSchemas
	WithExcludeSchemas = internalschema.WithExcludeSchemas
	WithExcludeTables  = internalschema.WithExcludeTables
)
```

- [ ] **Step 3: Verify it compiles**

Run: `go build ./...`
Expected: clean build, no output

- [ ] **Step 4: Commit**

```bash
git add pkg/diff/plan_generator.go pkg/schema/schema.go
git commit -m "Expose table exclusion via diff.WithExcludeTablePatterns and schema.WithExcludeTables"
```

---

### Task 5: Acceptance tests (`internal/migration_acceptance_tests/`)

**Files:**
- Create: `internal/migration_acceptance_tests/exclude_table_cases_test.go`

Harness notes for the implementer:
- The harness applies the plan to the old DB, then compares its pg_dump against a DB built from `expectedDBSchemaDDL`. Excluded tables remain in the old DB, so `expectedDBSchemaDDL` must include them.
- Not setting `expectedHazardTypes` asserts the plan has NO hazards.
- `expectEmptyPlan: true` asserts zero statements.

- [ ] **Step 1: Write the test file**

```go
package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var excludeTableAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "Excluded table only in old schema is not dropped",
		oldSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
			CREATE INDEX tmp_foo_idx ON tmp_foo(id);
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
		`},
		planOpts:        []diff.PlanOpt{diff.WithExcludeTablePatterns("tmp_.*")},
		expectEmptyPlan: true,
		expectedDBSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
			CREATE INDEX tmp_foo_idx ON tmp_foo(id);
		`},
	},
	{
		name: "Excluded table only in new schema is not created",
		oldSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
		`},
		planOpts:        []diff.PlanOpt{diff.WithExcludeTablePatterns("tmp_.*")},
		expectEmptyPlan: true,
		expectedDBSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
		`},
	},
	{
		name: "Changes to non-excluded tables are still planned",
		oldSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY, new_col TEXT);
		`},
		planOpts: []diff.PlanOpt{diff.WithExcludeTablePatterns("tmp_.*")},
		expectedDBSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY, new_col TEXT);
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
		`},
	},
	{
		name: "Schema-qualified pattern only excludes tables in that schema",
		oldSchemaDDL: []string{`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.tmp_foo(id INT PRIMARY KEY);
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
		`},
		newSchemaDDL: []string{`
			CREATE SCHEMA schema_1;
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
		`},
		planOpts:        []diff.PlanOpt{diff.WithExcludeTablePatterns(`schema_1\.tmp_foo`)},
		expectEmptyPlan: true,
		expectedDBSchemaDDL: []string{`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.tmp_foo(id INT PRIMARY KEY);
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
		`},
	},
	{
		name: "Partitions of an excluded partitioned table are also excluded",
		oldSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
			CREATE TABLE tmp_events(id INT) PARTITION BY RANGE (id);
			CREATE TABLE events_p1 PARTITION OF tmp_events FOR VALUES FROM (0) TO (100);
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
		`},
		planOpts:        []diff.PlanOpt{diff.WithExcludeTablePatterns("tmp_.*")},
		expectEmptyPlan: true,
		expectedDBSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
			CREATE TABLE tmp_events(id INT) PARTITION BY RANGE (id);
			CREATE TABLE events_p1 PARTITION OF tmp_events FOR VALUES FROM (0) TO (100);
		`},
	},
}

func TestExcludeTableTestCases(t *testing.T) {
	runTestCases(t, excludeTableAcceptanceTestCases)
}
```

- [ ] **Step 2: Run the acceptance tests**

Run: `go test ./internal/migration_acceptance_tests/ -run TestExcludeTableTestCases -v -race -timeout 15m`
Expected: PASS (5 subtests). DB-backed — same Postgres caveat as Task 3.

- [ ] **Step 3: Commit**

```bash
git add internal/migration_acceptance_tests/exclude_table_cases_test.go
git commit -m "Add acceptance tests for table exclusion patterns"
```

---

### Task 6: CLI flag for `plan` and `apply` (`cmd/pg-schema-diff/plan_cmd.go`)

`apply` reuses `createPlanOptionsFlags`/`parsePlanOptions`, so this covers both commands.

**Files:**
- Modify: `cmd/pg-schema-diff/plan_cmd.go` (`planOptionsFlags` ~line 113, `createPlanOptionsFlags` ~line 219, `parsePlanOptions` ~line 317)
- Test: `cmd/pg-schema-diff/plan_cmd_test.go`

- [ ] **Step 1: Write the failing unit test**

Append to `cmd/pg-schema-diff/plan_cmd_test.go` (add `github.com/stretchr/testify/require` to its imports):

```go
func TestParsePlanOptionsExcludeTablePatterns(t *testing.T) {
	_, err := parsePlanOptions(planOptionsFlags{excludeTablePatterns: []string{"tmp_.*"}})
	require.NoError(t, err)

	_, err = parsePlanOptions(planOptionsFlags{excludeTablePatterns: []string{"["}})
	require.ErrorContains(t, err, `invalid --exclude-table pattern "["`)
}
```

Also add an end-to-end case to the `TestPlanCmd` test case slice in the same file (this exercises cobra flag registration):

```go
		{
			name: "invalid exclude-table pattern",
			args: []string{"--exclude-table", "["},
			dynamicArgs: []dArgGenerator{
				tempDsnDArg(suite.pgEngine, "from-dsn", nil),
				tempDsnDArg(suite.pgEngine, "to-dsn", nil),
			},
			expectErrContains: []string{"invalid --exclude-table pattern"},
		},
```

- [ ] **Step 2: Run the unit test to verify it fails**

Run: `go test ./cmd/pg-schema-diff/ -run TestParsePlanOptionsExcludeTablePatterns -v`
Expected: FAIL to compile — `unknown field excludeTablePatterns`

- [ ] **Step 3: Write the implementation**

In `planOptionsFlags` (line ~113), add after `excludeSchemas`:

```go
		excludeTablePatterns []string
```

In `createPlanOptionsFlags` (line ~219), add after the `exclude-schema` flag registration:

```go
	cmd.Flags().StringArrayVar(&flags.excludeTablePatterns, "exclude-table", nil,
		"Exclude tables matching this Go regexp. The pattern is matched (fully anchored) against both the table "+
			"name and the schema-qualified name, e.g., 'tmp_.*' or 'public\\.tmp_.*'. Can be repeated.")
```

In `parsePlanOptions` (line ~317), add a validation helper call and the PlanOpt. First add the helper near `parsePlanOptions` (the file already imports `regexp` and `fmt`):

```go
// validateExcludeTablePatterns fail-fast validates regexes before any database work is done. The patterns are
// compiled for real inside schema.GetSchema; this exists purely for a clean CLI error.
func validateExcludeTablePatterns(patterns []string) error {
	for _, pattern := range patterns {
		if _, err := regexp.Compile(pattern); err != nil {
			return fmt.Errorf("invalid --exclude-table pattern %q: %w", pattern, err)
		}
	}
	return nil
}
```

Then at the top of `parsePlanOptions`:

```go
	if err := validateExcludeTablePatterns(p.excludeTablePatterns); err != nil {
		return planOptions{}, err
	}

	opts := []diff.PlanOpt{
		diff.WithIncludeSchemas(p.includeSchemas...),
		diff.WithExcludeSchemas(p.excludeSchemas...),
		diff.WithExcludeTablePatterns(p.excludeTablePatterns...),
	}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./cmd/pg-schema-diff/ -run 'TestParsePlanOptionsExcludeTablePatterns' -v`
Expected: PASS

Run (DB-backed): `go test ./cmd/pg-schema-diff/ -run 'TestCmdTestSuite' -v -timeout 15m`
Expected: PASS including the new "invalid exclude-table pattern" subtest. (Check the actual suite runner name with `grep -n "func TestCmd\|suite.Run" cmd/pg-schema-diff/main_test.go` and adjust `-run` accordingly.)

- [ ] **Step 5: Commit**

```bash
git add cmd/pg-schema-diff/plan_cmd.go cmd/pg-schema-diff/plan_cmd_test.go
git commit -m "Add --exclude-table flag to plan and apply commands"
```

---

### Task 7: CLI flag for `dump` (`cmd/pg-schema-diff/dump_cmd.go`)

**Files:**
- Modify: `cmd/pg-schema-diff/dump_cmd.go`
- Modify: `cmd/pg-schema-diff/main_test.go` (add `outputNotContains` assertion support)
- Test: `cmd/pg-schema-diff/dump_cmd_test.go`

- [ ] **Step 1: Add `outputNotContains` to the cmd test harness**

In `cmd/pg-schema-diff/main_test.go`, add to `runCmdWithAssertionsParams` (line ~31, after `outputContains`):

```go
	// outputNotContains is a list of substrings that are expected to NOT be contained in the stdout output.
	outputNotContains []string
```

And in `runCmdWithAssertions` (line ~46, after the `outputContains` loop):

```go
	for _, o := range tc.outputNotContains {
		suite.NotContains(stdOutStr, o)
	}
```

- [ ] **Step 2: Write the failing tests**

In `cmd/pg-schema-diff/dump_cmd_test.go`, add `outputNotContains []string` to the local `testCase` struct, pass it through in the `runCmdWithAssertions` call (`outputNotContains: tc.outputNotContains,`), and add two cases:

```go
		{
			name: "dump with exclude-table",
			args: []string{"--exclude-table", "tmp_.*"},
			dynamicArgs: []dArgGenerator{
				tempDsnDArg(suite.pgEngine, "dsn", []string{
					"CREATE TABLE foobar(id INT PRIMARY KEY)",
					"CREATE TABLE tmp_foo(id INT PRIMARY KEY)",
				}),
			},
			outputContains:    []string{"foobar"},
			outputNotContains: []string{"tmp_foo"},
		},
		{
			name: "dump with invalid exclude-table pattern",
			args: []string{"--exclude-table", "["},
			dynamicArgs: []dArgGenerator{
				tempDsnDArg(suite.pgEngine, "dsn", nil),
			},
			expectErrContains: []string{"invalid --exclude-table pattern"},
		},
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./cmd/pg-schema-diff/ -run 'TestCmdTestSuite' -v -timeout 15m` (adjust `-run` to the suite runner name found in Task 6)
Expected: the new dump subtests FAIL — `unknown flag: --exclude-table`

- [ ] **Step 4: Write the implementation**

In `cmd/pg-schema-diff/dump_cmd.go`:

After the `exclude-schema` flag (line ~27):

```go
	var excludeTablePatterns []string
	cmd.Flags().StringArrayVar(&excludeTablePatterns, "exclude-table", nil,
		"Exclude tables matching this Go regexp. The pattern is matched (fully anchored) against both the table "+
			"name and the schema-qualified name, e.g., 'tmp_.*' or 'public\\.tmp_.*'. Can be repeated.")
```

In `cmd.RunE`, after `parseConnectionFlags` succeeds and before `cmd.SilenceUsage = true`:

```go
		if err := validateExcludeTablePatterns(excludeTablePatterns); err != nil {
			return err
		}
```

Thread it through `generateDumpParams`:

```go
type generateDumpParams struct {
	connConfig           *pgx.ConnConfig
	includeSchemas       []string
	excludeSchemas       []string
	excludeTablePatterns []string
}
```

…populate it in `RunE` (`excludeTablePatterns: excludeTablePatterns,`), and add the option in `generateDump`'s `diff.Generate` call:

```go
		diff.WithExcludeSchemas(params.excludeSchemas...),
		diff.WithExcludeTablePatterns(params.excludeTablePatterns...),
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./cmd/pg-schema-diff/ -v -timeout 15m`
Expected: PASS, including both new dump subtests. (DB-backed — same Postgres caveat.)

- [ ] **Step 6: Commit**

```bash
git add cmd/pg-schema-diff/dump_cmd.go cmd/pg-schema-diff/dump_cmd_test.go cmd/pg-schema-diff/main_test.go
git commit -m "Add --exclude-table flag to dump command"
```

---

### Task 8: Full verification

- [ ] **Step 1: Lint**

Run: `make lint`
Expected: no errors. If `make lint_fix` changes files, review and include them.

- [ ] **Step 2: Build + vet**

Run: `go build ./... && go vet ./...`
Expected: clean.

- [ ] **Step 3: Run the affected test packages**

Run: `go test ./internal/schema/ ./pkg/diff/ ./pkg/schema/ ./cmd/pg-schema-diff/ ./internal/migration_acceptance_tests/ -race -timeout 30m`
Expected: PASS. (If Postgres is unavailable in the environment, report exactly which tests could not be run.)

- [ ] **Step 4: Manual smoke test of help text**

Run: `go run ./cmd/pg-schema-diff plan --help | grep -A2 exclude-table`
Expected: the new flag with its description appears.

- [ ] **Step 5: Commit any remaining changes**

```bash
git status   # verify only intended files changed
git add -A && git commit -m "Lint fixes for table exclusion feature"   # only if needed
```
