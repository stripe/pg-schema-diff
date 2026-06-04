# Design: `--exclude-table` flag (regex-based table exclusion)

**Date:** 2026-06-04
**Status:** Approved

## Problem

Users want to ignore certain tables when planning/applying/dumping schemas — e.g.
temp tables, shadow tables, or tooling-managed tables that should never appear in
a migration plan. Today only schema-level filtering exists (`--include-schema` /
`--exclude-schema`); there is no way to exclude individual tables by name pattern.

## Solution Overview

Add a repeatable `--exclude-table <pattern>` flag that excludes tables matching a
Go regexp from schema introspection, mirroring the existing `--exclude-schema`
plumbing end to end:

```
CLI flag --exclude-table
  → diff.WithExcludeTablePatterns(...) (PlanOpt)
    → schema.WithExcludeTables(...) (GetSchemaOpt)
      → table filter applied during schema fetch
```

Because the filter is a `GetSchemaOpt`, it applies symmetrically to both the
from-schema and to-schema (including DDL-dir sources, which are materialized in a
temp database and then introspected via the same `GetSchema` opts).

## CLI Surface

- New repeatable flag `--exclude-table <pattern>` on `plan`, `apply` (shares
  `planOptionsFlags`), and `dump`.
- Help text: "Exclude tables matching this Go regexp. The pattern is matched
  (fully anchored) against both the table name and the schema-qualified name,
  e.g. `tmp_.*` or `public\.tmp_.*`. Can be repeated."
- Invalid regexps fail fast with a clear error before any database work.

## Matching Semantics

- Each pattern is fully anchored: compiled as `^(?:<pattern>)$`. A plain name
  like `users` matches only a table named exactly `users`, never
  `audit_users_log` (same behavior as `pg_dump` pattern anchoring).
- A table is excluded if **any** pattern matches its **bare name**
  (`orders_tmp`) **or** its **qualified name** (`public.orders_tmp`).
  Matching is performed against unescaped (unquoted) identifiers.
- A partition whose **parent table** is excluded is also excluded, preventing
  orphaned partitions in the diff.

## Implementation Plan

### 1. `internal/schema`

- New `WithExcludeTables(patterns ...string) GetSchemaOpt`; patterns stored on
  `getSchemaOptions`.
- Regexes are compiled in `buildNameFilter` (or a sibling builder), returning
  errors through the existing error path in `GetSchema`.
- A new owning-table filter (reusing the `nameFilter` function type from
  `internal/schema/filters.go`) is added to `schemaFetcher` alongside the
  existing schema-name filter.

### 2. Fetch functions

- Tables: filtered by their own name (bare + qualified, per semantics above);
  partitions additionally inherit exclusion from their `ParentTable`.
- Dependent objects filtered by **owning table** name:
  - `Index.OwningRelName`
  - `ForeignKeyConstraint.OwningTable`
  - check constraints, triggers, policies (each carries its owning table)
- Check constraints, policies, and privileges that hang off the `Table` struct
  drop for free with the table.

### 3. `pkg/diff`

- New `WithExcludeTablePatterns(patterns ...string) PlanOpt` that appends
  `schema.WithExcludeTables(...)` to `getSchemaOpts` (same shape as
  `WithExcludeSchemas`).

### 4. CLI wiring

- Add the flag in `createPlanOptionsFlags` (`cmd/pg-schema-diff/plan_cmd.go`)
  → `diff.WithExcludeTablePatterns(...)` in plan option assembly.
- Add the flag in `cmd/pg-schema-diff/dump_cmd.go` analogous to
  `--exclude-schema` there.

## Edge Cases & Non-Goals

- FKs on a **kept** table referencing an excluded table are kept — consistent
  with current `--exclude-schema` behavior for cross-schema FKs.
- No `--include-table` counterpart (YAGNI; exclusion is what was requested).
- Views, materialized views, functions, sequences, and other non-table objects
  are not filtered by this flag — it filters tables (and their owned/dependent
  objects) only.

## Testing

- Unit tests in `internal/schema` for the filter: bare vs qualified matching,
  anchoring (no substring matches), partition inheritance, invalid regex error.
- Acceptance-style tests following the existing exclude-schema patterns,
  asserting that excluded tables produce no diff statements (e.g. a table that
  exists only on one side generates an empty plan when excluded).
- CLI flag parsing test in `cmd/pg-schema-diff`.
