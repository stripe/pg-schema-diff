# Move Removed Tables and Generate Cleanup Statements

Status: Proposed

## Summary

Normal migration statements must no longer emit `DROP TABLE`. When a table is
absent from the target schema, create a unique cleanup schema and move the table
into it with `ALTER TABLE ... SET SCHEMA`.

The move is cheap: PostgreSQL does not copy rows or rebuild indexes. It moves the
table heap, composite row type, associated indexes and constraints, and sequences
owned by table columns. Other table-local metadata remains attached.

At the same time, generate a separate ordered list of cleanup statements. These
statements describe how to delete the retained table and cleanup schemas later,
but they are not part of the ordinary migration statement list.

Extend `Plan` as follows:

```go
type Plan struct {
    Statements        []Statement `json:"statements"`
    CleanupStatements []Statement `json:"cleanup_statements,omitempty"`
    CurrentSchemaHash string      `json:"current_schema_hash"`
}
```

Example regular statements:

```sql
CREATE SCHEMA
    "pgschemadiff_archive_public_accounts_20260718T142301123456Z_a1B2c3D4";
COMMENT ON SCHEMA
    "pgschemadiff_archive_public_accounts_20260718T142301123456Z_a1B2c3D4"
    IS '<pg-schema-diff retention marker>';
ALTER TABLE "public"."accounts"
    SET SCHEMA
    "pgschemadiff_archive_public_accounts_20260718T142301123456Z_a1B2c3D4";
```

Example cleanup statements returned separately:

```sql
DROP TABLE
    "pgschemadiff_archive_public_accounts_20260718T142301123456Z_a1B2c3D4"."accounts"
    RESTRICT;
DROP SCHEMA
    "pgschemadiff_archive_public_accounts_20260718T142301123456Z_a1B2c3D4"
    RESTRICT;
```

This document designs generation of both lists. Scheduling, persistence, worker
coordination, and execution of cleanup statements are intentionally out of scope.

## Current Behavior

Table identity is its schema-qualified escaped name. `diffLists` in
`pkg/diff/diff.go` places a missing table in `tableDiffs.deletes`. It also emits
delete plus add when a table must be recreated.

`tableSQLVertexGenerator.deleteStatements` in `pkg/diff/sql_generator.go`
currently emits:

```sql
DROP TABLE <schema-qualified-name>
```

The statement has `DELETES_DATA`. Child partitions emit no SQL because the
parent drop removes them.

Other generators depend on that physical deletion:

- Index deletion is suppressed when its owning table is deleted.
- Owned sequence deletion is suppressed when its owning table is deleted.
- Child partitions rely on the parent drop.
- A recreated table is added after the delete vertex, assuming all old relation
  names have disappeared.
- Enum, extension, and named-schema deletion occurs after the table graph.
- Validation expects the migrated modeled schema to match the target exactly.

The implementation must separate two effects currently represented by one table
delete vertex:

- Immediate logical removal: move the table out of the managed schema.
- Deferred physical cleanup: generate, but do not apply, destructive statements.

## Goals

- Eliminate table-row deletion from ordinary migration statements.
- Move removed tables without copying rows or rebuilding indexes.
- Preserve a recoverable table-local snapshot in isolated schemas.
- Free source table, row-type, index, and owned-sequence names for replacements.
- Generate explicit, ordered, reviewable cleanup statements alongside the
  ordinary statements.
- Use the existing `Statement` type and hazard model for both lists.
- Generate cleanup statements for newly moved tables and previously retained
  marked tables.
- Validate both the retention migration and generated cleanup SQL.
- Keep repeated plan generation deterministic apart from newly allocated cleanup
  schema names.

## Non-goals

- Designing a cleanup worker or scheduler.
- Defining retention periods, eligibility, retries, persistence, or distributed
  locking for cleanup execution.
- Automatically applying `CleanupStatements`.
- Guaranteeing that cleanup statements generated today remain executable after
  arbitrary future schema changes. `RESTRICT` provides a fail-safe at execution.
- Minimizing retained disk usage before cleanup.
- Detecting user-intended table renames.
- Adding a CLI.
- Supporting traditional inheritance or foreign-table partitions in the first
  version.

## Public API

Use `pgschemadiff_archive` as the default cleanup-schema prefix. Add a scalar
`PlanOpt`:

```go
// WithTableRemovalSchemaPrefix overrides the schema prefix used to retain
// removed tables. Retention is enabled even when this option is absent.
func WithTableRemovalSchemaPrefix(prefix string) PlanOpt
```

Example:

```go
plan, err := diff.Generate(
    ctx,
    diff.DBSchemaSource(connPool),
    diff.DDLSchemaSource(targetDDL),
    diff.WithTempDbFactory(tempDbFactory),
    diff.WithTableRemovalSchemaPrefix("deleted"),
)
```

Omitting the option uses `pgschemadiff_archive`. Passing an empty or invalid
prefix returns an error; it never restores ordinary `DROP TABLE` generation.

Existing callers that apply only `plan.Statements` retain removed tables and do
not delete data. Consumers can inspect, serialize, or separately persist
`plan.CleanupStatements` using the same `Statement` JSON and `ToSQL` behavior as
regular statements.

`Plan.InsertStatement` continues to modify only the ordinary statement list. Do
not add cleanup-list mutation in the first version: marker cleanup digests are
generated from the ordered list, so callers must regenerate rather than insert a
statement that would make markers inconsistent.

## Cleanup Schema Naming

Capture `time.Now().UTC()` once per `Generate` call and use an independent
identifier-safe random nonce for each logical retention group.

Schema names use:

```text
<prefix>_<source_schema>_<source_table>_<timestamp>_<nonce>
```

The timestamp format is:

```text
YYYYMMDDTHHMMSSffffffZ
```

Use an eight-character nonce from `crypto/rand`. The timestamp improves
readability; the nonce protects concurrent generation and clock rollback.
`CREATE SCHEMA` remains the final uniqueness check.

PostgreSQL identifiers are limited to 63 bytes. Never truncate the prefix or
timestamp/nonce suffix. Truncate source-schema and table components on UTF-8 rune
boundaries while keeping at least one complete rune from each. Limit a simple
ASCII prefix to 21 bytes.

Validate the prefix with `pgidentifier.IsSimpleIdentifier`, reject PostgreSQL's
reserved namespace when `prefix == "pg"` or `strings.HasPrefix(prefix, "pg_")`,
and quote every generated name with `schema.EscapeIdentifier`. Checking only
`HasPrefix(prefix, "pg_")` is insufficient because generation appends `_`.

Build all names before graph construction and reject:

- Duplicate generated names, including truncation collisions.
- Current or target schema-name collisions.
- Target schemas using the reserved cleanup naming grammar.
- Relation/type collisions among objects PostgreSQL will move into an otherwise
  empty cleanup schema.

Use one cleanup schema per physical table or partition. Schemas belonging to one
partition tree share a marker group ID. This avoids collisions among same-named
relations originally located in different source schemas.

## Retention Marker

Mark each cleanup schema before moving a table into it:

```text
pg-schema-diff:retained-table:v1:<base64url-json>
```

The payload contains:

- Marker version and group ID.
- Source and cleanup schema-qualified table names.
- Group member schema names.
- Partition parent/child topology.
- Expected moved table-local object identities.
- Exclusive dependency schemas and objects.
- Shared cleanup-component edges to other marked groups.
- Original ACL/FK metadata changed for isolation.
- A digest of the generated cleanup statement sequence.

Use SQL-literal escaping for marker contents. A schema is treated as retained
state only when its marker, name, and catalog contents agree. Never ignore a
schema based only on its prefix.

Define the marker cleanup digest over canonical typed cleanup operations, not
rendered SQL. Each operation records version, kind, schema-qualified object
identity, dependency edges, and `RESTRICT` behavior. Canonically topologically
order operations with lexical vertex IDs as tie-breakers, then hash canonical
JSON with SHA-256 and domain `pg-schema-diff/cleanup-statements/v1`. Exclude SQL
whitespace, rendered quoting, hazard order/messages, and marker text. Formatting
or message changes therefore do not invalidate existing v1 markers; operation
semantic changes require a new digest version.

Markers have two generation purposes:

- Filter retained schemas from managed target comparison.
- Reconstruct `CleanupStatements` on later `Generate` calls, even when no new
  ordinary migration statements are needed.

If a later plan changes the global cleanup component, generate ordinary marker
updates for every affected group before returning the revised cleanup list.

No worker state, deadline, or execution progress is stored in the marker in this
design.

Progress is derived from marker intent plus current catalogs. On every
`Generate`:

- An empty valid marked schema means initialization completed but its move did
  not; reuse the recorded names and generate the missing regular move.
- A group with only some expected members moved is resumable only when every
  observed member matches the recorded topology; generate remaining moves and
  marker updates.
- A fully populated valid group is filtered from managed diffing and contributes
  cleanup vertices.
- A malformed marker, unexpected object, conflicting source object, or topology
  mismatch is a hard error.

Never feed a partial/invalid marked table back through ordinary table-removal
diffing, which could create a second retention group. Cleanup statements for a
partial group are generated from its predicted post-resume state and validated
only after the regular resumption statements run.

## Cheap Snapshot Move

The ordinary migration keeps table-local metadata rather than dropping it.

### Moved automatically

- Heap and rows.
- Composite row type and generated array type.
- Indexes, including constraint-backed indexes.
- Table constraints.
- Serial/identity sequences owned by table columns.

### Remain attached

- Columns, defaults, identity state, generated expressions, and fast-default
  `attmissingval` metadata.
- Triggers, user-defined rules, RLS state, and policies.
- Replica identity and clustering state.
- Comments, security labels, table/column settings, and table-local definitions.
- Partition attachments and bounds when the complete tree is retained.

### Explicit exceptions

- Extended statistics do not move automatically; generate
  `ALTER STATISTICS ... SET SCHEMA` into an appropriate marked group schema.
- TOAST relations remain PostgreSQL-owned objects in `pg_toast`; track them as
  internal marker members, not cleanup-schema relations.
- Revoke cleanup-schema access from all non-owner roles.
- For stronger isolation, revoke live table, column, and moved-sequence ACLs
  after recording their definitions in the marker.
- Drop every foreign key crossing the retention-group boundary and record its
  definition. An outgoing `CASCADE`, `SET NULL`, or `SET DEFAULT` FK could mutate
  retained rows when an active referenced row changes.
- Remove explicit publication membership. Moving out of a source schema ends
  source `FOR TABLES IN SCHEMA` membership. Reject `FOR ALL TABLES`.

ACL isolation requires a complete grant graph, not only grantee privilege names.
Inventory schema/table/column/sequence ACL entries with grantor, grantee, grant
option, and relevant default ACLs. Generate revokes in reverse grant-chain order,
or use a deliberately modeled `CASCADE` only after recording every affected grant.
Reject unsupported/cyclic/role-missing grant state rather than emitting a revoke
that can fail after the move.

Generate schema creation, ACL lockdown, and marker creation for every cleanup and
dependency schema in one compound `Statement`, implemented as a generated `DO`
block. PostgreSQL executes that statement transactionally, so failure rolls back
the complete logical group's initialization and cannot leave only some expected
member schemas. Subsequent move statements remain separately resumable through
the marker/catalog rules above.

## Consistent Catalog Snapshot

Cleanup generation, collision checks, marker validation, and dependency closure
must observe one catalog state. The current `schema.GetSchema` is explicitly
non-atomic and issues parallel queries through a pool, so it cannot be reused
unchanged.

For `DBSchemaSource`:

1. Acquire and pin one `pgx.Conn`.
2. Begin a `REPEATABLE READ, READ ONLY` transaction before any schema query.
3. Run the managed-schema fetch and every unfiltered safety/dependency query on
   that transaction. Sequential queries are acceptable initially.
4. Normalize the model and calculate the source hash before committing.

If parallel fetching remains necessary, export the transaction snapshot and
import it into parallel read-only transactions before they issue any query. Do
not mix unsnapshotted pool queries with snapshot-bound results.

DDL/directory sources already materialize into a temporary database. After all
DDL is applied, fetch the target and its relation/type namespace from one
read-only snapshot there as well.

Refactor the internal source/fetch interfaces to return a `SchemaSnapshot`
containing the modeled schema plus raw catalog safety inventory. All diff,
retention, cleanup, marker, and hash generation for one `Generate` call consumes
that immutable snapshot.

## Incoming Dependencies

Moving a table preserves OIDs. An unchanged dependent can therefore remain bound
to the retained table rather than a replacement created under the old name.

Inspect dependencies across all schemas, including excluded schemas.

The first version may automatically recreate only dependency types whose full
definition and metadata round-trip is supported. Foreign keys are the initial
safe allowlist. A dependent absent from the target can follow its normal explicit
deletion path only when that object is inside the managed schema scope and the
target explicitly omits it. A dependent excluded by schema filters is treated as
persistent and blocks the move; never generate deletion outside managed scope.

Reject persisting incoming views, materialized views, rules, routines, row-type
consumers, or other dependencies until owner, ACLs, comments, security labels,
definition, and transitive dependencies can be reproduced exactly. Do not hide
an expensive materialized-view rebuild inside the cheap move path.

Catalog dependency traversal cannot discover PL/pgSQL, dynamic SQL, or SQL
string-body references reliably. In the first version, a persisting routine in
an untrackable language/body form blocks table removal unless it is explicitly
deleted within managed scope. Only dependency forms PostgreSQL catalogs exactly,
such as supported SQL-standard `BEGIN ATOMIC` bodies and signatures, may be
classified automatically. Target routine/procedure additions that refer to the
replacement table or row type must run after replacement creation in the
cross-kind graph.

Reject a table-removal plan when:

- The same plan creates, updates, or drops an extension.
- The database has an enabled event trigger.
- The table or automatically moved/retained dependency is an extension member.
- An incoming dependency cannot be safely removed or recreated.

Independently of modeled table diffs, inspect extension membership before
generating extension statements. Reject an extension drop or update when that
extension owns any table/partition, because those relations are currently
excluded from table discovery and `DROP EXTENSION` or an update script could
delete their rows implicitly. This check applies even when no ordinary table
removal was otherwise detected.

These checks require a consistent source catalog snapshot and dependency
traversal outside `WithIncludeSchemaPatterns`/`WithExcludeSchemaPatterns`. Temporary database
validation alone cannot discover dependencies omitted from its modeled schema.

## Enums and Other Dependencies

`SET SCHEMA` does not move column enums, domains, collations, standalone
composite/range types, type I/O functions, trigger functions, partition support
objects, or extensions.

Classify the retained dependency closure:

- Leave compatible dependencies that remain managed by the target.
- Move an exclusive schema-scoped dependency into a marked dependency schema
  when its old name/schema must be freed.
- Keep shared dependencies in place when managed objects still use them.
- Reject removal or incompatible replacement of shared/non-movable dependencies.
- Reject extension removal/update when retained values depend on the extension.

Example exclusive enum move:

```sql
ALTER TYPE "public"."account_status"
    SET SCHEMA "<marked-group-dependency-schema>";
```

This preserves the enum OID and labels without rewriting table rows. The cleanup
statement graph later drops moved dependencies only after every retained table
that references them is dropped.

Do not convert custom-type columns to `text`; that would rewrite data and violate
the cheap migration goal.

## Partition Handling

Support declarative partition trees recursively.

For a complete removed tree:

- Create one cleanup schema per physical relation.
- Move descendants deepest-first and the root last.
- Preserve attachment and partition bounds.
- Generate one cleanup `DROP TABLE <root> RESTRICT`; PostgreSQL removes attached
  descendants as part of dropping the partitioned root.
- Follow with `DROP SCHEMA ... RESTRICT` for every now-empty member schema.

For a removed subtree whose parent remains active:

1. Create cleanup schemas for every subtree relation.
2. Move descendants and root while still attached.
3. Detach the moved subtree root from the active parent.
4. Record lost attachment/bound/index/clone-trigger metadata in the marker.
5. Generate cleanup against the moved subtree root, then its member schemas.

Reject traditional inheritance, multiple inheritance, and foreign-table
partitions in the first version. Current table fetching must be extended to
classify these cases explicitly.

## Regular Statement Generation

Introduce distinct graph phases for ordinary statements:

1. Create and mark cleanup/dependency schemas.
2. Drop allowed incoming dependencies and all cross-boundary FKs that must be
   absent before the move.
3. Move partition descendants/root or the ordinary table.
4. Detach standalone retained subtrees after their move.
5. Move extended statistics and exclusive dependency objects.
6. Remove explicit publication membership.
7. Create target replacement objects after source relation/type names are free.
8. Re-add supported incoming FKs/dependencies only after replacement tables,
   referenced tables, and required indexes/constraints exist.
9. Assert that markers and moved catalogs match the generated retention group.

The current table graph has only `DELETE` and `ADDALTER` vertices. Add schema
initialization, move, optional detach, dependency move/rewire, and assertion
vertices.

`deletedTablesByName` can no longer mean physical deletion. Split it into maps
for logically removed tables, moved tables, and tables physically deleted only by
cleanup statements. Update index and sequence generators so ordinary statements
do not drop or recreate objects that moved with the table.

Cross-kind dependencies are required. Target tables, views, materialized views,
sequences, indexes, enums, and supported types using a source relation/type name
must wait for the corresponding move.

## Cleanup Statement Generation

Generate `CleanupStatements` from a separate cleanup dependency graph after the
regular graph is complete. Inputs include:

- Retention groups predicted by the current plan.
- Existing valid marked groups discovered in the source database.
- Group member schemas, root tables/subtrees, and moved exclusive dependencies.

The cleanup graph is global across all newly predicted and existing marked
groups, not one isolated graph per table. Compute connected components through
dependencies used only by retained groups. If two retained roots use the same
enum/type/function, both table-drop vertices must precede the one shared
dependency-drop vertex.

When a dependency becomes retention-only after a later table removal, regular
statements may move it into a marked dependency schema and update every affected
existing marker with the shared cleanup component and new cleanup digest. A
dependency still used by the managed target remains outside cleanup. If marker
updates cannot be made consistently, reject the plan rather than assigning the
same dependency to multiple groups.

For each group, generate explicit operations in this order:

1. Drop each retained ordinary/root table with `DROP TABLE ... RESTRICT`.
2. Drop moved exclusive sequences, functions, types, collations, operators, or
   other dependency objects in dependency order when table drop does not remove
   them automatically.
3. Drop empty cleanup and dependency schemas with `DROP SCHEMA ... RESTRICT`.

Never generate `DROP SCHEMA ... CASCADE`. `RESTRICT` ensures later external
dependencies fail cleanup instead of deleting objects outside the retention
group.

Do not generate redundant child partition drops when dropping the retained root
already removes its attached tree. Detached subtree roots are independent cleanup
roots.

Topologically order the global cleanup graph. Use group ID/schema identity only
as the stable tie-breaker for independent vertices; no execution scheduling is
implied.

If a second `Generate` sees a completed retained group and no managed diff, it
returns empty `Statements` and reconstructs the same cleanup SQL in
`CleanupStatements`. Once the retained schemas are gone, their cleanup statements
are no longer generated.

## Plan Validation

Validation has a source-snapshot preflight plus two temporary-database
postconditions. The temporary database cannot prove facts about objects that the
current schema model does not reconstruct.

Before temporary execution:

- Validate all incoming dependencies, extension membership, event triggers,
  collisions, markers, and cleanup graph edges against the consistent source
  snapshot.
- Require every metadata category changed by generated SQL to be modeled and
  round-trippable, or reject the plan.
- Treat table-local metadata known to follow `SET SCHEMA` but not yet modeled as
  a statically checked preservation category with dedicated PostgreSQL
  integration tests; do not claim the synthetic database validated it.
- Validate role/ACL SQL through DBMS-wide tests or source inventory because the
  existing temporary path skips role-dependent statements and clears privileges
  before comparison.
- For existing retained groups, use source-snapshot dependency checks in addition
  to cleanup `RESTRICT`; omitted external objects cannot be recreated in the
  temporary database.

### Retention validation

1. Reconstruct the current modeled schema in a temporary database.
2. Apply `Plan.Statements` only.
3. Verify every predicted cleanup schema/marker/member.
4. Filter valid retained groups.
5. Assert the remaining managed modeled schema matches the target.

### Cleanup validation

Using the same post-retention temporary database:

1. Apply `Plan.CleanupStatements` in order.
2. Assert every referenced retention group/schema is gone.
3. Assert no cleanup statement deleted or changed a managed target object.
4. Assert the complete remaining modeled schema matches the target without
   retention filtering.

Cleanup validation proves generated SQL is correct for the modeled generated
retention state. Source preflight and dedicated integration tests cover
unmodeled/external state. It does not guarantee future executability after
arbitrary database drift.

The current acceptance harness compares complete schema-only dumps. For
retention tests, compare the filtered managed dump after regular statements, then
apply cleanup statements and compare the complete dump with the target database.
Add data tests with inserted rows to prove regular statements preserve values and
cleanup statements delete them only in the second phase.

## Hazards

Regular statements:

- Do not use `TABLE_REMOVAL`.
- Do not attach table-level `DELETES_DATA`; rows remain.
- Attach accurate strong-lock hazards to `SET SCHEMA` and partition detach.
- Attach `AUTHZ_UPDATE` when ACLs are revoked.
- Attach `CORRECTNESS` to every cross-boundary FK removal, even though the current
  FK delete generator emits no hazard. Also attach the accurate
  `ACQUIRES_ACCESS_EXCLUSIVE_LOCK` hazard for `ALTER TABLE ... DROP CONSTRAINT`.
  Apply correctness/lock hazards to other namespace/dependency/publication
  changes where applicable.

Cleanup statements:

- Attach `DELETES_DATA` to retained table drops.
- Attach `DELETES_DATA` to explicitly dropped standalone/moved sequences because
  their current counters are lost, matching existing sequence-drop behavior.
- Attach actual lock hazards.
- Attach index-removal hazards when dropping the retained table removes its
  indexes.
- Attach correctness hazards to moved type/function/dependency removal where
  applicable.

Keeping cleanup hazards on their own statement list makes the destructive phase
explicit without warning that ordinary migration statements delete rows.

## Compatibility

Adding `CleanupStatements` is additive for JSON and Go callers that use keyed
struct literals or accessors. It is a source-breaking change for unkeyed
`diff.Plan{...}` literals because `Plan` currently has two public fields; call
this out in release notes. Existing application loops over `Plan.Statements`
continue to apply only the ordinary, non-destructive migration.

Consumers that serialize complete plans gain a new `cleanup_statements` field.
Document that concatenating the two lists changes semantics and is unsupported.

Expand `CurrentSchemaHash` to cover the complete immutable source snapshot used
for ordinary generation: managed modeled objects, retention markers/groups,
excluded-schema safety dependencies, extension membership, enabled event
triggers, publication state, ACL inventory, and relation/type namespaces.
Refactor public `pkg/schema.GetSchemaHash` to acquire the same snapshot and use
the same filter/safety-closure semantics so callers can reproduce the plan hash.
This changes hash compatibility and must be versioned/documented.

This design does not introduce a future cleanup execution hash or cleanup
application contract.

## Implementation Outline

1. Add the default/custom schema prefix, plan timestamp, and random group nonce
   to `planOptions`.
2. Extend `Plan` with `CleanupStatements`.
3. Refactor schema sources/fetching to produce one immutable `SchemaSnapshot`
   from a pinned repeatable-read transaction (or imported exported snapshot).
4. Extend catalog inventory with raw identities, schema markers, partition kinds,
   extended statistics, complete ACL grant graphs/default ACLs, FK metadata,
   publications, extension/event-trigger state, target type namespaces, and
   incoming/outgoing dependencies.
5. Build retention-group, partial-state, shared cleanup-component, and retained-
   dependency models.
6. Discover/resume existing marked groups before filtering complete groups from
   managed diffing.
7. Build collision-checked cleanup/dependency schema names.
8. Define versioned canonical cleanup operations/digests and emit one compound
   initialization statement per logical group.
9. Split regular table deletion into schema initialization, move, optional
   detach, dependency move/rewire, and assertion graph phases.
10. Replace physical-delete assumptions in table/index/sequence generators.
11. Add cross-kind dependencies from target additions to source moves and from
    replacement creation to supported dependency re-adds.
12. Build one global cleanup graph from predicted/existing groups and shared
    retention-only dependencies; generate marker updates when components change.
13. Emit explicit `RESTRICT` table/dependency/schema drops into
    `CleanupStatements`.
14. Validate source preflight, regular statements, and cleanup statements as
    distinct layers.
15. Reject extension drops/updates that can implicitly remove member tables.
16. Remove ordinary generated `DROP TABLE` and its table-level `DELETES_DATA`.
17. Add package documentation and README examples for both statement lists.

## Testing

Unit tests should cover:

- Prefix (including `pg`/`pg_` rejection), timestamp/nonce, UTF-8 truncation,
  quoting, and collisions.
- Marker parsing, partial-group resumption/failure, marker-component updates, and
  cleanup statement reconstruction.
- Canonical cleanup operation/digest test vectors stable across SQL formatting
  and hazard-message changes.
- One compound initialization statement rolling back every group schema/marker
  on failure.
- `Plan` JSON round-trip with regular and cleanup lists.
- `InsertStatement` leaving `CleanupStatements` unchanged.
- Regular graph move ordering and cleanup graph deletion ordering.
- Cross-kind target ordering, retained dependency closure, and shared dependency
  edges across new/existing groups.
- Incoming FK drop-before-move and re-add-after-replacement/index ordering.
- Excluded dependencies and untrackable persisting routines rejected fail-closed.
- ACL grant-chain reverse ordering/default ACL handling and unsupported-chain
  rejection.
- Cleanup hazards appearing only in `CleanupStatements`.
- Explicit sequence `DELETES_DATA` and cross-boundary FK `CORRECTNESS` hazards.
- No ordinary `DROP TABLE`, `DELETE`, `TRUNCATE`, or table-level
  `DELETES_DATA`.

Database tests should cover:

- Ordinary tables retaining rows, indexes, constraints, sequences, triggers,
  policies, defaults, comments, and generated values after regular statements.
- Cleanup statements removing the retained table and empty schema.
- Recursive cross-schema partition trees and detached subtrees.
- Exclusive enums/domains/functions moved and later cleaned up in dependency
  order.
- Shared dependency and unsupported incoming-dependency rejection.
- Explicit/source-schema publications and all-table rejection.
- Extension-member/change and enabled-event-trigger rejection.
- Extension drop/update owning hidden table members rejected even without a
  modeled table diff.
- Consistent snapshot behavior under concurrent catalog changes.
- Unsupported/unmodeled metadata rejected or covered by static/integration
  validation rather than synthetic-database claims.
- Table recreation with all source relation/type names freed.
- Existing marked groups regenerating cleanup statements with no ordinary diff.
- Cleanup validation leaving the exact target schema.

## Alternatives Considered

### Store only cleanup descriptors

Rejected for this scope. Returning concrete cleanup statements is more consistent
with the existing `Plan` API and makes the intended eventual deletion directly
reviewable. Future execution tooling may add fresh-state validation separately.

### Append cleanup statements to ordinary statements

Rejected. It would immediately delete retained rows and defeat the retention
design. The separate field is a semantic boundary.

### One shared cleanup schema

Rejected. Repeated removals and cross-schema partitions can contain identical
table, index, sequence, or type names.

### Strip metadata after moving

Rejected. Unique schemas already isolate names, and retaining metadata keeps the
move cheap and recoverable.

### Copy rows into an archive table

Rejected. It requires a potentially long copy and unnecessary index rebuilds.

## Acceptance Criteria

- `Plan.Statements` never directly or implicitly deletes removed table rows.
- Every removed table/partition moves into a unique marked cleanup schema without
  copying rows or rebuilding indexes.
- Table-local metadata remains with the retained snapshot except explicitly
  isolated cross-boundary/security metadata recorded by the marker.
- `Plan.CleanupStatements` contains the complete ordered `RESTRICT` cleanup for
  every predicted or existing retention group.
- Applying only regular statements reaches the target after filtering marked
  cleanup schemas.
- Applying cleanup statements afterward removes retained groups and leaves the
  exact unfiltered target schema.
- Cleanup statements use the existing `Statement` type and carry destructive
  hazards; regular table moves do not carry `DELETES_DATA`.
- A later plan regenerates cleanup statements for retained marked groups even
  when ordinary statements are empty.
- Worker/scheduler/execution design remains explicitly out of scope.
