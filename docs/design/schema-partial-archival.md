# Schema Partial Archival

Status: Proposed; delivery Stages 0-8 complete

## Summary

Schema partial archival separates logical removal of selected objects from their
later physical deletion. The initial scope of this design archives tables,
partition trees, table-local objects, and dependencies required by retained table
data. Archival of unrelated standalone schema objects can extend the same model in
future designs.

Within this initial scope, normal migration statements must no longer emit
`DROP TABLE`. When a table is absent from the target schema, create a unique
cleanup schema and move the table into it with `ALTER TABLE ... SET SCHEMA`.

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
    IS '<pg-schema-diff archival marker>';
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

### Implemented foundation

The first eight delivery stages are complete. They do not yet retain tables or
generate cleanup statements; ordinary table deletion still has the behavior
described above.

The implemented foundation includes:

- `WithIncludeSchemaPatterns` and `WithExcludeSchemaPatterns` in `pkg/diff` and
  `pkg/schema`. Patterns are Go regular expressions automatically anchored to the
  complete schema name. Multiple include patterns form a union, multiple exclude
  patterns form a union, and exclusion wins when the sets overlap.
- `schema.DefaultCleanupSchemaPrefix`, currently `pgschemadiff_archive`.
- `diff.WithSchemaPartialArchivalPrefix`, including simple-identifier, `pg`/`pg_`,
  and 21-byte length validation.
- Automatic exclusion of schemas whose names start with the selected cleanup
  prefix from the current schema, target schema, plan validation, and
  `CurrentSchemaHash`. A custom prefix replaces the default; the two prefixes are
  not both excluded.
- Default cleanup-schema exclusion in `schema.GetSchemaHash`, implemented through
  `WithExcludeSchemaPatterns` so caller-provided exclusions accumulate with it.
- `Plan.CleanupStatements`, with empty cleanup lists omitted from JSON. Generated
  plans currently leave the list empty, and `Plan.InsertStatement` modifies only
  the ordinary `Statements` list.
- One UTC generation timestamp captured in the internal generation context per
  `Generate` call. The timestamp does not yet affect SQL or serialized plans.
- Consistent catalog snapshots for every database-backed schema fetch. Each
  fetch pins one connection, runs sequential catalog queries in a read-only
  repeatable-read transaction, and normalizes and hashes the modeled schema
  before committing.
- A normalized catalog inventory alongside each snapshot with unfiltered user
  schema, relation/type namespace, index, constraint, sequence, TOAST, extension,
  inheritance, and table-local preservation metadata. Typed expected-move
  identities distinguish cleanup-schema followers, attached subobjects,
  explicit extended-statistics moves, and PostgreSQL-owned TOAST state. The
  inventory does not yet affect hashes or diffs.
- Unfiltered raw dependency edges, complete foreign keys, dependent
  relation/routine and type-platform identities, extension membership, event
  triggers, and all publication forms. Routine body facts are classified
  conservatively as catalog-trackable or untrackable without making a safety
  decision.
- Expanded unfiltered schema, table/column, sequence, routine, type, and default
  ACLs plus role identities and membership options. A dormant typed planner
  validates explicit grant ancestry and orders dependent revokes without
  rendering SQL or using `CASCADE`.
- Dormant strict v1 marker and typed cleanup-operation codecs with canonical
  ordering, validation, and domain-separated cleanup digests.

The current prefix-only exclusion is transitional. It can hide an unrelated
user-created schema with the same prefix. The complete archival implementation
must replace this trust-by-name behavior with the marker and catalog validation
defined below.

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
- Validate both the archival migration and generated cleanup SQL.
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
- Archiving standalone schema objects that are unrelated to an archived table or
  its retained dependency closure.
- Adding a CLI.
- Supporting traditional inheritance or foreign-table partitions in the first
  version.

## Public API

The filtering foundation uses `pgschemadiff_archive` as the default
cleanup-schema prefix and exposes the feature-level API:

```go
// WithSchemaPartialArchivalPrefix configures the prefix used to identify archival
// schemas. Schemas whose names start with this prefix are excluded from plan
// generation.
func WithSchemaPartialArchivalPrefix(prefix string) PlanOpt
```

Example:

```go
plan, err := diff.Generate(
    ctx,
    diff.DBSchemaSource(connPool),
    diff.DDLSchemaSource(targetDDL),
    diff.WithTempDbFactory(tempDbFactory),
    diff.WithSchemaPartialArchivalPrefix("deleted"),
)
```

Omitting the option uses `pgschemadiff_archive`. Passing an empty or invalid
prefix returns an error. Passing a custom prefix replaces the default exclusion;
for example, `deleted` excludes `deleted.*` schema names but does not exclude
`pgschemadiff_archive.*` schema names. Pattern notation here describes the
equivalent anchored Go regular expression, not a PostgreSQL `LIKE` expression.

The underlying schema-filter APIs are:

```go
func WithIncludeSchemaPatterns(patterns ...string) GetSchemaOpt
func WithExcludeSchemaPatterns(patterns ...string) GetSchemaOpt
```

Each regular expression is compiled as `^(?:<pattern>)$`. Include and exclude
patterns accumulate. `Generate` derives an exclusion pattern from
`WithSchemaPartialArchivalPrefix` after processing other plan options, so callers
should configure plan archival filtering through that option. Additional schema
hash exclusions can be passed to `schema.GetSchemaHash`, for example:

```go
hash, err := schema.GetSchemaHash(
    ctx,
    connPool,
    schema.WithExcludeSchemaPatterns("deleted.*"),
)
```

The future archival implementation will use the same prefix for generated schema
names. Once archival is implemented, an invalid prefix must never restore
ordinary `DROP TABLE` generation.

`CleanupStatements` is currently always empty in generated plans. Once retention
is implemented, existing callers that apply only `plan.Statements` will retain
removed tables and not delete data. Consumers will be able to inspect, serialize,
or separately persist `plan.CleanupStatements` using the same `Statement` JSON and
`ToSQL` behavior as regular statements.

`Plan.InsertStatement` will continue to modify only the ordinary statement list.
Do not add cleanup-list mutation in the first version: marker cleanup digests are
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

## Archival Marker

Mark each cleanup schema before moving a table into it:

```text
pg-schema-diff:schema-partial-archival:v1:<base64url-json>
```

The payload contains:

- Marker version and group ID.
- Source and cleanup schema-qualified table names.
- Group member schema names.
- Partition parent/child topology.
- Expected moved table-local object identities.
- Exclusive dependency schemas and objects.
- Shared cleanup-component edges to other marked groups.
- Original ACL/FK and explicit publication-membership metadata changed for
  isolation.
- A digest of the generated cleanup statement sequence.

Use SQL-literal escaping for marker contents. In the completed design, a schema is
treated as retained state only when its marker, name, and catalog contents agree.
Never continue relying only on the transitional prefix filter once marker support
exists.

Define the marker cleanup digest over canonical typed cleanup operations, not
rendered SQL. Each operation records version, kind, schema-qualified object
identity, dependency edges, and `RESTRICT` behavior. Canonically topologically
order operations with lexical vertex IDs as tie-breakers, then hash canonical
JSON with SHA-256 and domain `pg-schema-diff/cleanup-statements/v1`. Exclude SQL
whitespace, rendered quoting, hazard order/messages, and marker text. Formatting
or message changes therefore do not invalidate existing v1 markers; operation
semantic changes require a new digest version.

The v1 envelope encodes canonical JSON with unpadded base64url. Cleanup digests
use lowercase `sha256:<64 hex>` and hash the UTF-8 domain, one `0x00` separator
byte, and canonical cleanup-operation JSON in that order.

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

Never feed a partial/invalid marked table back through ordinary table-archival
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
must observe one catalog state. Schema acquisition now provides that foundation
instead of issuing parallel queries through a pool.

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

Internal source/fetch interfaces return a `SchemaSnapshot` containing the
normalized modeled schema and its existing hash. Later inventory stages extend
that transport with raw catalog safety inventory. All diff, retention, cleanup,
marker, and hash generation for one `Generate` call consumes that immutable
snapshot.

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
an untrackable language/body form blocks table archival unless it is explicitly
deleted within managed scope. Only dependency forms PostgreSQL catalogs exactly,
such as supported SQL-standard `BEGIN ATOMIC` bodies and signatures, may be
classified automatically. Target routine/procedure additions that refer to the
replacement table or row type must run after replacement creation in the
cross-kind graph.

Reject a table-archival plan when:

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

When a dependency becomes retention-only after a later table archival, regular
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

The implemented filtering foundation renamed `WithIncludeSchemas` and
`WithExcludeSchemas` to `WithIncludeSchemaPatterns` and
`WithExcludeSchemaPatterns`. This is a source-breaking API change. Patterns now
support full Go regular expressions and are automatically anchored to the entire
schema name.

`CurrentSchemaHash` omits schemas matching the active cleanup pattern.
`schema.GetSchemaHash` omits schemas matching the default cleanup prefix and any
additional patterns supplied through `WithExcludeSchemaPatterns`.

The addition of `CleanupStatements` is additive for JSON and Go callers that use
keyed struct literals or accessors. It is a source-breaking change for unkeyed
`diff.Plan{...}` literals because `Plan` now has three public fields; this must be
called out in release notes. Existing application loops over `Plan.Statements`
continue to apply the ordinary migration only.

Consumers that serialize complete plans gain a new `cleanup_statements` field.
Document that concatenating the two lists changes semantics and is unsupported.

Expand `CurrentSchemaHash` to cover the complete immutable source snapshot used
for ordinary generation: managed modeled objects, archival markers/groups,
excluded-schema safety dependencies, extension membership, enabled event
triggers, publication state, ACL inventory, and relation/type namespaces.
Refactor public `pkg/schema.GetSchemaHash` to acquire the same snapshot and use
the same filter/safety-closure semantics so callers can reproduce the plan hash.
This changes hash compatibility and must be versioned/documented.

This design does not introduce a future cleanup execution hash or cleanup
application contract.

## Delivery Stages

Each stage is a separate, independently mergeable delivery unit. An agent assigned
to a stage should implement only that stage, start from revisions containing every
listed prerequisite, and leave the repository compiling with its full existing
test suite passing. Generated files, migrations of test fakes, and package-level
documentation needed by an API change are part of the stage that causes them.

Stages 1 through 18 must not activate schema partial archival in public
`Generate`. Until Stage 19, ordinary table removal continues to use the legacy
physical-delete path. Dormant retention components may be exercised through
internal tests. Unsupported state must be rejected by a dormant component rather
than handled partially. Stage 1 intentionally changes the public `Plan` envelope,
but Stage 10's independent extension safety check is the only intended runtime
plan-generation behavior change before activation. The versioned hash built in
Stage 18 also remains dormant until Stage 19.

When a stage is merged, update its status here. If implementation reveals that a
stage contract is unsafe or incomplete, revise this design before expanding that
stage's scope.

| Stage | Deliverable | Status | Depends on |
| --- | --- | --- | --- |
| 0 | Schema-filtering foundation | Complete | None |
| 1 | Plan envelope and generation context | Complete | 0 |
| 2 | Consistent catalog snapshots | Complete | 0 |
| 3 | Namespace and partition inventory | Complete | 2 |
| 4 | Table-local metadata inventory | Complete | 3 |
| 5 | Dependency and platform inventory | Complete | 3 |
| 6 | ACL inventory and revoke planner | Complete | 3 |
| 7 | Archival name allocation | Complete | 1, 3 |
| 8 | Marker and cleanup-operation codecs | Complete | 4, 5, 6, 7 |
| 9 | Archived-state resolver | Pending | 8 |
| 10 | Source safety preflight | Pending | 5, 9 |
| 11 | Archived dependency closure | Pending | 5, 9, 10 |
| 12 | Dormant plain-table move engine | Pending | 7, 9, 10, 11 |
| 13 | Replacement-aware regular graph | Pending | 12 |
| 14 | Isolation and dependency rewiring | Pending | 6, 10, 11, 13 |
| 15 | Declarative partition retention | Pending | 3, 14 |
| 16 | Global cleanup graph | Pending | 1, 8, 11, 15 |
| 17 | Two-phase plan validation | Pending | 4, 6, 10, 16 |
| 18 | Versioned snapshot hash | Pending | 2, 4, 5, 6, 9, 10, 11, 16 |
| 19 | Schema partial archival activation and documentation | Pending | 13-18 |

### Stage 0: Schema-filtering foundation

Status: Complete.

Depends on: None.

Deliverables:

- Anchored Go regular expressions for include and exclude schema filters.
- Cleanup-prefix filtering through the accumulating exclude-schema patterns.
- Default/custom cleanup prefixes, validation, transitional plan filtering, and
  matching `schema.GetSchemaHash` behavior.
- Tests listed under "Implemented foundation tests" below.

Out of scope:

- Marker-aware filtering, archived-table moves, cleanup statements, or any change
  to ordinary `DROP TABLE` generation.

Acceptance gate:

- Existing schema-filter, prefix, plan-filtering, and public hash tests pass.

### Stage 1: Plan envelope and generation context

Status: Complete.

Depends on: Stage 0.

Deliverables:

- Add `Plan.CleanupStatements []Statement` with
  `json:"cleanup_statements,omitempty"`.
- Capture one UTC generation timestamp per `Generate` call and carry it in the
  internal generation context without using it to change SQL yet.
- Rename `WithTableRemovalSchemaPrefix` to
  `WithSchemaPartialArchivalPrefix` across the public API, implementation, and
  tests.
- Add JSON round-trip tests with both statement lists, omission tests for an empty
  cleanup list, and tests proving `Plan.InsertStatement` changes only
  `Statements`.
- Add a deterministic clock seam and tests proving `Generate` reads it once,
  normalizes the value to UTC, and stores that single value in the generation
  context.
- Add package/API documentation for the two-list boundary and the source break for
  callers using unkeyed `Plan{...}` literals.

Out of scope:

- Cleanup SQL, marker digests, schema-name allocation, and table-archival behavior
  changes.

Acceptance gate:

- Existing plan SQL is unchanged and every generated cleanup list is empty.
- The generation timestamp contract is tested without depending on wall-clock
  timing.

### Stage 2: Consistent catalog snapshots

Status: Complete.

Depends on: Stage 0.

Deliverables:

- Introduce an immutable internal `SchemaSnapshot` transport.
- Refactor catalog fetchers to accept transaction-capable query interfaces rather
  than independently using a pool.
- Acquire one connection and start `REPEATABLE READ, READ ONLY` before the first
  catalog query. Run source normalization and the existing hash against that
  snapshot before commit.
- Apply all DDL for file/directory sources before opening one read-only target
  snapshot.
- Update schema-source interfaces, fakes, and public hash plumbing without
  changing modeled output.

Out of scope:

- New inventory categories, expanded hash semantics, marker filtering, and
  retention SQL.

Acceptance gate:

- Existing plans and hashes remain behaviorally unchanged.
- Tests prove all queries for one source fetch are snapshot-bound and concurrent
  DDL cannot produce a mixed catalog state.

### Stage 3: Namespace and partition inventory

Status: Complete.

Depends on: Stage 2.

Deliverables:

- Add unfiltered identities for schemas/comments/owners, relations, row and array
  types, indexes, constraints, sequences, TOAST objects, and relation/type
  namespaces.
- Inventory relation kinds, `relispartition`, every inheritance edge, extension
  ownership, and source/target OIDs used only inside the snapshot.
- Classify ordinary tables, partitioned tables, declarative partitions, foreign
  tables, traditional inheritance, and multiple inheritance explicitly.
- Add SQL queries, regenerated sqlc output, normalized models, and database
  fixtures for these categories.

Out of scope:

- Dependency traversal, ACLs, marker interpretation, and generated SQL.

Acceptance gate:

- Tests cover recursive cross-schema partitions, foreign partitions, traditional
  and multiple inheritance, extension-hidden relations, TOAST objects, and
  relation/type namespace collisions.
- Inventory remains unfiltered when managed-schema filters exclude a schema.

### Stage 4: Table-local metadata inventory

Status: Complete.

Depends on: Stage 3.

Deliverables:

- Inventory columns and `attmissingval`, defaults, identity/generated
  expressions, indexes, constraints, triggers, rules, RLS/policies, replica
  identity, clustering, comments, security labels, and table/column settings.
- Inventory owned sequences, extended statistics, partition bounds/attachments,
  and cloned/internal trigger relationships.
- Define expected moved-object identities and distinguish cleanup-schema objects
  from PostgreSQL-owned TOAST members.
- Add integration fixtures for every metadata category expected to survive
  `SET SCHEMA`.

Out of scope:

- ACL grant chains, external dependencies, marker encoding, and move SQL.

Acceptance gate:

- A snapshot can enumerate everything expected to follow a table move, identify
  extended statistics requiring an explicit move, and detect unexpected local
  objects in a retained schema.

### Stage 5: Dependency and platform inventory

Status: Complete.

Depends on: Stage 3.

Deliverables:

- Add unfiltered dependency edges and complete FK definitions/actions/validation.
- Inventory views, materialized views, rules, routines, row-type consumers,
  routine language/body forms, domains, enums, composite/range types, collations,
  operators, functions/type-I/O functions, and standalone sequences.
- Inventory explicit/schema/all-table publications, extension membership,
  extension-owned tables, and enabled event triggers.
- Add target dependency/namespace data and classifications for catalog-trackable
  versus untrackable routine references.

Out of scope:

- Dependency movement decisions, ACL analysis, rewiring SQL, and retained-schema
  filtering.

Acceptance gate:

- Tests discover dependencies through excluded schemas, distinguish every
  publication form, find extension members hidden from modeled discovery, and
  classify routine references conservatively.

### Stage 6: ACL inventory and revoke planner

Status: Complete.

Depends on: Stage 3.

Deliverables:

- Model relevant schema, table, column, sequence, function/type, and default ACLs
  with owner, grantor, grantee, privilege, grant option, and role identity.
- Build a deterministic grant graph and reverse grant-chain revoke plan.
- Detect missing roles, cycles, and unsupported chains before SQL generation.
- Add DBMS-wide fixtures because temporary databases do not recreate role state.

Out of scope:

- Emitting retention revokes or changing existing privilege generation.

Acceptance gate:

- Tests cover multi-hop grants, grant options, `PUBLIC`, column/sequence grants,
  default ACL effects, reverse ordering, cycles, and missing roles.
- Unsupported ACL state fails closed.

### Stage 7: Archival name allocation

Status: Complete.

Depends on: Stages 1 and 3.

Deliverables:

- Allocate group IDs and cleanup/dependency schema names from the selected prefix,
  the per-plan timestamp, and an independent eight-character identifier-safe
  nonce from `crypto/rand`.
- Enforce the 63-byte identifier limit without truncating the prefix or suffix.
  Truncate source components on UTF-8 rune boundaries while preserving at least
  one rune from each.
- Build and quote every name before graph construction.
- Reject allocation duplicates, truncation collisions, source/target namespace
  collisions, target use of the reserved naming grammar, and moved relation/type
  collisions.
- Allocate one schema per physical relation and one shared group ID per partition
  tree.

Out of scope:

- Creating schemas, parsing markers, and moving tables.

Acceptance gate:

- Pure unit tests cover timestamp format, nonce uniqueness, deterministic random
  seams, UTF-8 truncation, quoting, every collision class, and no partial output on
  failure.
- Integration tests prove every group allocated by one plan uses the same captured
  timestamp while receiving an independent nonce.

### Stage 8: Marker and cleanup-operation codecs

Status: Complete.

Depends on: Stages 4, 5, 6, and 7.

Deliverables:

- Define the versioned archival marker with group/member identities, source and
  cleanup names, topology, expected local/TOAST objects, exclusive dependencies,
  shared-component edges, original ACL/FK and explicit publication-membership
  metadata, and cleanup digest.
- Implement strict marshal, parse, validation, SQL-literal escaping, and the
  `pg-schema-diff:schema-partial-archival:v1:` envelope.
- Define typed canonical cleanup operations with version, kind, object identity,
  dependency edges, and `RESTRICT` semantics.
- Canonically topologically order operations with lexical vertex-ID tie-breaking,
  serialize canonical JSON, and hash it with SHA-256 under
  `pg-schema-diff/cleanup-statements/v1`.

Out of scope:

- Catalog discovery, SQL rendering, global cleanup planning, and worker state.

Acceptance gate:

- Stable vectors prove formatting, quoting, hazard order/messages, and marker text
  do not alter a digest while semantic operation changes do.
- Malformed versions, payloads, topology, or duplicate vertices fail closed.

### Stage 9: Archived-state resolver

Status: Pending.

Depends on: Stage 8.

Deliverables:

- Discover candidate retained schemas from the unfiltered snapshot.
- Validate marker structure, configured naming pattern, catalog contents, expected
  local identities, and topology together.
- Classify groups as structurally valid candidates, empty-initialized,
  partial-resumable, or invalid. Dependency-edge, shared-component, and cleanup-
  digest trust is finalized only in Stages 11 and 16.
- Produce candidate-group filtering and resume descriptors that reuse recorded
  names, but do not treat candidates as trusted or expose this view to public
  `Generate` yet.
- Reject malformed markers, unexpected objects, conflicting source objects,
  topology mismatches, and target schemas using the reserved grammar.

Out of scope:

- Emitting resume moves, cleanup SQL, and replacing transitional public prefix
  filtering.

Acceptance gate:

- Tests cover every state, marker/name disagreement, custom prefixes, unexpected
  contents, partial partition trees, source conflicts, and direct calls with
  cleanup exclusion disabled.
- Partial or invalid state is never returned to ordinary table-archival diffing,
  and no candidate is trusted solely from Stage 9 validation.

### Stage 10: Source safety preflight

Status: Pending.

Depends on: Stages 5 and 9.

Deliverables:

- Classify incoming dependencies by managed scope and target intent before graph
  construction. Only managed objects explicitly absent from the target may follow
  their normal deletion path.
- Reject persisting/excluded views, materialized views, rules, row-type consumers,
  unsupported routines, and other dependencies that cannot round-trip safely.
- Use FKs as the only initial incoming-dependency allowlist.
- Reject enabled event triggers, extension-member retained objects, retention
  during extension create/update/drop, and `FOR ALL TABLES` publications.
- Independently reject extension drop/update when the extension owns any hidden
  table or partition, even without a modeled table diff.

Out of scope:

- FK SQL, dependency moves, table moves, and expansion of the dependency
  allowlist.

Acceptance gate:

- Tests cover managed explicit deletion, excluded persistent dependencies,
  catalog-trackable SQL routines, untrackable routine blockers, event triggers,
  extension members, and hidden extension-owned relations.
- Every rejection happens before SQL graph construction.

### Stage 11: Archived dependency closure

Status: Pending.

Depends on: Stages 5, 9, and 10.

Deliverables:

- Classify dependencies required by retained rows as target-compatible, exclusive
  movable, shared with the managed target, shared only by retained groups, or
  unsupported/non-movable.
- Cover enums, domains, standalone composite/range types, collations, functions,
  type-support functions, operators, standalone sequences, and extensions.
- Produce deterministic transitive closure, dependency-schema assignments, and
  shared cleanup-component edges.
- Validate candidate marker dependency identities and edges against the computed
  closure; leave global component and digest validation to Stage 16.
- Reject incompatible replacement/removal of shared dependencies and extension
  changes affecting retained values.

Out of scope:

- Rendering dependency moves or cleanup drops and coercing custom types to
  `text`.

Acceptance gate:

- Tests cover exclusive, target-shared, retained-only-shared, transitive,
  unsupported, and extension-backed dependency cases.

### Stage 12: Dormant plain-table move engine

Status: Pending.

Depends on: Stages 7, 9, 10, and 11.

Deliverables:

- Add dormant regular graph phases for cleanup-group initialization, ordinary
  table moves, resume moves, and post-move catalog assertions.
- Render one transactional compound `DO` statement per logical group that creates
  all member/dependency schemas, locks down schema access, and installs finalized
  marker payloads supplied by the caller.
- Emit `ALTER TABLE ... SET SCHEMA` for non-partition ordinary tables and use
  `CREATE SCHEMA` as the final uniqueness check.
- Emit required marker updates for empty and partially populated groups before the
  final post-move catalog assertion.
- Add accurate strong-lock hazards for moves.

Out of scope:

- Public activation, partitions, cross-boundary FK handling, live-object ACL
  revokes, publication changes, dependency moves, and cleanup output.

Acceptance gate:

- Internal tests prove rows are not copied, heap OIDs remain stable, local names
  leave the source schema, and heap, row/composite type, index, TOAST, and owned-
  sequence OIDs remain stable. They also prove initialization precedes moves,
  assertions run last, partial states resume with updated marker contents, and
  compound initialization rolls back atomically.
- The dormant engine rejects cases assigned to later stages and never emits
  `DROP TABLE` or table-level `DELETES_DATA`.

### Stage 13: Replacement-aware regular graph

Status: Pending.

Depends on: Stage 12.

Deliverables:

- Split table disposition into logically removed, moved, and physically deleted
  only by cleanup.
- Stop ordinary index, owned-sequence, trigger, and other independently generated
  table-local child deletion paths from dropping or recreating objects that move
  with a table.
- Hold target replacements until source relation, row-type, index, and
  owned-sequence names are free.
- Add cross-kind ordering for target tables, views, materialized views, sequences,
  indexes, enums, supported types, functions, and procedures.
- Support recreation-required tables through retain-then-create in internal graph
  tests.

Out of scope:

- Rename detection, isolation/rewiring SQL, partitions, cleanup output, and public
  activation. The public legacy path retains its physical-delete disposition.

Acceptance gate:

- Graph tests cover removal, recreation, namespace conflicts, dependent target
  additions, trigger preservation, other table-local child preservation, and
  unchanged non-removal migrations.

### Stage 14: Isolation and dependency rewiring

Status: Pending.

Depends on: Stages 6, 10, 11, and 13.

Deliverables:

- Record and revoke cleanup-schema, table, column, and moved-sequence access using
  reverse grant-chain order.
- Drop every cross-boundary FK before a move. Recreate supported managed incoming
  FKs only after replacement tables and required indexes/constraints exist.
- Preserve self-referential and same-group FKs that move with their tables, and
  suppress their ordinary FK-delete vertices.
- Move extended statistics and exclusive dependencies into their assigned marked
  schemas.
- Remove explicit publication membership and assert source-schema publication
  behavior.
- Record changed ACL/FK/publication metadata in markers and add final catalog
  assertions.
- Attach `AUTHZ_UPDATE`, FK `CORRECTNESS` and access-exclusive-lock, and accurate
  dependency/publication hazards.

Out of scope:

- Unsupported dependency recreation, deletion outside managed scope, `CASCADE`
  cleanup, partitions, and public activation.

Acceptance gate:

- DBMS-wide tests cover grant chains/default ACLs, outgoing and incoming FK
  actions, self-referential and same-group FK preservation, FK re-add ordering,
  publication forms, moved statistics/dependencies, and fail-closed unsupported
  states.

### Stage 15: Declarative partition retention

Status: Pending.

Depends on: Stages 3 and 14.

Deliverables:

- Extend the dormant engine to recursive declarative partition trees, one schema
  per physical relation and one group ID per tree.
- For complete removals, move descendants deepest-first and the root last while
  preserving attachment and bounds.
- For a removed subtree whose parent remains active, move the subtree while
  attached and detach its root afterward.
- Record topology and lost attachment/bound/index/clone-trigger metadata.
- Define one cleanup-root intent per complete tree or detached subtree.
- Attach the accurate strong-lock hazard to partition detach statements.
- Explicitly reject traditional inheritance, multiple inheritance, and
  foreign-table partitions.

Out of scope:

- Support for rejected inheritance forms and redundant child cleanup drops.

Acceptance gate:

- Database tests cover multi-level/cross-schema trees, partitioned partitions,
  complete removals, retained-parent subtree removals, metadata preservation,
  access-exclusive detach hazards, and every unsupported form. Pre/post identity
  checks cover heap, row/composite type, index, TOAST, and owned-sequence OIDs for
  every physical tree member.

### Stage 16: Global cleanup graph

Status: Pending.

Depends on: Stages 1, 8, 11, and 15.

Deliverables:

- Build one cleanup graph from the predicted post-regular state and every existing
  structurally valid candidate retained group.
- Connect groups through retained-only shared dependencies, finalize canonical
  operations/digests, feed finalized markers into regular initialization, and
  generate marker updates when components change.
- Validate candidate marker shared components and cleanup digests against the
  global graph. Only groups passing this stage become trusted and eligible for
  managed-schema filtering.
- When a previously shared dependency becomes retention-only, emit its move in the
  regular graph and update every affected marker before returning the revised
  cleanup list. Reject duplicate ownership or inconsistent marker updates.
- Emit explicit `DROP TABLE ... RESTRICT`, supported dependency drops in
  dependency order, and `DROP SCHEMA ... RESTRICT` into `CleanupStatements`.
- Avoid `CASCADE` and redundant attached-child drops. Use lexical stable
  tie-breaking for independent vertices.
- Reconstruct cleanup for existing groups and stop returning it once groups are
  gone.
- Attach table/sequence `DELETES_DATA`, actual lock, index-removal, and dependency
  correctness hazards to cleanup statements only.

Out of scope:

- Scheduling, retention periods, persistence, coordination, retries, and automatic
  cleanup application.

Acceptance gate:

- Tests cover new/existing groups, empty ordinary plans, shared dependencies,
  later retention-only dependency moves, component changes, marker updates,
  candidate digest validation, detached roots, deterministic ordering, explicit
  hazards, and rejection when existing markers cannot be updated safely.

### Stage 17: Two-phase plan validation

Status: Pending.

Depends on: Stages 4, 6, 10, and 16.

Deliverables:

- Run source-snapshot preflight before temporary execution.
- Reconstruct the managed current schema plus the modeled schemas, objects, and
  markers of existing trusted retained groups. Apply only regular statements,
  verify every predicted marker/member, filter valid retained groups, and compare
  the managed schema with the target.
- Apply cleanup statements to the same post-retention database, verify all groups
  are gone, prove target objects were unchanged, and compare the complete
  unfiltered schema with the target.
- Validate predicted post-resume state and classify metadata that requires static
  or DBMS-wide verification instead of synthetic reconstruction.
- Extend the acceptance harness for filtered regular dumps, unfiltered final
  dumps, and inserted-row preservation/deletion checks.

Out of scope:

- Guarantees about cleanup after arbitrary future drift and inferring omitted
  external dependencies from a temporary database.

Acceptance gate:

- Tests cover regular-phase row survival, cleanup-only deletion, metadata
  preservation, existing/partial groups, exact final schemas, ACL SQL, unmodeled
  metadata rejection, and excluded external dependencies found by preflight.
- Cleanup operations are proven to target only retained-group objects, and
  pre/post identity plus metadata checks prove every managed target object remains
  unchanged rather than being dropped and recreated.

### Stage 18: Versioned snapshot hash

Status: Pending.

Depends on: Stages 2, 4, 5, 6, 9, 10, 11, and 16.

Deliverables:

- Build a versioned canonical hash of the immutable
  generation snapshot: managed objects, validated groups/markers, excluded-schema
  safety dependencies, extension membership, event triggers, publications, ACLs,
  default ACLs, and relation/type namespaces.
- Normalize ordering deterministically.
- Add dormant integration seams so `CurrentSchemaHash` and public
  `schema.GetSchemaHash` can use the same snapshot, filter, marker validation, and
  safety closure when Stage 19 activates the hasher.
- Preserve include/exclude accumulation and custom-prefix reproducibility.
- Keep the legacy public and plan hashes unchanged in this stage.

Out of scope:

- Cleanup execution hashes and cleanup application contracts.

Acceptance gate:

- Internal candidate plan/public hash paths match for default/custom prefixes,
  every safety category changes the candidate hash, ordering-only catalog changes
  do not, retained-group changes do, and concurrent catalog changes cannot yield a
  mixed-state hash.

### Stage 19: Schema partial archival activation and documentation

Status: Pending.

Depends on: Stages 13 through 18.

Deliverables:

- Wire the complete archival engine into `Generate` as the only path for every
  table deletion and recreation.
- Replace transitional trust-by-prefix filtering with marker/name/catalog/digest
  validation.
- Return both statement lists by default and use the selected prefix for all
  generated names.
- Activate the Stage 18 versioned snapshot hash for `CurrentSchemaHash` and public
  `schema.GetSchemaHash`, with matching package compatibility documentation.
- Remove ordinary `DROP TABLE` generation and any fallback to it. Confine physical
  drops to cleanup generation.
- Enforce target reserved-name grammar and complete a final regular/cleanup hazard
  audit.
- Update existing table-archival acceptance expectations.
- Update package docs, README examples, this design's status, and release notes to
  describe activated behavior.
- Document default/custom prefixes and matching `GetSchemaHash`, JSON shape, the
  two-list semantic boundary, hazards, and supported partition/dependency limits.
- Warn that concatenating the lists is unsupported and cleanup is never applied
  automatically.
- Document the unkeyed `Plan{...}` source break and hash-version compatibility.

Out of scope:

- A destructive opt-out, public feature flag, CLI cleanup support, scheduling,
  retention policy, persistence, distributed locking, retries, cleanup execution
  APIs, and rename detection.

Acceptance gate:

- Full CI proves ordinary statements contain no removed-table `DROP TABLE`,
  `DELETE`, `TRUNCATE`, implicit extension-driven row deletion, or table-level
  `DELETES_DATA`, and no regular statement uses the `TABLE_REMOVAL` hazard.
- Every supported removed relation moves into a unique valid marked schema without
  copying rows or rebuilding indexes.
- Regular statements reach the filtered target; cleanup statements then reach the
  exact unfiltered target.
- Invalid prefixes and unsupported states fail without restoring destructive
  behavior, including when temporary validation is disabled.
- Existing callers that apply only `Statements` retain removed rows.
- Documentation examples compile where applicable and clearly distinguish regular
  migration behavior from optional later cleanup.

### Sequencing invariants

- Snapshot consistency precedes all safety inventory and marker decisions.
- Safety inventory remains unfiltered; managed-schema filters affect diff scope,
  never dependency, extension, ACL, publication, or event-trigger checks.
- Marker-aware filtering does not replace transitional prefix filtering until
  strict state validation and partial-state handling exist.
- Dependency, ACL, FK, publication, and extension checks precede move SQL.
- The global cleanup graph is finalized before marker-bearing regular SQL because
  markers contain cleanup digests and shared-component edges.
- Two-phase validation and reproducible snapshot hashing land before activation.
- Activation is one-way: supported removals retain data, unsupported removals fail
  closed, and no option or error path falls back to ordinary `DROP TABLE`.

## Testing

Implemented foundation tests cover:

- Full-name regex anchoring, include/exclude behavior, overlapping patterns, and
  invalid pattern errors.
- Default and custom prefix validation, including empty, non-simple, `pg`/`pg_`,
  and over-length prefixes.
- Cleanup filtering in source and target plan schemas.
- Custom-prefix replacement of the default exclusion.
- Default cleanup filtering and accumulated caller exclusions in
  `schema.GetSchemaHash`.

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
