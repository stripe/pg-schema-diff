package diff

import (
	"testing"

	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type schemaMigrationPlanTestCase struct {
	name                    string
	oldSchema               schema.Schema
	newSchema               schema.Schema
	expectedStatements      []Statement
	expectedDiffErrIs       error
	expectedDiffErrContains string
}

// schemaMigrationPlanTestCases -- these test cases assert the exact migration plan that is expected
// to be generated when migrating from the oldSchema to the newSchema.
//
// Most test cases should be added to //pg-schema-diff/internal/migration_acceptance_test_cases (acceptance
// tests) instead of here.
//
// The acceptance tests actually fetch the old/new schemas; run the migration; and validate the migration
// updates the old schema to be equivalent to the new schema. However, they do not assert any DDL; they have
// no expectation on how the migration should be done.
//
// The tests added here should just cover niche cases where you want to assert HOW the migration should be done (e.g.,
// adding an index concurrently) AND the schema cannot be derived via DDL, e.g., an invalid index.
var (
	defaultCollation = schema.SchemaQualifiedName{
		EscapedName: `"default"`,
		SchemaName:  "pg_catalog",
	}

	schemaMigrationPlanTestCases = []schemaMigrationPlanTestCase{
		{
			name: "Invalid index re-created",
			oldSchema: schema.Schema{
				Tables: []schema.Table{
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
					},
				},
				Indexes: []schema.Index{
					{
						OwningTable: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						Name:        "some_idx", Columns: []string{"foo", "bar"},
						GetIndexDefStmt: "CREATE INDEX some_idx ON public.foobar USING btree (foo, bar)",
						IsUnique:        true, IsInvalid: true,
					},
				},
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
					},
				},
				Indexes: []schema.Index{

					{
						OwningTable: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						Name:        "some_idx", Columns: []string{"foo", "bar"},
						GetIndexDefStmt: "CREATE INDEX some_idx ON public.foobar USING btree (foo, bar)",
						IsUnique:        true,
					},
				},
			},
			expectedStatements: []Statement{
				{
					DDL:         "ALTER INDEX \"public\".\"some_idx\" RENAME TO \"pgschemadiff_tmpidx_some_idx_AAECAwQFRgeICQoLDA0ODw\"",
					Timeout:     statementTimeoutDefault,
					LockTimeout: lockTimeoutDefault,
				},
				{
					DDL:         "CREATE INDEX CONCURRENTLY some_idx ON public.foobar USING btree (foo, bar)",
					Timeout:     statementTimeoutConcurrentIndexBuild,
					LockTimeout: lockTimeoutDefault,
					Hazards:     []MigrationHazard{buildIndexBuildHazard()},
				},
				{
					DDL:         "DROP INDEX CONCURRENTLY \"public\".\"pgschemadiff_tmpidx_some_idx_AAECAwQFRgeICQoLDA0ODw\"",
					Timeout:     statementTimeoutConcurrentIndexDrop,
					LockTimeout: lockTimeoutDefault,
					Hazards:     []MigrationHazard{buildIndexDroppedQueryPerfHazard()},
				},
			},
		},
		{
			name: "Invalid index of partitioned index re-created but original index remains untouched",
			oldSchema: schema.Schema{
				Tables: []schema.Table{
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
						PartitionKeyDef:  "LIST(foo)",
					},
					{
						ParentTable:         &schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar_1\""},
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
						ForValues:        "FOR VALUES IN ('some_val')",
					},
				},
				Indexes: []schema.Index{
					// foobar indexes
					{
						OwningTable: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						Name:        "some_idx", Columns: []string{"foo, bar"},
						GetIndexDefStmt: "CREATE INDEX some_idx ON ONLY public.foobar USING btree (foo, bar)",
						IsInvalid:       true,
					},
					// foobar_1 indexes
					{
						OwningTable: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar_1\""},
						Name:        "foobar_1_some_idx", Columns: []string{"foo", "bar"},
						GetIndexDefStmt: "CREATE INDEX foobar_1_some_idx ON public.foobar_1 USING btree (foo, bar)",
						IsInvalid:       true,
					},
				},
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
						PartitionKeyDef:  "LIST(foo)",
					},
					{
						ParentTable:         &schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar_1\""},
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
						ForValues:        "FOR VALUES IN ('some_val')",
					},
				},
				Indexes: []schema.Index{
					// foobar indexes
					{
						OwningTable: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						Name:        "some_idx", Columns: []string{"foo, bar"},
						GetIndexDefStmt: "CREATE INDEX some_idx ON ONLY public.foobar USING btree (foo, bar)",
					},
					// foobar_1 indexes
					{
						OwningTable: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar_1\""},
						Name:        "foobar_1_some_idx", Columns: []string{"foo", "bar"},
						ParentIdx:       &schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"some_idx\""},
						GetIndexDefStmt: "CREATE INDEX foobar_1_some_idx ON public.foobar_1 USING btree (foo, bar)",
					},
				},
			},
			expectedStatements: []Statement{
				{
					DDL:         "ALTER INDEX \"public\".\"foobar_1_some_idx\" RENAME TO \"pgschemadiff_tmpidx_foobar_1_some_idx_EBESExQVRheYGRobHB0eHw\"",
					Timeout:     statementTimeoutDefault,
					LockTimeout: lockTimeoutDefault,
				},
				{
					DDL:         "CREATE INDEX CONCURRENTLY foobar_1_some_idx ON public.foobar_1 USING btree (foo, bar)",
					Timeout:     statementTimeoutConcurrentIndexBuild,
					LockTimeout: lockTimeoutDefault,
					Hazards: []MigrationHazard{
						buildIndexBuildHazard(),
					},
				},
				{
					DDL:         "ALTER INDEX \"public\".\"some_idx\" ATTACH PARTITION \"public\".\"foobar_1_some_idx\"",
					Timeout:     statementTimeoutDefault,
					LockTimeout: lockTimeoutDefault,
				},
				{
					DDL:         "DROP INDEX CONCURRENTLY \"public\".\"pgschemadiff_tmpidx_foobar_1_some_idx_EBESExQVRheYGRobHB0eHw\"",
					Timeout:     statementTimeoutConcurrentIndexDrop,
					LockTimeout: lockTimeoutDefault,
					Hazards: []MigrationHazard{
						buildIndexDroppedQueryPerfHazard(),
					},
				},
			},
		},
		{
			name: "Fails on duplicate column in old schema",
			oldSchema: schema.Schema{
				Tables: []schema.Table{
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "id", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
					},
				},
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "something", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
					},
				},
			},
			expectedStatements: nil,
			expectedDiffErrIs:  errDuplicateIdentifier,
		},
		{
			name: "Handle infinite index loop without panicking",
			oldSchema: schema.Schema{
				Tables: []schema.Table{
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
						PartitionKeyDef:  "LIST(foo)",
					},
					{
						ParentTable:         &schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar_1\""},
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
						ForValues:        "FOR VALUES IN ('some_val')",
					},
				},
				Indexes: []schema.Index{
					// foobar indexes
					{
						OwningTable: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						// This index points to its child, which is wrong, but induces a loop
						Name: "some_idx", Columns: []string{"foo", "bar"},
						ParentIdx:       &schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar_1_some_idx\""},
						GetIndexDefStmt: "CREATE INDEX some_idx ON ONLY public.foobar USING btree (foo, bar)",
					},
					// foobar_1 indexes
					{
						OwningTable: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar_1\""},
						Name:        "foobar_1_some_idx", Columns: []string{"foo", "bar"},
						ParentIdx:       &schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"some_idx\""},
						GetIndexDefStmt: "CREATE INDEX foobar_1_some_idx ON public.foobar_1 USING btree (foo, bar)",
					},
				},
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
						PartitionKeyDef:  "LIST(foo)",
					},
					{
						ParentTable:         &schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar_1\""},
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
						ForValues:        "FOR VALUES IN ('some_val')",
					},
				},
				Indexes: []schema.Index{
					// foobar indexes
					{
						OwningTable: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						// This index points to its child, which is wrong, but induces a loop
						Name: "some_idx", Columns: []string{"foo", "bar"},
						ParentIdx:       &schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar_1_some_idx\""},
						GetIndexDefStmt: "CREATE INDEX some_idx ON ONLY public.foobar USING btree (foo, bar)",
					},
					// foobar_1 indexes
					{
						OwningTable: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar_1\""},
						Name:        "foobar_1_some_idx", Columns: []string{"foo", "bar"},
						ParentIdx:       &schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"some_idx\""},
						GetIndexDefStmt: "CREATE INDEX foobar_1_some_idx ON public.foobar_1 USING btree (foo, bar)",
					},
				},
			},
			expectedDiffErrContains: "loop detected",
		},
	}
)

type deterministicRandReader struct {
	counter int8
}

func (r *deterministicRandReader) Read(p []byte) (int, error) {
	for i := 0; i < len(p); i++ {
		p[i] = byte(r.counter)
		r.counter++
	}
	return len(p), nil
}

func TestSchemaMigrationPlanTest(t *testing.T) {
	randReader := &deterministicRandReader{}
	for _, testCase := range schemaMigrationPlanTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			schemaDiff, _, err := buildSchemaDiff(testCase.oldSchema, testCase.newSchema)
			if testCase.expectedDiffErrIs != nil {
				require.ErrorIs(t, err, testCase.expectedDiffErrIs)
			} else if testCase.expectedDiffErrContains != "" {
				require.ErrorContains(t, err, testCase.expectedDiffErrContains)
			} else {
				require.NoError(t, err)
			}
			stmts, err := newSchemaSQLGenerator(randReader).Alter(schemaDiff)
			require.NoError(t, err)
			assert.Equal(t, testCase.expectedStatements, stmts, "actual:\n %# v", pretty.Formatter(stmts))
		})
	}
}

func buildIndexBuildHazard() MigrationHazard {
	return MigrationHazard{
		Type: MigrationHazardTypeIndexBuild,
		Message: "This might affect database performance. " +
			"Concurrent index builds require a non-trivial amount of CPU, potentially affecting database performance. " +
			"They also can take a while but do not lock out writes.",
	}
}

func buildIndexDroppedQueryPerfHazard() MigrationHazard {
	return MigrationHazard{
		Type: MigrationHazardTypeIndexDropped,
		Message: "Dropping this index means queries that use this index might perform worse because " +
			"they will no longer will be able to leverage it.",
	}
}
