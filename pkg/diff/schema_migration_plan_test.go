package diff

import (
	"testing"

	"github.com/google/uuid"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type schemaMigrationPlanTestCase struct {
	name               string
	oldSchema          schema.Schema
	newSchema          schema.Schema
	expectedStatements []Statement
	expectedDiffErrIs  error
}

// schemaMigrationPlanTestCases -- these test cases assert the exact migration plan that is expected
// to be generated when migrating from the oldSchema to the newSchema. They assert how the migration
// should occur by asserting the DDL
//
// Most test cases should be added to //pg-schema-diff/internal/migration_acceptance_test_cases (acceptance
// tests) instead of here.
//
// The acceptance tests actually fetch the old/new schemas; run the migration; and validate the migration
// updates the old schema to be equivalent to the new schema. However, they do not assert any DDL; they have
// no expectation on how the migration should be done.
//
// The tests added here should just cover niche cases where you want to assert HOW the migration should be done (e.g.,
// adding an index concurrently)
var (
	defaultCollation = schema.SchemaQualifiedName{
		EscapedName: `"default"`,
		SchemaName:  "pg_catalog",
	}
	cCollation = schema.SchemaQualifiedName{
		EscapedName: `"C"`,
		SchemaName:  "pg_catalog",
	}

	schemaMigrationPlanTestCases = []schemaMigrationPlanTestCase{
		{
			name: "No-op",
			oldSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
							{Name: "fizz", Type: "boolean", Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "id_check", Expression: "(id > 0)", IsInheritable: true},
							{Name: "bar_check", Expression: "(id > LENGTH(foo))", IsValid: true},
						},
					},
					{
						Name: "bar",
						Columns: []schema.Column{
							{Name: "id", Type: "character varying(255)", Default: "", IsNullable: false, Collation: defaultCollation},
							{Name: "foo", Type: "integer", Default: "", IsNullable: true},
							{Name: "bar", Type: "double precision", Default: "8.8", IsNullable: false},
							{Name: "fizz", Type: "timestamp with time zone", Default: "CURRENT_TIMESTAMP", IsNullable: true},
							{Name: "buzz", Type: "real", Default: "", IsNullable: false},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "id_check", Expression: "(id + 10 > 0 )", IsValid: true},
							{Name: "bar_check", Expression: "(foo > buzz)", IsInheritable: true},
						},
					},
					{
						Name:             "fizz",
						Columns:          nil,
						CheckConstraints: nil,
					},
				},
				Indexes: []schema.Index{
					// bar indexes
					{
						TableName: "bar",
						Name:      "bar_pkey", Columns: []string{"bar"}, IsPk: true, IsUnique: true, ConstraintName: "bar_pkey_non_default_name",
						GetIndexDefStmt: "CREATE UNIQUE INDEX bar_pkey ON public.bar USING btree (bar)",
					},
					{
						TableName: "bar",
						Name:      "bar_normal_idx", Columns: []string{"fizz"},
						GetIndexDefStmt: "CREATE INDEX bar_normal_idx ON public.bar USING btree (fizz)",
					},
					{
						TableName: "bar",
						Name:      "bar_unique_idx", IsUnique: true, Columns: []string{"fizz", "buzz"},
						GetIndexDefStmt: "CREATE UNIQUE INDEX bar_unique_idx ON public.bar USING btree (fizz, buzz)",
					},
					// foobar indexes
					{
						TableName: "foobar",
						Name:      "foobar_pkey", Columns: []string{"id"}, IsPk: true, IsUnique: true, ConstraintName: "foobar_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_pkey ON public.foobar USING btree (id)",
					},
					{
						TableName: "foobar",
						Name:      "foobar_normal_idx", Columns: []string{"fizz"},
						GetIndexDefStmt: "CREATE INDEX foobar_normal_idx ON public.foobar USING btree (fizz)",
					},
					{
						TableName: "foobar",
						Name:      "foobar_unique_idx", IsUnique: true, Columns: []string{"foo", "bar"},
						GetIndexDefStmt: "CREATE UNIQUE INDEX foobar_unique_idx ON public.foobar USING btree (foo, bar)",
					},
				},
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
							{Name: "fizz", Type: "boolean", Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "id_check", Expression: "(id > 0)", IsInheritable: true},
							{Name: "bar_check", Expression: "(id > LENGTH(foo))", IsValid: true},
						},
					},
					{
						Name: "bar",
						Columns: []schema.Column{
							{Name: "id", Type: "character varying(255)", Default: "", IsNullable: false, Collation: defaultCollation},
							{Name: "foo", Type: "integer", Default: "", IsNullable: true},
							{Name: "bar", Type: "double precision", Default: "8.8", IsNullable: false},
							{Name: "fizz", Type: "timestamp with time zone", Default: "CURRENT_TIMESTAMP", IsNullable: true},
							{Name: "buzz", Type: "real", Default: "", IsNullable: false},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "id_check", Expression: "(id + 10 > 0 )", IsValid: true},
							{Name: "bar_check", Expression: "(foo > buzz)", IsInheritable: true},
						},
					},
					{
						Name:             "fizz",
						Columns:          nil,
						CheckConstraints: nil,
					},
				},
				Indexes: []schema.Index{
					// bar indexes
					{
						TableName: "bar",
						Name:      "bar_pkey", Columns: []string{"bar"}, IsPk: true, IsUnique: true, ConstraintName: "bar_pkey_non_default_name",
						GetIndexDefStmt: "CREATE UNIQUE INDEX bar_pkey ON public.bar USING btree (bar)",
					},
					{
						TableName: "bar",
						Name:      "bar_normal_idx", Columns: []string{"fizz"},
						GetIndexDefStmt: "CREATE INDEX bar_normal_idx ON public.bar USING btree (fizz)",
					},
					{
						TableName: "bar",
						Name:      "bar_unique_idx", IsUnique: true, Columns: []string{"fizz", "buzz"},
						GetIndexDefStmt: "CREATE UNIQUE INDEX bar_unique_idx ON public.bar USING btree (fizz, buzz)",
					},
					// foobar indexes
					{
						TableName: "foobar",
						Name:      "foobar_pkey", Columns: []string{"id"}, IsPk: true, IsUnique: true, ConstraintName: "foobar_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_pkey ON public.foobar USING btree (id)",
					},
					{
						TableName: "foobar",
						Name:      "foobar_normal_idx", Columns: []string{"fizz"},
						GetIndexDefStmt: "CREATE INDEX foobar_normal_idx ON public.foobar USING btree (fizz)",
					},
					{
						TableName: "foobar",
						Name:      "foobar_unique_idx", IsUnique: true, Columns: []string{"foo", "bar"},
						GetIndexDefStmt: "CREATE UNIQUE INDEX foobar_unique_idx ON public.foobar USING btree (foo, bar)",
					},
				},
			},
			expectedStatements: nil,
		},
		{
			name: "Index replacement",
			oldSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
					},
				},
				Indexes: []schema.Index{
					{
						TableName: "foobar",
						Name:      "foo_idx", Columns: []string{"foo"},
						GetIndexDefStmt: "CREATE INDEX foo_idx ON public.foobar USING btree (foo)",
					},
					{
						TableName: "foobar",
						Name:      "replaced_with_same_name_idx", Columns: []string{"bar"},
						GetIndexDefStmt: "CREATE INDEX replaced_with_same_name_idx ON ONLY public.foobar USING btree (bar)",
					},
				},
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
					},
				},
				Indexes: []schema.Index{
					{
						TableName: "foobar",
						Name:      "new_foo_idx", Columns: []string{"foo"},
						GetIndexDefStmt: "CREATE INDEX new_foo_idx ON public.foobar USING btree (foo)",
					},
					{
						TableName: "foobar",
						Name:      "replaced_with_same_name_idx", Columns: []string{"bar", "foo"},
						GetIndexDefStmt: "CREATE INDEX replaced_with_same_name_idx ON ONLY public.foobar USING btree (bar)",
					},
				},
			},
			expectedStatements: []Statement{
				{
					DDL:     "ALTER INDEX \"replaced_with_same_name_idx\" RENAME TO \"replaced_with_same_name_id_00010203-0405-4607-8809-0a0b0c0d0e0f\"",
					Timeout: statementTimeoutDefault,
					Hazards: nil,
				},
				{
					DDL:     "CREATE INDEX CONCURRENTLY new_foo_idx ON public.foobar USING btree (foo)",
					Timeout: statementTimeoutConcurrentIndexBuild,
					Hazards: []MigrationHazard{buildIndexBuildHazard()},
				},
				{
					DDL:     "CREATE INDEX CONCURRENTLY replaced_with_same_name_idx ON ONLY public.foobar USING btree (bar)",
					Timeout: statementTimeoutConcurrentIndexBuild,
					Hazards: []MigrationHazard{buildIndexBuildHazard()},
				},
				{
					DDL:     "DROP INDEX CONCURRENTLY \"foo_idx\"",
					Timeout: statementTimeoutConcurrentIndexDrop,
					Hazards: []MigrationHazard{
						{Type: "INDEX_DROPPED", Message: "Dropping this index means queries that use this index might perform worse because they will no longer will be able to leverage it."},
					},
				},
				{
					DDL:     "DROP INDEX CONCURRENTLY \"replaced_with_same_name_id_00010203-0405-4607-8809-0a0b0c0d0e0f\"",
					Timeout: statementTimeoutConcurrentIndexDrop,
					Hazards: []MigrationHazard{
						{Type: "INDEX_DROPPED", Message: "Dropping this index means queries that use this index might perform worse because they will no longer will be able to leverage it."},
					},
				},
			},
		},
		{
			name: "Index dropped concurrently before columns dropped",
			oldSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
					},
				},
				Indexes: []schema.Index{
					{
						TableName: "foobar",
						Name:      "foobar_pkey", Columns: []string{"id"}, IsPk: true, IsUnique: true, ConstraintName: "foobar_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foobar_pkey ON public.foobar USING btree (id)",
					},
					{
						TableName: "foobar",
						Name:      "some_idx", Columns: []string{"foo, bar"},
						GetIndexDefStmt: "CREATE INDEX some_idx ON public.foobar USING btree (foo, bar)",
					},
				},
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
						},
						CheckConstraints: nil,
					},
				},
				Indexes: []schema.Index{
					{
						TableName: "foobar",
						Name:      "foobar_pkey", Columns: []string{"id"}, IsPk: true, IsUnique: true, ConstraintName: "foobar_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foobar_pkey ON public.foobar USING btree (id)",
					},
				},
			},
			expectedStatements: []Statement{
				{
					DDL:     "DROP INDEX CONCURRENTLY \"some_idx\"",
					Timeout: statementTimeoutConcurrentIndexDrop,
					Hazards: []MigrationHazard{buildIndexDroppedQueryPerfHazard()},
				},
				{
					DDL:     "ALTER TABLE \"foobar\" DROP COLUMN \"bar\"",
					Timeout: statementTimeoutDefault,
					Hazards: []MigrationHazard{buildColumnDataDeletionHazard()},
				},
				{
					DDL:     "ALTER TABLE \"foobar\" DROP COLUMN \"foo\"",
					Timeout: statementTimeoutDefault,
					Hazards: []MigrationHazard{buildColumnDataDeletionHazard()},
				},
			},
		},
		{
			name: "Invalid index re-created",
			oldSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
					},
				},
				Indexes: []schema.Index{
					{
						TableName: "foobar",
						Name:      "some_idx", Columns: []string{"foo", "bar"},
						GetIndexDefStmt: "CREATE INDEX some_idx ON public.foobar USING btree (foo, bar)",
						IsUnique:        true, IsInvalid: true,
					},
				},
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
					},
				},
				Indexes: []schema.Index{

					{
						TableName: "foobar",
						Name:      "some_idx", Columns: []string{"foo", "bar"},
						GetIndexDefStmt: "CREATE INDEX some_idx ON public.foobar USING btree (foo, bar)",
						IsUnique:        true,
					},
				},
			},
			expectedStatements: []Statement{
				{
					DDL:     "ALTER INDEX \"some_idx\" RENAME TO \"some_idx_10111213-1415-4617-9819-1a1b1c1d1e1f\"",
					Timeout: statementTimeoutDefault,
				},
				{
					DDL:     "CREATE INDEX CONCURRENTLY some_idx ON public.foobar USING btree (foo, bar)",
					Timeout: statementTimeoutConcurrentIndexBuild,
					Hazards: []MigrationHazard{buildIndexBuildHazard()},
				},
				{
					DDL:     "DROP INDEX CONCURRENTLY \"some_idx_10111213-1415-4617-9819-1a1b1c1d1e1f\"",
					Timeout: statementTimeoutConcurrentIndexDrop,
					Hazards: []MigrationHazard{buildIndexDroppedQueryPerfHazard()},
				},
			},
		},
		{
			name:      "Create partitioned table",
			oldSchema: schema.Schema{},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
						},
						PartitionKeyDef: "LIST(foo)",
					},
					{
						ParentTableName: "foobar",
						Name:            "foobar_1",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
						},
						ForValues: "FOR VALUES IN ('some_val')",
					},
				},
			},
			expectedStatements: []Statement{
				{
					DDL:     "CREATE TABLE \"foobar\" (\n\t\"id\" integer NOT NULL,\n\t\"foo\" character varying(255) COLLATE \"pg_catalog\".\"default\" NOT NULL DEFAULT ''::character varying\n) PARTITION BY LIST(foo)",
					Timeout: statementTimeoutDefault,
				},
				{
					DDL:     "CREATE TABLE \"foobar_1\" (\n\t\"id\" integer NOT NULL,\n\t\"foo\" character varying(255) COLLATE \"pg_catalog\".\"default\" NOT NULL DEFAULT ''::character varying\n)",
					Timeout: statementTimeoutDefault,
				},
				{
					DDL:     "ALTER TABLE \"foobar\" ATTACH PARTITION \"foobar_1\" FOR VALUES IN ('some_val')",
					Timeout: statementTimeoutDefault,
				},
			},
		},
		{
			name: "Index replacement on partitioned table",
			oldSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						PartitionKeyDef:  "LIST(foo)",
					},
					{
						ParentTableName: "foobar",
						Name:            "foobar_1",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ForValues:        "FOR VALUES IN ('some_val')",
					},
					{
						ParentTableName: "foobar",
						Name:            "foobar_2",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ForValues:        "FOR VALUES IN ('some_other_val')",
					},
				},
				Indexes: []schema.Index{
					// foobar indexes
					{
						TableName: "foobar",
						Name:      "some_idx", Columns: []string{"foo", "bar"},
						GetIndexDefStmt: "CREATE INDEX some_idx ON ONLY public.foobar USING btree (foo, bar)",
					},
					{
						TableName: "foobar",
						Name:      "replaced_with_same_name_idx", Columns: []string{"bar"},
						GetIndexDefStmt: "CREATE INDEX replaced_with_same_name_idx ON ONLY public.foobar USING btree (bar)",
					},
					// foobar_1 indexes
					{
						TableName: "foobar_1",
						Name:      "foobar_1_some_idx", Columns: []string{"foo", "bar"}, ParentIdxName: "some_idx",
						GetIndexDefStmt: "CREATE INDEX foobar_1_some_idx ON public.foobar_1 USING btree (foo, bar)",
					},
					{
						TableName: "foobar_1",
						Name:      "foobar_1_replaced_with_same_name_idx", Columns: []string{"bar"}, ParentIdxName: "replaced_with_same_name_idx",
						GetIndexDefStmt: "CREATE INDEX foobar_1_replaced_with_same_name_idx ON ONLY public.foobar USING btree (bar)",
					},
					{
						TableName: "foobar_1",
						Name:      "foobar_1_some_local_idx", Columns: []string{"foo", "bar", "id"},
						GetIndexDefStmt: "CREATE INDEX foobar_1_some_local_idx ON public.foobar_1 USING btree (foo, bar, id)",
					},
					// foobar_2 indexes
					{
						TableName: "foobar_2",
						Name:      "foobar_2_some_idx", Columns: []string{"foo", "bar"}, ParentIdxName: "some_idx",
						GetIndexDefStmt: "CREATE INDEX foobar_2_some_idx ON public.foobar_2 USING btree (foo, bar)",
					},
					{
						TableName: "foobar_2",
						Name:      "foobar_2_replaced_with_same_name_idx", Columns: []string{"bar"}, ParentIdxName: "replaced_with_same_name_idx",
						GetIndexDefStmt: "CREATE INDEX foobar_2_replaced_with_same_name_idx ON ONLY public.foobar USING btree (bar)",
					},
				},
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						PartitionKeyDef:  "LIST(foo)",
					},
					{
						ParentTableName: "foobar",
						Name:            "foobar_1",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ForValues:        "FOR VALUES IN ('some_val')",
					},
					{
						ParentTableName: "foobar",
						Name:            "foobar_2",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ForValues:        "FOR VALUES IN ('some_other_val')",
					},
				},
				Indexes: []schema.Index{
					// foobar indexes
					{
						TableName: "foobar",
						Name:      "new_some_idx", Columns: []string{"foo", "bar"},
						GetIndexDefStmt: "CREATE INDEX new_some_idx ON ONLY public.foobar USING btree (foo, bar)",
					},
					{
						TableName: "foobar",
						Name:      "replaced_with_same_name_idx", Columns: []string{"bar", "foo"},
						GetIndexDefStmt: "CREATE INDEX replaced_with_same_name_idx ON ONLY public.foobar USING btree (bar, foo)",
					},
					// foobar_1 indexes
					{
						TableName: "foobar_1",
						Name:      "new_foobar_1_some_idx", Columns: []string{"foo", "bar"}, ParentIdxName: "new_some_idx",
						GetIndexDefStmt: "CREATE INDEX new_foobar_1_some_idx ON public.foobar_1 USING btree (foo, bar)",
					},
					{
						TableName: "foobar_1",
						Name:      "foobar_1_replaced_with_same_name_idx", Columns: []string{"bar", "foo"}, ParentIdxName: "replaced_with_same_name_idx",
						GetIndexDefStmt: "CREATE INDEX foobar_1_replaced_with_same_name_idx ON public.foobar USING btree (bar, foo)",
					},
					{
						TableName: "foobar_1",
						Name:      "new_foobar_1_some_local_idx", Columns: []string{"foo", "bar", "id"},
						GetIndexDefStmt: "CREATE INDEX new_foobar_1_some_local_idx ON public.foobar_1 USING btree (foo, bar, id)",
					},
					// foobar_2 indexes
					{
						TableName: "foobar_2",
						Name:      "new_foobar_2_some_idx", Columns: []string{"foo", "bar"}, ParentIdxName: "new_some_idx",
						GetIndexDefStmt: "CREATE INDEX new_foobar_2_some_idx ON public.foobar_2 USING btree (foo, bar)",
					},
					{
						TableName: "foobar_2",
						Name:      "foobar_2_replaced_with_same_name_idx", Columns: []string{"bar", "foo"}, ParentIdxName: "replaced_with_same_name_idx",
						GetIndexDefStmt: "CREATE INDEX foobar_2_replaced_with_same_name_idx ON public.foobar_2 USING btree (bar, foo)",
					},
				},
			},
			expectedStatements: []Statement{
				{
					DDL:     "ALTER INDEX \"foobar_1_replaced_with_same_name_idx\" RENAME TO \"foobar_1_replaced_with_sam_30313233-3435-4637-b839-3a3b3c3d3e3f\"",
					Timeout: statementTimeoutDefault,
				},
				{
					DDL:     "ALTER INDEX \"foobar_2_replaced_with_same_name_idx\" RENAME TO \"foobar_2_replaced_with_sam_40414243-4445-4647-8849-4a4b4c4d4e4f\"",
					Timeout: statementTimeoutDefault,
				},
				{
					DDL:     "ALTER INDEX \"replaced_with_same_name_idx\" RENAME TO \"replaced_with_same_name_id_20212223-2425-4627-a829-2a2b2c2d2e2f\"",
					Timeout: statementTimeoutDefault,
				},
				{
					DDL:     "CREATE INDEX new_some_idx ON ONLY public.foobar USING btree (foo, bar)",
					Timeout: statementTimeoutDefault,
				},
				{
					DDL:     "CREATE INDEX replaced_with_same_name_idx ON ONLY public.foobar USING btree (bar, foo)",
					Timeout: statementTimeoutDefault,
				},
				{
					DDL:     "CREATE INDEX CONCURRENTLY foobar_1_replaced_with_same_name_idx ON public.foobar USING btree (bar, foo)",
					Timeout: statementTimeoutConcurrentIndexDrop,
					Hazards: []MigrationHazard{
						buildIndexBuildHazard(),
					},
				},
				{
					DDL:     "ALTER INDEX \"replaced_with_same_name_idx\" ATTACH PARTITION \"foobar_1_replaced_with_same_name_idx\"",
					Timeout: statementTimeoutDefault,
					Hazards: nil,
				},
				{
					DDL:     "CREATE INDEX CONCURRENTLY new_foobar_1_some_idx ON public.foobar_1 USING btree (foo, bar)",
					Timeout: statementTimeoutConcurrentIndexBuild,
					Hazards: []MigrationHazard{
						buildIndexBuildHazard(),
					},
				},
				{
					DDL:     "ALTER INDEX \"new_some_idx\" ATTACH PARTITION \"new_foobar_1_some_idx\"",
					Timeout: statementTimeoutDefault,
					Hazards: nil,
				},
				{
					DDL:     "CREATE INDEX CONCURRENTLY new_foobar_1_some_local_idx ON public.foobar_1 USING btree (foo, bar, id)",
					Timeout: statementTimeoutConcurrentIndexBuild,
					Hazards: []MigrationHazard{
						buildIndexBuildHazard(),
					},
				},
				{
					DDL:     "CREATE INDEX CONCURRENTLY foobar_2_replaced_with_same_name_idx ON public.foobar_2 USING btree (bar, foo)",
					Timeout: statementTimeoutConcurrentIndexBuild,
					Hazards: []MigrationHazard{
						buildIndexBuildHazard(),
					},
				},
				{
					DDL:     "ALTER INDEX \"replaced_with_same_name_idx\" ATTACH PARTITION \"foobar_2_replaced_with_same_name_idx\"",
					Timeout: statementTimeoutDefault,
					Hazards: nil,
				},
				{
					DDL:     "CREATE INDEX CONCURRENTLY new_foobar_2_some_idx ON public.foobar_2 USING btree (foo, bar)",
					Timeout: statementTimeoutConcurrentIndexBuild,
					Hazards: []MigrationHazard{
						buildIndexBuildHazard(),
					},
				},
				{
					DDL:     "ALTER INDEX \"new_some_idx\" ATTACH PARTITION \"new_foobar_2_some_idx\"",
					Timeout: statementTimeoutDefault,
					Hazards: nil,
				},
				{
					DDL:     "DROP INDEX CONCURRENTLY \"foobar_1_some_local_idx\"",
					Timeout: statementTimeoutConcurrentIndexDrop,
					Hazards: []MigrationHazard{
						buildIndexDroppedQueryPerfHazard(),
					},
				},
				{
					DDL:     "DROP INDEX \"replaced_with_same_name_id_20212223-2425-4627-a829-2a2b2c2d2e2f\"",
					Timeout: statementTimeoutDefault,
					Hazards: []MigrationHazard{
						buildIndexDroppedAcquiresLockHazard(),
						buildIndexDroppedQueryPerfHazard(),
					},
				},
				{
					DDL:     "DROP INDEX \"some_idx\"",
					Timeout: statementTimeoutDefault,
					Hazards: []MigrationHazard{
						buildIndexDroppedAcquiresLockHazard(),
						buildIndexDroppedQueryPerfHazard(),
					},
				},
			},
		},
		{
			name: "Local Index dropped concurrently before columns dropped; partitioned index just dropped",
			oldSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						PartitionKeyDef:  "LIST(foo)",
					},
					{
						ParentTableName: "foobar",
						Name:            "foobar_1",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ForValues:        "FOR VALUES IN ('some_val')",
					},
				},
				Indexes: []schema.Index{
					// foobar indexes
					{
						TableName: "foobar",
						Name:      "foobar_pkey", Columns: []string{"foo", "id"}, IsPk: true, IsUnique: true, ConstraintName: "foobar_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foobar_pkey ON ONLY public.foobar USING btree (foo, id)",
					},
					{
						TableName: "foobar",
						Name:      "some_idx", Columns: []string{"foo, bar"},
						GetIndexDefStmt: "CREATE INDEX some_idx ON ONLY public.foobar USING btree (foo, bar)",
					},
					// foobar_1 indexes
					{
						TableName: "foobar_1",
						Name:      "foobar_1_pkey", Columns: []string{"foo", "id"}, IsPk: true, IsUnique: true, ConstraintName: "foobar_pkey", ParentIdxName: "foobar_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foobar_1_pkey ON public.foobar_1 USING btree (foo, id)",
					},
					{
						TableName: "foobar_1",
						Name:      "foobar_1_some_idx", Columns: []string{"foo", "bar"}, ParentIdxName: "some_idx",
						GetIndexDefStmt: "CREATE INDEX foobar_1_some_idx ON public.foobar_1 USING btree (foo, bar)",
					},
					{
						TableName: "foobar_1",
						Name:      "foobar_1_some_local_idx", Columns: []string{"foo", "bar", "id"},
						GetIndexDefStmt: "CREATE INDEX foobar_1_some_local_idx ON public.foobar_1 USING btree (foo, bar, id)",
					},
				},
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
						},
						CheckConstraints: nil,
						PartitionKeyDef:  "LIST(foo)",
					},
					{
						ParentTableName: "foobar",
						Name:            "foobar_1",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
						},
						CheckConstraints: nil,
						ForValues:        "FOR VALUES IN ('some_val')",
					},
				},
				Indexes: []schema.Index{
					// foobar indexes
					{
						TableName: "foobar",
						Name:      "foobar_pkey", Columns: []string{"foo", "id"}, IsPk: true, IsUnique: true, ConstraintName: "foobar_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foobar_pkey ON ONLY public.foobar USING btree (foo, id)",
					},
					// foobar_1 indexes
					{
						TableName: "foobar_1",
						Name:      "foobar_1_pkey", Columns: []string{"foo", "id"}, IsPk: true, IsUnique: true, ConstraintName: "foobar_pkey", ParentIdxName: "foobar_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foobar_1_pkey ON public.foobar_1 USING btree (foo, id)",
					},
				},
			},
			expectedStatements: []Statement{
				{
					DDL:     "DROP INDEX CONCURRENTLY \"foobar_1_some_local_idx\"",
					Timeout: statementTimeoutConcurrentIndexDrop,
					Hazards: []MigrationHazard{
						buildIndexDroppedQueryPerfHazard(),
					},
				},
				{
					DDL:     "DROP INDEX \"some_idx\"",
					Timeout: statementTimeoutDefault,
					Hazards: []MigrationHazard{
						buildIndexDroppedAcquiresLockHazard(),
						buildIndexDroppedQueryPerfHazard(),
					},
				},
				{
					DDL:     "ALTER TABLE \"foobar\" DROP COLUMN \"bar\"",
					Timeout: statementTimeoutDefault,
					Hazards: []MigrationHazard{
						buildColumnDataDeletionHazard(),
					},
				},
			},
		},
		{
			name: "Invalid index of partitioned index re-created but original index remains untouched",
			oldSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						PartitionKeyDef:  "LIST(foo)",
					},
					{
						ParentTableName: "foobar",
						Name:            "foobar_1",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ForValues:        "FOR VALUES IN ('some_val')",
					},
				},
				Indexes: []schema.Index{
					// foobar indexes
					{
						TableName: "foobar",
						Name:      "some_idx", Columns: []string{"foo, bar"},
						GetIndexDefStmt: "CREATE INDEX some_idx ON ONLY public.foobar USING btree (foo, bar)",
						IsInvalid:       true,
					},
					// foobar_1 indexes
					{
						TableName: "foobar_1",
						Name:      "foobar_1_some_idx", Columns: []string{"foo", "bar"},
						GetIndexDefStmt: "CREATE INDEX foobar_1_some_idx ON public.foobar_1 USING btree (foo, bar)",
						IsInvalid:       true,
					},
				},
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						PartitionKeyDef:  "LIST(foo)",
					},
					{
						ParentTableName: "foobar",
						Name:            "foobar_1",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "foo", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
							{Name: "bar", Type: "timestamp without time zone", IsNullable: true, Default: "CURRENT_TIMESTAMP"},
						},
						CheckConstraints: nil,
						ForValues:        "FOR VALUES IN ('some_val')",
					},
				},
				Indexes: []schema.Index{
					// foobar indexes
					{
						TableName: "foobar",
						Name:      "some_idx", Columns: []string{"foo, bar"},
						GetIndexDefStmt: "CREATE INDEX some_idx ON ONLY public.foobar USING btree (foo, bar)",
					},
					// foobar_1 indexes
					{
						TableName: "foobar_1",
						Name:      "foobar_1_some_idx", Columns: []string{"foo", "bar"}, ParentIdxName: "some_idx",
						GetIndexDefStmt: "CREATE INDEX foobar_1_some_idx ON public.foobar_1 USING btree (foo, bar)",
					},
				},
			},
			expectedStatements: []Statement{
				{
					DDL:     "ALTER INDEX \"foobar_1_some_idx\" RENAME TO \"foobar_1_some_idx_50515253-5455-4657-9859-5a5b5c5d5e5f\"",
					Timeout: statementTimeoutDefault,
				},
				{
					DDL:     "CREATE INDEX CONCURRENTLY foobar_1_some_idx ON public.foobar_1 USING btree (foo, bar)",
					Timeout: statementTimeoutConcurrentIndexBuild,
					Hazards: []MigrationHazard{
						buildIndexBuildHazard(),
					},
				},
				{
					DDL:     "ALTER INDEX \"some_idx\" ATTACH PARTITION \"foobar_1_some_idx\"",
					Timeout: statementTimeoutDefault,
				},
				{
					DDL:     "DROP INDEX CONCURRENTLY \"foobar_1_some_idx_50515253-5455-4657-9859-5a5b5c5d5e5f\"",
					Timeout: statementTimeoutConcurrentIndexDrop,
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
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "id", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
						},
						CheckConstraints: nil,
					},
				},
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
							{Name: "something", Type: "character varying(255)", Default: "''::character varying", Collation: defaultCollation},
						},
						CheckConstraints: nil,
					},
				},
			},
			expectedStatements: nil,
			expectedDiffErrIs:  errDuplicateIdentifier,
		},
		{
			name: "Invalid check constraint made valid",
			oldSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "id_check", Expression: "(id > 0)", IsInheritable: true},
						},
					},
				},
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "id_check", Expression: "(id > 0)", IsInheritable: true, IsValid: true},
						},
					},
				},
			},
			expectedStatements: []Statement{
				{
					DDL:     "ALTER TABLE \"foobar\" VALIDATE CONSTRAINT \"id_check\"",
					Timeout: statementTimeoutDefault,
					Hazards: nil,
				},
			},
		},
		{
			name: "Invalid check constraint re-created if expression changes",
			oldSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "id_check", Expression: "(id > 0)", IsInheritable: true},
						},
					},
				},
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer"},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "id_check", Expression: "(id < 0)", IsInheritable: true, IsValid: true},
						},
					},
				},
			},
			expectedStatements: []Statement{
				{
					DDL:     "ALTER TABLE \"foobar\" DROP CONSTRAINT \"id_check\"",
					Timeout: statementTimeoutDefault,
				},
				{
					DDL:     "ALTER TABLE \"foobar\" ADD CONSTRAINT \"id_check\" CHECK((id < 0))",
					Timeout: statementTimeoutDefault,
				},
			},
		},
		{
			name: "BIGINT to TIMESTAMP type conversion",
			oldSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "baz", Type: "bigint"},
						},
						CheckConstraints: nil,
					},
				},
				Indexes: nil,
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "baz", Type: "timestamp without time zone", Default: "current_timestamp"},
						},
						CheckConstraints: nil,
					},
				},
				Indexes: nil,
			},
			expectedStatements: []Statement{
				{
					DDL:     "ALTER TABLE \"foobar\" ALTER COLUMN \"baz\" SET DATA TYPE timestamp without time zone using to_timestamp(\"baz\" / 1000)",
					Timeout: statementTimeoutDefault,
					Hazards: []MigrationHazard{{
						Type: MigrationHazardTypeAcquiresAccessExclusiveLock,
						Message: "This will completely lock the table while the data is being " +
							"re-written for a duration of time that scales with the size of your " +
							"data. The values previously stored as BIGINT will be translated into a " +
							"TIMESTAMP value via the PostgreSQL to_timestamp() function. This " +
							"translation will assume that the values stored in BIGINT represent a " +
							"millisecond epoch value.",
					}},
				},
				{
					DDL:     "ANALYZE \"foobar\" (\"baz\")",
					Timeout: statementTimeoutAnalyzeColumn,
					Hazards: []MigrationHazard{buildAnalyzeColumnMigrationHazard()},
				},
				{
					DDL:     "ALTER TABLE \"foobar\" ALTER COLUMN \"baz\" SET DEFAULT current_timestamp",
					Timeout: statementTimeoutDefault,
				},
			},
		},
		{
			name: "Collation migration and Type Migration",
			oldSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "migrate_to_c_coll", Type: "text", Collation: defaultCollation},
							{Name: "migrate_type", Type: "text", Collation: defaultCollation},
						},
						CheckConstraints: nil,
					},
				},
			},
			newSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "migrate_to_c_coll", Type: "text", Collation: cCollation},
							{Name: "migrate_type", Type: "character varying(255)", Collation: defaultCollation},
						},
						CheckConstraints: nil,
					},
				},
			},
			expectedStatements: []Statement{
				{
					DDL:     "ALTER TABLE \"foobar\" ALTER COLUMN \"migrate_to_c_coll\" SET DATA TYPE text COLLATE \"pg_catalog\".\"C\" using \"migrate_to_c_coll\"::text",
					Timeout: statementTimeoutDefault,
					Hazards: []MigrationHazard{buildColumnTypeChangeHazard()},
				},
				{
					DDL:     "ANALYZE \"foobar\" (\"migrate_to_c_coll\")",
					Timeout: statementTimeoutAnalyzeColumn,
					Hazards: []MigrationHazard{buildAnalyzeColumnMigrationHazard()},
				},
				{
					DDL:     "ALTER TABLE \"foobar\" ALTER COLUMN \"migrate_type\" SET DATA TYPE character varying(255) COLLATE \"pg_catalog\".\"default\" using \"migrate_type\"::character varying(255)",
					Timeout: statementTimeoutDefault,
					Hazards: []MigrationHazard{buildColumnTypeChangeHazard()},
				},
				{
					DDL:     "ANALYZE \"foobar\" (\"migrate_type\")",
					Timeout: statementTimeoutAnalyzeColumn,
					Hazards: []MigrationHazard{buildAnalyzeColumnMigrationHazard()},
				},
			},
		},
		{
			name:      "Extension installation",
			oldSchema: schema.Schema{},
			newSchema: schema.Schema{
				Extensions: []schema.Extension{{Name: "pg_trgm"}},
			},
			expectedStatements: []Statement{
				{
					DDL:     "CREATE EXTENSION pg_trgm",
					Timeout: statementTimeoutDefault,
				},
			},
		},
		{
			name:      "Extension installation with explicit version",
			oldSchema: schema.Schema{},
			newSchema: schema.Schema{
				Extensions: []schema.Extension{{
					Name:    "pg_trgm",
					Version: "1.5",
				}},
			},
			expectedStatements: []Statement{
				{
					DDL:     "CREATE EXTENSION pg_trgm WITH VERSION '1.5'",
					Timeout: statementTimeoutDefault,
				},
			},
		},
		{
			name: "Extension removal",
			oldSchema: schema.Schema{
				Extensions: []schema.Extension{{Name: "pg_trgm"}},
			},
			newSchema: schema.Schema{},
			expectedStatements: []Statement{
				{
					DDL:     "DROP EXTENSION pg_trgm",
					Timeout: statementTimeoutDefault,
					Hazards: []MigrationHazard{migrationHazardExtensionDroppedCannotTrackDependencies},
				},
			},
		},
		{
			name: "Extension with explicit version upgrade",
			oldSchema: schema.Schema{
				Extensions: []schema.Extension{{
					Name:    "pg_trgm",
					Version: "1.5",
				}},
			},
			newSchema: schema.Schema{
				Extensions: []schema.Extension{{
					Name:    "pg_trgm",
					Version: "1.6",
				}},
			},
			expectedStatements: []Statement{
				{
					DDL:     "ALTER EXTENSION pg_trgm UPDATE TO '1.6'",
					Timeout: statementTimeoutDefault,
					Hazards: []MigrationHazard{migrationHazardExtensionAlteredVersionUpgraded},
				},
			},
		},
		{
			name: "Extension with implicit version upgrade",
			oldSchema: schema.Schema{
				Extensions: []schema.Extension{{
					Name:    "pg_trgm",
					Version: "1.5",
				}},
			},
			newSchema: schema.Schema{
				Extensions: []schema.Extension{{
					Name: "pg_trgm",
				}},
			},
			expectedStatements: []Statement{
				{
					DDL:     "ALTER EXTENSION pg_trgm UPDATE",
					Timeout: statementTimeoutDefault,
					Hazards: []MigrationHazard{migrationHazardExtensionAlteredVersionUpgraded},
				},
			},
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
	uuid.SetRand(&deterministicRandReader{})

	for _, testCase := range schemaMigrationPlanTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			schemaDiff, _, err := buildSchemaDiff(testCase.oldSchema, testCase.newSchema)
			if testCase.expectedDiffErrIs != nil {
				require.ErrorIs(t, err, testCase.expectedDiffErrIs)
			} else {
				require.NoError(t, err)
			}
			stmts, err := schemaSQLGenerator{}.Alter(schemaDiff)
			require.NoError(t, err)
			assert.Equal(t, testCase.expectedStatements, stmts, "actual:\n %# v", pretty.Formatter(stmts))
		})
	}
}

func buildColumnDataDeletionHazard() MigrationHazard {
	return MigrationHazard{
		Type:    MigrationHazardTypeDeletesData,
		Message: "Deletes all values in the column",
	}
}

func buildColumnTypeChangeHazard() MigrationHazard {
	return MigrationHazard{
		Type: MigrationHazardTypeAcquiresAccessExclusiveLock,
		Message: "This will completely lock the table while the data is being re-written. The duration of this " +
			"conversion depends on if the type conversion is trivial or not. A non-trivial conversion will require a " +
			"table rewrite. A trivial conversion is one where the binary values are coercible and the column contents " +
			"are not changing.",
	}
}

func buildAnalyzeColumnMigrationHazard() MigrationHazard {
	return MigrationHazard{
		Type: MigrationHazardTypeImpactsDatabasePerformance,
		Message: "Running analyze will read rows from the table, putting increased load " +
			"on the database and consuming database resources. It won't prevent reads/writes to " +
			"the table, but it could affect performance when executing queries.",
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

func buildIndexDroppedAcquiresLockHazard() MigrationHazard {
	return MigrationHazard{
		Type:    MigrationHazardTypeAcquiresAccessExclusiveLock,
		Message: "Index drops will lock out all accesses to the table. They should be fast",
	}
}
