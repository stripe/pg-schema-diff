package diff

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/internal/testdb"
	externalschema "github.com/stripe/pg-schema-diff/pkg/schema"
)

type fakeSchemaSource struct {
	t *testing.T

	expectedDeps schemaSourcePlanDeps
	snapshot     schema.SchemaSnapshot
	err          error
}

func (f fakeSchemaSource) GetSchemaSnapshot(_ context.Context, deps schemaSourcePlanDeps) (schema.SchemaSnapshot, error) {
	assert.Equal(f.t, f.expectedDeps.logger, deps.logger)
	assert.Equal(f.t, f.expectedDeps.tempDBFactory, deps.tempDBFactory)
	// We can't easily compare the function pointers, so we'll just assert the length of the slices.
	assert.Len(f.t, f.expectedDeps.getSchemaOpts, len(deps.getSchemaOpts))
	if f.err != nil {
		return schema.SchemaSnapshot{}, f.err
	}
	return f.snapshot, nil
}

func mustApplyDDLToTestDB(t testing.TB, db *pgxpool.Pool, ddl []string) {
	t.Helper()
	for _, stmt := range ddl {
		_, err := db.Exec(t.Context(), stmt)
		assert.NoError(t, err)
	}
}

func mustApplyMigrationPlan(t testing.TB, db *pgxpool.Pool, plan Plan) {
	t.Helper()
	// Run the migration
	for _, stmt := range plan.Statements {
		_, err := db.Exec(t.Context(), stmt.ToSQL())
		require.NoError(t, err)
	}
}

func TestSimpleMigratorTestSuite(t *testing.T) {
	t.Parallel()

	t.Run("TestGenerate", func(t *testing.T) {
		t.Parallel()
		factory := testdb.MustNewFactory(t)
		db := factory.CreateDatabase(t)

		initialDDL := `
	CREATE TABLE foobar(
	    id CHAR(16) PRIMARY KEY
    ); `
		newSchemaDDL := `
	CREATE TABLE foobar(
	    id  CHAR(16) PRIMARY KEY,
		new_column VARCHAR(128) NOT NULL
    );
	`

		mustApplyDDLToTestDB(t, db.ConnPool, []string{initialDDL})

		plan, err := Generate(t.Context(), DBSchemaSource(db.ConnPool),
			DDLSchemaSource([]string{newSchemaDDL}), WithTempDbFactory(factory))
		assert.NoError(t, err)

		mustApplyMigrationPlan(t, db.ConnPool, plan)
		// Ensure that some sort of migration ran. we're really not testing the correctness of the
		// migration in this test suite
		_, err = db.ConnPool.Exec(t.Context(),
			"SELECT new_column FROM foobar;")
		assert.NoError(t, err)
	})

	t.Run("TestGeneratePlan_SchemaSourceErr", func(t *testing.T) {
		t.Parallel()
		factory := testdb.MustNewFactory(t)
		db := factory.CreateDatabase(t)

		logger := slog.Default()

		getSchemaOpts := []externalschema.GetSchemaOpt{
			externalschema.WithIncludeSchemaPatterns("schema_1"),
			externalschema.WithIncludeSchemaPatterns("schema_2"),
		}
		expectedGetSchemaOpts := append([]externalschema.GetSchemaOpt{}, getSchemaOpts...)
		expectedGetSchemaOpts = append(expectedGetSchemaOpts,
			externalschema.WithExcludeSchemaPatterns(defaultSchemaPartialArchivalPrefix+".*"))

		expectedErr := fmt.Errorf("some error")
		fakeSchemaSource := fakeSchemaSource{
			t: t,
			expectedDeps: schemaSourcePlanDeps{
				tempDBFactory: factory,
				logger:        logger,
				getSchemaOpts: expectedGetSchemaOpts,
			},
			err: expectedErr,
		}

		_, err := Generate(
			t.Context(), DBSchemaSource(db.ConnPool), fakeSchemaSource,
			WithTempDbFactory(factory),
			WithGetSchemaOpts(getSchemaOpts...),
			WithLogger(logger),
		)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("TestGenerate_CannotBuildMigrationFromDDLWithoutTempDbFactory", func(t *testing.T) {
		t.Parallel()
		factory := testdb.MustNewFactory(t)
		db := factory.CreateDatabase(t)

		_, err := Generate(
			t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource([]string{``}),
			WithIncludeSchemaPatterns("public"),
			WithDoNotValidatePlan(),
		)
		assert.ErrorContains(t, err, "tempDbFactory is required")
	})

	t.Run("TestGenerate_CannotValidateWithoutTempDbFactory", func(t *testing.T) {
		t.Parallel()
		factory := testdb.MustNewFactory(t)
		db := factory.CreateDatabase(t)

		_, err := Generate(
			t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource([]string{``}),
			WithIncludeSchemaPatterns("public"),
			WithDoNotValidatePlan(),
		)
		assert.ErrorContains(t, err, "tempDbFactory is required")
	})
}

func TestValidateSchemaPartialArchivalPrefix(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		prefix      string
		expectError bool
	}{
		{name: "default", prefix: defaultSchemaPartialArchivalPrefix},
		{name: "custom", prefix: "deleted"},
		{name: "empty", prefix: "", expectError: true},
		{name: "not simple", prefix: "deleted-schema", expectError: true},
		{name: "reserved pg", prefix: "pg", expectError: true},
		{name: "reserved pg prefix", prefix: "pg_deleted", expectError: true},
		{name: "too long", prefix: "abcdefghijklmnopqrstuv", expectError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSchemaPartialArchivalPrefix(tc.prefix)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGenerateIgnoresCleanupSchemaPrefixes(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		prefix       string
		schemaPrefix string
		inTarget     bool
		expectEmpty  bool
	}{
		{name: "default prefix", prefix: defaultSchemaPartialArchivalPrefix, expectEmpty: true},
		{name: "custom prefix", prefix: "deleted", expectEmpty: true},
		{
			name:         "custom prefix replaces default exclusion",
			prefix:       "deleted",
			schemaPrefix: defaultSchemaPartialArchivalPrefix,
		},
		{name: "target schema", prefix: defaultSchemaPartialArchivalPrefix, inTarget: true, expectEmpty: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			factory := testdb.MustNewFactory(t)
			db := factory.CreateDatabase(t)
			schemaPrefix := tc.schemaPrefix
			if schemaPrefix == "" {
				schemaPrefix = tc.prefix
			}
			ddl := fmt.Sprintf(`
				CREATE SCHEMA %s_snapshot;
				CREATE TABLE %s_snapshot.foobar (id bigint PRIMARY KEY);
			`, schemaPrefix, schemaPrefix)
			var targetDDL []string
			if tc.inTarget {
				targetDDL = []string{ddl}
			} else {
				mustApplyDDLToTestDB(t, db.ConnPool, []string{ddl})
			}

			opts := []PlanOpt{WithTempDbFactory(factory)}
			if tc.prefix != defaultSchemaPartialArchivalPrefix {
				opts = append(opts, WithSchemaPartialArchivalPrefix(tc.prefix))
			}
			plan, err := Generate(t.Context(), DBSchemaSource(db.ConnPool), DDLSchemaSource(targetDDL), opts...)
			require.NoError(t, err)
			assert.Empty(t, plan.CleanupStatements)
			if tc.expectEmpty {
				assert.Empty(t, plan.Statements)
			} else {
				assert.NotEmpty(t, plan.Statements)
			}
		})
	}
}

func TestGenerateCapturesGenerationTimestamp(t *testing.T) {
	t.Parallel()

	clockTime := time.Date(2026, time.July, 21, 9, 10, 11, 123456789,
		time.FixedZone("test", -7*60*60))
	clockReads := 0
	var capturedOptions *planOptions
	clockOpt := func(opts *planOptions) {
		capturedOptions = opts
		opts.now = func() time.Time {
			clockReads++
			return clockTime
		}
	}
	source := fakeSchemaSource{
		t: t,
		expectedDeps: schemaSourcePlanDeps{
			logger:        slog.Default(),
			getSchemaOpts: make([]schema.GetSchemaOpt, 1),
		},
		snapshot: schema.SchemaSnapshot{Hash: "snapshot-hash"},
	}

	plan, err := Generate(t.Context(), source, source, WithDoNotValidatePlan(), clockOpt)
	require.NoError(t, err)
	require.NotNil(t, capturedOptions)
	assert.Equal(t, 1, clockReads)
	assert.Equal(t, clockTime.UTC(), capturedOptions.generationTimestamp)
	assert.Equal(t, time.UTC, capturedOptions.generationTimestamp.Location())
	assert.Empty(t, plan.CleanupStatements)
	assert.Equal(t, "snapshot-hash", plan.CurrentSchemaHash)
}

func TestGenerateRejectsChangedExtensionOwningHiddenTableWithoutValidation(t *testing.T) {
	t.Parallel()

	extension := schema.CatalogExtensionIdentity{
		Name: "hidden_owner", Version: "1", SchemaName: "public",
	}
	expectedDeps := schemaSourcePlanDeps{
		logger:        slog.Default(),
		getSchemaOpts: make([]schema.GetSchemaOpt, 1),
	}
	current := fakeSchemaSource{
		t: t, expectedDeps: expectedDeps,
		snapshot: schema.SchemaSnapshot{
			Schema: schema.Schema{Extensions: []schema.Extension{{
				SchemaQualifiedName: schema.SchemaQualifiedName{
					SchemaName: "public", EscapedName: `"hidden_owner"`,
				},
				Version: extension.Version,
			}}},
			Inventory: schema.CatalogInventory{
				Extensions: []schema.CatalogExtensionIdentity{extension},
				Relations: []schema.CatalogRelation{{
					OID: 10, SchemaName: "hidden", Name: "member", Kind: schema.RelKindOrdinaryTable,
					Extension: &schema.CatalogExtension{Name: extension.Name, OID: 1},
				}},
			},
		},
	}
	target := fakeSchemaSource{t: t, expectedDeps: expectedDeps}

	_, err := Generate(t.Context(), current, target, WithDoNotValidatePlan())
	require.ErrorContains(t, err, "extension owns table-like relation hidden.member")
}

func TestGenerateKeepsSourceSafetyPreflightDormant(t *testing.T) {
	t.Parallel()

	expectedDeps := schemaSourcePlanDeps{
		logger:        slog.Default(),
		getSchemaOpts: make([]schema.GetSchemaOpt, 1),
	}
	current := fakeSchemaSource{
		t: t, expectedDeps: expectedDeps,
		snapshot: schema.SchemaSnapshot{
			Schema: schema.Schema{Tables: []schema.Table{{
				SchemaQualifiedName: schema.SchemaQualifiedName{
					SchemaName: "public", EscapedName: `"archived"`,
				},
			}}},
			Inventory: schema.CatalogInventory{
				Relations: []schema.CatalogRelation{
					{OID: 10, SchemaName: "public", Name: "archived", Kind: schema.RelKindOrdinaryTable},
					{OID: 20, SchemaName: "excluded", Name: "dependent", Kind: schema.RelKindView},
				},
				Views: []schema.CatalogView{{
					RelationOID: 20, SchemaName: "excluded", Name: "dependent", Kind: schema.RelKindView,
				}},
				Dependencies: []schema.CatalogDependency{{
					Dependent: schema.CatalogDependencyObject{
						ClassOID: pgRewriteCatalogOID, ObjectOID: 21, ObjectType: "rule",
						Identity: "_RETURN on excluded.dependent",
					},
					Referenced: schema.CatalogDependencyObject{
						ClassOID: pgClassCatalogOID, ObjectOID: 10, ObjectType: "table",
						Identity: "public.archived",
					},
				}},
			},
		},
	}
	target := fakeSchemaSource{t: t, expectedDeps: expectedDeps}

	plan, err := Generate(t.Context(), current, target, WithDoNotValidatePlan())
	require.NoError(t, err)
	require.Len(t, plan.Statements, 1)
	assert.Equal(t, Statement{
		DDL: `DROP TABLE "public"."archived"`,
		Hazards: []MigrationHazard{{
			Type:    MigrationHazardTypeDeletesData,
			Message: "Deletes all rows in the table (and the table itself)",
		}},
	}, plan.Statements[0])
}

func TestGenerateKeepsArchivedDependencyClosureDormant(t *testing.T) {
	t.Parallel()

	expectedDeps := schemaSourcePlanDeps{
		logger: slog.Default(), getSchemaOpts: make([]schema.GetSchemaOpt, 1),
	}
	current := fakeSchemaSource{
		t: t, expectedDeps: expectedDeps,
		snapshot: schema.SchemaSnapshot{
			Hash: "legacy-hash",
			Schema: schema.Schema{Tables: []schema.Table{{
				SchemaQualifiedName: schema.SchemaQualifiedName{
					SchemaName: "public", EscapedName: `"archived"`,
				},
			}}},
			Inventory: schema.CatalogInventory{
				Relations: []schema.CatalogRelation{{
					OID: 10, SchemaName: "public", Name: "archived", Kind: schema.RelKindOrdinaryTable,
				}},
				Types: []schema.CatalogType{{
					OID: 20, SchemaName: "public", Name: "status", Kind: schema.CatalogTypeKindEnum,
				}},
				Dependencies: []schema.CatalogDependency{{
					Dependent:  closureTableAddress(10, "public", "archived"),
					Referenced: closureAddress(pgTypeCatalogOID, 20, "public", "status", "public.status"),
				}},
			},
		},
	}
	target := fakeSchemaSource{t: t, expectedDeps: expectedDeps}

	plan, err := Generate(t.Context(), current, target, WithDoNotValidatePlan())
	require.NoError(t, err)
	require.Len(t, plan.Statements, 1)
	assert.Equal(t, `DROP TABLE "public"."archived"`, plan.Statements[0].DDL)
	assert.Empty(t, plan.CleanupStatements)
	assert.Equal(t, "legacy-hash", plan.CurrentSchemaHash)
}

func TestGenerateKeepsLegacyTableRecreationPhysical(t *testing.T) {
	t.Parallel()

	expectedDeps := schemaSourcePlanDeps{
		logger: slog.Default(), getSchemaOpts: make([]schema.GetSchemaOpt, 1),
	}
	tableName := schema.SchemaQualifiedName{SchemaName: "public", EscapedName: `"recreated"`}
	current := fakeSchemaSource{
		t: t, expectedDeps: expectedDeps,
		snapshot: schema.SchemaSnapshot{Schema: schema.Schema{Tables: []schema.Table{{
			SchemaQualifiedName: tableName,
			Columns:             []schema.Column{{Name: "id", Type: "bigint"}},
			ReplicaIdentity:     schema.ReplicaIdentityDefault,
		}}}},
	}
	target := fakeSchemaSource{
		t: t, expectedDeps: expectedDeps,
		snapshot: schema.SchemaSnapshot{Schema: schema.Schema{Tables: []schema.Table{{
			SchemaQualifiedName: tableName,
			Columns:             []schema.Column{{Name: "id", Type: "bigint"}},
			PartitionKeyDef:     "HASH (id)",
			ReplicaIdentity:     schema.ReplicaIdentityDefault,
		}}}},
	}

	plan, err := Generate(t.Context(), current, target, WithDoNotValidatePlan())
	require.NoError(t, err)
	assert.Equal(t, []Statement{
		{
			DDL: `DROP TABLE "public"."recreated"`,
			Hazards: []MigrationHazard{{
				Type:    MigrationHazardTypeDeletesData,
				Message: "Deletes all rows in the table (and the table itself)",
			}},
		},
		{DDL: "CREATE TABLE \"public\".\"recreated\" (\n\t\"id\" bigint NOT NULL\n) PARTITION BY HASH (id)"},
	}, plan.Statements)
	assert.Empty(t, plan.CleanupStatements)
}
