package diff

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

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
	schema       schema.Schema
	err          error
}

func (f fakeSchemaSource) GetSchema(_ context.Context, deps schemaSourcePlanDeps) (*schema.Schema, error) {
	assert.Equal(f.t, f.expectedDeps.logger, deps.logger)
	assert.Equal(f.t, f.expectedDeps.tempDBFactory, deps.tempDBFactory)
	// We can't easily compare the function pointers, so we'll just assert the length of the slices.
	assert.Len(f.t, f.expectedDeps.getSchemaOpts, len(deps.getSchemaOpts))
	if f.err != nil {
		return nil, f.err
	}
	return &f.schema, nil
}

func mustApplyDDLToTestDB(t testing.TB, db *pgxpool.Pool, ddl []string) {
	t.Helper()
	for _, stmt := range ddl {
		_, err := db.Exec(context.Background(), stmt)
		assert.NoError(t, err)
	}
}

func mustApplyMigrationPlan(t testing.TB, db *pgxpool.Pool, plan Plan) {
	t.Helper()
	// Run the migration
	for _, stmt := range plan.Statements {
		_, err := db.Exec(context.Background(), stmt.ToSQL())
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

		plan, err := Generate(context.Background(), DBSchemaSource(db.ConnPool),
			DDLSchemaSource([]string{newSchemaDDL}), WithTempDbFactory(factory))
		assert.NoError(t, err)

		mustApplyMigrationPlan(t, db.ConnPool, plan)
		// Ensure that some sort of migration ran. we're really not testing the correctness of the
		// migration in this test suite
		_, err = db.ConnPool.Exec(context.Background(),
			"SELECT new_column FROM foobar;")
		assert.NoError(t, err)
	})

	t.Run("TestGeneratePlan_SchemaSourceErr", func(t *testing.T) {
		t.Parallel()
		factory := testdb.MustNewFactory(t)
		db := factory.CreateDatabase(t)

		logger := slog.Default()

		getSchemaOpts := []externalschema.GetSchemaOpt{
			externalschema.WithIncludeSchemas("schema_1"),
			externalschema.WithIncludeSchemas("schema_2"),
		}

		expectedErr := fmt.Errorf("some error")
		fakeSchemaSource := fakeSchemaSource{
			t: t,
			expectedDeps: schemaSourcePlanDeps{
				tempDBFactory: factory,
				logger:        logger,
				getSchemaOpts: getSchemaOpts,
			},
			err: expectedErr,
		}

		_, err := Generate(
			context.Background(), DBSchemaSource(db.ConnPool), fakeSchemaSource,
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
			context.Background(), DBSchemaSource(db.ConnPool), DDLSchemaSource([]string{``}),
			WithIncludeSchemas("public"),
			WithDoNotValidatePlan(),
		)
		assert.ErrorContains(t, err, "tempDbFactory is required")
	})

	t.Run("TestGenerate_CannotValidateWithoutTempDbFactory", func(t *testing.T) {
		t.Parallel()
		factory := testdb.MustNewFactory(t)
		db := factory.CreateDatabase(t)

		_, err := Generate(
			context.Background(), DBSchemaSource(db.ConnPool), DDLSchemaSource([]string{``}),
			WithIncludeSchemas("public"),
			WithDoNotValidatePlan(),
		)
		assert.ErrorContains(t, err, "tempDbFactory is required")
	})
}
