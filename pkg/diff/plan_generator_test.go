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
	externalschema "github.com/stripe/pg-schema-diff/pkg/schema"
)

type fakeSchemaSource struct {
	t *testing.T

	expectedDeps schemaSourcePlanDeps
	schema       schema.Schema
	err          error
}

func (f fakeSchemaSource) GetSchema(_ context.Context, deps schemaSourcePlanDeps) (schema.Schema, error) {
	assert.Equal(f.t, f.expectedDeps.logger, deps.logger)
	assert.Equal(f.t, f.expectedDeps.tempDBFactory, deps.tempDBFactory)
	// We can't easily compare the function pointers, so we'll just assert the length of the slices.
	assert.Len(f.t, f.expectedDeps.getSchemaOpts, len(deps.getSchemaOpts))
	return f.schema, f.err
}

func applyDDLToTestDB(t *testing.T, conn *pgxpool.Pool, ddl []string) {
	t.Helper()
	for _, stmt := range ddl {
		_, err := conn.Exec(context.Background(), stmt)
		require.NoError(t, err)
	}
}

func applyMigrationPlan(t *testing.T, db *pgxpool.Pool, plan Plan) {
	t.Helper()
	for _, stmt := range plan.Statements {
		_, err := db.Exec(context.Background(), stmt.ToSQL())
		require.NoError(t, err)
	}
}

func TestGenerate(t *testing.T) {
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

	connPool := poolFactory.Pool(t)
	applyDDLToTestDB(t, connPool, []string{initialDDL})

	tempDbFactory := testTempDBFactory{t: t}

	plan, err := Generate(context.Background(), DBSchemaSource(connPool),
		DDLSchemaSource([]string{newSchemaDDL}), WithTempDbFactory(tempDbFactory))
	require.NoError(t, err)

	applyMigrationPlan(t, connPool, plan)
	// Ensure that some sort of migration ran. we're really not testing the correctness of the
	// migration in this test suite
	_, err = connPool.Exec(context.Background(),
		"SELECT new_column FROM foobar;")
	require.NoError(t, err)
}

func TestGeneratePlanSchemaSourceErr(t *testing.T) {
	tempDbFactory := testTempDBFactory{t: t}
	logger := slog.Default()

	getSchemaOpts := []externalschema.GetSchemaOpt{
		externalschema.WithIncludeSchemas("schema_1"),
		externalschema.WithIncludeSchemas("schema_2"),
	}

	expectedErr := fmt.Errorf("some error")
	fakeSchemaSource := fakeSchemaSource{
		t: t,
		expectedDeps: schemaSourcePlanDeps{
			tempDBFactory: tempDbFactory,
			logger:        logger,
			getSchemaOpts: getSchemaOpts,
		},
		err: expectedErr,
	}

	connPool := poolFactory.Pool(t)

	_, err := Generate(
		context.Background(), DBSchemaSource(connPool), fakeSchemaSource,
		WithTempDbFactory(tempDbFactory),
		WithGetSchemaOpts(getSchemaOpts...),
		WithLogger(logger),
	)
	require.ErrorIs(t, err, expectedErr)
}

func TestGenerateCannotPackNewTablesWithoutIgnoringChangesToColumnOrder(t *testing.T) {
	tempDbFactory := testTempDBFactory{t: t}
	connPool := poolFactory.Pool(t)

	_, err := Generate(
		context.Background(), DBSchemaSource(connPool), DDLSchemaSource([]string{``}),
		WithTempDbFactory(tempDbFactory),
		WithDataPackNewTables(),
		WithRespectColumnOrder(),
	)
	require.ErrorContains(t, err, "cannot data pack new tables without also ignoring changes to column order")
}

func TestGenerateCannotBuildMigrationFromDDLWithoutTempDbFactory(t *testing.T) {
	pool := poolFactory.Pool(t)
	_, err := Generate(
		context.Background(), DBSchemaSource(pool), DDLSchemaSource([]string{``}),
		WithIncludeSchemas("public"),
		WithDoNotValidatePlan(),
	)
	require.ErrorContains(t, err, "tempDbFactory is required")
}

func TestGenerateCannotValidateWithoutTempDbFactory(t *testing.T) {
	pool := poolFactory.Pool(t)
	_, err := Generate(
		context.Background(), DBSchemaSource(pool), DDLSchemaSource([]string{``}),
		WithIncludeSchemas("public"),
		WithDoNotValidatePlan(),
	)
	require.ErrorContains(t, err, "tempDbFactory is required")
}
