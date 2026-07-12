package diff

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/internal/testdb"
	externalschema "github.com/stripe/pg-schema-diff/pkg/schema"

	"github.com/stripe/pg-schema-diff/pkg/tempdb"
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

type planGeneratorTestSuite struct {
	suite.Suite

	factory *testdb.Factory
	db      *tempdb.Database
}

func (suite *planGeneratorTestSuite) mustApplyDDLToTestDb(ddl []string) {
	for _, stmt := range ddl {
		_, err := suite.db.ConnPool.Exec(context.Background(), stmt)
		suite.NoError(err)
	}
}

func (suite *planGeneratorTestSuite) SetupSuite() {
	factory, err := testdb.NewFactory(context.Background())
	suite.Require().NoError(err)
	suite.factory = factory
}

func (suite *planGeneratorTestSuite) TearDownSuite() {
	suite.Require().NoError(suite.factory.Close())
}

func (suite *planGeneratorTestSuite) SetupTest() {
	db, err := suite.factory.Create(context.Background())
	suite.NoError(err)
	suite.db = db
}

func (suite *planGeneratorTestSuite) TearDownTest() {
	suite.Require().NoError(suite.db.Close(context.Background()))
}

func (suite *planGeneratorTestSuite) TestGenerate() {
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

	suite.mustApplyDDLToTestDb([]string{initialDDL})

	plan, err := Generate(context.Background(), DBSchemaSource(suite.db.ConnPool),
		DDLSchemaSource([]string{newSchemaDDL}), WithTempDbFactory(suite.factory))
	suite.NoError(err)

	suite.mustApplyMigrationPlan(suite.db.ConnPool, plan)
	// Ensure that some sort of migration ran. we're really not testing the correctness of the
	// migration in this test suite
	_, err = suite.db.ConnPool.Exec(context.Background(),
		"SELECT new_column FROM foobar;")
	suite.NoError(err)
}

func (suite *planGeneratorTestSuite) TestGeneratePlan_SchemaSourceErr() {
	logger := slog.Default()

	getSchemaOpts := []externalschema.GetSchemaOpt{
		externalschema.WithIncludeSchemas("schema_1"),
		externalschema.WithIncludeSchemas("schema_2"),
	}

	expectedErr := fmt.Errorf("some error")
	fakeSchemaSource := fakeSchemaSource{
		t: suite.T(),
		expectedDeps: schemaSourcePlanDeps{
			tempDBFactory: suite.factory,
			logger:        logger,
			getSchemaOpts: getSchemaOpts,
		},
		err: expectedErr,
	}

	_, err := Generate(
		context.Background(), DBSchemaSource(suite.db.ConnPool), fakeSchemaSource,
		WithTempDbFactory(suite.factory),
		WithGetSchemaOpts(getSchemaOpts...),
		WithLogger(logger),
	)
	suite.ErrorIs(err, expectedErr)
}

func (suite *planGeneratorTestSuite) mustApplyMigrationPlan(db *pgxpool.Pool, plan Plan) {
	// Run the migration
	for _, stmt := range plan.Statements {
		_, err := db.Exec(context.Background(), stmt.ToSQL())
		suite.Require().NoError(err)
	}
}

func (suite *planGeneratorTestSuite) TestGenerate_CannotPackNewTablesWithoutIgnoringChangesToColumnOrder() {
	_, err := Generate(
		context.Background(), DBSchemaSource(suite.db.ConnPool), DDLSchemaSource([]string{``}),
		WithTempDbFactory(suite.factory),
		WithDataPackNewTables(),
		WithRespectColumnOrder(),
	)
	suite.ErrorContains(err, "cannot data pack new tables without also ignoring changes to column order")
}

func (suite *planGeneratorTestSuite) TestGenerate_CannotBuildMigrationFromDDLWithoutTempDbFactory() {
	_, err := Generate(
		context.Background(), DBSchemaSource(suite.db.ConnPool), DDLSchemaSource([]string{``}),
		WithIncludeSchemas("public"),
		WithDoNotValidatePlan(),
	)
	suite.ErrorContains(err, "tempDbFactory is required")
}

func (suite *planGeneratorTestSuite) TestGenerate_CannotValidateWithoutTempDbFactory() {
	_, err := Generate(
		context.Background(), DBSchemaSource(suite.db.ConnPool), DDLSchemaSource([]string{``}),
		WithIncludeSchemas("public"),
		WithDoNotValidatePlan(),
	)
	suite.ErrorContains(err, "tempDbFactory is required")
}

func TestSimpleMigratorTestSuite(t *testing.T) {
	suite.Run(t, new(planGeneratorTestSuite))
}
