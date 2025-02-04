package diff

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/stripe/pg-schema-diff/internal/pgengine"
	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/pkg/log"
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

	pgEngine *pgengine.Engine
	db       *pgengine.DB
}

func (suite *planGeneratorTestSuite) mustGetTestDBPool() *sql.DB {
	pool, err := sql.Open("pgx", suite.db.GetDSN())
	suite.NoError(err)
	return pool
}

func (suite *planGeneratorTestSuite) mustBuildTempDbFactory(ctx context.Context) tempdb.Factory {
	tempDbFactory, err := tempdb.NewOnInstanceFactory(ctx, func(ctx context.Context, dbName string) (*sql.DB, error) {
		return sql.Open("pgx", suite.pgEngine.GetPostgresDatabaseConnOpts().With("dbname", dbName).ToDSN())
	})
	suite.Require().NoError(err)
	return tempDbFactory
}

func (suite *planGeneratorTestSuite) mustApplyDDLToTestDb(ddl []string) {
	conn := suite.mustGetTestDBPool()
	defer conn.Close()

	for _, stmt := range ddl {
		_, err := conn.Exec(stmt)
		suite.NoError(err)
	}
}

func (suite *planGeneratorTestSuite) SetupSuite() {
	engine, err := pgengine.StartEngine()
	suite.Require().NoError(err)
	suite.pgEngine = engine
}

func (suite *planGeneratorTestSuite) TearDownSuite() {
	suite.pgEngine.Close()
}

func (suite *planGeneratorTestSuite) SetupTest() {
	db, err := suite.pgEngine.CreateDatabase()
	suite.NoError(err)
	suite.db = db
}

func (suite *planGeneratorTestSuite) TearDownTest() {
	suite.db.DropDB()
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

	connPool := suite.mustGetTestDBPool()
	defer connPool.Close()

	tempDbFactory := suite.mustBuildTempDbFactory(context.Background())
	defer tempDbFactory.Close()

	plan, err := Generate(context.Background(), DBSchemaSource(connPool), DDLSchemaSource([]string{newSchemaDDL}), WithTempDbFactory(tempDbFactory))
	suite.NoError(err)

	suite.mustApplyMigrationPlan(connPool, plan)
	// Ensure that some sort of migration ran. we're really not testing the correctness of the
	// migration in this test suite
	_, err = connPool.ExecContext(context.Background(),
		"SELECT new_column FROM foobar;")
	suite.NoError(err)
}

func (suite *planGeneratorTestSuite) TestGeneratePlan_SchemaSourceErr() {
	tempDbFactory := suite.mustBuildTempDbFactory(context.Background())
	defer tempDbFactory.Close()

	logger := log.SimpleLogger()

	getSchemaOpts := []externalschema.GetSchemaOpt{
		externalschema.WithIncludeSchemas("schema_1"),
		externalschema.WithIncludeSchemas("schema_2"),
	}

	expectedErr := fmt.Errorf("some error")
	fakeSchemaSource := fakeSchemaSource{
		t: suite.T(),
		expectedDeps: schemaSourcePlanDeps{
			tempDBFactory: tempDbFactory,
			logger:        logger,
			getSchemaOpts: getSchemaOpts,
		},
		err: expectedErr,
	}

	connPool := suite.mustGetTestDBPool()
	defer connPool.Close()

	_, err := Generate(context.Background(), DBSchemaSource(connPool), fakeSchemaSource,
		WithTempDbFactory(tempDbFactory),
		WithGetSchemaOpts(getSchemaOpts...),
		WithLogger(logger),
	)
	suite.ErrorIs(err, expectedErr)
}

func (suite *planGeneratorTestSuite) mustApplyMigrationPlan(db *sql.DB, plan Plan) {
	// Run the migration
	for _, stmt := range plan.Statements {
		_, err := db.ExecContext(context.Background(), stmt.ToSQL())
		suite.Require().NoError(err)
	}
}

func (suite *planGeneratorTestSuite) TestGenerate_CannotPackNewTablesWithoutIgnoringChangesToColumnOrder() {
	tempDbFactory := suite.mustBuildTempDbFactory(context.Background())
	defer tempDbFactory.Close()

	connPool := suite.mustGetTestDBPool()
	defer connPool.Close()

	_, err := Generate(context.Background(), DBSchemaSource(connPool), DDLSchemaSource([]string{``}),
		WithTempDbFactory(tempDbFactory),
		WithDataPackNewTables(),
		WithRespectColumnOrder(),
	)
	suite.ErrorContains(err, "cannot data pack new tables without also ignoring changes to column order")
}

func (suite *planGeneratorTestSuite) TestGenerate_CannotBuildMigrationFromDDLWithoutTempDbFactory() {
	pool := suite.mustGetTestDBPool()
	defer pool.Close()
	_, err := Generate(context.Background(), DBSchemaSource(pool), DDLSchemaSource([]string{``}),
		WithIncludeSchemas("public"),
		WithDoNotValidatePlan(),
	)
	suite.ErrorContains(err, "tempDbFactory is required")
}

func (suite *planGeneratorTestSuite) TestGenerate_CannotValidateWithoutTempDbFactory() {
	pool := suite.mustGetTestDBPool()
	defer pool.Close()
	_, err := Generate(context.Background(), DBSchemaSource(pool), DDLSchemaSource([]string{``}),
		WithIncludeSchemas("public"),
		WithDoNotValidatePlan(),
	)
	suite.ErrorContains(err, "tempDbFactory is required")
}

func TestSimpleMigratorTestSuite(t *testing.T) {
	suite.Run(t, new(planGeneratorTestSuite))
}
