package diff_test

import (
	"context"
	"database/sql"
	"io"
	"testing"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/suite"
	"github.com/stripe/pg-schema-diff/internal/pgengine"
	"github.com/stripe/pg-schema-diff/pkg/diff"

	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

type simpleMigratorTestSuite struct {
	suite.Suite

	pgEngine *pgengine.Engine
	db       *pgengine.DB
}

func (suite *simpleMigratorTestSuite) mustGetTestDBPool() *sql.DB {
	pool, err := sql.Open("pgx", suite.db.GetDSN())
	suite.NoError(err)
	return pool
}

func (suite *simpleMigratorTestSuite) mustGetTestDBConn() (conn *sql.Conn, poolCloser io.Closer) {
	pool := suite.mustGetTestDBPool()
	conn, err := pool.Conn(context.Background())
	suite.Require().NoError(err)
	return conn, pool
}

func (suite *simpleMigratorTestSuite) mustBuildTempDbFactory(ctx context.Context) tempdb.Factory {
	tempDbFactory, err := tempdb.NewOnInstanceFactory(ctx, func(ctx context.Context, dbName string) (*sql.DB, error) {
		return sql.Open("pgx", suite.pgEngine.GetPostgresDatabaseConnOpts().With("dbname", dbName).ToDSN())
	})
	suite.Require().NoError(err)
	return tempDbFactory
}

func (suite *simpleMigratorTestSuite) mustApplyDDLToTestDb(ddl []string) {
	conn := suite.mustGetTestDBPool()
	defer conn.Close()

	for _, stmt := range ddl {
		_, err := conn.Exec(stmt)
		suite.NoError(err)
	}
}

func (suite *simpleMigratorTestSuite) SetupSuite() {
	engine, err := pgengine.StartEngine()
	suite.Require().NoError(err)
	suite.pgEngine = engine
}

func (suite *simpleMigratorTestSuite) TearDownSuite() {
	suite.pgEngine.Close()
}

func (suite *simpleMigratorTestSuite) SetupTest() {
	db, err := suite.pgEngine.CreateDatabase()
	suite.NoError(err)
	suite.db = db
}

func (suite *simpleMigratorTestSuite) TearDownTest() {
	suite.db.DropDB()
}

func (suite *simpleMigratorTestSuite) TestPlanAndApplyMigration() {
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

	plan, err := diff.GeneratePlan(context.Background(), connPool, tempDbFactory, []string{newSchemaDDL})
	suite.NoError(err)

	// Run the migration
	for _, stmt := range plan.Statements {
		_, err = connPool.ExecContext(context.Background(), stmt.ToSQL())
		suite.Require().NoError(err)
	}
	// Ensure that some sort of migration ran. we're really not testing the correctness of the
	// migration in this test suite
	_, err = connPool.ExecContext(context.Background(),
		"SELECT new_column FROM foobar;")
	suite.NoError(err)
}

func (suite *simpleMigratorTestSuite) TestCannotPackNewTablesWithoutIgnoringChangesToColumnOrder() {
	tempDbFactory := suite.mustBuildTempDbFactory(context.Background())
	defer tempDbFactory.Close()

	conn, poolCloser := suite.mustGetTestDBConn()
	defer poolCloser.Close()
	defer conn.Close()

	_, err := diff.GeneratePlan(context.Background(), conn, tempDbFactory, []string{``},
		diff.WithDataPackNewTables(),
		diff.WithRespectColumnOrder(),
	)
	suite.ErrorContains(err, "cannot data pack new tables without also ignoring changes to column order")
}

func TestSimpleMigratorTestSuite(t *testing.T) {
	suite.Run(t, new(simpleMigratorTestSuite))
}
