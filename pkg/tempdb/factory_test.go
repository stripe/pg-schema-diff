package tempdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/suite"
	"github.com/stripe/pg-schema-diff/internal/pgengine"

	"github.com/stripe/pg-schema-diff/pkg/log"
)

type onInstanceTempDbFactorySuite struct {
	suite.Suite

	engine *pgengine.Engine
}

func (suite *onInstanceTempDbFactorySuite) SetupSuite() {
	engine, err := pgengine.StartEngine()
	suite.Require().NoError(err)
	suite.engine = engine
}

func (suite *onInstanceTempDbFactorySuite) TearDownSuite() {
	suite.engine.Close()
}

func (suite *onInstanceTempDbFactorySuite) mustBuildFactory(opt ...OnInstanceFactoryOpt) Factory {
	factory, err := NewOnInstanceFactory(context.Background(), func(ctx context.Context, dbName string) (*sql.DB, error) {
		return suite.getConnPoolForDb(dbName)
	}, opt...)
	suite.Require().NoError(err)
	return factory
}

func (suite *onInstanceTempDbFactorySuite) getConnPoolForDb(dbName string) (*sql.DB, error) {
	return sql.Open("pgx", suite.engine.GetPostgresDatabaseConnOpts().With("dbname", dbName).ToDSN())
}

func (suite *onInstanceTempDbFactorySuite) mustRunSQL(conn *sql.Conn) {
	_, err := conn.ExecContext(context.Background(), `
		CREATE TABLE foobar(
		  id INT PRIMARY KEY,
		  message TEXT
		);
		CREATE INDEX message_idx ON foobar(message);
 	`)
	suite.Require().NoError(err)

	_, err = conn.ExecContext(context.Background(), `
		INSERT INTO foobar VALUES (1, 'some message'), (2, 'some other message'), (3, 'a final message');
	`)
	suite.Require().NoError(err)

	res, err := conn.QueryContext(context.Background(), `
		SELECT id, message FROM foobar;
	`)
	suite.Require().NoError(err)

	var rows [][]any
	for res.Next() {
		var id int
		var message string
		suite.Require().NoError(res.Scan(&id, &message))
		rows = append(rows, []any{
			id, message,
		})
	}
	suite.ElementsMatch([][]any{
		{1, "some message"},
		{2, "some other message"},
		{3, "a final message"},
	}, rows)
}

func (suite *onInstanceTempDbFactorySuite) TestNew_ConnectsToWrongDatabase() {
	db, err := suite.engine.CreateDatabaseWithName("not-postgres")
	suite.Require().NoError(err)
	defer db.DropDB()

	_, err = NewOnInstanceFactory(context.Background(), func(ctx context.Context, dbName string) (*sql.DB, error) {
		return suite.getConnPoolForDb("not-postgres")
	})
	suite.ErrorContains(err, "connection pool is on")
}

func (suite *onInstanceTempDbFactorySuite) TestNew_ErrorsOnNonSimpleDbPrefix() {
	_, err := NewOnInstanceFactory(context.Background(), func(ctx context.Context, dbName string) (*sql.DB, error) {
		suite.Fail("shouldn't be reached")
		return nil, nil
	}, WithDbPrefix("non-simple identifier"))
	suite.ErrorContains(err, "must be a simple Postgres identifier")
}

func (suite *onInstanceTempDbFactorySuite) TestCreate_CreateAndDropFlow() {
	const (
		dbPrefix       = "some_prefix"
		metadataSchema = "some metadata schema"
		metadataTable  = "some metadata table"
	)
	factory := suite.mustBuildFactory(
		WithDbPrefix(dbPrefix),
		WithMetadataSchema(metadataSchema),
		WithMetadataTable(metadataTable),
		WithLogger(log.SimpleLogger()),
	)
	defer func(factory Factory) {
		suite.Require().NoError(factory.Close())
	}(factory)

	db, dropper, err := factory.Create(context.Background())
	suite.Require().NoError(err)
	// don't defer dropping. we want to run assertions after it drops. if dropping fails,
	// it shouldn't be a problem because names shouldn't conflict
	afterTimeOfCreation := time.Now()

	conn1, err := db.Conn(context.Background())
	suite.Require().NoError(err)

	var dbName string
	suite.Require().NoError(conn1.QueryRowContext(context.Background(), "SELECT current_database()").Scan(&dbName))
	suite.True(strings.HasPrefix(dbName, dbPrefix))
	suite.Len(dbName, len(dbPrefix)+36) // should be length of prefix + length of uuid

	// Make sure SQL can run on the connection
	suite.mustRunSQL(conn1)

	// check the metadata entry exists
	var createdAt time.Time
	metadataQuery := fmt.Sprintf(`
		SELECT * FROM "%s"."%s"
	`, metadataSchema, metadataTable)
	suite.Require().NoError(conn1.QueryRowContext(context.Background(), metadataQuery).Scan(&createdAt))
	suite.True(createdAt.Before(afterTimeOfCreation))

	// get another connection from the pool and make sure it's also set to the correct db while
	// the other connection is still open
	conn2, err := db.Conn(context.Background())
	suite.Require().NoError(err)
	var dbNameFromConn2 string
	suite.Require().NoError(conn2.QueryRowContext(context.Background(), "SELECT current_database()").Scan(&dbNameFromConn2))
	suite.Equal(dbName, dbNameFromConn2)

	suite.Require().NoError(conn1.Close())
	suite.Require().NoError(conn2.Close())

	// drop database
	suite.Require().NoError(db.Close())
	suite.Require().NoError(dropper(context.Background()))

	// expect an error when attempting to query the database, since it should be dropped.
	// when a db pool is opened, it has no connections.
	// a query is needed in order to find if the database still exists.
	conn, err := suite.getConnPoolForDb(dbName)
	suite.Require().NoError(err)
	suite.Require().ErrorContains(conn.QueryRowContext(context.Background(), metadataQuery).Scan(&createdAt), "SQLSTATE 3D000")
	suite.True(createdAt.Before(afterTimeOfCreation))
}

func (suite *onInstanceTempDbFactorySuite) TestCreate_ConnectsToWrongDatabase() {
	factory, err := NewOnInstanceFactory(context.Background(), func(ctx context.Context, dbName string) (*sql.DB, error) {
		return suite.getConnPoolForDb("postgres")
	})
	suite.Require().NoError(err)
	defer func(factory Factory) {
		suite.Require().NoError(factory.Close())
	}(factory)

	_, _, err = factory.Create(context.Background())
	suite.ErrorContains(err, "connection pool is on")
}

func (suite *onInstanceTempDbFactorySuite) TestDropTempDB_CannotDropNonTempDb() {
	factory := suite.mustBuildFactory()
	defer func(factory Factory) {
		suite.Require().NoError(factory.Close())
	}(factory)

	suite.ErrorContains(factory.(*onInstanceFactory).dropTempDatabase(context.Background(), "some_db"), "drop non-temporary database")
}

func TestOnInstanceFactorySuite(t *testing.T) {
	suite.Run(t, new(onInstanceTempDbFactorySuite))
}
