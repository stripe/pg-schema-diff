package tempdb

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
	internalschema "github.com/stripe/pg-schema-diff/internal/schema"
)

type onInstanceTempDbFactorySuite struct {
	suite.Suite

	config *pgxpool.Config
}

func (suite *onInstanceTempDbFactorySuite) SetupSuite() {
	connString := os.Getenv("TEST_DATABASE_URL")
	suite.Require().NotEmpty(connString)
	config, err := pgxpool.ParseConfig(connString)
	suite.Require().NoError(err)
	suite.config = config
}

func (suite *onInstanceTempDbFactorySuite) mustBuildFactory(opt ...FactoryOption) Factory {
	factory, err := NewFactory(context.Background(), suite.config, opt...)
	suite.Require().NoError(err)
	return factory
}

func (suite *onInstanceTempDbFactorySuite) getConnPoolForDb(dbName string) (*pgxpool.Pool, error) {
	config := suite.config.Copy()
	config.ConnConfig.Database = dbName
	return pgxpool.NewWithConfig(context.Background(), config)
}

func (suite *onInstanceTempDbFactorySuite) mustRunSQL(conn *pgxpool.Conn) {
	_, err := conn.Exec(context.Background(), `
		CREATE TABLE foobar(
		  id INT PRIMARY KEY,
		  message TEXT
		);
		CREATE INDEX message_idx ON foobar(message);
	  	`)
	suite.Require().NoError(err)

	_, err = conn.Exec(context.Background(), `
		INSERT INTO foobar VALUES (1, 'some message'), (2, 'some other message'), (3, 'a final message');
	`)
	suite.Require().NoError(err)

	res, err := conn.Query(context.Background(), `
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

	// Drop the table we just created
	_, err = conn.Exec(context.Background(), "DROP TABLE foobar")
	suite.Require().NoError(err)
}

func (suite *onInstanceTempDbFactorySuite) TestNew_ErrorsOnNonSimpleDbPrefix() {
	_, err := NewFactory(context.Background(), suite.config, WithDbPrefix("non-simple identifier"))
	suite.ErrorContains(err, "must be a simple Postgres identifier")
}

func (suite *onInstanceTempDbFactorySuite) TestNew_ErrorsOnNilConfig() {
	_, err := NewFactory(context.Background(), nil)
	suite.ErrorContains(err, "rootConfig must not be nil")
}

func (suite *onInstanceTempDbFactorySuite) TestCreate_CreateAndDropFlow() {
	const (
		dbPrefix = "some_prefix"
	)

	factory := suite.mustBuildFactory(
		WithDbPrefix(dbPrefix),
		WithLogger(slog.Default()),
	)
	defer func(factory Factory) {
		suite.Require().NoError(factory.Close())
	}(factory)

	tempDb, err := factory.Create(context.Background())
	suite.Require().NoError(err)
	// Don't defer dropping. we want to run assertions after it drops. if dropping fails,
	// it shouldn't be a problem because names shouldn't conflict

	conn1, err := tempDb.ConnPool.Acquire(context.Background())
	suite.Require().NoError(err)

	var dbName string
	suite.Require().NoError(conn1.QueryRow(context.Background(), "SELECT current_database()").Scan(&dbName))
	suite.True(strings.HasPrefix(dbName, dbPrefix))
	suite.Regexp(`^`+dbPrefix+`[a-z]+_[a-z]+_[0-9a-f]{10}$`, dbName)

	// Make sure SQL can run on the connection
	suite.mustRunSQL(conn1)

	// Get another connection from the pool and make sure it's also set to the correct db while
	// the other connection is still open
	conn2, err := tempDb.ConnPool.Acquire(context.Background())
	suite.Require().NoError(err)
	var dbNameFromConn2 string
	suite.Require().NoError(conn2.QueryRow(context.Background(), "SELECT current_database()").Scan(&dbNameFromConn2))
	suite.Equal(dbName, dbNameFromConn2)

	conn1.Release()
	conn2.Release()

	// A newly created temporary database should contain no user-defined objects.
	schema, err := internalschema.GetSchema(context.Background(), tempDb.ConnPool)
	suite.Require().NoError(err)
	suite.Equal(internalschema.Schema{
		NamedSchemas: []internalschema.NamedSchema{{
			Name: "public",
		}},
	}, schema)

	// Drop database
	suite.Require().NoError(tempDb.Close(context.Background()))
	suite.Require().NoError(tempDb.Close(context.Background()))

	// Expect an error when attempting to query the database, since it should be dropped.
	// when a db pool is opened, it has no connections.
	// a query is needed in order to find if the database still exists.
	conn, err := suite.getConnPoolForDb(dbName)
	suite.Require().NoError(err)
	defer conn.Close()
	var one int
	suite.Require().ErrorContains(conn.QueryRow(context.Background(), "SELECT 1").Scan(&one), "SQLSTATE 3D000")
}

func (suite *onInstanceTempDbFactorySuite) TestDropTempDB_CannotDropNonTempDb() {
	factory := suite.mustBuildFactory()
	defer func(factory Factory) {
		suite.Require().NoError(factory.Close())
	}(factory)

	suite.ErrorContains(factory.(*onInstanceFactory).dropTempDatabase(context.Background(),
		"some_db"), "drop non-temporary database")
}

func TestOnInstanceFactorySuite(t *testing.T) {
	suite.Run(t, new(onInstanceTempDbFactorySuite))
}
