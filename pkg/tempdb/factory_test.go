package tempdb

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	internalschema "github.com/stripe/pg-schema-diff/internal/schema"
)

func mustBuildFactory(t testing.TB, config *pgxpool.Config, opt ...FactoryOption) Factory {
	t.Helper()
	factory, err := NewFactory(context.Background(), config, opt...)
	require.NoError(t, err)
	return factory
}

func getConnPoolForDb(t testing.TB, config *pgxpool.Config, dbName string) (*pgxpool.Pool, error) {
	t.Helper()
	config = config.Copy()
	config.ConnConfig.Database = dbName
	return pgxpool.NewWithConfig(context.Background(), config)
}

func mustRunSQL(t testing.TB, conn *pgxpool.Conn) {
	t.Helper()
	_, err := conn.Exec(context.Background(), `
		CREATE TABLE foobar(
		  id INT PRIMARY KEY,
		  message TEXT
		);
		CREATE INDEX message_idx ON foobar(message);
	  	`)
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), `
		INSERT INTO foobar VALUES (1, 'some message'), (2, 'some other message'), (3, 'a final message');
	`)
	require.NoError(t, err)

	res, err := conn.Query(context.Background(), `
		SELECT id, message FROM foobar;
	`)
	require.NoError(t, err)

	var rows [][]any
	for res.Next() {
		var id int
		var message string
		require.NoError(t, res.Scan(&id, &message))
		rows = append(rows, []any{
			id, message,
		})
	}
	assert.ElementsMatch(t, [][]any{
		{1, "some message"},
		{2, "some other message"},
		{3, "a final message"},
	}, rows)

	// Drop the table we just created
	_, err = conn.Exec(context.Background(), "DROP TABLE foobar")
	require.NoError(t, err)
}

func TestOnInstanceFactorySuite(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("TEST_DATABASE_URL")
	require.NotEmpty(t, connString)
	config, err := pgxpool.ParseConfig(connString)
	require.NoError(t, err)

	t.Run("TestNew_ErrorsOnNonSimpleDbPrefix", func(t *testing.T) {
		t.Parallel()

		_, err := NewFactory(context.Background(), config, WithDbPrefix("non-simple identifier"))
		assert.ErrorContains(t, err, "must be a simple Postgres identifier")
	})

	t.Run("TestNew_ErrorsOnNilConfig", func(t *testing.T) {
		t.Parallel()

		_, err := NewFactory(context.Background(), nil)
		assert.ErrorContains(t, err, "rootConfig must not be nil")
	})

	t.Run("TestCreate_CreateAndDropFlow", func(t *testing.T) {
		t.Parallel()

		const (
			dbPrefix = "some_prefix"
		)

		factory := mustBuildFactory(
			t, config,
			WithDbPrefix(dbPrefix),
			WithLogger(slog.Default()),
		)
		defer func(factory Factory) {
			require.NoError(t, factory.Close())
		}(factory)

		tempDb, err := factory.Create(context.Background())
		require.NoError(t, err)
		// Don't defer dropping. we want to run assertions after it drops. if dropping fails,
		// it shouldn't be a problem because names shouldn't conflict

		conn1, err := tempDb.ConnPool.Acquire(context.Background())
		require.NoError(t, err)

		var dbName string
		require.NoError(t, conn1.QueryRow(context.Background(), "SELECT current_database()").Scan(&dbName))
		assert.True(t, strings.HasPrefix(dbName, dbPrefix))
		assert.Regexp(t, `^`+dbPrefix+`[a-z]+_[a-z]+_[0-9a-f]{10}$`, dbName)

		// Make sure SQL can run on the connection
		mustRunSQL(t, conn1)

		// Get another connection from the pool and make sure it's also set to the correct db while
		// the other connection is still open
		conn2, err := tempDb.ConnPool.Acquire(context.Background())
		require.NoError(t, err)
		var dbNameFromConn2 string
		require.NoError(t, conn2.QueryRow(context.Background(), "SELECT current_database()").Scan(&dbNameFromConn2))
		assert.Equal(t, dbName, dbNameFromConn2)

		conn1.Release()
		conn2.Release()

		// A newly created temporary database should contain no user-defined objects.
		schema, err := internalschema.GetSchema(context.Background(), tempDb.ConnPool)
		require.NoError(t, err)
		assert.Equal(t, &internalschema.Schema{
			NamedSchemas: []internalschema.NamedSchema{{
				Name: "public",
			}},
		}, schema)

		// Drop database
		require.NoError(t, tempDb.Close(context.Background()))
		require.NoError(t, tempDb.Close(context.Background()))

		// Expect an error when attempting to query the database, since it should be dropped.
		// when a db pool is opened, it has no connections.
		// a query is needed in order to find if the database still exists.
		conn, err := getConnPoolForDb(t, config, dbName)
		require.NoError(t, err)
		defer conn.Close()
		var one int
		require.ErrorContains(t, conn.QueryRow(context.Background(), "SELECT 1").Scan(&one), "SQLSTATE 3D000")
	})

	t.Run("TestDropTempDB_CannotDropNonTempDb", func(t *testing.T) {
		t.Parallel()

		factory := mustBuildFactory(t, config)
		defer func(factory Factory) {
			require.NoError(t, factory.Close())
		}(factory)

		assert.ErrorContains(t, factory.(*onInstanceFactory).dropTempDatabase(context.Background(),
			"some_db"), "drop non-temporary database")
	})
}
