package pgengine_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stripe/pg-schema-diff/internal/pgengine"

	"github.com/stretchr/testify/require"
)

func TestPgEngine(t *testing.T) {
	engine, err := pgengine.StartEngine()
	require.NoError(t, err)
	defer func() {
		// Drops should be idempotent
		require.NoError(t, engine.Close())
		require.NoError(t, engine.Close())
	}()

	unnamedDb, err := engine.CreateDatabase()
	require.NoError(t, err)
	assertDatabaseIsValid(t, unnamedDb, "")

	namedDb, err := engine.CreateDatabaseWithName("some-name")
	require.NoError(t, err)
	assertDatabaseIsValid(t, namedDb, "some-name")

	// Assert no extra databases were created
	assert.ElementsMatch(t, getAllDatabaseNames(t, engine), []string{unnamedDb.GetName(), "some-name", "postgres", "template0", "template1"})

	// Hold open a connection before we try to drop the database. The drop should still pass, despite the open
	// connection
	connPool, err := sql.Open("pgx", unnamedDb.GetDSN())
	require.NoError(t, err)
	defer connPool.Close()
	conn, err := connPool.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	// Drops should be idempotent
	require.NoError(t, unnamedDb.DropDB())
	require.NoError(t, unnamedDb.DropDB())
	require.NoError(t, namedDb.DropDB())
	require.NoError(t, namedDb.DropDB())

	// Assert only the expected databases were dropped
	assert.ElementsMatch(t, getAllDatabaseNames(t, engine), []string{"postgres", "template0", "template1"})
}

func assertDatabaseIsValid(t *testing.T, db *pgengine.DB, expectedName string) {
	connPool, err := sql.Open("pgx", db.GetDSN())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, connPool.Close())
	}()

	var fetchedName string
	require.NoError(t, connPool.QueryRowContext(context.Background(), "SELECT current_database();").
		Scan(&fetchedName))

	assert.Equal(t, db.GetName(), fetchedName)
	if len(expectedName) > 0 {
		assert.Equal(t, fetchedName, expectedName)
	}

	// Validate writing and reading works
	_, err = connPool.ExecContext(context.Background(), `CREATE TABLE foobar(id serial NOT NULL)`)
	require.NoError(t, err)

	_, err = connPool.ExecContext(context.Background(), `INSERT INTO foobar DEFAULT VALUES`)
	require.NoError(t, err)

	var id string
	require.NoError(t, connPool.QueryRowContext(context.Background(), "SELECT * FROM foobar LIMIT 1;").
		Scan(&id))
	assert.NotEmptyf(t, t, id)
}

func getAllDatabaseNames(t *testing.T, engine *pgengine.Engine) []string {
	conn, err := sql.Open("pgx", engine.GetPostgresDatabaseDSN())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	rows, err := conn.QueryContext(context.Background(), "SELECT datname FROM pg_database")
	require.NoError(t, err)
	defer rows.Close()

	// Iterate over the rows and put the database names in an array
	var dbNames []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			require.NoError(t, err)
		}
		dbNames = append(dbNames, dbName)
	}
	require.NoError(t, rows.Err())

	return dbNames
}
