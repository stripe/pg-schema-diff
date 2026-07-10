package pgengine_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
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
	assert.ElementsMatch(t, getAllDatabaseNames(t, engine), []string{
		unnamedDb.GetName(), "some-name", "postgres", "template0", "template1",
	})

	// Hold open a connection before we try to drop the database. The drop should still pass, despite the open
	// connection
	connPool, err := pgxpool.New(context.Background(), unnamedDb.GetDSN())
	require.NoError(t, err)
	defer connPool.Close()
	conn, err := connPool.Acquire(context.Background())
	require.NoError(t, err)
	defer conn.Release()

	// Drops should be idempotent
	require.NoError(t, unnamedDb.DropDB())
	require.NoError(t, unnamedDb.DropDB())
	require.NoError(t, namedDb.DropDB())
	require.NoError(t, namedDb.DropDB())

	// Assert only the expected databases were dropped
	assert.ElementsMatch(t, getAllDatabaseNames(t, engine), []string{
		"postgres", "template0", "template1",
	})
}

func assertDatabaseIsValid(t *testing.T, db *pgengine.DB, expectedName string) {
	connPool, err := pgxpool.New(context.Background(), db.GetDSN())
	require.NoError(t, err)
	defer connPool.Close()

	var fetchedName string
	require.NoError(t, connPool.QueryRow(context.Background(), "SELECT current_database();").
		Scan(&fetchedName))

	assert.Equal(t, db.GetName(), fetchedName)
	if len(expectedName) > 0 {
		assert.Equal(t, fetchedName, expectedName)
	}

	// Validate writing and reading works
	_, err = connPool.Exec(context.Background(), `CREATE TABLE foobar(id serial NOT NULL)`)
	require.NoError(t, err)

	_, err = connPool.Exec(context.Background(), `INSERT INTO foobar DEFAULT VALUES`)
	require.NoError(t, err)

	var id string
	require.NoError(t, connPool.QueryRow(context.Background(), "SELECT * FROM foobar LIMIT 1;").
		Scan(&id))
	assert.NotEmptyf(t, t, id)
}

func getAllDatabaseNames(t *testing.T, engine *pgengine.Engine) []string {
	conn, err := pgxpool.New(context.Background(), engine.GetPostgresDatabaseDSN())
	require.NoError(t, err)
	defer conn.Close()

	rows, err := conn.Query(context.Background(), "SELECT datname FROM pg_database")
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
