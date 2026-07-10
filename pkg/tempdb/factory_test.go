package tempdb

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	internalschema "github.com/stripe/pg-schema-diff/internal/schema"
)

func buildFactory(t *testing.T, rootPool *pgxpool.Pool, opt ...OnInstanceFactoryOpt) Factory {
	t.Helper()
	rootDbName := rootPool.Config().ConnConfig.Database
	opt = append(opt, WithRootDatabase(rootDbName))
	factory, err := NewOnInstanceFactory(context.Background(), func(ctx context.Context, dbName string) (*pgxpool.Pool, error) {
		if dbName == rootDbName {
			return rootPool, nil
		}
		return newPoolForDB(rootPool, dbName)
	}, opt...)
	require.NoError(t, err)
	return factory
}

func newPoolForDB(rootPool *pgxpool.Pool, dbName string) (*pgxpool.Pool, error) {
	config := rootPool.Config().Copy()
	config.ConnConfig.Database = dbName
	return pgxpool.NewWithConfig(context.Background(), config)
}

func runSQL(t *testing.T, conn *pgxpool.Conn) {
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

func TestNewOnInstanceFactoryConnectsToWrongDatabase(t *testing.T) {
	rootPool := poolFactory.Pool(t)
	_, err := NewOnInstanceFactory(context.Background(), func(ctx context.Context, dbName string) (*pgxpool.Pool, error) {
		return rootPool, nil
	})
	require.ErrorContains(t, err, "connection pool is on")
}

func TestNewOnInstanceFactoryErrorsOnNonSimpleDbPrefix(t *testing.T) {
	_, err := NewOnInstanceFactory(context.Background(), func(ctx context.Context, dbName string) (*pgxpool.Pool, error) {
		t.Fatal("shouldn't be reached")
		return nil, nil
	}, WithDbPrefix("non-simple identifier"))
	require.ErrorContains(t, err, "must be a simple Postgres identifier")
}

func TestOnInstanceFactoryCreateAndDropFlow(t *testing.T) {
	const (
		dbPrefix       = "some_prefix"
		metadataSchema = "some metadata schema"
		metadataTable  = "some metadata table"
	)

	rootPool := poolFactory.Pool(t)
	factory := buildFactory(
		t, rootPool,
		WithDbPrefix(dbPrefix),
		WithMetadataSchema(metadataSchema),
		WithMetadataTable(metadataTable),
		WithLogger(slog.Default()),
	)
	defer func(factory Factory) {
		require.NoError(t, factory.Close())
	}(factory)

	tempDb, err := factory.Create(context.Background())
	require.NoError(t, err)
	// Don't defer dropping. we want to run assertions after it drops. if dropping fails,
	// it shouldn't be a problem because names shouldn't conflict
	afterTimeOfCreation := time.Now()

	conn1, err := tempDb.ConnPool.Acquire(context.Background())
	require.NoError(t, err)

	var dbName string
	require.NoError(t, conn1.QueryRow(context.Background(), "SELECT current_database()").Scan(&dbName))
	assert.True(t, strings.HasPrefix(dbName, dbPrefix))
	assert.Len(t, dbName, len(dbPrefix)+36) // should be length of prefix + length of uuid

	// Make sure SQL can run on the connection
	runSQL(t, conn1)

	// Check the metadata entry exists
	var createdAt time.Time
	metadataQuery := fmt.Sprintf(`
		SELECT * FROM "%s"."%s"
	`, metadataSchema, metadataTable)
	require.NoError(t, conn1.QueryRow(context.Background(), metadataQuery).Scan(&createdAt))
	assert.True(t, createdAt.Before(afterTimeOfCreation))

	// Get another connection from the pool and make sure it's also set to the correct db while
	// the other connection is still open
	conn2, err := tempDb.ConnPool.Acquire(context.Background())
	require.NoError(t, err)
	var dbNameFromConn2 string
	require.NoError(t, conn2.QueryRow(context.Background(), "SELECT current_database()").Scan(&dbNameFromConn2))
	assert.Equal(t, dbName, dbNameFromConn2)

	conn1.Release()
	conn2.Release()

	// Get the schema without the exclude options. It should not be empty because of the metadata schema.
	schema, err := internalschema.GetSchema(context.Background(), tempDb.ConnPool)
	require.NoError(t, err)
	assert.NotEmpty(t, schema)

	// Get the schema with the exclude options (it should be empty)
	schema, err = internalschema.GetSchema(context.Background(), tempDb.ConnPool, tempDb.ExcludeMetadataOptions...)
	require.NoError(t, err)
	assert.Equal(t, internalschema.Schema{
		NamedSchemas: []internalschema.NamedSchema{{
			Name: "public",
		}},
	}, schema)

	// Drop database
	require.NoError(t, tempDb.Close(context.Background()))

	// Expect an error when attempting to query the database, since it should be dropped.
	// when a db pool is opened, it has no connections.
	// a query is needed in order to find if the database still exists.
	conn, err := newPoolForDB(rootPool, dbName)
	require.NoError(t, err)
	defer conn.Close()
	require.ErrorContains(t, conn.QueryRow(context.Background(), metadataQuery).Scan(&createdAt), "SQLSTATE 3D000")
	assert.True(t, createdAt.Before(afterTimeOfCreation))
}

func TestOnInstanceFactoryCreateConnectsToWrongDatabase(t *testing.T) {
	rootPool := poolFactory.Pool(t)
	rootDbName := rootPool.Config().ConnConfig.Database
	factory, err := NewOnInstanceFactory(
		context.Background(),
		func(ctx context.Context, dbName string) (*pgxpool.Pool, error) {
			if dbName == rootDbName {
				return rootPool, nil
			}
			return newPoolForDB(rootPool, rootDbName)
		},
		WithRootDatabase(rootDbName),
	)
	require.NoError(t, err)
	defer func(factory Factory) {
		require.NoError(t, factory.Close())
	}(factory)

	_, err = factory.Create(context.Background())
	require.ErrorContains(t, err, "connection pool is on")
}

func TestOnInstanceFactoryDropTempDBCannotDropNonTempDB(t *testing.T) {
	rootPool := poolFactory.Pool(t)
	factory := buildFactory(t, rootPool)
	defer func(factory Factory) {
		require.NoError(t, factory.Close())
	}(factory)

	require.ErrorContains(t, factory.(*onInstanceFactory).dropTempDatabase(context.Background(),
		"some_db"), "drop non-temporary database")
}
