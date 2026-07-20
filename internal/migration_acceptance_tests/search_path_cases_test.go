package migration_acceptance_tests

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/pgengine"
	"github.com/stripe/pg-schema-diff/pkg/diff"
	"github.com/stripe/pg-schema-diff/pkg/log"
	"github.com/stripe/pg-schema-diff/pkg/sqldb"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

// Regression for CVE-2018-1058 / SAT-28189: a shadow public.to_timestamp(bigint)
// must not run during apply when search_path is pinned to pg_catalog.
func TestSearchPathPinningPreventsToTimestampShadow(t *testing.T) {
	t.Parallel()

	db, err := pgEngine.CreateDatabaseWithName(fmt.Sprintf("pgtemp_%s", uuid.NewString()))
	require.NoError(t, err)
	defer db.DropDB()

	conn, err := sql.Open("pgx", db.GetDSN())
	require.NoError(t, err)
	defer conn.Close()

	const epochMillis int64 = 1700000000000

	_, err = conn.Exec(`
		CREATE TABLE public.events (id integer PRIMARY KEY, ts bigint NOT NULL);
		CREATE OR REPLACE FUNCTION public.to_timestamp(bigint)
		RETURNS timestamp without time zone LANGUAGE plpgsql AS $$
		BEGIN
			RETURN TIMESTAMP '1970-01-01 00:00:00';
		END $$;
	`)
	require.NoError(t, err)
	_, err = conn.Exec(`INSERT INTO public.events VALUES (1, $1)`, epochMillis)
	require.NoError(t, err)

	oldDBConnPool, err := sql.Open("pgx", db.GetDSN())
	require.NoError(t, err)
	defer oldDBConnPool.Close()
	oldDBConnPool.SetMaxOpenConns(1)

	tempDbFactory, err := tempdb.NewOnInstanceFactory(context.Background(), func(ctx context.Context, dbName string) (*sql.DB, error) {
		return sql.Open("pgx", pgEngine.GetPostgresDatabaseConnOpts().With("dbname", dbName).ToDSN())
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, tempDbFactory.Close())
	}()

	newSchemaDDL := []string{`
		CREATE TABLE public.events (
			id integer PRIMARY KEY,
			ts timestamp without time zone NOT NULL
		);
	`}

	plan, err := diff.Generate(context.Background(),
		diff.DBSchemaSource(oldDBConnPool),
		diff.DDLSchemaSource(newSchemaDDL),
		diff.WithTempDbFactory(tempDbFactory),
		diff.WithLogger(log.SimpleLogger()),
	)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Statements)

	require.NoError(t, applyPlan(db, plan))

	var ts time.Time
	err = conn.QueryRow(`SELECT ts FROM public.events WHERE id = 1`).Scan(&ts)
	require.NoError(t, err)

	assert.NotEqual(t, 1970, ts.Year(),
		"shadow to_timestamp(bigint) would have returned 1970-01-01 if search_path were not pinned")
	assert.Equal(t, 2023, ts.Year())
}

func TestPinSearchPath(t *testing.T) {
	t.Parallel()

	engine, err := pgengine.StartEngine()
	require.NoError(t, err)
	defer engine.Close()

	conn, err := sql.Open("pgx", engine.GetPostgresDatabaseDSN())
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	require.NoError(t, sqldb.PinSearchPath(ctx, conn))

	var searchPath string
	require.NoError(t, conn.QueryRowContext(ctx, `SHOW search_path`).Scan(&searchPath))
	assert.Equal(t, "pg_catalog", searchPath)
}
