package migration_acceptance_tests

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/pgengine"
	"github.com/stripe/pg-schema-diff/pkg/diff"
	"github.com/stripe/pg-schema-diff/pkg/log"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

// CVE-2018-1058 / SAT-28189: pg-schema-diff must not emit search_path-sensitive
// function calls or cast type names in ALTER COLUMN ... USING clauses.

var (
	unqualifiedToTimestampInUsing = regexp.MustCompile(`(?i)\busing\s+to_timestamp\s*\(`)
	qualifiedToTimestampInUsing   = regexp.MustCompile(`(?i)\busing\s+pg_catalog\.to_timestamp\s*\(`)
	unqualifiedCastInUsing        = regexp.MustCompile(`(?i)\busing\s+[^;]*::`)
	pgCatalogCastInUsing          = regexp.MustCompile(`(?i)\busing\s+cast\s*\([^)]+\bas\s+pg_catalog\.`)
)

// SAT repro: a low-privileged user plants public.to_timestamp(bigint). Unqualified
// calls resolve to the shadow; pg-schema-diff apply must emit pg_catalog.to_timestamp
// so migration converts epoch millis correctly instead of invoking the shadow.
func TestCVE20181058_BigintToTimestampApplyDoesNotInvokeShadowedBuiltin(t *testing.T) {
	t.Parallel()

	const epochMillis int64 = 1700000000000

	db, conn := newCVETestDatabase(t)
	plantShadowToTimestamp(t, conn)
	requireShadowToTimestampActive(t, conn)

	_, err := conn.Exec(`
		CREATE SCHEMA app;
		CREATE TABLE app.events (id integer PRIMARY KEY, ts bigint NOT NULL);
	`)
	require.NoError(t, err)
	_, err = conn.Exec(`INSERT INTO app.events VALUES (1, $1), (2, $1 + 1000)`, epochMillis)
	require.NoError(t, err)

	plan := generateCVEPlan(t, db, []string{`
		CREATE SCHEMA app;
		CREATE TABLE app.events (
			id integer PRIMARY KEY,
			ts timestamp without time zone NOT NULL
		);
	`})
	alterStmt := requireAlterColumnUsingStmt(t, plan, `"ts"`)
	assert.NotRegexp(t, unqualifiedToTimestampInUsing, alterStmt,
		"emitted DDL must not call unqualified to_timestamp in USING (CVE-2018-1058)")
	assert.Regexp(t, qualifiedToTimestampInUsing, alterStmt,
		"emitted DDL must schema-qualify to_timestamp in USING")

	require.NoError(t, applyPlan(db, plan))

	var ts time.Time
	require.NoError(t, conn.QueryRow(`SELECT ts FROM app.events WHERE id = 1`).Scan(&ts))
	assert.NotEqual(t, 1970, ts.Year(),
		"shadow to_timestamp(bigint) returns 1970-01-01; correct pg_catalog conversion should not")
	assert.Equal(t, 2023, ts.Year())
}

// SAT sibling sink: generic type transforms used col::type, resolving the cast target
// via search_path. Emitted DDL must use CAST(... AS "pg_catalog".<type>) instead.
func TestCVE20181058_GenericCastApplyDoesNotUseSearchPathSensitiveCastSyntax(t *testing.T) {
	t.Parallel()

	db, conn := newCVETestDatabase(t)
	plantShadowIntegerDomain(t, conn)

	_, err := conn.Exec(`
		CREATE SCHEMA app2;
		CREATE TABLE app2.metrics (id integer PRIMARY KEY, val character varying(32) NOT NULL);
		INSERT INTO app2.metrics VALUES (1, '42'), (2, '99');
	`)
	require.NoError(t, err)

	plan := generateCVEPlan(t, db, []string{`
		CREATE SCHEMA app2;
		CREATE TABLE app2.metrics (
			id integer PRIMARY KEY,
			val integer NOT NULL
		);
	`})
	alterStmt := requireAlterColumnUsingStmt(t, plan, `"val"`)
	assert.NotRegexp(t, unqualifiedCastInUsing, alterStmt,
		"emitted DDL must not use ::type cast syntax in USING (CVE-2018-1058)")
	assert.Regexp(t, pgCatalogCastInUsing, alterStmt,
		"emitted DDL must CAST to a pg_catalog-qualified type in USING")

	require.NoError(t, applyPlan(db, plan))

	rows, err := conn.Query(`SELECT id, val FROM app2.metrics ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		id  int
		val int
	}
	var got []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.id, &r.val))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, got, 2)
	assert.Equal(t, []row{{1, 42}, {2, 99}}, got)
}

func newCVETestDatabase(t *testing.T) (*pgengine.DB, *sql.DB) {
	t.Helper()

	db, err := pgEngine.CreateDatabaseWithName(fmt.Sprintf("pgtemp_%s", uuid.NewString()))
	require.NoError(t, err)

	conn, err := sql.Open("pgx", db.GetDSN())
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
		db.DropDB()
	})

	return db, conn
}

func plantShadowToTimestamp(t *testing.T, conn *sql.DB) {
	t.Helper()

	_, err := conn.Exec(`
		CREATE OR REPLACE FUNCTION public.to_timestamp(bigint)
		RETURNS timestamp without time zone LANGUAGE plpgsql AS $$
		BEGIN
		  RETURN TIMESTAMP '1970-01-01 00:00:00';
		END $$;
	`)
	require.NoError(t, err)
}

func requireShadowToTimestampActive(t *testing.T, conn *sql.DB) {
	t.Helper()

	var shadowTS time.Time
	require.NoError(t, conn.QueryRow(
		`SELECT to_timestamp(1700000000000::bigint)`,
	).Scan(&shadowTS))
	assert.Equal(t, 1970, shadowTS.Year(),
		"precondition: unqualified to_timestamp(bigint) must resolve to public shadow")

	var regproc string
	require.NoError(t, conn.QueryRow(
		`SELECT 'to_timestamp(bigint)'::regprocedure::text`,
	).Scan(&regproc))
	assert.Equal(t, "to_timestamp(bigint)", regproc,
		"precondition: bigint overload must bind to attacker shadow, not pg_catalog")
}

func plantShadowIntegerDomain(t *testing.T, conn *sql.DB) {
	t.Helper()

	_, err := conn.Exec(`
		DROP DOMAIN IF EXISTS public.integer;
		CREATE DOMAIN public.integer AS pg_catalog.int4 DEFAULT 666;
	`)
	require.NoError(t, err)
}

func generateCVEPlan(t *testing.T, db *pgengine.DB, newSchemaDDL []string) diff.Plan {
	t.Helper()

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

	plan, err := diff.Generate(context.Background(),
		diff.DBSchemaSource(oldDBConnPool),
		diff.DDLSchemaSource(newSchemaDDL),
		diff.WithTempDbFactory(tempDbFactory),
		diff.WithLogger(log.SimpleLogger()),
	)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Statements)
	return plan
}

func requireAlterColumnUsingStmt(t *testing.T, plan diff.Plan, column string) string {
	t.Helper()

	for _, stmt := range plan.Statements {
		ddl := stmt.DDL
		if !strings.Contains(strings.ToUpper(ddl), "ALTER COLUMN") {
			continue
		}
		if !strings.Contains(strings.ToUpper(ddl), " USING ") {
			continue
		}
		if strings.Contains(ddl, column) {
			return ddl
		}
	}
	require.Fail(t, "no ALTER COLUMN ... USING statement found for column "+column, prettySprintPlan(plan))
	return ""
}
