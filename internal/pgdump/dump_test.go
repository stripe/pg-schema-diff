package pgdump_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/pgdump"
	"github.com/stripe/pg-schema-diff/internal/pgengine"
)

func TestGetDump(t *testing.T) {
	pgEngine, err := pgengine.StartEngine()
	require.NoError(t, err)
	defer pgEngine.Close()

	db, err := pgEngine.CreateDatabase()
	require.NoError(t, err)
	defer db.DropDB()

	connPool, err := sql.Open("pgx", db.GetDSN())
	require.NoError(t, err)
	defer connPool.Close()

	_, err = connPool.ExecContext(context.Background(), `
			CREATE TABLE foobar(foobar_id text);

			INSERT INTO foobar VALUES ('some-id');

			CREATE SCHEMA test;
			CREATE TABLE test.bar(bar_id text);
		`)
	require.NoError(t, err)

	dump, err := pgdump.GetDump(db)
	require.NoError(t, err)
	require.Contains(t, dump, "public.foobar")
	require.Contains(t, dump, "test.bar")
	require.Contains(t, dump, "some-id")

	onlySchemasDump, err := pgdump.GetDump(db, pgdump.WithSchemaOnly())
	require.NoError(t, err)
	require.Contains(t, onlySchemasDump, "public.foobar")
	require.Contains(t, onlySchemasDump, "test.bar")
	require.NotContains(t, onlySchemasDump, "some-id")

	onlyPublicSchemaDump, err := pgdump.GetDump(db, pgdump.WithSchemaOnly(), pgdump.WithExcludeSchema("test"))
	require.NoError(t, err)
	require.Contains(t, onlyPublicSchemaDump, "public.foobar")
	require.NotContains(t, onlyPublicSchemaDump, "test.bar")
	require.NotContains(t, onlyPublicSchemaDump, "some-id")
}

func TestParseVersion(t *testing.T) {
	testCases := []struct {
		name            string
		versionString   string
		expectedVersion string
		expectError     bool
	}{
		{
			name:            "version 17.6",
			versionString:   "pg_dump (PostgreSQL) 17.6",
			expectedVersion: "17.6.0",
			expectError:     false,
		},
		{
			name:            "version 17",
			versionString:   "pg_dump (PostgreSQL) 17",
			expectedVersion: "17.0.0",
			expectError:     false,
		},
		{
			name:          "invalid version string",
			versionString: "invalid version",
			expectError:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			version, err := pgdump.ParseVersion(tc.versionString)
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.Equal(t, tc.expectedVersion, version.String())
		})
	}
}
