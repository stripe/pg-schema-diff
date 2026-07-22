package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	internalschema "github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/internal/testdb"
)

func TestCandidateSchemaHashMatchesPlanSnapshotForFiltersAndPrefixes(t *testing.T) {
	t.Parallel()

	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	_, err := db.ConnPool.Exec(t.Context(), `
		CREATE SCHEMA managed_hash;
		CREATE TABLE managed_hash.kept (id bigint);
		CREATE SCHEMA hidden_hash;
		CREATE TABLE hidden_hash.omitted (id bigint);
		CREATE SCHEMA pgschemadiff_archive_old;
		CREATE TABLE pgschemadiff_archive_old.retained (id bigint);
		CREATE SCHEMA custom_archive_old;
		CREATE TABLE custom_archive_old.retained (id bigint);
	`)
	require.NoError(t, err)

	testCases := []struct {
		name       string
		prefix     string
		callerOpts []GetSchemaOpt
		tables     []string
	}{
		{
			name: "default prefix and include filter", prefix: DefaultCleanupSchemaPrefix,
			callerOpts: []GetSchemaOpt{WithIncludeSchemaPatterns("managed_hash", "pgschemadiff_archive_old")},
			tables:     []string{"managed_hash"},
		},
		{
			name: "custom prefix and accumulated exclusion", prefix: "custom_archive",
			callerOpts: []GetSchemaOpt{WithExcludeSchemaPatterns("hidden_hash")},
			tables:     []string{"managed_hash", "pgschemadiff_archive_old"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			publicSnapshot, err := getCandidateSchemaSnapshot(t.Context(), db.ConnPool,
				tc.prefix, tc.callerOpts...)
			require.NoError(t, err)
			publicCandidate, err := buildCandidateSchemaHash(publicSnapshot, nil)
			require.NoError(t, err)

			planCandidate, err := internalschema.BuildSchemaSnapshotHashV1(publicSnapshot, nil)
			require.NoError(t, err)

			assert.Equal(t, planCandidate, publicCandidate)
			require.NoError(t, internalschema.ValidateSchemaSnapshotHashV1(publicCandidate))

			var tableSchemas []string
			for _, table := range publicSnapshot.Schema.Tables {
				tableSchemas = append(tableSchemas, table.SchemaName)
			}
			assert.ElementsMatch(t, tc.tables, tableSchemas)
		})
	}
}

func TestGetSchemaHashKeepsLegacyHashWhileCandidateIsDormant(t *testing.T) {
	t.Parallel()

	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	_, err := db.ConnPool.Exec(t.Context(), `CREATE TABLE legacy_hash_active (id bigint)`)
	require.NoError(t, err)

	legacy, err := GetSchemaHash(t.Context(), db.ConnPool)
	require.NoError(t, err)
	legacySnapshot, err := internalschema.GetSchemaSnapshot(t.Context(), db.ConnPool,
		internalschema.WithExcludeSchemaPatterns(DefaultCleanupSchemaPrefix+".*"))
	require.NoError(t, err)
	assert.Equal(t, legacySnapshot.Hash, legacy)
	assert.NotContains(t, legacy, internalschema.SchemaSnapshotHashV1Prefix)

	candidate, err := getCandidateSchemaHash(t.Context(), db.ConnPool, DefaultCleanupSchemaPrefix, nil)
	require.NoError(t, err)
	assert.NotEqual(t, legacy, candidate)
	assert.True(t, len(legacy) < len(candidate))
}
