package schema

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	internalschema "github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/internal/testdb"
	"github.com/stripe/pg-schema-diff/pkg/diff"
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
		planOpts   []diff.PlanOpt
		tables     []string
	}{
		{
			name: "default prefix and include filter", prefix: DefaultCleanupSchemaPrefix,
			callerOpts: []GetSchemaOpt{WithIncludeSchemaPatterns("managed_hash", "pgschemadiff_archive_old")},
			planOpts: []diff.PlanOpt{
				diff.WithIncludeSchemaPatterns("managed_hash", "pgschemadiff_archive_old"),
			},
			tables: []string{"managed_hash", "pgschemadiff_archive_old"},
		},
		{
			name: "custom prefix and accumulated exclusion", prefix: "custom_archive",
			callerOpts: []GetSchemaOpt{WithExcludeSchemaPatterns("hidden_hash")},
			planOpts: []diff.PlanOpt{
				diff.WithSchemaPartialArchivalPrefix("custom_archive"),
				diff.WithExcludeSchemaPatterns("hidden_hash"),
			},
			tables: []string{"managed_hash", "pgschemadiff_archive_old", "custom_archive_old"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			publicSnapshot, err := internalschema.GetSchemaSnapshot(t.Context(), db.ConnPool,
				tc.callerOpts...)
			require.NoError(t, err)
			publicCandidate, err := GetSchemaHashWithArchivalPrefix(t.Context(), db.ConnPool,
				tc.prefix, tc.callerOpts...)
			require.NoError(t, err)
			planOpts := append([]diff.PlanOpt{diff.WithDoNotValidatePlan()}, tc.planOpts...)

			require.NoError(t, internalschema.ValidateSchemaSnapshotHashV1(publicCandidate))
			requirePlanHashEventuallyMatches(
				t,
				func() (string, error) {
					return GetSchemaHashWithArchivalPrefix(t.Context(), db.ConnPool,
						tc.prefix, tc.callerOpts...)
				},
				func() (diff.Plan, error) {
					return diff.Generate(t.Context(), diff.DBSchemaSource(db.ConnPool),
						diff.DBSchemaSource(db.ConnPool), planOpts...)
				},
			)

			var tableSchemas []string
			for _, table := range publicSnapshot.Schema.Tables {
				tableSchemas = append(tableSchemas, table.SchemaName)
			}
			assert.ElementsMatch(t, tc.tables, tableSchemas)
		})
	}
}

func TestCustomPrefixSchemaHashMatchesPlanWithTrustedArchivalGroup(t *testing.T) {
	t.Parallel()

	const prefix = "deleted_archive"
	factory := testdb.MustNewFactory(t)
	currentDB := factory.CreateDatabase(t)
	targetDB := factory.CreateDatabase(t)
	_, err := currentDB.ConnPool.Exec(t.Context(), `
		CREATE TABLE archived_accounts (id bigint PRIMARY KEY, balance bigint);
		INSERT INTO archived_accounts VALUES (1, 100);
	`)
	require.NoError(t, err)

	archivePlan, err := diff.Generate(t.Context(), diff.DBSchemaSource(currentDB.ConnPool),
		diff.DBSchemaSource(targetDB.ConnPool), diff.WithDoNotValidatePlan(),
		diff.WithSchemaPartialArchivalPrefix(prefix))
	require.NoError(t, err)
	require.NotEmpty(t, archivePlan.CleanupStatements)
	for _, statement := range archivePlan.Statements {
		_, err := currentDB.ConnPool.Exec(t.Context(), statement.ToSQL())
		require.NoError(t, err)
	}

	requirePlanHashEventuallyMatches(
		t,
		func() (string, error) {
			return GetSchemaHashWithArchivalPrefix(t.Context(), currentDB.ConnPool, prefix)
		},
		func() (diff.Plan, error) {
			plan, err := diff.Generate(t.Context(), diff.DBSchemaSource(currentDB.ConnPool),
				diff.DBSchemaSource(targetDB.ConnPool), diff.WithDoNotValidatePlan(),
				diff.WithSchemaPartialArchivalPrefix(prefix))
			if err == nil {
				assert.Empty(t, plan.Statements)
			}
			return plan, err
		},
	)
}

func TestGetSchemaHashUsesVersionedSnapshotContract(t *testing.T) {
	t.Parallel()

	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	_, err := db.ConnPool.Exec(t.Context(), `CREATE TABLE legacy_hash_active (id bigint)`)
	require.NoError(t, err)

	actual, err := GetSchemaHash(t.Context(), db.ConnPool)
	require.NoError(t, err)
	assert.Contains(t, actual, internalschema.SchemaSnapshotHashV1Prefix)
}

func requirePlanHashEventuallyMatches(
	t *testing.T,
	getPublicHash func() (string, error),
	generatePlan func() (diff.Plan, error),
) {
	t.Helper()
	var before, after string
	var plan diff.Plan
	for range 20 {
		var err error
		before, err = getPublicHash()
		require.NoError(t, err)
		plan, err = generatePlan()
		require.NoError(t, err)
		after, err = getPublicHash()
		require.NoError(t, err)
		if before == after && plan.CurrentSchemaHash == after {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.Equal(t, before, after, "source hash did not stabilize")
	assert.Equal(t, after, plan.CurrentSchemaHash)
}
