package migration_acceptance_tests

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/pgdump"
	"github.com/stripe/pg-schema-diff/pkg/diff"
	"github.com/stripe/pg-schema-diff/pkg/sqldb"

	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

var errValidatingPlan = fmt.Errorf("validating migration plan")

type (
	planFactory func(ctx context.Context, connPool sqldb.Queryable, tempDbFactory tempdb.Factory,
		newSchemaDDL []string, opts ...diff.PlanOpt) (diff.Plan, error)

	acceptanceTestCase struct {
		name string

		// planOpts is a list of options that should be passed to the plan generator
		planOpts []diff.PlanOpt

		// roles is a list of roles that should be created before the DDL is applied
		roles        []string
		oldSchemaDDL []string
		newSchemaDDL []string

		// planFactory is used to generate the actual plan. This is useful for testing different plan generation paths
		// outside of the normal path. If not specified, a plan will be generated using a default.
		planFactory planFactory

		// expectedHazardTypes should contain all the unique migration hazard types that are expected to be within the
		// generated plan
		expectedHazardTypes       []diff.MigrationHazardType
		expectedPlanErrorIs       error
		expectedPlanErrorContains string
		// expectedPlanDDL is used to assert the exact DDL (of the statements) that  generated. This is useful when asserting
		// exactly how a migration is performed
		expectedPlanDDL []string
		// expectEmptyPlan asserts the plan is expectEmptyPlan
		expectEmptyPlan bool
		// expectedDBSchemaDDL should be the DDL required to reconstruct the expected output state of the database
		//
		// The expectedDBSchemaDDL might differ from the newSchemaDDL due to options passed to the migrator. For example,
		// the data packing option will cause the column ordering for new tables in the expectedDBSchemaDDL to differ from
		// the column ordering of those tables defined in newSchemaDDL
		//
		// If no expectedDBSchemaDDL is specified, the newSchemaDDL will be used
		expectedDBSchemaDDL []string
	}
)

var roleMu sync.Mutex

// Simulates migrating a database and uses pgdump to compare the actual state to the expected state
func runTestCases(t *testing.T, acceptanceTestCases []acceptanceTestCase) {
	t.Parallel()
	for _, _tc := range acceptanceTestCases {
		// Copy the test case since we are using t.Parallel (effectively spinning out a go routine).
		tc := _tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTest(t, tc)
		})
	}
}

func runTest(t *testing.T, tc acceptanceTestCase) {
	deterministicRandReader := &deterministicRandReader{}
	tc.planOpts = append([]diff.PlanOpt{
		diff.WithLogger(slog.Default()),
		diff.WithRandReader(deterministicRandReader),
	}, tc.planOpts...)
	if tc.expectedDBSchemaDDL == nil {
		tc.expectedDBSchemaDDL = tc.newSchemaDDL
	}
	if tc.planFactory == nil {
		tc.planFactory = func(ctx context.Context, connPool sqldb.Queryable,
			tempDbFactory tempdb.Factory, newSchemaDDL []string, opts ...diff.PlanOpt,
		) (diff.Plan, error) {
			connSource := diff.DBSchemaSource(connPool)

			return diff.Generate(ctx, connSource, diff.DDLSchemaSource(newSchemaDDL),
				append(
					tc.planOpts,
					diff.WithTempDbFactory(tempDbFactory),
				)...)
		}
	}

	if len(tc.roles) > 0 {
		roleMu.Lock()

		rootDb, err := pgxpool.New(context.Background(), os.Getenv("TEST_DATABASE_URL"))
		require.NoError(t, err)
		require.NoError(t, dropRoles(context.Background(), rootDb, tc.roles))
		t.Cleanup(func() {
			defer roleMu.Unlock()
			defer rootDb.Close()
			require.NoError(t, dropRoles(context.Background(), rootDb, tc.roles))
		})

		for _, r := range tc.roles {
			_, err := rootDb.Exec(context.Background(), fmt.Sprintf("CREATE ROLE %s", r))
			require.NoError(t, err)
		}
	}

	// Apply old schema DDL to old DB
	oldDBConnPool := poolFactory.Pool(t)
	// Apply the old schema
	require.NoError(t, applyDDL(oldDBConnPool, tc.oldSchemaDDL))

	tempDbFactory := testTempDBFactory{t: t}

	plan, err := tc.planFactory(context.Background(), oldDBConnPool, tempDbFactory, tc.newSchemaDDL, tc.planOpts...)
	if tc.expectedPlanErrorIs != nil || len(tc.expectedPlanErrorContains) > 0 {
		if tc.expectedPlanErrorIs != nil {
			assert.ErrorIs(t, err, tc.expectedPlanErrorIs)
		}
		if len(tc.expectedPlanErrorContains) > 0 {
			assert.ErrorContains(t, err, tc.expectedPlanErrorContains)
		}
		return
	}
	require.NoError(t, err)

	assertValidPlan(t, plan)
	if tc.expectEmptyPlan {
		// It shouldn't be necessary, but we'll run all checks below this point just in case rather than exiting early
		assert.Empty(t, plan.Statements)
	}
	assert.ElementsMatch(t, tc.expectedHazardTypes,
		getUniqueHazardTypesFromStatements(plan.Statements), prettySprintPlan(plan))

	// Apply the plan
	require.NoError(t, applyPlan(oldDBConnPool, plan), prettySprintPlan(plan))

	// Make sure the pgdump after running the migration is the same as the
	// pgdump from a database where we directly run the newSchemaDDL
	oldDbDump, err := pgdump.GetDump(oldDBConnPool, pgdump.WithSchemaOnly(),
		pgdump.WithRestrictKey(pgdump.FixedRestrictKey))
	require.NoError(t, err)

	newDbDump := directlyRunDDLAndGetDump(t, tc.expectedDBSchemaDDL)
	assert.Equal(t, newDbDump, oldDbDump, prettySprintPlan(plan))

	if tc.expectedPlanDDL != nil {
		var generatedDDL []string
		for _, stmt := range plan.Statements {
			generatedDDL = append(generatedDDL, stmt.DDL)
		}
		// In the future, we might want to allow users to assert expectations for vanilla options and data packing
		//
		// We can also make the system more advanced by using tokens in place of the "randomly" generated UUIDs, such
		// the test case doesn't need to be updated if the UUID generation changes. If we built this functionality, we
		// should also integrate it with the schema_migration_plan_test.go tests.
		assert.Equal(t, tc.expectedPlanDDL, generatedDDL,
			"data packing can change the the generated UUID and DDL")
	}

	// Make sure no diff is found if we try to regenerate a plan
	plan, err = tc.planFactory(context.Background(), oldDBConnPool, tempDbFactory, tc.newSchemaDDL, tc.planOpts...)
	require.NoError(t, err)
	assert.Empty(t, plan.Statements, prettySprintPlan(plan))
}

func assertValidPlan(t *testing.T, plan diff.Plan) {
	for _, stmt := range plan.Statements {
		assert.Greater(t, stmt.Timeout.Nanoseconds(), int64(0),
			"timeout should be greater than 0. stmt=%+v", stmt)
		assert.Greater(t, stmt.LockTimeout.Nanoseconds(), int64(0),
			"lock timeout should be greater than 0. stmt=%+v", stmt)
	}
}

func directlyRunDDLAndGetDump(t *testing.T, ddl []string) string {
	newDb := poolFactory.Pool(t)
	require.NoError(t, applyDDL(newDb, ddl))

	newDbDump, err := pgdump.GetDump(newDb, pgdump.WithSchemaOnly(),
		pgdump.WithRestrictKey(pgdump.FixedRestrictKey))
	require.NoError(t, err)
	return newDbDump
}

func applyDDL(conn *pgxpool.Pool, ddl []string) error {
	for _, stmt := range ddl {
		_, err := conn.Exec(context.Background(), stmt)
		if err != nil {
			return fmt.Errorf("DDL:\n: %w"+stmt, err)
		}
	}
	return nil
}

func applyPlan(db *pgxpool.Pool, plan diff.Plan) error {
	var ddl []string
	for _, stmt := range plan.Statements {
		ddl = append(ddl, stmt.ToSQL())
	}
	return applyDDL(db, ddl)
}

func dropRoles(ctx context.Context, db *pgxpool.Pool, roleNames []string) error {
	for _, roleName := range roleNames {
		if _, err := db.Exec(ctx, fmt.Sprintf("DROP ROLE IF EXISTS %s", roleName)); err != nil {
			if err := dropOwnedByRoleInAllDatabases(ctx, db, roleName); err != nil {
				return err
			}
			if _, err := db.Exec(ctx, fmt.Sprintf("DROP ROLE IF EXISTS %s", roleName)); err != nil {
				return fmt.Errorf("dropping role %q: %w", roleName, err)
			}
		}
	}

	return nil
}

func dropOwnedByRoleInAllDatabases(ctx context.Context, rootDB *pgxpool.Pool, roleName string) error {
	rows, err := rootDB.Query(ctx, "SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate")
	if err != nil {
		return fmt.Errorf("listing databases: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return fmt.Errorf("scanning database name: %w", err)
		}

		config := rootDB.Config().Copy()
		config.ConnConfig.Database = dbName
		db, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			if strings.Contains(err.Error(), "SQLSTATE 3D000") ||
				strings.Contains(err.Error(), "does not exist") {
				continue
			}
			return fmt.Errorf("connecting to database %q: %w", dbName, err)
		}
		_, err = db.Exec(ctx, fmt.Sprintf("DROP OWNED BY %s", roleName))
		db.Close()
		if err != nil {
			return fmt.Errorf("dropping objects owned by role %q in database %q: %w", roleName, dbName, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("listing databases: %w", err)
	}

	return nil
}

func getUniqueHazardTypesFromStatements(statements []diff.Statement) []diff.MigrationHazardType {
	seenHazardTypes := make(map[diff.MigrationHazardType]bool)
	var hazardTypes []diff.MigrationHazardType
	for _, stmt := range statements {
		for _, hazard := range stmt.Hazards {
			if _, hasHazard := seenHazardTypes[hazard.Type]; !hasHazard {
				seenHazardTypes[hazard.Type] = true
				hazardTypes = append(hazardTypes, hazard.Type)
			}
		}
	}
	return hazardTypes
}

func prettySprintPlan(plan diff.Plan) string {
	return fmt.Sprintf("%# v", pretty.Formatter(plan.Statements))
}

type deterministicRandReader struct {
	counter int8
}

func (r *deterministicRandReader) Read(p []byte) (int, error) {
	for i := 0; i < len(p); i++ {
		p[i] = byte(r.counter)
		r.counter++
	}
	return len(p), nil
}
