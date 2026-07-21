package migration_acceptance_tests

import (
	"context"
	"database/sql"
	"fmt"
	stdlog "log"
	"os"
	"testing"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/pgdump"
	"github.com/stripe/pg-schema-diff/internal/pgengine"
	"github.com/stripe/pg-schema-diff/pkg/diff"
	"github.com/stripe/pg-schema-diff/pkg/log"
	"github.com/stripe/pg-schema-diff/pkg/sqldb"

	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

var (
	errValidatingPlan = fmt.Errorf("validating migration plan")
)

type (
	planFactory func(ctx context.Context, connPool sqldb.Queryable, tempDbFactory tempdb.Factory, newSchemaDDL []string, opts ...diff.PlanOpt) (diff.Plan, error)

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

var pgEngine *pgengine.Engine

func TestMain(m *testing.M) {
	engine, err := pgengine.StartEngine()
	if err != nil {
		stdlog.Fatalf("Failed to start engine: %v", err)
	}
	pgEngine = engine
	exitCode := m.Run()
	if err := pgEngine.Close(); err != nil {
		stdlog.Fatalf("Failed to close engine: %v", err)
	}
	os.Exit(exitCode)
}

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
	// We moved a call to the random  when we made tests run in parallel. This caused assertions on exact statements to fail.
	// To keep the assertions passing, we will generate a UUID and throw it out. In the future, we should just create
	// a more advanced system for asserting random statements that captures variables and allows them to be referenced
	// in future assertions.
	_, err := uuid.NewRandomFromReader(deterministicRandReader)
	require.NoError(t, err)
	tc.planOpts = append([]diff.PlanOpt{diff.WithLogger(log.SimpleLogger()), diff.WithRandReader(deterministicRandReader)}, tc.planOpts...)
	if tc.expectedDBSchemaDDL == nil {
		tc.expectedDBSchemaDDL = tc.newSchemaDDL
	}
	if tc.planFactory == nil {
		tc.planFactory = func(ctx context.Context, connPool sqldb.Queryable, tempDbFactory tempdb.Factory, newSchemaDDL []string, opts ...diff.PlanOpt) (diff.Plan, error) {

			connSource := diff.DBSchemaSource(connPool)

			return diff.Generate(ctx, connSource, diff.DDLSchemaSource(newSchemaDDL),
				append(tc.planOpts,
					diff.WithTempDbFactory(tempDbFactory),
				)...)
		}
	}

	engine := pgEngine
	if len(tc.roles) > 0 {
		// If the test needs roles (server-wide), provide isolation by spinning out a dedicated pgengine.
		dedicatedEngine, err := pgengine.StartEngine()
		require.NoError(t, err)
		defer dedicatedEngine.Close()
		engine = dedicatedEngine
	}

	// Create roles since they are global
	rootDb, err := sql.Open("pgx", engine.GetPostgresDatabaseDSN())
	require.NoError(t, err)
	defer rootDb.Close()
	for _, r := range tc.roles {
		_, err := rootDb.Exec(fmt.Sprintf("CREATE ROLE %s", r))
		require.NoError(t, err)
	}

	// Apply old schema DDL to old DB
	require.NoError(t, err)
	oldDb, err := engine.CreateDatabaseWithName(fmt.Sprintf("pgtemp_%s", uuid.NewString()))
	require.NoError(t, err)
	defer oldDb.DropDB()
	// Apply the old schema
	require.NoError(t, applyDDL(oldDb, tc.oldSchemaDDL))

	// Migrate the old DB
	oldDBConnPool, err := sql.Open("pgx", oldDb.GetDSN())
	require.NoError(t, err)
	defer oldDBConnPool.Close()
	oldDBConnPool.SetMaxOpenConns(1)

	tempDbFactory, err := tempdb.NewOnInstanceFactory(context.Background(), func(ctx context.Context, dbName string) (*sql.DB, error) {
		return sql.Open("pgx", engine.GetPostgresDatabaseConnOpts().With("dbname", dbName).ToDSN())
	}, tempdb.WithRandReader(deterministicRandReader))
	require.NoError(t, err)
	defer func(tempDbFactory tempdb.Factory) {
		// It's important that this closes properly (the temp database is dropped),
		// so assert it has no error for acceptance tests
		require.NoError(t, tempDbFactory.Close())
	}(tempDbFactory)

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
	assert.ElementsMatch(t, tc.expectedHazardTypes, getUniqueHazardTypesFromStatements(plan.Statements), prettySprintPlan(plan))

	// Apply the plan
	require.NoError(t, applyPlan(oldDb, plan), prettySprintPlan(plan))

	// Make sure the pgdump after running the migration is the same as the
	// pgdump from a database where we directly run the newSchemaDDL
	oldDbDump, err := pgdump.GetDump(oldDb, pgdump.WithSchemaOnly(), pgdump.WithRestrictKey(pgdump.FixedRestrictKey))
	require.NoError(t, err)

	newDbDump := directlyRunDDLAndGetDump(t, engine, tc.expectedDBSchemaDDL)
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
		assert.Equal(t, tc.expectedPlanDDL, generatedDDL, "data packing can change the the generated UUID and DDL")
	}

	// Make sure no diff is found if we try to regenerate a plan
	plan, err = tc.planFactory(context.Background(), oldDBConnPool, tempDbFactory, tc.newSchemaDDL, tc.planOpts...)
	require.NoError(t, err)
	assert.Empty(t, plan.Statements, prettySprintPlan(plan))
}

func assertValidPlan(t *testing.T, plan diff.Plan) {
	for _, stmt := range plan.Statements {
		assert.Greater(t, stmt.Timeout.Nanoseconds(), int64(0), "timeout should be greater than 0. stmt=%+v", stmt)
		assert.Greater(t, stmt.LockTimeout.Nanoseconds(), int64(0), "lock timeout should be greater than 0. stmt=%+v", stmt)
	}
}

func directlyRunDDLAndGetDump(t *testing.T, engine *pgengine.Engine, ddl []string) string {
	newDb, err := engine.CreateDatabase()
	require.NoError(t, err)
	defer newDb.DropDB()
	require.NoError(t, applyDDL(newDb, ddl))

	newDbDump, err := pgdump.GetDump(newDb, pgdump.WithSchemaOnly(), pgdump.WithRestrictKey(pgdump.FixedRestrictKey))
	require.NoError(t, err)
	return newDbDump
}

func applyDDL(db *pgengine.DB, ddl []string) error {
	conn, err := sql.Open("pgx", db.GetDSN())
	if err != nil {
		return err
	}
	defer conn.Close()

	for _, stmt := range ddl {
		_, err := conn.Exec(stmt)
		if err != nil {
			return fmt.Errorf("DDL:\n: %w"+stmt, err)
		}
	}
	return nil
}

func applyPlan(db *pgengine.DB, plan diff.Plan) error {
	var ddl []string
	for _, stmt := range plan.Statements {
		ddl = append(ddl, stmt.ToSQL())
	}
	return applyDDL(db, ddl)
}

func getUniqueHazardTypesFromStatements(statements []diff.Statement) []diff.MigrationHazardType {
	var seenHazardTypes = make(map[diff.MigrationHazardType]bool)
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
