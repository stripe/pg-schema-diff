package migration_acceptance_tests

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/pgdump"
	"github.com/stripe/pg-schema-diff/internal/testdb"
	"github.com/stripe/pg-schema-diff/pkg/diff"

	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

var errValidatingPlan = fmt.Errorf("validating migration plan")

type (
	planFactory func(ctx context.Context, connPool *pgxpool.Pool, tempDbFactory tempdb.Factory,
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
		// If no expectedDBSchemaDDL is specified, the newSchemaDDL will be used
		expectedDBSchemaDDL []string
	}

	// DBMSWideAcceptanceTestCase describes an acceptance test that requires cluster-level objects.
	DBMSWideAcceptanceTestCase struct {
		Name                string
		Roles               []string
		OldSchemaDDL        []string
		NewSchemaDDL        []string
		ExpectedHazardTypes []diff.MigrationHazardType
		ExpectedPlanErrorIs error
		ExpectedPlanDDL     []string
		ExpectEmptyPlan     bool
		ExpectedDBSchemaDDL []string
	}
)

// RunDBMSWideTestCases runs acceptance tests that use cluster-level objects serially.
func RunDBMSWideTestCases(t *testing.T, testCases []DBMSWideAcceptanceTestCase) {
	t.Helper()

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			runTest(t, acceptanceTestCase{
				name:                tc.Name,
				roles:               tc.Roles,
				oldSchemaDDL:        tc.OldSchemaDDL,
				newSchemaDDL:        tc.NewSchemaDDL,
				expectedHazardTypes: tc.ExpectedHazardTypes,
				expectedPlanErrorIs: tc.ExpectedPlanErrorIs,
				expectedPlanDDL:     tc.ExpectedPlanDDL,
				expectEmptyPlan:     tc.ExpectEmptyPlan,
				expectedDBSchemaDDL: tc.ExpectedDBSchemaDDL,
			})
		})
	}
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
	tc.planOpts = append([]diff.PlanOpt{
		diff.WithLogger(slog.Default()),
		diff.WithRandReader(deterministicRandReader),
	}, tc.planOpts...)
	if tc.expectedDBSchemaDDL == nil {
		tc.expectedDBSchemaDDL = tc.newSchemaDDL
	}
	if tc.planFactory == nil {
		tc.planFactory = func(ctx context.Context, connPool *pgxpool.Pool,
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

	tempDbFactory := testdb.MustNewFactory(t)
	if len(tc.roles) > 0 {
		roleGuard := tempDbFactory.LockRoles(t, tc.roles...)
		roleGuard.CreateRoles()
	}

	// Apply old schema DDL to old DB
	oldDb := tempDbFactory.CreateDatabase(t)
	// Apply the old schema
	require.NoError(t, applyDDL(t.Context(), oldDb.ConnPool, tc.oldSchemaDDL))

	plan, err := tc.planFactory(t.Context(), oldDb.ConnPool, tempDbFactory, tc.newSchemaDDL, tc.planOpts...)
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

	if tc.expectEmptyPlan {
		// It shouldn't be necessary, but we'll run all checks below this point just in case rather than exiting early
		assert.Empty(t, plan.Statements)
	}
	assert.ElementsMatch(t, tc.expectedHazardTypes,
		getUniqueHazardTypesFromStatements(plan.Statements), prettySprintPlan(plan))

	// Apply the plan
	require.NoError(t, applyPlan(t.Context(), oldDb.ConnPool, plan), prettySprintPlan(plan))

	// Make sure the pgdump after running the migration is the same as the
	// pgdump from a database where we directly run the newSchemaDDL
	oldDbDump, err := pgdump.GetDump(oldDb.ConnPool, pgdump.WithSchemaOnly(),
		pgdump.WithRestrictKey(pgdump.FixedRestrictKey))
	require.NoError(t, err)

	newDbDump := directlyRunDDLAndGetDump(t, tempDbFactory, tc.expectedDBSchemaDDL)
	assert.Equal(t, newDbDump, oldDbDump, prettySprintPlan(plan))

	if tc.expectedPlanDDL != nil {
		var generatedDDL []string
		for _, stmt := range plan.Statements {
			generatedDDL = append(generatedDDL, stmt.DDL)
		}
		// We can also make the system more advanced by using tokens in place of the "randomly" generated UUIDs, such
		// the test case doesn't need to be updated if the UUID generation changes. If we built this functionality, we
		// should also integrate it with the schema_migration_plan_test.go tests.
		assert.Equal(t, tc.expectedPlanDDL, generatedDDL)
	}

	// Make sure no diff is found if we try to regenerate a plan
	plan, err = tc.planFactory(t.Context(), oldDb.ConnPool, tempDbFactory, tc.newSchemaDDL, tc.planOpts...)
	require.NoError(t, err)
	assert.Empty(t, plan.Statements, prettySprintPlan(plan))
}

func directlyRunDDLAndGetDump(t *testing.T, factory *testdb.Factory, ddl []string) string {
	newDb := factory.CreateDatabase(t)
	require.NoError(t, applyDDL(t.Context(), newDb.ConnPool, ddl))

	newDbDump, err := pgdump.GetDump(newDb.ConnPool, pgdump.WithSchemaOnly(),
		pgdump.WithRestrictKey(pgdump.FixedRestrictKey))
	require.NoError(t, err)
	return newDbDump
}

func applyDDL(ctx context.Context, db *pgxpool.Pool, ddl []string) error {
	for _, stmt := range ddl {
		_, err := db.Exec(ctx, stmt)
		if err != nil {
			return fmt.Errorf("executing DDL:\n%s\n: %w", stmt, err)
		}
	}
	return nil
}

func applyPlan(ctx context.Context, db *pgxpool.Pool, plan diff.Plan) error {
	var ddl []string
	for _, stmt := range plan.Statements {
		ddl = append(ddl, stmt.ToSQL())
	}
	return applyDDL(ctx, db, ddl)
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
