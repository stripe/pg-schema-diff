package migration_acceptance_tests

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/suite"
	"github.com/stripe/pg-schema-diff/internal/pgdump"
	"github.com/stripe/pg-schema-diff/internal/pgengine"
	"github.com/stripe/pg-schema-diff/pkg/diff"

	"github.com/stripe/pg-schema-diff/pkg/log"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

type (
	expectations struct {
		planErrorIs       error
		planErrorContains string
		// outputState should be the DDL required to reconstruct the expected output state of the database
		//
		// The outputState might differ from the newSchemaDDL due to options passed to the migrator. For example,
		// the data packing option will cause the column ordering for new tables in the outputState to differ from
		// the column ordering of those tables defined in newSchemaDDL
		//
		// If no outputState is specified, the newSchemaDDL will be used
		outputState []string
	}

	acceptanceTestCase struct {
		name         string
		oldSchemaDDL []string
		newSchemaDDL []string

		// expectedHazardTypes should contain all the unique migration hazard types that are expected to be within the
		// generated plan
		expectedHazardTypes []diff.MigrationHazardType

		// vanillaExpectations refers to the expectations of the migration if no additional opts are used
		vanillaExpectations expectations
		// dataPackingExpectations refers to the expectations of the migration if table packing and ignore column order are used
		dataPackingExpectations expectations
	}

	acceptanceTestSuite struct {
		suite.Suite
		pgEngine *pgengine.Engine
	}
)

func (suite *acceptanceTestSuite) SetupSuite() {
	engine, err := pgengine.StartEngine()
	suite.Require().NoError(err)
	suite.pgEngine = engine
}

func (suite *acceptanceTestSuite) TearDownSuite() {
	suite.pgEngine.Close()
}

// Simulates migrating a database and uses pgdump to compare the actual state to the expected state
func (suite *acceptanceTestSuite) runTestCases(acceptanceTestCases []acceptanceTestCase) {
	for _, tc := range acceptanceTestCases {
		suite.Run(tc.name, func() {
			suite.Run("vanilla", func() {
				suite.runSubtest(tc, tc.vanillaExpectations, nil)
			})
			suite.Run("with data packing (and ignoring column order)", func() {
				suite.runSubtest(tc, tc.dataPackingExpectations, []diff.PlanOpt{
					diff.WithDataPackNewTables(),
					diff.WithIgnoreChangesToColOrder(),
					diff.WithLogger(log.SimpleLogger()),
				})
			})
		})
	}
}

func (suite *acceptanceTestSuite) runSubtest(tc acceptanceTestCase, expects expectations, planOpts []diff.PlanOpt) {
	// onDbInitQueries will be run on both the old database before the migration and the new database before pg_dump
	onDbInitQueries := []string{
		// Enable an extension to enforce that diffing works with extensions enabled
		`CREATE EXTENSION amcheck;`,
	}

	// normalize the subtest
	if expects.outputState == nil {
		expects.outputState = tc.newSchemaDDL
	}

	// Apply old schema DDL to old DB
	oldDb, err := suite.pgEngine.CreateDatabase()
	suite.Require().NoError(err)
	defer oldDb.DropDB()
	// Apply the old schema
	suite.Require().NoError(applyDDL(oldDb, append(onDbInitQueries, tc.oldSchemaDDL...)))

	// Migrate the old DB
	oldDBConnPool, err := sql.Open("pgx", oldDb.GetDSN())
	suite.Require().NoError(err)
	defer oldDBConnPool.Close()
	oldDbConn, _ := oldDBConnPool.Conn(context.Background())
	defer oldDbConn.Close()

	tempDbFactory, err := tempdb.NewOnInstanceFactory(context.Background(), func(ctx context.Context, dbName string) (*sql.DB, error) {
		return sql.Open("pgx", suite.pgEngine.GetPostgresDatabaseConnOpts().With("dbname", dbName).ToDSN())
	})
	suite.Require().NoError(err)
	defer func(tempDbFactory tempdb.Factory) {
		// It's important that this closes properly (the temp database is dropped),
		// so assert it has no error for acceptance tests
		suite.Require().NoError(tempDbFactory.Close())
	}(tempDbFactory)

	plan, err := diff.GeneratePlan(context.Background(), oldDbConn, tempDbFactory, tc.newSchemaDDL, planOpts...)

	if expects.planErrorIs != nil || len(expects.planErrorContains) > 0 {
		if expects.planErrorIs != nil {
			suite.ErrorIs(err, expects.planErrorIs)
		}
		if len(expects.planErrorContains) > 0 {
			suite.ErrorContains(err, expects.planErrorContains)
		}
		return
	}
	suite.Require().NoError(err)

	suite.ElementsMatch(tc.expectedHazardTypes, getUniqueHazardTypesFromStatements(plan.Statements), prettySprintPlan(plan))

	// Apply the plan
	suite.Require().NoError(applyPlan(oldDb, plan), prettySprintPlan(plan))

	// Make sure the pgdump after running the migration is the same as the
	// pgdump from a database where we directly run the newSchemaDDL
	oldDbDump, err := pgdump.GetDump(oldDb, pgdump.WithSchemaOnly())
	suite.Require().NoError(err)

	newDbDump := suite.directlyRunDDLAndGetDump(append(onDbInitQueries, expects.outputState...))
	suite.Equal(newDbDump, oldDbDump, prettySprintPlan(plan))

	// Make sure no diff is found if we try to regenerate a plan
	plan, err = diff.GeneratePlan(context.Background(), oldDbConn, tempDbFactory, tc.newSchemaDDL, planOpts...)
	suite.Require().NoError(err)
	suite.Empty(plan.Statements, prettySprintPlan(plan))
}

func (suite *acceptanceTestSuite) directlyRunDDLAndGetDump(ddl []string) string {
	newDb, err := suite.pgEngine.CreateDatabase()
	suite.Require().NoError(err)
	defer newDb.DropDB()
	suite.Require().NoError(applyDDL(newDb, ddl))

	newDbDump, err := pgdump.GetDump(newDb, pgdump.WithSchemaOnly())
	suite.Require().NoError(err)
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

func TestAcceptanceSuite(t *testing.T) {
	suite.Run(t, new(acceptanceTestSuite))
}
