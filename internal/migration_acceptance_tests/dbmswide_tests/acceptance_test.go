package dbmswide_tests

import (
	"testing"

	acceptance "github.com/stripe/pg-schema-diff/internal/migration_acceptance_tests"
	"github.com/stripe/pg-schema-diff/pkg/diff"
)

type acceptanceTestCase struct {
	name                string
	roles               []string
	oldSchemaDDL        []string
	newSchemaDDL        []string
	expectedHazardTypes []diff.MigrationHazardType
	expectedPlanErrorIs error
	expectedPlanDDL     []string
	expectEmptyPlan     bool
	expectedDBSchemaDDL []string
}

func runTestCases(t *testing.T, testCases []acceptanceTestCase) {
	t.Helper()

	dbmswideTestCases := make([]acceptance.DBMSWideAcceptanceTestCase, 0, len(testCases))
	for _, tc := range testCases {
		dbmswideTestCases = append(dbmswideTestCases, acceptance.DBMSWideAcceptanceTestCase{
			Name:                tc.name,
			Roles:               tc.roles,
			OldSchemaDDL:        tc.oldSchemaDDL,
			NewSchemaDDL:        tc.newSchemaDDL,
			ExpectedHazardTypes: tc.expectedHazardTypes,
			ExpectedPlanErrorIs: tc.expectedPlanErrorIs,
			ExpectedPlanDDL:     tc.expectedPlanDDL,
			ExpectEmptyPlan:     tc.expectEmptyPlan,
			ExpectedDBSchemaDDL: tc.expectedDBSchemaDDL,
		})
	}
	acceptance.RunDBMSWideTestCases(t, dbmswideTestCases)
}
