package migration_acceptance_tests

import "github.com/stripe/pg-schema-diff/pkg/diff"

var extensionAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "no-op",
		oldSchemaDDL: []string{
			`
			CREATE EXTENSION pg_trgm;
			CREATE EXTENSION amcheck;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE EXTENSION pg_trgm;
			CREATE EXTENSION amcheck;
			`,
		},
		vanillaExpectations: expectations{
			empty: true,
		},
		dataPackingExpectations: expectations{
			empty: true,
		},
	},
	{
		name:         "create multiple extensions",
		oldSchemaDDL: []string{},
		newSchemaDDL: []string{
			`
			CREATE EXTENSION pg_trgm;
			CREATE EXTENSION amcheck;
			`,
		},
	},
	{
		name: "drop one extension",
		oldSchemaDDL: []string{
			`
			CREATE EXTENSION pg_trgm;
			CREATE EXTENSION amcheck;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE EXTENSION pg_trgm;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
}

func (suite *acceptanceTestSuite) TestExtensionAcceptanceTestCases() {
	suite.runTestCases(extensionAcceptanceTestCases)
}
