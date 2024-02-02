package migration_acceptance_tests

var namedSchemaAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "no op",
		oldSchemaDDL: []string{`
			CREATE SCHEMA "schema 1";
			CREATE SCHEMA "schema 2";
		`},
		newSchemaDDL: []string{`
			CREATE SCHEMA "schema 1";	
			CREATE SCHEMA "schema 2";
		`},
		vanillaExpectations: expectations{
			empty: true,
		},
		dataPackingExpectations: expectations{
			empty: true,
		},
	},
	{
		name: "create schema",
		oldSchemaDDL: []string{`
			CREATE SCHEMA "schema 1";	
		`},
		newSchemaDDL: []string{`
			CREATE SCHEMA "schema 1";	
			CREATE SCHEMA "schema 2";	
		`},
	},
	{
		name: "Drop schema",
		oldSchemaDDL: []string{`
			CREATE SCHEMA "schema 1";	
			CREATE SCHEMA "schema 2";	
		`},
		newSchemaDDL: []string{`
			CREATE SCHEMA "schema 1";	
		`},
	},
}

func (suite *acceptanceTestSuite) TestNamedSchemaAcceptanceTestCases() {
	suite.runTestCases(namedSchemaAcceptanceTestCases)
}