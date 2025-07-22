package migration_acceptance_tests

import "testing"

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
		expectEmptyPlan: true,
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

func TestNamedSchemaTestCases(t *testing.T) {
	runTestCases(t, namedSchemaAcceptanceTestCases)
}
