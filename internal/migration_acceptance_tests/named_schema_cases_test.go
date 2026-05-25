package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

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
	{
		name:  "Grant usage on existing schema",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{`
            CREATE SCHEMA app_schema;
		`},
		newSchemaDDL: []string{`
            CREATE SCHEMA app_schema;
            GRANT USAGE ON SCHEMA app_schema TO app_user;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
		expectedPlanDDL: []string{`GRANT USAGE ON SCHEMA "app_schema" TO "app_user"`},
	},
	{
		name:  "Revoke usage on existing schema",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{`
            CREATE SCHEMA app_schema;
            GRANT USAGE ON SCHEMA app_schema TO app_user;
		`},
		newSchemaDDL: []string{`
            CREATE SCHEMA app_schema;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
		expectedPlanDDL: []string{`REVOKE USAGE ON SCHEMA "app_schema" FROM "app_user"`},
	},
	{
		name:  "Change schema grant option",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{`
            CREATE SCHEMA app_schema;
            GRANT USAGE ON SCHEMA app_schema TO app_user;
		`},
		newSchemaDDL: []string{`
            CREATE SCHEMA app_schema;
            GRANT USAGE ON SCHEMA app_schema TO app_user WITH GRANT OPTION;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
}

func TestNamedSchemaTestCases(t *testing.T) {
	runTestCases(t, namedSchemaAcceptanceTestCases)
}
