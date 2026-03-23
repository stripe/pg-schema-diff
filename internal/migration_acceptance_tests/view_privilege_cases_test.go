package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var viewPrivilegeAcceptanceTestCases = []acceptanceTestCase{
	{
		name:  "no-op: view with existing privilege",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				CREATE VIEW foobar_view AS SELECT id FROM foobar;
				GRANT SELECT ON foobar_view TO app_user;
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				CREATE VIEW foobar_view AS SELECT id FROM foobar;
				GRANT SELECT ON foobar_view TO app_user;
			`,
		},
		expectEmptyPlan: true,
	},
	{
		name:  "Grant SELECT on view",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				CREATE VIEW foobar_view AS SELECT id FROM foobar;
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				CREATE VIEW foobar_view AS SELECT id FROM foobar;
				GRANT SELECT ON foobar_view TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Revoke SELECT on view",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				CREATE VIEW foobar_view AS SELECT id FROM foobar;
				GRANT SELECT ON foobar_view TO app_user;
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				CREATE VIEW foobar_view AS SELECT id FROM foobar;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Grant multiple privileges on view",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				CREATE VIEW foobar_view AS SELECT id FROM foobar;
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				CREATE VIEW foobar_view AS SELECT id FROM foobar;
				GRANT SELECT, INSERT, UPDATE, DELETE ON foobar_view TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Grant WITH GRANT OPTION on view",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				CREATE VIEW foobar_view AS SELECT id FROM foobar;
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				CREATE VIEW foobar_view AS SELECT id FROM foobar;
				GRANT SELECT ON foobar_view TO app_user WITH GRANT OPTION;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Grant on non-public schema view",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE SCHEMA app_schema;
				CREATE TABLE app_schema.foobar(id INT);
				CREATE VIEW app_schema.foobar_view AS SELECT id FROM app_schema.foobar;
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE SCHEMA app_schema;
				CREATE TABLE app_schema.foobar(id INT);
				CREATE VIEW app_schema.foobar_view AS SELECT id FROM app_schema.foobar;
				GRANT SELECT ON app_schema.foobar_view TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Grant on new view (no hazards since view is new)",
		roles: []string{"app_user"},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				CREATE VIEW foobar_view AS SELECT id FROM foobar;
				GRANT SELECT ON foobar_view TO app_user;
			`,
		},
	},
	{
		name:  "Drop view with privileges",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				CREATE VIEW foobar_view AS SELECT id FROM foobar;
				GRANT SELECT ON foobar_view TO app_user;
			`,
		},
		newSchemaDDL: []string{
			`CREATE TABLE foobar(id INT);`,
		},
	},
}

func TestViewPrivilegeCases(t *testing.T) {
	runTestCases(t, viewPrivilegeAcceptanceTestCases)
}
