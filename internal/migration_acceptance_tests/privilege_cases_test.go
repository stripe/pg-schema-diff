package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var privilegeAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "no-op",
		roles: []string{
			"app_user",
		},
		oldSchemaDDL: []string{
			`
                CREATE TABLE foobar(id INT);
                GRANT SELECT ON foobar TO app_user;
			`,
		},
		newSchemaDDL: []string{
			`
                CREATE TABLE foobar(id INT);
                GRANT SELECT ON foobar TO app_user;
			`,
		},
		expectEmptyPlan: true,
	},
	{
		name:  "Grant multiple privileges to role",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`CREATE TABLE foobar(id INT);`,
		},
		newSchemaDDL: []string{
			`
                CREATE TABLE foobar(id INT);
                GRANT SELECT, INSERT, UPDATE, DELETE ON foobar TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Revoke privilege from role",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				GRANT SELECT ON foobar TO app_user;
			`,
		},
		newSchemaDDL: []string{
			`CREATE TABLE foobar(id INT);`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Grant WITH GRANT OPTION",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`CREATE TABLE foobar(id INT);`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				GRANT SELECT ON foobar TO app_user WITH GRANT OPTION;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Change GRANT OPTION (recreates privilege)",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				GRANT SELECT ON foobar TO app_user;
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				GRANT SELECT ON foobar TO app_user WITH GRANT OPTION;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Remove GRANT OPTION (recreates privilege)",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				GRANT SELECT ON foobar TO app_user WITH GRANT OPTION;
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				GRANT SELECT ON foobar TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Grant on new table (no hazards since table is new)",
		roles: []string{"app_user"},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				GRANT SELECT ON foobar TO app_user;
			`,
		},
		// No hazards expected since table is brand new
	},
	{
		name:  "Drop table with privileges (only DeletesData hazard)",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT);
				GRANT SELECT ON foobar TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name:  "Grant on non-public schema table",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE SCHEMA app_schema;
				CREATE TABLE app_schema.foobar(id INT);
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE SCHEMA app_schema;
				CREATE TABLE app_schema.foobar(id INT);
				GRANT SELECT ON app_schema.foobar TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Grant on partitioned parent table",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(
					category TEXT
				) partition by list (category);
				CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('category_1');
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(
					category TEXT
				) partition by list (category);
				CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('category_1');
				GRANT SELECT ON foobar TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Privilege on new partition (not implemented)",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(
					category TEXT
				) partition by list (category);
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(
					category TEXT
				) partition by list (category);
				CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('category');
				GRANT SELECT ON foobar_1 TO app_user;
			`,
		},
		expectedPlanErrorIs: diff.ErrNotImplemented,
	},
	{
		name:  "Add privilege on existing partition (not implemented)",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(
					category TEXT
				) partition by list (category);
				CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('category');
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(
					category TEXT
				) partition by list (category);
				CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('category');
				GRANT SELECT ON foobar_1 TO app_user;
			`,
		},
		expectedPlanErrorIs: diff.ErrNotImplemented,
	},
}

func TestPrivilegeCases(t *testing.T) {
	runTestCases(t, privilegeAcceptanceTestCases)
}
