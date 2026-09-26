package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var columnPrivilegeAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "no-op",
		roles: []string{
			"app_user",
		},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT, data TEXT);
				GRANT SELECT (id) ON foobar TO app_user;
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT, data TEXT);
				GRANT SELECT (id) ON foobar TO app_user;
			`,
		},
		expectEmptyPlan: true,
	},
	{
		name:  "Grant column privileges on new table (no hazards since table is new)",
		roles: []string{"app_user"},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT, data TEXT);
				GRANT SELECT (data), UPDATE (data) ON foobar TO app_user;
			`,
		},
		// No hazards expected since table is brand new
	},
	{
		name:  "Grant column privilege on existing table",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`CREATE TABLE foobar(id INT, data TEXT);`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT, data TEXT);
				GRANT SELECT (data) ON foobar TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Grant column privilege on multiple columns",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`CREATE TABLE foobar(id INT, data TEXT, extra TEXT);`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT, data TEXT, extra TEXT);
				GRANT SELECT (data, extra) ON foobar TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Revoke column privilege",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT, data TEXT);
				GRANT SELECT (data) ON foobar TO app_user;
			`,
		},
		newSchemaDDL: []string{
			`CREATE TABLE foobar(id INT, data TEXT);`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Grant column privilege on new column",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`CREATE TABLE foobar(id INT);`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT, data TEXT);
				GRANT SELECT (data) ON foobar TO app_user;
			`,
		},
		// The column must be added before its privilege is granted.
		expectedPlanDDL: []string{
			`ALTER TABLE "public"."foobar" ADD COLUMN "data" text COLLATE "pg_catalog"."default"`,
			`GRANT SELECT ("data") ON "public"."foobar" TO "app_user"`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Drop column with column privilege (only DeletesData hazard)",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT, data TEXT);
				GRANT SELECT (data) ON foobar TO app_user;
			`,
		},
		newSchemaDDL: []string{
			`CREATE TABLE foobar(id INT);`,
		},
		// The privilege is dropped together with the column, so no revoke is emitted.
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name:  "Change column privilege GRANT OPTION (recreates privilege)",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT, data TEXT);
				GRANT SELECT (data) ON foobar TO app_user;
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT, data TEXT);
				GRANT SELECT (data) ON foobar TO app_user WITH GRANT OPTION;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Remove column privilege GRANT OPTION (recreates privilege)",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT, data TEXT);
				GRANT SELECT (data) ON foobar TO app_user WITH GRANT OPTION;
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(id INT, data TEXT);
				GRANT SELECT (data) ON foobar TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Column privilege on non-public schema table",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
				CREATE SCHEMA app_schema;
				CREATE TABLE app_schema.foobar(id INT, data TEXT);
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE SCHEMA app_schema;
				CREATE TABLE app_schema.foobar(id INT, data TEXT);
				GRANT SELECT (data) ON app_schema.foobar TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Column privilege on new partition (not implemented)",
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
				GRANT SELECT (category) ON foobar_1 TO app_user;
			`,
		},
		expectedPlanErrorIs: diff.ErrNotImplemented,
	},
	{
		name:  "Add column privilege on existing partition (not implemented)",
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
				GRANT SELECT (category) ON foobar_1 TO app_user;
			`,
		},
		expectedPlanErrorIs: diff.ErrNotImplemented,
	},
}

func TestColumnPrivilegeCases(t *testing.T) {
	runTestCases(t, columnPrivilegeAcceptanceTestCases)
}
