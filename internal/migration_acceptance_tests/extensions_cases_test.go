package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var extensionAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "no-op",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE EXTENSION pg_trgm WITH SCHEMA schema_1;
            CREATE EXTENSION amcheck;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE EXTENSION pg_trgm WITH SCHEMA schema_1;
            CREATE EXTENSION amcheck;
			`,
		},
		expectEmptyPlan: true,
	},
	{
		name: "create multiple extensions",
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE EXTENSION pg_trgm WITH SCHEMA schema_1;
            CREATE EXTENSION amcheck;
			`,
		},
	},
	{
		name: "drop one extension",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE EXTENSION pg_trgm;
            CREATE EXTENSION amcheck WITH SCHEMA schema_1;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE EXTENSION pg_trgm;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "upgrade an extension implicitly and explicitly",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE EXTENSION pg_trgm WITH VERSION '1.5';
            CREATE EXTENSION amcheck WITH SCHEMA schema_1 VERSION '1.3'; 
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE EXTENSION pg_trgm WITH VERSION '1.6';
            CREATE EXTENSION AMCHECK WITH SCHEMA schema_1;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeExtensionVersionUpgrade},
	},
	{
		name: "reject extension drop owning hidden table",
		oldSchemaDDL: []string{
			`
			CREATE EXTENSION pg_trgm;
			CREATE SCHEMA hidden;
			CREATE TABLE hidden.extension_table (id INTEGER);
			ALTER EXTENSION pg_trgm ADD TABLE hidden.extension_table;
			`,
		},
		expectedPlanErrorContains: "extension owns table-like relation hidden.extension_table",
	},
	{
		name: "reject extension drop owning hidden partition",
		oldSchemaDDL: []string{
			`
			CREATE EXTENSION pg_trgm;
			CREATE SCHEMA hidden;
			CREATE TABLE hidden.extension_root (id INTEGER) PARTITION BY RANGE (id);
			CREATE TABLE hidden.extension_child PARTITION OF hidden.extension_root
				FOR VALUES FROM (0) TO (100);
			ALTER EXTENSION pg_trgm ADD TABLE hidden.extension_root;
			ALTER EXTENSION pg_trgm ADD TABLE hidden.extension_child;
			`,
		},
		expectedPlanErrorContains: "extension owns table-like relation hidden.extension_child",
	},
	{
		name: "reject extension update owning hidden table without modeled table diff",
		oldSchemaDDL: []string{
			`
			CREATE EXTENSION pg_trgm WITH VERSION '1.5';
			CREATE SCHEMA hidden;
			CREATE TABLE hidden.extension_table (id INTEGER);
			ALTER EXTENSION pg_trgm ADD TABLE hidden.extension_table;
			`,
		},
		newSchemaDDL: []string{
			`CREATE EXTENSION pg_trgm WITH VERSION '1.6';`,
		},
		expectedPlanErrorContains: "extension owns table-like relation hidden.extension_table",
	},
	{
		name: "reject hidden extension table with validation disabled",
		oldSchemaDDL: []string{
			`
			CREATE EXTENSION pg_trgm;
			CREATE SCHEMA hidden;
			CREATE TABLE hidden.extension_table (id INTEGER);
			ALTER EXTENSION pg_trgm ADD TABLE hidden.extension_table;
			`,
		},
		planOpts:                  []diff.PlanOpt{diff.WithDoNotValidatePlan()},
		expectedPlanErrorContains: "extension owns table-like relation hidden.extension_table",
	},
	{
		name: "unchanged extension owning hidden table remains allowed",
		oldSchemaDDL: []string{
			`
			CREATE EXTENSION pg_trgm;
			CREATE SCHEMA hidden;
			CREATE TABLE hidden.extension_table (id INTEGER);
			ALTER EXTENSION pg_trgm ADD TABLE hidden.extension_table;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE EXTENSION pg_trgm;
			CREATE SCHEMA hidden;
			CREATE TABLE hidden.extension_table (id INTEGER);
			ALTER EXTENSION pg_trgm ADD TABLE hidden.extension_table;
			`,
		},
		expectEmptyPlan: true,
	},
}

func TestExtensionTestCases(t *testing.T) {
	runTestCases(t, extensionAcceptanceTestCases)
}
