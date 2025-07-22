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
}

func TestExtensionTestCases(t *testing.T) {
	runTestCases(t, extensionAcceptanceTestCases)
}
