package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var defaultPrivilegeAcceptanceTestCases = []acceptanceTestCase{
	{
		name:  "no-op",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1 GRANT SELECT ON TABLES TO app_user;
			`,
		},
		newSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1 GRANT SELECT ON TABLES TO app_user;
			`,
		},
		expectEmptyPlan: true,
	},
	{
		name:  "grant default privileges on a new schema",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			``,
		},
		newSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1
                    GRANT SELECT, INSERT ON TABLES TO app_user;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1
                    GRANT USAGE ON SEQUENCES TO app_user;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1
                    GRANT EXECUTE ON FUNCTIONS TO app_user;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1
                    GRANT USAGE ON TYPES TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "grant default privileges on an existing schema",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
                CREATE TABLE schema_1.foo(id INT);
			`,
		},
		newSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
                CREATE TABLE schema_1.foo(id INT);
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1 GRANT SELECT ON TABLES TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "revoke default privileges",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1
                    GRANT SELECT, INSERT ON TABLES TO app_user;
			`,
		},
		newSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "change the granted default privileges",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1
                    GRANT SELECT, INSERT ON TABLES TO app_user;
			`,
		},
		newSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1
                    GRANT SELECT, UPDATE ON TABLES TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "add and remove the grant option",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1
                    GRANT SELECT ON TABLES TO app_user;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1
                    GRANT INSERT ON TABLES TO app_user WITH GRANT OPTION;
			`,
		},
		newSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1
                    GRANT SELECT ON TABLES TO app_user WITH GRANT OPTION;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1
                    GRANT INSERT ON TABLES TO app_user;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "grant default privileges to PUBLIC",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
			`,
		},
		newSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1 GRANT SELECT ON TABLES TO PUBLIC;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "drop the schema holding the default privileges",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1 GRANT SELECT ON TABLES TO app_user;
			`,
		},
		newSchemaDDL: []string{
			``,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		// A database-wide rule is not attached to any schema, so it is not extracted and never
		// shows up in a plan — not even as the revoke that would otherwise pair with the
		// schema-scoped grant below.
		name:  "database-wide default privileges are out of scope",
		roles: []string{"app_user"},
		oldSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
                ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO app_user;
			`,
		},
		newSchemaDDL: []string{
			`
                CREATE SCHEMA schema_1;
                ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO app_user;
                ALTER DEFAULT PRIVILEGES IN SCHEMA schema_1 GRANT SELECT ON TABLES TO app_user;
			`,
		},
		expectedPlanDDL: []string{
			`ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "schema_1" GRANT SELECT ON TABLES TO "app_user"`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
}

func TestDefaultPrivilegeTestCases(t *testing.T) {
	runTestCases(t, defaultPrivilegeAcceptanceTestCases)
}
