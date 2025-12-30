package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var procedureAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "No-op",
		oldSchemaDDL: []string{
			`
            CREATE OR REPLACE PROCEDURE some_procedure(i integer) AS $$
                    BEGIN
                            RAISE NOTICE 'foobar';
                    END;
            $$ LANGUAGE plpgsql;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE OR REPLACE PROCEDURE some_procedure(i integer) AS $$
                    BEGIN
                            RAISE NOTICE 'foobar';
                    END;
            $$ LANGUAGE plpgsql;
			`,
		},

		expectEmptyPlan: true,
	},
	{
		name:         "Create procedure with no dependencies",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
            CREATE OR REPLACE PROCEDURE some_procedure(val INTEGER) LANGUAGE plpgsql AS $$
            BEGIN
                RAISE NOTICE 'Val, %', val;
            END
            $$;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name:         "Create procedure with dependencies that also must be created",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
            CREATE SEQUENCE user_id_seq;

            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );

            CREATE OR REPLACE FUNCTION get_name(input_name TEXT) RETURNS TEXT AS $$
                SELECT input_name || '_some_fixed_val'
            $$ LANGUAGE SQL;

            CREATE OR REPLACE PROCEDURE "Add User"(name TEXT) LANGUAGE SQL AS $$
            INSERT INTO users (id, name) VALUES (NEXTVAL('user_id_seq'), get_name(name));
            $$;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Alter a procedure to have dependencies that must be created",
		oldSchemaDDL: []string{
			`
            CREATE TABLE users (); 
            CREATE OR REPLACE PROCEDURE "Add User"(name TEXT) LANGUAGE SQL AS $$
            INSERT INTO users DEFAULT VALUES; 
            $$;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SEQUENCE user_id_seq;

            CREATE TABLE users (
                id INTEGER, 
                name TEXT NOT NULL
            );

            CREATE OR REPLACE FUNCTION get_name(input_name TEXT) RETURNS TEXT AS $$
                SELECT input_name || '_some_fixed_val'
            $$ LANGUAGE SQL;

            CREATE OR REPLACE PROCEDURE "Add User"(name TEXT) LANGUAGE SQL AS $$
            INSERT INTO users (id, name) VALUES (NEXTVAL('user_id_seq'), get_name(name));
            $$;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeHasUntrackableDependencies,
		},
	},
	{
		name: "Drop procedure and its dependencies",
		oldSchemaDDL: []string{
			`
            CREATE SEQUENCE user_id_seq;

            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );

            CREATE OR REPLACE FUNCTION get_name(input_name TEXT) RETURNS TEXT AS $$
                SELECT input_name || '_some_fixed_val'
            $$ LANGUAGE SQL;

            CREATE OR REPLACE PROCEDURE "Add User"(name TEXT) LANGUAGE SQL AS $$
            INSERT INTO users (id, name) VALUES (NEXTVAL('user_id_seq'), get_name(name));
            $$;
			`,
		},
		newSchemaDDL: []string{
			`
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
			diff.MigrationHazardTypeHasUntrackableDependencies,
		},
	},
	{
		// This reveals Postgres does not actually track dependencies of procedures outside of creation time.
		name: "Drop a procedure's dependencies but not the procedure",
		oldSchemaDDL: []string{
			`
            CREATE TABLE users ();

            CREATE OR REPLACE PROCEDURE "Add User"(name TEXT) LANGUAGE SQL AS $$
            INSERT INTO users DEFAULT VALUES;
            $$;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE users();

            CREATE OR REPLACE PROCEDURE "Add User"(name TEXT) LANGUAGE SQL AS $$
            INSERT INTO users DEFAULT VALUES;
            $$;

            -- Drop the table the procedure depends on. This allows us to actually create a database with this schema.
            DROP TABLE users;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
		planOpts: []diff.PlanOpt{
			// Skip plan validation because the acceptance test attempts to regenerate the plan after migrating and
			// assert it's empty. As part of this plan regeneration, plan validation attempts to create a database with
			// just an "Add User" procedure through normal SQL generation, which inherently fails because the users
			// table does not exist.
			diff.WithDoNotValidatePlan(),
		},
	},
}

func TestProcedureTestCases(t *testing.T) {
	runTestCases(t, procedureAcceptanceTestCases)
}
