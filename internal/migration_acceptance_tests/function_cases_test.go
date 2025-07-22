package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var functionAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "No-op",
		oldSchemaDDL: []string{
			`
            CREATE FUNCTION add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;

            CREATE OR REPLACE FUNCTION increment(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE FUNCTION add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;

            CREATE OR REPLACE FUNCTION increment(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;
			`,
		},

		expectEmptyPlan: true,
	},
	{
		name:         "Create functions (with conflicting names)",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
            CREATE FUNCTION add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;
            CREATE FUNCTION add(a text, b text) RETURNS text
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN CONCAT(a, b);

            CREATE SCHEMA schema_1;
            CREATE FUNCTION schema_1.add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;
            CREATE FUNCTION schema_1.add(a text, b text) RETURNS text
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN CONCAT(a, b);
			`,
		},
	},
	{
		name:         "Create functions with quoted names (with conflicting names)",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
            CREATE FUNCTION "some add"(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;
            CREATE FUNCTION "some add"(a text, b text) RETURNS text
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN CONCAT(a, b);
			`,
		},
	},
	{
		name:         "Create non-sql function",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
            CREATE FUNCTION non_sql_func(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name:         "Create function with dependencies",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE FUNCTION schema_1.add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;

            CREATE FUNCTION "increment func"(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;

            CREATE FUNCTION function_with_dependencies(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN schema_1.add(a, b) + "increment func"(a);

            -- function with conflicting name to ensure the deps specify param name
            CREATE FUNCTION add(a text, b text) RETURNS text
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN CONCAT(a, b);

            -- identical function on a different schem
            CREATE SCHEMA schema_2;
            CREATE FUNCTION schema_2.add(a integer, b integer) RETURNS text
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN CONCAT(a, b); 
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Create function with an extension that also creates functions installed",
		oldSchemaDDL: []string{
			`
            CREATE EXTENSION amcheck;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE EXTENSION amcheck;

            CREATE FUNCTION add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;
			`,
		},
	},
	{
		name: "Drop functions (with conflicting names)",
		oldSchemaDDL: []string{
			`
            CREATE FUNCTION add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;
            CREATE FUNCTION add(a text, b text) RETURNS text
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN CONCAT(a, b);
			`,
		},
		newSchemaDDL: nil,
	},
	{
		name: "Drop functions with quoted names (with conflicting names)",
		oldSchemaDDL: []string{
			`
            CREATE FUNCTION "some add"(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;
            CREATE FUNCTION "some add"(a text, b text) RETURNS text
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN CONCAT(a, b);
			`,
		},
		newSchemaDDL: nil,
	},
	{
		name: "Drop non-sql function",
		oldSchemaDDL: []string{
			`
            CREATE FUNCTION non_sql_func(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;
		`},
		newSchemaDDL:        nil,
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Drop function with dependencies",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE FUNCTION schema_1.add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;

            CREATE FUNCTION "increment func"(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;

            CREATE FUNCTION function_with_dependencies(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN schema_1.add(a, b) + "increment func"(a);

            -- function with conflicting name to ensure the deps specify param name
            CREATE FUNCTION add(a text, b text) RETURNS text
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN CONCAT(a, b);

            -- identical function on a different schema to ensure schemas are specified correctly
            CREATE SCHEMA schema_2;
            CREATE FUNCTION schema_2.add(a integer, b integer) RETURNS text
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN CONCAT(a, b); 
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Add and drop functions with dependencies (conflicting schemas)",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE FUNCTION schema_1.add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;

            CREATE FUNCTION schema_1."increment func"(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;

            CREATE FUNCTION schema_1.function_with_dependencies(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN schema_1.add(a, b) + schema_1."increment func"(a);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_2;
            CREATE FUNCTION schema_2.add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;

            CREATE FUNCTION schema_2."increment func"(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;

            CREATE FUNCTION schema_2.function_with_dependencies(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN schema_2.add(a, b) + schema_2."increment func"(a);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Alter functions (with conflicting names)",
		oldSchemaDDL: []string{
			`
            CREATE FUNCTION add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;
            CREATE FUNCTION add(a TEXT, b TEXT) RETURNS TEXT
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN CONCAT(a, b);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE FUNCTION add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + a + b;
            CREATE FUNCTION add(a TEXT, b TEXT) RETURNS TEXT
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN CONCAT(CONCAT(a, a), b);
			`,
		},
	},
	{
		name: "Alter functions with quoted names (with conflicting names)",
		oldSchemaDDL: []string{
			`
            CREATE FUNCTION "some add"(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;
            CREATE FUNCTION "some add"(a TEXT, b TEXT) RETURNS TEXT
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN CONCAT(a, b);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE FUNCTION "some add"(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + a + b;
            CREATE FUNCTION "some add"(a TEXT, b TEXT) RETURNS TEXT
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN CONCAT(CONCAT(a, a), b);
			`,
		},
	},
	{
		name: "Alter non-sql function",
		oldSchemaDDL: []string{
			`
            CREATE FUNCTION non_sql_func(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;
		`},
		newSchemaDDL: []string{
			`
            CREATE FUNCTION non_sql_func(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 5;
                    END;
            $$ LANGUAGE plpgsql;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Alter sql function to be non-sql function",
		oldSchemaDDL: []string{
			`
            CREATE FUNCTION some_func(i integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURN i + 5;
		`},
		newSchemaDDL: []string{
			`
            CREATE FUNCTION some_func(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Alter non-sql function to be sql function (no dependency tracking error)",
		oldSchemaDDL: []string{
			`
            CREATE FUNCTION some_func(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;
		`},
		newSchemaDDL: []string{
			`
            CREATE FUNCTION some_func(i integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURN i + 5;
		`},
	},
	{
		name: "Alter a function's dependencies",
		oldSchemaDDL: []string{
			`
            CREATE FUNCTION add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;

            CREATE FUNCTION "increment func"(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;

            CREATE FUNCTION function_with_dependencies(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN add(a, b) + "increment func"(a);

            -- function with conflicting name to ensure the deps specify param name
            CREATE FUNCTION add(a text, b text) RETURNS text
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN CONCAT(a, b);
			`,
		},
		newSchemaDDL: []string{
			`
        CREATE FUNCTION add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;

            CREATE FUNCTION "increment func"(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;

            CREATE FUNCTION "decrement func"(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;

            CREATE FUNCTION function_with_dependencies(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN add(a, b) + "decrement func"(a);

            -- function with conflicting name to ensure the deps specify param name
            CREATE FUNCTION add(a text, b text) RETURNS text
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN CONCAT(a, b);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Alter a dependent function",
		oldSchemaDDL: []string{
			`
            CREATE FUNCTION add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;

            CREATE FUNCTION "increment func"(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;

            CREATE FUNCTION function_with_dependencies(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN add(a, b) + "increment func"(a);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE FUNCTION add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;

            CREATE FUNCTION "increment func"(i integer) RETURNS int AS $$
                    BEGIN
                            RETURN i + 5;
                    END;
            $$ LANGUAGE plpgsql;

            CREATE FUNCTION function_with_dependencies(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN add(a, b) + "increment func"(a);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Alter a function to no longer depend on a function and drop that function",
		oldSchemaDDL: []string{
			`
            CREATE FUNCTION add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;

            CREATE FUNCTION "increment func"(i integer) RETURNS integer AS $$
                    BEGIN
                            RETURN i + 1;
                    END;
            $$ LANGUAGE plpgsql;

            CREATE FUNCTION function_with_dependencies(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN add(a, b) + "increment func"(a);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE FUNCTION add(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN a + b;


            CREATE FUNCTION function_with_dependencies(a integer, b integer) RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURNS NULL ON NULL INPUT
                RETURN add(a, b);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
}

func TestFunctionTestCases(t *testing.T) {
	runTestCases(t, functionAcceptanceTestCases)
}
