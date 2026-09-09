package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var domainAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "no-op",
		oldSchemaDDL: []string{
			`
            CREATE DOMAIN positive_numeric AS NUMERIC(10, 2)
                DEFAULT 1.0
                NOT NULL
                CONSTRAINT positive_numeric_check CHECK (VALUE > 0);
            CREATE TABLE foo(
                amount positive_numeric
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE DOMAIN positive_numeric AS NUMERIC(10, 2)
                DEFAULT 1.0
                NOT NULL
                CONSTRAINT positive_numeric_check CHECK (VALUE > 0);
            CREATE TABLE foo(
                amount positive_numeric
            );
			`,
		},

		expectEmptyPlan: true,
	},
	{
		name: "create domain used by a table column",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foo();
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE DOMAIN positive_numeric AS NUMERIC(10, 2)
                DEFAULT 1.0
                NOT NULL
                CONSTRAINT positive_numeric_check CHECK (VALUE > 0);
            CREATE TABLE foo(
                amount positive_numeric
            );
			`,
		},
	},
	{
		name: "create domain in a non-public schema used by a table column added to an existing table",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foo(
                id INT PRIMARY KEY
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE DOMAIN schema_1.positive_numeric AS NUMERIC(10, 2)
                CONSTRAINT positive_numeric_check CHECK (VALUE > 0);
            CREATE TABLE foo(
                id INT PRIMARY KEY,
                amount schema_1.positive_numeric
            );
			`,
		},
	},
	{
		name: "create domain used by a function parameter and return type",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foo();
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE DOMAIN schema_1.positive_numeric AS NUMERIC(10, 2)
                CONSTRAINT positive_numeric_check CHECK (VALUE > 0);
            CREATE TABLE foo();
            CREATE FUNCTION schema_1.double(val schema_1.positive_numeric)
                RETURNS schema_1.positive_numeric
                LANGUAGE sql IMMUTABLE
                AS $$ SELECT (val * 2)::schema_1.positive_numeric $$;
			`,
		},
	},
	{
		name: "drop domain and the column using it",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE DOMAIN schema_1.positive_numeric AS NUMERIC(10, 2)
                CONSTRAINT positive_numeric_check CHECK (VALUE > 0);
            CREATE TABLE foo(
                id INT PRIMARY KEY,
                amount schema_1.positive_numeric
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE TABLE foo(
                id INT PRIMARY KEY
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "drop domain used by a function, dropping the function too",
		oldSchemaDDL: []string{
			`
            CREATE DOMAIN positive_numeric AS NUMERIC(10, 2)
                CONSTRAINT positive_numeric_check CHECK (VALUE > 0);
            CREATE FUNCTION double(val positive_numeric)
                RETURNS positive_numeric
                LANGUAGE sql IMMUTABLE
                AS $$ SELECT (val * 2)::positive_numeric $$;
			`,
		},
		newSchemaDDL: []string{
			``,
		},
	},
	{
		name: "add a constraint",
		oldSchemaDDL: []string{
			`
            CREATE DOMAIN positive_numeric AS NUMERIC(10, 2);
            CREATE TABLE foo(
                amount positive_numeric
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE DOMAIN positive_numeric AS NUMERIC(10, 2)
                CONSTRAINT positive_numeric_check CHECK (VALUE > 0);
            CREATE TABLE foo(
                amount positive_numeric
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
		},
	},
	{
		name: "drop a constraint",
		oldSchemaDDL: []string{
			`
            CREATE DOMAIN positive_numeric AS NUMERIC(10, 2)
                CONSTRAINT positive_numeric_check CHECK (VALUE > 0);
            CREATE TABLE foo(
                amount positive_numeric
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE DOMAIN positive_numeric AS NUMERIC(10, 2);
            CREATE TABLE foo(
                amount positive_numeric
            );
			`,
		},
	},
	{
		name: "change a constraint expression, keeping its name",
		oldSchemaDDL: []string{
			`
            CREATE DOMAIN positive_numeric AS NUMERIC(10, 2)
                CONSTRAINT positive_numeric_check CHECK (VALUE > 0);
            CREATE TABLE foo(
                amount positive_numeric
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE DOMAIN positive_numeric AS NUMERIC(10, 2)
                CONSTRAINT positive_numeric_check CHECK (VALUE > 10);
            CREATE TABLE foo(
                amount positive_numeric
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
		},
	},
	{
		name: "set and drop the default and not null",
		oldSchemaDDL: []string{
			`
            CREATE DOMAIN with_default AS TEXT DEFAULT 'a';
            CREATE DOMAIN with_not_null AS TEXT NOT NULL;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE DOMAIN with_default AS TEXT NOT NULL;
            CREATE DOMAIN with_not_null AS TEXT DEFAULT 'b';
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
		},
	},
	{
		name: "rename a domain is a drop and a create",
		oldSchemaDDL: []string{
			`
            CREATE DOMAIN old_name AS NUMERIC(10, 2)
                CONSTRAINT old_name_check CHECK (VALUE > 0);
            CREATE TABLE foo(
                amount old_name
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE DOMAIN new_name AS NUMERIC(10, 2)
                CONSTRAINT new_name_check CHECK (VALUE > 0);
            CREATE TABLE foo(
                amount new_name
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "change the base type of an unused domain re-creates it",
		oldSchemaDDL: []string{
			`
            CREATE DOMAIN some_domain AS NUMERIC(10, 2);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE DOMAIN some_domain AS NUMERIC(20, 4);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeHasUntrackableDependencies,
		},
	},
	{
		name: "change the base type of a domain used by a function re-creates both",
		oldSchemaDDL: []string{
			`
            CREATE DOMAIN some_domain AS NUMERIC(10, 2);
            CREATE FUNCTION identity(val some_domain)
                RETURNS some_domain
                LANGUAGE sql IMMUTABLE
                AS $$ SELECT val $$;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE DOMAIN some_domain AS NUMERIC(20, 4);
            CREATE FUNCTION identity(val some_domain)
                RETURNS some_domain
                LANGUAGE sql IMMUTABLE
                AS $$ SELECT val $$;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeHasUntrackableDependencies,
		},
	},
	{
		name: "change the base type of a domain used by a table column is unsupported",
		oldSchemaDDL: []string{
			`
            CREATE DOMAIN some_domain AS NUMERIC(10, 2);
            CREATE TABLE foo(
                amount some_domain
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE DOMAIN some_domain AS NUMERIC(20, 4);
            CREATE TABLE foo(
                amount some_domain
            );
			`,
		},
		expectedPlanErrorIs: diff.ErrNotImplemented,
	},
	{
		name: "create a domain built on another domain",
		oldSchemaDDL: []string{
			``,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE DOMAIN schema_1.base_domain AS NUMERIC(10, 2)
                CONSTRAINT base_domain_check CHECK (VALUE > 0);
            CREATE DOMAIN schema_1.derived_domain AS schema_1.base_domain
                CONSTRAINT derived_domain_check CHECK (VALUE < 100);
            CREATE TABLE foo(
                amount schema_1.derived_domain
            );
			`,
		},
	},
	{
		name: "create a domain whose CHECK calls an immutable function",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE FUNCTION schema_1.assert_finite_numeric(val NUMERIC)
                RETURNS BOOLEAN
                LANGUAGE plpgsql IMMUTABLE
                AS $$
                BEGIN
                    IF val IS NULL OR val = 'NaN'::NUMERIC THEN
                        RAISE EXCEPTION 'SCHEMA_1.ASSERT_FINITE_NUMERIC.NOT_FINITE';
                    END IF;
                    RETURN TRUE;
                END;
                $$;
            CREATE DOMAIN schema_1.finite_numeric AS NUMERIC
                CONSTRAINT finite_numeric_check CHECK (schema_1.assert_finite_numeric(VALUE));
            CREATE TABLE foo(
                amount schema_1.finite_numeric
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeHasUntrackableDependencies,
		},
	},
	{
		name: "drop a domain whose CHECK calls an immutable function, dropping the function too",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE FUNCTION schema_1.assert_finite_numeric(val NUMERIC)
                RETURNS BOOLEAN
                LANGUAGE plpgsql IMMUTABLE
                AS $$
                BEGIN
                    IF val IS NULL OR val = 'NaN'::NUMERIC THEN
                        RAISE EXCEPTION 'SCHEMA_1.ASSERT_FINITE_NUMERIC.NOT_FINITE';
                    END IF;
                    RETURN TRUE;
                END;
                $$;
            CREATE DOMAIN schema_1.finite_numeric AS NUMERIC
                CONSTRAINT finite_numeric_check CHECK (schema_1.assert_finite_numeric(VALUE));
            CREATE TABLE foo(
                amount schema_1.finite_numeric
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE TABLE foo();
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
			diff.MigrationHazardTypeHasUntrackableDependencies,
		},
	},
	{
		name: "change the CHECK of a domain that calls an immutable function",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE FUNCTION schema_1.assert_finite_numeric(val NUMERIC)
                RETURNS BOOLEAN
                LANGUAGE plpgsql IMMUTABLE
                AS $$
                BEGIN
                    IF val IS NULL OR val = 'NaN'::NUMERIC THEN
                        RAISE EXCEPTION 'SCHEMA_1.ASSERT_FINITE_NUMERIC.NOT_FINITE';
                    END IF;
                    RETURN TRUE;
                END;
                $$;
            CREATE DOMAIN schema_1.finite_numeric AS NUMERIC
                CONSTRAINT finite_numeric_check CHECK (schema_1.assert_finite_numeric(VALUE));
            CREATE TABLE foo(
                amount schema_1.finite_numeric
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE FUNCTION schema_1.assert_finite_numeric(val NUMERIC)
                RETURNS BOOLEAN
                LANGUAGE plpgsql IMMUTABLE
                AS $$
                BEGIN
                    IF val IS NULL OR val = 'NaN'::NUMERIC THEN
                        RAISE EXCEPTION 'SCHEMA_1.ASSERT_FINITE_NUMERIC.NOT_FINITE';
                    END IF;
                    RETURN TRUE;
                END;
                $$;
            CREATE DOMAIN schema_1.finite_numeric AS NUMERIC
                CONSTRAINT finite_numeric_check CHECK (
                    schema_1.assert_finite_numeric(VALUE) AND VALUE >= 0
                );
            CREATE TABLE foo(
                amount schema_1.finite_numeric
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
		},
	},
	{
		name: "create a domain with a collation",
		oldSchemaDDL: []string{
			``,
		},
		newSchemaDDL: []string{
			`
            CREATE DOMAIN c_text AS TEXT COLLATE "C"
                CONSTRAINT c_text_check CHECK (VALUE <> '');
            CREATE TABLE foo(
                val c_text
            );
			`,
		},
	},
}

func TestDomainTestCases(t *testing.T) {
	runTestCases(t, domainAcceptanceTestCases)
}
