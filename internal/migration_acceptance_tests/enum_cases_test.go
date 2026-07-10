package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var enumAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "no-op",
		oldSchemaDDL: []string{
			`
            CREATE TYPE color AS ENUM ('red', 'green', 'blue');
            CREATE TABLE foo(
                color color DEFAULT 'green'
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TYPE color AS ENUM ('red', 'green', 'blue');
            CREATE TABLE foo(
                color color DEFAULT 'green'
            );
			`,
		},

		expectEmptyPlan: true,
	},
	{
		name: "create enum",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foo();
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE TYPE schema_1.color AS ENUM ('red', 'green', 'blue');
            CREATE TABLE foo(
                color schema_1.color DEFAULT 'green'
            );
			`,
		},
	},
	{
		name: "drop enum",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE TYPE schema_1.color AS ENUM ('red', 'green', 'blue');
            CREATE TABLE foo(
                color schema_1.color DEFAULT 'green'
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE TABLE foo(
                color VARCHAR(255) DEFAULT 'green'
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "add values",
		oldSchemaDDL: []string{
			`
            CREATE TYPE some_enum_1 AS ENUM ('1', '2', '3');
            CREATE TABLE foo(
                val some_enum_1
            );
		`,
		},
		newSchemaDDL: []string{
			`
            CREATE TYPE some_enum_1 AS ENUM ('0', '1', '1.5', '2', '2.5', '3', '4');
            CREATE TABLE foo(
                val some_enum_1 DEFAULT '1.5'
            );
		`,
		},
	},
	{
		name: "delete value and add value (enum not used)",
		oldSchemaDDL: []string{
			`
            CREATE TYPE some_enum_1 AS ENUM ('1', '2', '3');
		`,
		},
		newSchemaDDL: []string{
			`
            CREATE TYPE some_enum_1 AS ENUM ('0', '1', '3');
		`,
		},
	},
	{
		name: "delete value and add value (enum used)",
		oldSchemaDDL: []string{
			`
            CREATE TYPE some_enum_1 AS ENUM ('1', '2', '3');
            CREATE TABLE foo(
                val some_enum_1
            );
		`,
		},
		newSchemaDDL: []string{
			`
            CREATE TYPE some_enum_1 AS ENUM ('0', '1', '3');
            CREATE TABLE foo(
                val some_enum_1
            );
		`,
		},

		// Removing a value from an enum in-use is impossible in Postgres. pg-schema-diff will currently identify this
		// as a validation error. In the future, we can identify this in the actual plan generation stage.
		expectedPlanErrorContains: errValidatingPlan.Error(),
	},
	{
		// Exercises the CREATE TYPE ... AS ENUM path with labels containing single
		// quotes. Verifies the generated DDL is correctly escaped, executes against
		// Postgres, and stores the labels verbatim.
		name: "create enum with single-quote labels",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foo();
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TYPE quote_enum AS ENUM ('O''Brien', 'it''s', 'plain');
            CREATE TABLE foo(
                val quote_enum DEFAULT 'plain'
            );
			`,
		},
	},
	{
		// Exercises the ALTER TYPE ... ADD VALUE path (value appended at the end)
		// with a label containing a single quote.
		name: "add value with single-quote label",
		oldSchemaDDL: []string{
			`
            CREATE TYPE quote_enum AS ENUM ('a', 'b');
            CREATE TABLE foo(
                val quote_enum
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TYPE quote_enum AS ENUM ('a', 'b', 'won''t');
            CREATE TABLE foo(
                val quote_enum
            );
			`,
		},
	},
	{
		// Exercises the ALTER TYPE ... ADD VALUE ... BEFORE path. The new value is
		// inserted in the middle, so a BEFORE clause is emitted referencing a
		// following label that contains a single quote.
		name: "add value before a single-quote label",
		oldSchemaDDL: []string{
			`
            CREATE TYPE quote_enum AS ENUM ('a', 'it''s');
            CREATE TABLE foo(
                val quote_enum
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TYPE quote_enum AS ENUM ('a', 'mid', 'it''s');
            CREATE TABLE foo(
                val quote_enum
            );
			`,
		},
	},
	{
		// Regression test for the enum-label SQL injection: a label crafted as an
		// injection payload must be stored inertly, not executed. If escaping ever
		// regresses, applying the generated plan would attempt to DROP the table and
		// the pg_dump comparison would fail.
		name: "enum label with sql injection payload",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foo();
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TYPE evil AS ENUM ('x''); DROP TABLE foo; --');
            CREATE TABLE foo(
                val evil
            );
			`,
		},
	},
}

func TestEnumTestCases(t *testing.T) {
	runTestCases(t, enumAcceptanceTestCases)
}
