package migration_acceptance_tests

import "github.com/stripe/pg-schema-diff/pkg/diff"

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
		`},
		newSchemaDDL: []string{
			`
			CREATE TYPE some_enum_1 AS ENUM ('0', '1', '1.5', '2', '2.5', '3', '4');
			CREATE TABLE foo(
				val some_enum_1 DEFAULT '1.5'
			);
		`},
	},
	{
		name: "delete value and add value (enum not used)",
		oldSchemaDDL: []string{
			`
			CREATE TYPE some_enum_1 AS ENUM ('1', '2', '3');
		`},
		newSchemaDDL: []string{
			`
			CREATE TYPE some_enum_1 AS ENUM ('0', '1', '3');
		`},
	},
	{
		name: "delete value and add value (enum used)",
		oldSchemaDDL: []string{
			`
			CREATE TYPE some_enum_1 AS ENUM ('1', '2', '3');
			CREATE TABLE foo(
				val some_enum_1
			);
		`},
		newSchemaDDL: []string{
			`
			CREATE TYPE some_enum_1 AS ENUM ('0', '1', '3');
			CREATE TABLE foo(
				val some_enum_1
			);
		`},

		// Removing a value from an enum in-use is impossible in Postgres. pg-schema-diff will currently identify this
		// as a validation error. In the future, we can identify this in the actual plan generation stage.
		expectedPlanErrorContains: errValidatingPlan.Error(),
	},
}

func (suite *acceptanceTestSuite) TestEnumTestCases() {
	suite.runTestCases(enumAcceptanceTestCases)
}
