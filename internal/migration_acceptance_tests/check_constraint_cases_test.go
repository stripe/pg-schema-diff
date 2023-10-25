package migration_acceptance_tests

import (
	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var checkConstraintCases = []acceptanceTestCase{
	{
		name: "No-op",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT CHECK ( bar > id )
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT CHECK ( bar > id )
			);
			`,
		},
		vanillaExpectations: expectations{
			empty: true,
		},
		dataPackingExpectations: expectations{
			empty: true,
		},
	},
	{
		name: "Add check constraint",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT CHECK ( bar > id )
			);
			`,
		},
	},
	{
		name: "Add check constraint with UDF dependency should error",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar INT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE FUNCTION add(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN a + b;

			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar INT CHECK ( add(bar, id) > 0 )
			);
			`,
		},
		vanillaExpectations: expectations{
			planErrorIs: diff.ErrNotImplemented,
		},
		dataPackingExpectations: expectations{
			planErrorIs: diff.ErrNotImplemented,
		},
	},
	{
		name: "Add check constraint with system function dependency should not error",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar INT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar INT CHECK ( to_timestamp(id) <= CURRENT_TIMESTAMP )
			);
			`,
		},
	},
	{
		name: "Add multiple check constraints",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT,
				CHECK ( bar > id ), CHECK ( bar IS NOT NULL ), CHECK (bar > 0)
			);
			`,
		},
	},
	{
		name: "Add check constraints to new column",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255)
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT CHECK ( bar > id ), CHECK ( bar IS NOT NULL ), CHECK (bar > 0)
			);
			`,
		},
	},
	{
		name: "Add check constraint with quoted identifiers",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    "ID" INT PRIMARY KEY,
				foo VARCHAR(255),
				"Bar" BIGINT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    "ID" INT PRIMARY KEY,
				foo VARCHAR(255),
			   	"Bar" BIGINT
			);
			ALTER TABLE foobar ADD CONSTRAINT "BAR_CHECK" CHECK ( "Bar" < "ID" );
			`,
		},
	},
	{
		name: "Add no inherit check constraint",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT
			);
			ALTER TABLE foobar ADD CONSTRAINT bar_check CHECK ( bar > id ) NO INHERIT;
			`,
		},
	},
	{
		name: "Add No-Inherit, Not-Valid check constraint",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT
			);
			ALTER TABLE foobar ADD CONSTRAINT bar_check CHECK ( bar > id ) NO INHERIT NOT VALID;
			`,
		},
	},
	{
		name: "Drop check constraint",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT CHECK ( bar > id )
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT
			);
			`,
		},
	},
	{
		name: "Drop check constraint with quoted identifiers",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    "ID" INT PRIMARY KEY,
				foo VARCHAR(255),
				"Bar" BIGINT
			);
			ALTER TABLE foobar ADD CONSTRAINT "BAR_CHECK" CHECK ( "Bar" < "ID" );
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    "ID" INT PRIMARY KEY,
				foo VARCHAR(255),
			   	"Bar" BIGINT
			);
			`,
		},
	},
	{
		name: "Drop column with check constraints",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT,
				CHECK ( bar > id ), CHECK ( bar IS NOT NULL ), CHECK (bar > 0)
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255)
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeDeletesData},
	},
	{
		name: "Drop check constraint with UDF dependency should error",
		oldSchemaDDL: []string{
			`
			CREATE FUNCTION add(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN a + b;

			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar INT CHECK ( add(bar, id) > 0 )
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar INT
			);
			`,
		},
		vanillaExpectations: expectations{
			planErrorIs: diff.ErrNotImplemented,
		},
		dataPackingExpectations: expectations{
			planErrorIs: diff.ErrNotImplemented,
		},
	},
	{
		name: "Drop check constraint with system function dependency should not error",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar INT CHECK ( to_timestamp(id) <= CURRENT_TIMESTAMP )
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar INT
			);
			`,
		},
	},
	{
		name: "Alter an invalid check constraint to be valid",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT
			);
			ALTER TABLE foobar ADD CONSTRAINT bar_check CHECK ( bar > id ) NOT VALID;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT
			);
			ALTER TABLE foobar ADD CONSTRAINT bar_check CHECK ( bar > id );
			`,
		},
	},
	{
		name: "Alter a valid check constraint to be invalid",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT
			);
			ALTER TABLE foobar ADD CONSTRAINT bar_check CHECK ( bar > id );
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT
			);
			ALTER TABLE foobar ADD CONSTRAINT bar_check CHECK ( bar > id ) NOT VALID;
			`,
		},
	},
	{
		name: "Alter a no-Inherit check constraint to be Inheritable",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT
			);
			ALTER TABLE foobar ADD CONSTRAINT bar_check CHECK ( bar > id ) NO INHERIT;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT
			);
			ALTER TABLE foobar ADD CONSTRAINT bar_check CHECK ( bar > id );
			`,
		},
	},
	{
		name: "Alter an Inheritable check constraint to be no-inherit",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT
			);
			ALTER TABLE foobar ADD CONSTRAINT bar_check CHECK ( bar > id );
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT
			);
			ALTER TABLE foobar ADD CONSTRAINT bar_check CHECK ( bar > id ) NO INHERIT;
			`,
		},
	},
	{
		name: "Alter a check constraint expression",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT CHECK (bar > id)
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar BIGINT CHECK (bar < id)
			);
			`,
		},
	},
	{
		name: "Alter check constraint with UDF dependency should error",
		oldSchemaDDL: []string{
			`
			CREATE FUNCTION add(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN a + b;

			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar INT
			);
			ALTER TABLE foobar ADD CONSTRAINT some_constraint CHECK ( add(bar, id) > 0 ) NOT VALID;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE FUNCTION add(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN a + b;

			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar INT
			);
			ALTER TABLE foobar ADD CONSTRAINT some_constraint CHECK ( add(bar, id) > 0 );
			`,
		},
		vanillaExpectations: expectations{
			planErrorIs: diff.ErrNotImplemented,
		},
		dataPackingExpectations: expectations{
			planErrorIs: diff.ErrNotImplemented,
		},
	},
	{
		name: "Alter check constraint with system function dependency should not error",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar INT
			);
			ALTER TABLE foobar ADD CONSTRAINT some_constraint CHECK ( to_timestamp(id) <= CURRENT_TIMESTAMP ) NOT VALID;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255),
				bar INT
			);
			ALTER TABLE foobar ADD CONSTRAINT some_constraint CHECK ( to_timestamp(id) <= CURRENT_TIMESTAMP );
			`,
		},
	},
}

func (suite *acceptanceTestSuite) TestCheckConstraintAcceptanceTestCases() {
	suite.runTestCases(checkConstraintCases)
}
