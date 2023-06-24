package migration_acceptance_tests

import "github.com/stripe/pg-schema-diff/pkg/diff"

var sequenceAcceptanceTests = []acceptanceTestCase{
	// Write test case for ownership change between two separate tables and from none to something

	// write acceptance test cases for sequences
	// Write noop test case
	{
		name: "No-op",
		oldSchemaDDL: []string{
			`
			CREATE SEQUENCE foobar_sequence
					AS BIGINT
					INCREMENT BY 2
					MINVALUE 5 MAXVALUE 100
					START WITH 10 CACHE 5 CYCLE
					OWNED BY NONE;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SEQUENCE foobar_sequence
					AS BIGINT
					INCREMENT BY 2
					MINVALUE 5 MAXVALUE 100
					START WITH 10 CACHE 5 CYCLE
					OWNED BY NONE;
			`,
		},
	},
	{
		name: "Add sequence",
		newSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
					AS BIGINT
					INCREMENT BY 2
					MINVALUE 5 MAXVALUE 100
					START WITH 10 CACHE 5 CYCLE
					OWNED BY NONE;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeHasUntrackableDependencies,
		},
	},
	{
		name: "Add sequence via serial",
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    "some id" SERIAL
			)
			`,
		},
	},
	{
		name: "Drop sequence",
		oldSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
					AS BIGINT
					INCREMENT BY 2
					MINVALUE 5 MAXVALUE 100
					START WITH 10 CACHE 5 CYCLE
					OWNED BY NONE;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeHasUntrackableDependencies,
		},
	},
	{
		name: "Drop sequence via deleting column",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    "some id" SERIAL
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Drop sequence via deleting column (partitioned table)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    "some id" SERIAL,
				type TEXT
			) PARTITION BY LIST (type);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN (1);
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN (2);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
				type TEXT
			) PARTITION BY LIST (type);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN (1);
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN (2);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Drop sequence via changing column type",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    "some id" SERIAL
			)
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    "some id" TEXT
			)
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "Drop sequence via changing column type (partitioned)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    "some id" SERIAL,
				type TEXT
			) PARTITION BY LIST (type);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN (1);
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN (2);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    "some id" TEXT,
				type TEXT
			) PARTITION BY LIST (type);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN (1);
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN (2);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "Drop sequence via table drop",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id SERIAL
			)
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Drop sequence via table drop (partitioned)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    "some id" SERIAL,
				type TEXT
			) PARTITION BY LIST (type);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN (1);
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN (2);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Alter ownership (from none to table)",
		oldSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
						AS BIGINT
						INCREMENT BY 2
						MINVALUE 5 MAXVALUE 100
						START WITH 10 CACHE 5 CYCLE
						OWNED BY NONE;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
						AS BIGINT
						INCREMENT BY 2
						MINVALUE 5 MAXVALUE 100
						START WITH 10 CACHE 5 CYCLE
						OWNED BY NONE;
			CREATE TABLE "some foobar"(
				"some id" BIGINT
			);
			ALTER SEQUENCE "foobar sequence" OWNED BY "some foobar"."some id";
			`,
		},
	},
	{
		name: "Alter ownership (from table to none)",
		oldSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
						AS BIGINT
						INCREMENT BY 2
						MINVALUE 5 MAXVALUE 100
						START WITH 10 CACHE 5 CYCLE
						OWNED BY NONE;
			CREATE TABLE "some foobar"(
				"some id" BIGINT
			);
			ALTER SEQUENCE "foobar sequence" OWNED BY "some foobar"."some id";
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
						AS BIGINT
						INCREMENT BY 2
						MINVALUE 5 MAXVALUE 100
						START WITH 10 CACHE 5 CYCLE
						OWNED BY NONE;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Alter ownership (from table to table)",
		oldSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
						AS BIGINT
						INCREMENT BY 2
						MINVALUE 5 MAXVALUE 100
						START WITH 10 CACHE 5 CYCLE
						OWNED BY NONE;
			CREATE TABLE "some foobar"(
				"some id" BIGINT
			);
			ALTER SEQUENCE "foobar sequence" OWNED BY "some foobar"."some id";
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
						AS BIGINT
						INCREMENT BY 2
						MINVALUE 5 MAXVALUE 100
						START WITH 10 CACHE 5 CYCLE
						OWNED BY NONE;
			CREATE TABLE "some foobar"(
				"some id" BIGINT
			);
			CREATE TABLE "some other foobar"(
				"some id" BIGINT
			);
			ALTER SEQUENCE "foobar sequence" OWNED BY "some other foobar"."some id";
			`,
		},
	},
	{
		name: "Alter ownership (from table to table; original column dropped)",
		oldSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
						AS BIGINT
						INCREMENT BY 2
						MINVALUE 5 MAXVALUE 100
						START WITH 10 CACHE 5 CYCLE
						OWNED BY NONE;
			CREATE TABLE "some foobar"(
				"some id" BIGINT
			);
			ALTER SEQUENCE "foobar sequence" OWNED BY "some foobar"."some id";
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
						AS BIGINT
						INCREMENT BY 2
						MINVALUE 5 MAXVALUE 100
						START WITH 10 CACHE 5 CYCLE
						OWNED BY NONE;
			CREATE TABLE "some foobar"(
			);
			CREATE TABLE "some other foobar"(
				"some id" BIGINT
			);
			ALTER SEQUENCE "foobar sequence" OWNED BY "some other foobar"."some id";
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Alter ownership (from table to table; original table dropped)",
		oldSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
						AS BIGINT
						INCREMENT BY 2
						MINVALUE 5 MAXVALUE 100
						START WITH 10 CACHE 5 CYCLE
						OWNED BY NONE;
			CREATE TABLE "some foobar"(
				"some id" BIGINT
			);
			ALTER SEQUENCE "foobar sequence" OWNED BY "some foobar"."some id";
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
						AS BIGINT
						INCREMENT BY 2
						MINVALUE 5 MAXVALUE 100
						START WITH 10 CACHE 5 CYCLE
						OWNED BY NONE;
			CREATE TABLE "some other foobar"(
				"some id" BIGINT
			);
			ALTER SEQUENCE "foobar sequence" OWNED BY "some other foobar"."some id";
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Alter ownership (from table to table; original partitioned table dropped)",
		oldSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
						AS BIGINT
						INCREMENT BY 2
						MINVALUE 5 MAXVALUE 100
						START WITH 10 CACHE 5 CYCLE
						OWNED BY NONE;

			CREATE TABLE foobar(
			    "some id" SERIAL,
				type TEXT
			) PARTITION BY LIST (type);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN (1);
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN (2);

			ALTER SEQUENCE "foobar sequence" OWNED BY foobar."some id";
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
						AS BIGINT
						INCREMENT BY 2
						MINVALUE 5 MAXVALUE 100
						START WITH 10 CACHE 5 CYCLE
						OWNED BY NONE;

			CREATE TABLE some_other_foobar(
			    "some id" SERIAL,
				type TEXT
			) PARTITION BY LIST (type);

			CREATE TABLE some_other_foobar_1 PARTITION OF some_other_foobar FOR VALUES IN (1);
			CREATE TABLE some_other_foobar_2 PARTITION OF some_other_foobar FOR VALUES IN (2);

			ALTER SEQUENCE "foobar sequence" OWNED BY some_other_foobar."some id";
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Alter ownership data type",
		oldSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
						AS INT
						INCREMENT BY 2
						MINVALUE 5 MAXVALUE 100
						START WITH 10 CACHE 5 CYCLE
						OWNED BY NONE;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SEQUENCE "foobar sequence"
						AS BIGINT
						INCREMENT BY 2
						MINVALUE 5 MAXVALUE 100
						START WITH 10 CACHE 5 CYCLE
						OWNED BY NONE;
			`,
		},
		vanillaExpectations: expectations{
			planErrorIs: diff.ErrNotImplemented,
		},
		dataPackingExpectations: expectations{
			planErrorIs: diff.ErrNotImplemented,
		},
	},
}

func (suite *acceptanceTestSuite) TestSequenceAcceptanceTestCases() {
	suite.runTestCases(sequenceAcceptanceTests)
}
