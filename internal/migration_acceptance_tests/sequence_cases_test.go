package migration_acceptance_tests

import "github.com/stripe/pg-schema-diff/pkg/diff"

var sequenceAcceptanceTests = []acceptanceTestCase{
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
            CREATE SCHEMA schema_1;
            CREATE TABLE schema_1.foobar(
                "some id" SERIAL
            )
			`,
		},
	},
	{
		name: "Drop sequence",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE SEQUENCE schema_1."foobar sequence"
                    AS BIGINT
                    INCREMENT BY 2
                    MINVALUE 5 MAXVALUE 100
                    START WITH 10 CACHE 5 CYCLE
                    OWNED BY NONE;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
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
			diff.MigrationHazardTypeDeletesData,
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
			diff.MigrationHazardTypeDeletesData,
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
		name: "And and Drop sequences (conflicting schemas)",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE SEQUENCE schema_1."foobar sequence"
                    AS BIGINT
                    INCREMENT BY 2
                    MINVALUE 5 MAXVALUE 100
                    START WITH 10 CACHE 5 CYCLE
                    OWNED BY NONE;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_2;
            CREATE SEQUENCE schema_2."foobar sequence"
                    AS BIGINT
                    INCREMENT BY 2
                    MINVALUE 5 MAXVALUE 100
                    START WITH 10 CACHE 5 CYCLE
                    OWNED BY NONE;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
			diff.MigrationHazardTypeHasUntrackableDependencies,
		},
	},
	{
		name: "Alter data type",
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
	},
	{
		name: "Alter increment",
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
                        AS INT
                        INCREMENT BY 3
                        MINVALUE 5 MAXVALUE 100
                        START WITH 10 CACHE 5 CYCLE
                        OWNED BY NONE;
			`,
		},
	},
	{
		name: "Alter min value",
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
                        AS INT
                        INCREMENT BY 2
                        MINVALUE 6 MAXVALUE 100
                        START WITH 10 CACHE 5 CYCLE
                        OWNED BY NONE;
			`,
		},
	},
	{
		name: "Alter from no min value and change type",
		oldSchemaDDL: []string{
			`
            CREATE SEQUENCE "foobar sequence"
                        AS BIGINT
                        INCREMENT BY 2
                        NO MINVALUE MAXVALUE 100
                        START WITH 10 CACHE 5 CYCLE
                        OWNED BY NONE;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SEQUENCE "foobar sequence"
                        AS INT
                        INCREMENT BY 2
                        MINVALUE 5 MAXVALUE 100
                        START WITH 10 CACHE 5 CYCLE
                        OWNED BY NONE;
			`,
		},
	},
	{
		name: "Alter to no min value and change type",
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
                        AS INT
                        INCREMENT BY 2
                        NO MINVALUE MAXVALUE 100
                        START WITH 10 CACHE 5 CYCLE
                        OWNED BY NONE;
			`,
		},
	},
	{
		name: "Alter max value",
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
                        AS INT
                        INCREMENT BY 2
                        MINVALUE 5 MAXVALUE 101
                        START WITH 10 CACHE 5 CYCLE
                        OWNED BY NONE;
			`,
		},
	},
	{
		name: "Alter from no max value and change type",
		oldSchemaDDL: []string{
			`
            CREATE SEQUENCE "foobar sequence"
                        AS BIGINT
                        INCREMENT BY 2
                        MINVALUE 5 NO MAXVALUE
                        START WITH 10 CACHE 5 CYCLE
                        OWNED BY NONE;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SEQUENCE "foobar sequence"
                        AS INT
                        INCREMENT BY 2
                        MINVALUE 5 MAXVALUE 100
                        START WITH 10 CACHE 5 CYCLE
                        OWNED BY NONE;
			`,
		},
	},
	{
		name: "Alter to no max value and change type",
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
                        AS INT
                        INCREMENT BY 2
                        MINVALUE 5 NO MAXVALUE
                        START WITH 10 CACHE 5 CYCLE
                        OWNED BY NONE;
			`,
		},
	},
	{
		name: "Alter start with",
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
                        AS INT
                        INCREMENT BY 2
                        MINVALUE 5 MAXVALUE 100
                        START WITH 11 CACHE 5 CYCLE
                        OWNED BY NONE;
			`,
		},
	},
	{
		name: "Alter cache",
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
                        AS INT
                        INCREMENT BY 2
                        MINVALUE 5 MAXVALUE 100
                        START WITH 10 CACHE 6 CYCLE
                        OWNED BY NONE;
			`,
		},
	},
	{
		name: "Alter cycle",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE SEQUENCE schema_1."foobar sequence"
                        AS INT
                        INCREMENT BY 2
                        MINVALUE 5 MAXVALUE 100
                        START WITH 10 CACHE 5 CYCLE
                        OWNED BY NONE;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE SEQUENCE schema_1."foobar sequence"
                        AS INT
                        INCREMENT BY 2
                        MINVALUE 5 MAXVALUE 100
                        START WITH 10 CACHE 5 NO CYCLE
                        OWNED BY NONE;
			`,
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
		name: "Alter ownership (from table to table) and sequence properties (new type is not compatible with old table)",
		oldSchemaDDL: []string{
			`
            CREATE SEQUENCE "foobar sequence"
                        AS INT
                        INCREMENT BY 2
                        MINVALUE 5 MAXVALUE 100
                        START WITH 10 CACHE 5 CYCLE
                        OWNED BY NONE;
            CREATE TABLE "some foobar"(
                "some id" INT
            );
            ALTER SEQUENCE "foobar sequence" OWNED BY "some foobar"."some id";
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SEQUENCE "foobar sequence"
                        AS BIGINT
                        INCREMENT BY 3
                        MINVALUE 6 MAXVALUE 101
                        START WITH 11 CACHE 6 NO CYCLE
                        OWNED BY NONE;
            CREATE TABLE "some foobar"(
                "some id" INT
            );
            CREATE TABLE "some other foobar"(
                "some id" BIGINT
            );
            ALTER SEQUENCE "foobar sequence" OWNED BY "some other foobar"."some id";
			`,
		},
	},
	{
		name: "Alter ownership (from table to table) and sequence properties (old type is not compatible with new table)",
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
                        AS INT
                        INCREMENT BY 3
                        MINVALUE 6 MAXVALUE 101
                        START WITH 11 CACHE 6 NO CYCLE
                        OWNED BY NONE;
            CREATE TABLE "some foobar"(
                "some id" BIGINT
            );
            CREATE TABLE "some other foobar"(
                "some id" INT
            );
            ALTER SEQUENCE "foobar sequence" OWNED BY "some other foobar"."some id";
			`,
		},
	},
}

func (suite *acceptanceTestSuite) TestSequenceTestCases() {
	suite.runTestCases(sequenceAcceptanceTests)
}
