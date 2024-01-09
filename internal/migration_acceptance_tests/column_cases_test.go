package migration_acceptance_tests

import (
	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var columnAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "No-op",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255) COLLATE "C" DEFAULT '' NOT NULL,
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    fizz BOOLEAN NOT NULL
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255) COLLATE "C" DEFAULT '' NOT NULL,
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    fizz BOOLEAN NOT NULL
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
		name: "Add one column with default",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				my_new_column VARCHAR(255) DEFAULT 'a'
			);
			`,
		},
	},
	{
		name: "Add one column with quoted names",
		oldSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id INT PRIMARY KEY
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id INT PRIMARY KEY,
				"My_new_column" VARCHAR(255) DEFAULT 'a'
			);
			`,
		},
	},
	{
		name: "Add one column with nullability",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				my_new_column VARCHAR(255) NOT NULL
			);
			`,
		},
	},
	{
		name: "Add one column with serial",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				my_new_column SERIAL NOT NULL
			);
			`,
		},
	},
	{
		name: "Add one column with all options",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				my_new_column VARCHAR(255) COLLATE "C" NOT NULL DEFAULT 'a'
			);
			`,
		},
	},
	{
		name: "Add one column and change ordering",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
				my_new_column VARCHAR(255) NOT NULL DEFAULT 'a',
			    id INT PRIMARY KEY
			);
			`,
		},
		vanillaExpectations: expectations{
			outputState: []string{`
					CREATE TABLE foobar(
						id INT PRIMARY KEY,
						my_new_column VARCHAR(255) NOT NULL DEFAULT 'a'
					)
				`},
		},
		dataPackingExpectations: expectations{
			outputState: []string{`
					CREATE TABLE foobar(
						id INT PRIMARY KEY,
						my_new_column VARCHAR(255) NOT NULL DEFAULT 'a'
					)
				`},
		},
	},
	{
		name: "Delete one column",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY
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
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeDeletesData,
			diff.MigrationHazardTypeIndexDropped,
		},
	},
	{
		name: "Delete one column with quoted name",
		oldSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    "Id" INT PRIMARY KEY
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeDeletesData,
			diff.MigrationHazardTypeIndexDropped,
		},
	},
	{
		name: "Delete one column with serial",
		oldSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id BIGSERIAL PRIMARY KEY
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeDeletesData,
			diff.MigrationHazardTypeIndexDropped,
		},
	},
	{
		name: "Delete column with valid not null check constraint",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foo VARCHAR(255) NOT NULL CHECK ( foo IS NOT NULL )
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Modify data type (int -> serial)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar INT NOT NULL
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar SERIAL NOT NULL
			);
			`,
		},
	},
	{
		name: "Modify data type (serial -> int)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar SERIAL NOT NULL
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar INT NOT NULL
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Modify data type (varchar -> TEXT) with compatible default",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) DEFAULT 'some default' NOT NULL
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar TEXT DEFAULT 'some default' NOT NULL
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "Modify data type and collation (varchar -> char)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) COLLATE "C" NOT NULL
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar CHAR COLLATE "POSIX" NOT NULL
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "Modify data type to incompatible (bytea -> char)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar bytea NOT NULL
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar CHAR NOT NULL
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "Modify collation (default -> non-default)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) COLLATE "C" NOT NULL
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) COLLATE "POSIX" NOT NULL
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "Modify collation (non-default -> non-default)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) NOT NULL
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) COLLATE "POSIX" NOT NULL
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "Add Default",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255)
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) DEFAULT ''
			);
			`,
		},
	},
	{
		name: "Remove Default",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) DEFAULT ''
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255)
			);
			`,
		},
	},
	{
		name: "Change Default",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) DEFAULT 'Something else'
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) DEFAULT ''
			);
			`,
		},
	},
	{
		name: "Set NOT NULL",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255)
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) NOT NULL
			);
			`,
		},
		ddl: []string{
			"ALTER TABLE \"public\".\"foobar\" ADD CONSTRAINT \"not_null_10111213-1415-4617-9819-1a1b1c1d1e1f\" CHECK(\"foobar\" IS NOT NULL) NOT VALID",
			"ALTER TABLE \"public\".\"foobar\" VALIDATE CONSTRAINT \"not_null_10111213-1415-4617-9819-1a1b1c1d1e1f\"",
			"ALTER TABLE \"public\".\"foobar\" ALTER COLUMN \"foobar\" SET NOT NULL",
			"ALTER TABLE \"public\".\"foobar\" DROP CONSTRAINT \"not_null_10111213-1415-4617-9819-1a1b1c1d1e1f\"",
		},
	},
	{
		name: "Set NOT NULL (add invalid CC)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255)
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) NOT NULL
			);
			ALTER TABLE foobar ADD CONSTRAINT foobar CHECK (foobar IS NOT NULL) NOT VALID;
			`,
		},
		ddl: []string{
			"ALTER TABLE \"public\".\"foobar\" ADD CONSTRAINT \"not_null_10111213-1415-4617-9819-1a1b1c1d1e1f\" CHECK(\"foobar\" IS NOT NULL) NOT VALID",
			"ALTER TABLE \"public\".\"foobar\" VALIDATE CONSTRAINT \"not_null_10111213-1415-4617-9819-1a1b1c1d1e1f\"",
			"ALTER TABLE \"public\".\"foobar\" ALTER COLUMN \"foobar\" SET NOT NULL",
			"ALTER TABLE \"public\".\"foobar\" ADD CONSTRAINT \"foobar\" CHECK((foobar IS NOT NULL)) NOT VALID",
			"ALTER TABLE \"public\".\"foobar\" DROP CONSTRAINT \"not_null_10111213-1415-4617-9819-1a1b1c1d1e1f\"",
		},
	},

	{
		name: "Set NOT NULL (invalid CC already exists)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255)
			);
			ALTER TABLE foobar ADD CONSTRAINT foobar CHECK (foobar IS NOT NULL) NOT VALID;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) NOT NULL
			);
			ALTER TABLE foobar ADD CONSTRAINT foobar CHECK (foobar IS NOT NULL) NOT VALID;
			`,
		},
		ddl: []string{
			"ALTER TABLE \"public\".\"foobar\" ADD CONSTRAINT \"not_null_10111213-1415-4617-9819-1a1b1c1d1e1f\" CHECK(\"foobar\" IS NOT NULL) NOT VALID",
			"ALTER TABLE \"public\".\"foobar\" VALIDATE CONSTRAINT \"not_null_10111213-1415-4617-9819-1a1b1c1d1e1f\"",
			"ALTER TABLE \"public\".\"foobar\" ALTER COLUMN \"foobar\" SET NOT NULL",
			"ALTER TABLE \"public\".\"foobar\" DROP CONSTRAINT \"not_null_10111213-1415-4617-9819-1a1b1c1d1e1f\"",
		},
	},
	{
		name: "Set NOT NULL (invalid to valid CC)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255)
			);
			ALTER TABLE foobar ADD CONSTRAINT foobar CHECK (foobar IS NOT NULL) NOT VALID;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) NOT NULL
			);
			ALTER TABLE foobar ADD CONSTRAINT foobar CHECK (foobar IS NOT NULL);
			`,
		},
		ddl: []string{
			"ALTER TABLE \"public\".\"foobar\" VALIDATE CONSTRAINT \"foobar\"",
			"ALTER TABLE \"public\".\"foobar\" ALTER COLUMN \"foobar\" SET NOT NULL",
		},
	},
	{
		name: "Set NOT NULL (add valid CC)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255)
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) NOT NULL
			);
			ALTER TABLE foobar ADD CONSTRAINT foobar CHECK (foobar IS NOT NULL);
			`,
		},
		ddl: []string{
			"ALTER TABLE \"public\".\"foobar\" ADD CONSTRAINT \"foobar\" CHECK((foobar IS NOT NULL)) NOT VALID",
			"ALTER TABLE \"public\".\"foobar\" VALIDATE CONSTRAINT \"foobar\"",
			"ALTER TABLE \"public\".\"foobar\" ALTER COLUMN \"foobar\" SET NOT NULL",
		},
	},
	{
		name: "Set NOT NULL (valid CC already exists)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255)
			);
			ALTER TABLE foobar ADD CONSTRAINT foobar CHECK (foobar IS NOT NULL);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) NOT NULL
			);
			ALTER TABLE foobar ADD CONSTRAINT foobar CHECK (foobar IS NOT NULL);
			`,
		},
		ddl: []string{
			"ALTER TABLE \"public\".\"foobar\" ALTER COLUMN \"foobar\" SET NOT NULL",
		},
	},
	{
		name: "Set NOT NULL (dropping valid CC)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255)
			);
			ALTER TABLE foobar ADD CONSTRAINT foobar CHECK (foobar IS NOT NULL);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) NOT NULL
			);
			`,
		},
		ddl: []string{
			"ALTER TABLE \"public\".\"foobar\" ALTER COLUMN \"foobar\" SET NOT NULL",
			"ALTER TABLE \"public\".\"foobar\" DROP CONSTRAINT \"foobar\"",
		},
	},
	{
		name: "Set NOT NULL (dropping valid CC via recreation)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255)
			);
			ALTER TABLE foobar ADD CONSTRAINT foobar CHECK (foobar IS NOT NULL);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) NOT NULL
			);
			ALTER TABLE foobar ADD CONSTRAINT foobar CHECK (LENGTH(foobar) > 0);
			`,
		},
		ddl: []string{
			"ALTER TABLE \"public\".\"foobar\" ALTER COLUMN \"foobar\" SET NOT NULL",
			"ALTER TABLE \"public\".\"foobar\" DROP CONSTRAINT \"foobar\"",
			"ALTER TABLE \"public\".\"foobar\" ADD CONSTRAINT \"foobar\" CHECK((length((foobar)::text) > 0)) NOT VALID",
			"ALTER TABLE \"public\".\"foobar\" VALIDATE CONSTRAINT \"foobar\"",
		},
	},
	{
		name: "Set NOT NULL (data type change with additional CC)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255)
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar INT NOT NULL CHECK (foobar > 0)
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
		ddl: []string{
			"ALTER TABLE \"public\".\"foobar\" ADD CONSTRAINT \"not_null_10111213-1415-4617-9819-1a1b1c1d1e1f\" CHECK(\"foobar\" IS NOT NULL) NOT VALID",
			"ALTER TABLE \"public\".\"foobar\" VALIDATE CONSTRAINT \"not_null_10111213-1415-4617-9819-1a1b1c1d1e1f\"",
			"ALTER TABLE \"public\".\"foobar\" ALTER COLUMN \"foobar\" SET NOT NULL",
			"ALTER TABLE \"public\".\"foobar\" ALTER COLUMN \"foobar\" SET DATA TYPE integer using \"foobar\"::integer",
			"ANALYZE \"foobar\" (\"foobar\")",
			"ALTER TABLE \"public\".\"foobar\" ADD CONSTRAINT \"foobar_foobar_check\" CHECK((foobar > 0)) NOT VALID",
			"ALTER TABLE \"public\".\"foobar\" VALIDATE CONSTRAINT \"foobar_foobar_check\"",
			"ALTER TABLE \"public\".\"foobar\" DROP CONSTRAINT \"not_null_10111213-1415-4617-9819-1a1b1c1d1e1f\"",
		},
	},
	// TODO(bplunkett) Add not null migration where valid cc is being dropped
	{
		name: "Remove NOT NULL",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) NOT NULL
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255)
			);
			`,
		},
	},
	{
		name: "Add default and change data type (new default is incompatible with old type)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar INT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) DEFAULT 'SOMETHING'
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "Change default and data type (new default is incompatible with old type)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar INT DEFAULT 0
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar VARCHAR(255) DEFAULT 'SOMETHING'
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "Change default and data type (old default is incompatible with new type)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar TEXT DEFAULT 'SOMETHING'
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar INT DEFAULT 8
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
		vanillaExpectations: expectations{
			planErrorContains: errValidatingPlan.Error(),
		},
		dataPackingExpectations: expectations{
			planErrorContains: errValidatingPlan.Error(),
		},
	},
	{
		name: "Change from NULL default to no default and NOT NULL",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar INT DEFAULT NULL
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar INT NOT NULL
			);
			`,
		},
	},
	{
		name: "Change from NOT NULL to no NULL default",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar INT NOT NULL
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar INT DEFAULT NULL
			);
			`,
		},
	},
	{
		name: "Change data type and to nullable",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar INT NOT NULL
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar SMALLINT NULL
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "Change data type and to not nullable",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar INT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar SMALLINT NOT NULL
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "Change data type, nullability (NOT NULL), and default",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar TEXT DEFAULT 'some default'
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar CHAR NOT NULL DEFAULT 'A'
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "Change data type and collation, nullability (NOT NULL), and default with quoted names",
		oldSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id INT PRIMARY KEY,
				"Foobar" TEXT DEFAULT 'some default'
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id INT PRIMARY KEY,
				"Foobar" CHAR COLLATE "POSIX" NOT NULL DEFAULT 'A'
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "Change BIGINT to TIMESTAMP, nullability (NOT NULL), and default with current_timestamp",
		oldSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id INT PRIMARY KEY,
				some_time_col BIGINT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id INT PRIMARY KEY,
				some_time_col TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
}

func (suite *acceptanceTestSuite) TestColumnAcceptanceTestCases() {
	suite.runTestCases(columnAcceptanceTestCases)
}
