package migration_acceptance_tests

import (
	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var tableAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "No-op",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY CHECK (id > 0), CHECK (id < buzz),
				foo VARCHAR(255) COLLATE "POSIX" DEFAULT '' NOT NULL,
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    fizz SERIAL NOT NULL,
				buzz REAL CHECK (buzz IS NOT NULL)
			);
			CREATE INDEX normal_idx ON foobar(fizz);
			CREATE UNIQUE INDEX unique_idx ON foobar(foo, bar);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY CHECK (id > 0), CHECK (id < buzz),
				foo VARCHAR(255) COLLATE "POSIX" DEFAULT '' NOT NULL,
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    fizz SERIAL NOT NULL,
				buzz REAL CHECK (buzz IS NOT NULL)
			);
			CREATE INDEX normal_idx ON foobar(fizz);
			CREATE UNIQUE INDEX unique_idx ON foobar(foo, bar);
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
		name:         "Create table",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY CHECK (id > 0), CHECK (id < buzz),
				foo VARCHAR(255) COLLATE "POSIX" DEFAULT '' NOT NULL,
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    fizz SERIAL NOT NULL,
				buzz REAL CHECK (buzz IS NOT NULL)
			);
			CREATE INDEX normal_idx ON foobar(fizz);
			CREATE UNIQUE INDEX unique_idx ON foobar(foo, bar);
			`,
		},
		dataPackingExpectations: expectations{
			outputState: []string{`
			CREATE TABLE foobar(
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    id INT PRIMARY KEY CHECK (id > 0), CHECK (id < buzz),
			    fizz SERIAL NOT NULL,
				buzz REAL CHECK (buzz IS NOT NULL),
				foo VARCHAR(255) COLLATE "POSIX" DEFAULT '' NOT NULL
			);
			CREATE INDEX normal_idx ON foobar(fizz);
			CREATE UNIQUE INDEX unique_idx ON foobar(foo, bar);
				`},
		},
	},
	{
		name:         "Create table with quoted names",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id INT PRIMARY KEY,
				"Foo" VARCHAR(255) COLLATE "POSIX" DEFAULT '' NOT NULL CHECK (LENGTH("Foo") > 0),
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    fizz SERIAL NOT NULL
			);
			CREATE INDEX normal_idx ON "Foobar" USING hash (fizz);
			CREATE UNIQUE INDEX unique_idx ON "Foobar"("Foo" DESC, bar);
			`,
		},
		dataPackingExpectations: expectations{
			outputState: []string{
				`
				CREATE TABLE "Foobar"(
					bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					id INT PRIMARY KEY,
					fizz SERIAL NOT NULL,
					"Foo" VARCHAR(255) COLLATE "POSIX" DEFAULT '' NOT NULL CHECK (LENGTH("Foo") > 0)
				);
				CREATE INDEX normal_idx ON "Foobar" USING hash (fizz);
				CREATE UNIQUE INDEX unique_idx ON "Foobar"("Foo" DESC, bar);
				`,
			},
		},
	},
	{
		name: "Drop table",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY CHECK (id > 0), CHECK (id < buzz),
				foo VARCHAR(255) COLLATE "C" DEFAULT '' NOT NULL,
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    fizz SERIAL NOT NULL,
				buzz REAL CHECK (buzz IS NOT NULL)
			);
			CREATE INDEX normal_idx ON foobar(fizz);
			CREATE UNIQUE INDEX unique_idx ON foobar(foo, bar);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
		newSchemaDDL: nil,
	},
	{
		name: "Drop a table with quoted names",
		oldSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id INT PRIMARY KEY,
				"Foo" VARCHAR(255) COLLATE "C" DEFAULT '' NOT NULL CHECK (LENGTH("Foo") > 0),
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    fizz SERIAL NOT NULL
			);
			CREATE INDEX normal_idx ON "Foobar"(fizz);
			CREATE UNIQUE INDEX unique_idx ON "Foobar"("Foo", "bar");
			`,
		},
		newSchemaDDL: nil,
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Alter table: New primary key, change column types, delete unique index, new index, validate check constraint",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY CHECK (id > 0), CHECK (id < buzz),
				foo VARCHAR(255) COLLATE "POSIX" DEFAULT '' NOT NULL,
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    fizz BOOLEAN NOT NULL,
				buzz REAL,
				fizzbuzz TEXT
			);
			ALTER TABLE foobar ADD CONSTRAINT buzz_check CHECK (buzz IS NOT NULL) NOT VALID;
			CREATE INDEX normal_idx ON foobar(fizz);
			CREATE UNIQUE INDEX unique_idx ON foobar(foo, bar);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT CHECK (id > 0),
				foo CHAR COLLATE "C" DEFAULT '5' NOT NULL PRIMARY KEY,
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL ,
			    fizz BOOLEAN NOT NULL,
			    buzz REAL,
				fizzbuzz TEXT COLLATE "POSIX"
			);
			ALTER TABLE foobar ADD CONSTRAINT buzz_check CHECK (buzz IS NOT NULL);
			CREATE INDEX normal_idx ON foobar(fizz);
			CREATE INDEX other_idx ON foobar(bar);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "Alter table: New column, new primary key, alter column to nullable, alter column types, drop column, drop index, drop check constraints",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY CHECK (id > 0), CHECK (id < buzz),
				foo VARCHAR(255) COLLATE "POSIX" DEFAULT '' NOT NULL,
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    fizz SERIAL NOT NULL,
			   	buzz REAL CHECK (buzz IS NOT NULL)
			);
			CREATE INDEX normal_idx ON foobar(fizz);
			CREATE UNIQUE INDEX unique_idx ON foobar(foo DESC, bar);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id SMALLSERIAL,
				foo CHAR DEFAULT '5',
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
			    new_fizz DECIMAL(65, 10) DEFAULT 5.25 NOT NULL PRIMARY KEY
			);
			CREATE INDEX other_idx ON foobar(bar);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
			diff.MigrationHazardTypeDeletesData,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Alter table: effectively drop by changing everything",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY CHECK (id > 0), CHECK (id < buzz),
				foo VARCHAR(255) COLLATE "POSIX" DEFAULT '' NOT NULL,
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    fizz SERIAL NOT NULL,
			    buzz REAL CHECK (buzz IS NOT NULL)
			);
			CREATE INDEX normal_idx ON foobar USING hash (fizz);
			CREATE UNIQUE INDEX unique_idx ON foobar(foo DESC, bar);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    new_id INT PRIMARY KEY CHECK (new_id > 0), CHECK (new_id < new_buzz),
				new_foo VARCHAR(255) COLLATE "POSIX" DEFAULT '' NOT NULL,
			    new_bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    new_fizz SERIAL NOT NULL,
				new_buzz REAL CHECK (new_buzz IS NOT NULL)
			);
			CREATE INDEX normal_idx ON foobar USING hash (new_fizz);
			CREATE UNIQUE INDEX unique_idx ON foobar(new_foo DESC, new_bar);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeDeletesData,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Alter table: translate BIGINT type to TIMESTAMP, set to not null, set default",
		oldSchemaDDL: []string{
			`
			CREATE TABLE alexrhee_testing(
			    id INT PRIMARY KEY,
			    obj_attr__c_time BIGINT,
				obj_attr__m_time BIGINT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE alexrhee_testing(
			    id INT PRIMARY KEY,
				obj_attr__c_time TIMESTAMP NOT NULL,
				obj_attr__m_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
}

func (suite *acceptanceTestSuite) TestTableAcceptanceTestCases() {
	suite.runTestCases(tableAcceptanceTestCases)
}
