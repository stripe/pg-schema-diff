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
			    fizz SERIAL NOT NULL UNIQUE ,
				buzz REAL CHECK (buzz IS NOT NULL)
			);
			ALTER TABLE foobar REPLICA IDENTITY FULL;
			ALTER TABLE foobar ENABLE ROW LEVEL SECURITY;
			ALTER TABLE foobar FORCE ROW LEVEL SECURITY;
			CREATE INDEX normal_idx ON foobar(fizz);
			CREATE UNIQUE INDEX unique_idx ON foobar(foo, bar);

			CREATE TABLE foobar_fk(
			    bar TIMESTAMP,
			    foo VARCHAR(255)
			);
			CREATE UNIQUE INDEX foobar_fk_unique_idx ON foobar_fk(foo, bar);
			-- create a circular dependency of foreign keys (this is allowed)
			ALTER TABLE foobar_fk ADD CONSTRAINT foobar_fk_fk FOREIGN KEY (foo, bar) REFERENCES foobar(foo, bar);
			ALTER TABLE foobar ADD CONSTRAINT foobar_fk FOREIGN KEY (foo, bar) REFERENCES foobar_fk(foo, bar);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY CHECK (id > 0), CHECK (id < buzz),
				foo VARCHAR(255) COLLATE "POSIX" DEFAULT '' NOT NULL,
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    fizz SERIAL NOT NULL UNIQUE,
				buzz REAL CHECK (buzz IS NOT NULL)
			);
			ALTER TABLE foobar REPLICA IDENTITY FULL;
			ALTER TABLE foobar ENABLE ROW LEVEL SECURITY;
			ALTER TABLE foobar FORCE ROW LEVEL SECURITY;
			CREATE INDEX normal_idx ON foobar(fizz);
			CREATE UNIQUE INDEX unique_idx ON foobar(foo, bar);

			CREATE TABLE foobar_fk(
			    bar TIMESTAMP,
			    foo VARCHAR(255)
			);
			CREATE UNIQUE INDEX foobar_fk_unique_idx ON foobar_fk(foo, bar);
			-- create a circular dependency of foreign keys (this is allowed)
			ALTER TABLE foobar_fk ADD CONSTRAINT foobar_fk_fk FOREIGN KEY (foo, bar) REFERENCES foobar(foo, bar);
			ALTER TABLE foobar ADD CONSTRAINT foobar_fk FOREIGN KEY (foo, bar) REFERENCES foobar_fk(foo, bar);
			`,
		},
		expectations: expectations{
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
			    fizz SERIAL NOT NULL UNIQUE,
				buzz REAL CHECK (buzz IS NOT NULL)
			);
			ALTER TABLE foobar REPLICA IDENTITY FULL;
			ALTER TABLE foobar ENABLE ROW LEVEL SECURITY;
			ALTER TABLE foobar FORCE ROW LEVEL SECURITY;
			CREATE INDEX normal_idx ON foobar(fizz);
			CREATE UNIQUE INDEX foobar_unique_idx ON foobar(foo, bar);

			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar_fk(
			    bar TIMESTAMP,
			    foo VARCHAR(255)
			);
			CREATE UNIQUE INDEX foobar_fk_unique_idx ON schema_1.foobar_fk(foo, bar);
			-- create a circular dependency of foreign keys (this is allowed)
			ALTER TABLE schema_1.foobar_fk ADD CONSTRAINT foobar_fk_fk FOREIGN KEY (foo, bar) REFERENCES foobar(foo, bar);
			ALTER TABLE foobar ADD CONSTRAINT foobar_fk FOREIGN KEY (foo, bar) REFERENCES schema_1.foobar_fk(foo, bar);
			`,
		},
	},
	{
		name:         "Create table with RLS enabled",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
				bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				id INT PRIMARY KEY,
				fizz SERIAL NOT NULL,
				"Foo" VARCHAR(255) COLLATE "POSIX" DEFAULT '' NOT NULL CHECK (LENGTH("Foo") > 0)
			);
			ALTER TABLE "Foobar" ENABLE ROW LEVEL SECURITY;
			CREATE INDEX normal_idx ON "Foobar" USING hash (fizz);
			CREATE UNIQUE INDEX unique_idx ON "Foobar"("Foo" DESC, bar);
			`,
		},
	},
	{
		name:         "Create table with force RLS enabled",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
				bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				id INT PRIMARY KEY,
				fizz SERIAL NOT NULL,
				"Foo" VARCHAR(255) COLLATE "POSIX" DEFAULT '' NOT NULL CHECK (LENGTH("Foo") > 0)
			);
			ALTER TABLE "Foobar" FORCE ROW LEVEL SECURITY;
			CREATE INDEX normal_idx ON "Foobar" USING hash (fizz);
			CREATE UNIQUE INDEX unique_idx ON "Foobar"("Foo" DESC, bar);
			`,
		},
	},
	{
		name:         "Create table with index replica identity",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
			    foobar TEXT NOT NULL
			);
			CREATE UNIQUE INDEX some_idx ON foobar(foobar);
			ALTER TABLE foobar REPLICA IDENTITY USING INDEX some_idx;
			`,
		},
		expectations: expectations{
			planErrorIs: diff.ErrNotImplemented,
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
			    fizz SERIAL NOT NULL UNIQUE,
				buzz REAL CHECK (buzz IS NOT NULL)
			);
			CREATE INDEX normal_idx ON foobar(fizz);
			CREATE UNIQUE INDEX foobar_unique_idx ON foobar(foo, bar);
			
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar_fk(
			    bar TIMESTAMP,
			    foo VARCHAR(255)
			);
			CREATE UNIQUE INDEX foobar_fk_unique_idx ON schema_1.foobar_fk(foo, bar);
			-- create a circular dependency of foreign keys (this is allowed)
			ALTER TABLE schema_1.foobar_fk ADD CONSTRAINT foobar_fk_fk FOREIGN KEY (foo, bar) REFERENCES foobar(foo, bar);
			ALTER TABLE foobar ADD CONSTRAINT foobar_fk FOREIGN KEY (foo, bar) REFERENCES schema_1.foobar_fk(foo, bar);
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
		name: "Add and drop a table (conflicting schemas)",
		oldSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1."Foobar"(
			    id INT PRIMARY KEY
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SCHEMA schema_2;
			CREATE TABLE schema_2."Foobar"(
			    id INT PRIMARY KEY
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Alter replica identity",
		oldSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1."Foobar"(
			    id INT PRIMARY KEY
			);
			ALTER TABLE schema_1."Foobar" REPLICA IDENTITY FULL;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1."Foobar"(
			    id INT PRIMARY KEY
			);
			ALTER TABLE schema_1."Foobar" REPLICA IDENTITY DEFAULT;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeCorrectness,
		},
	},
	{
		name: "Alter replica identity to index replica identity",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY
			);
			ALTER TABLE foobar REPLICA IDENTITY FULL;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
				foobar TEXT NOT NULL
			);
			CREATE UNIQUE INDEX some_idx ON foobar(foobar);
			ALTER TABLE foobar REPLICA IDENTITY USING INDEX some_idx;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeCorrectness,
		},
		expectations: expectations{
			planErrorIs: diff.ErrNotImplemented,
		},
	},
	{
		name: "Enable RLS",
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
			    id INT PRIMARY KEY
			);
			ALTER TABLE foobar ENABLE ROW LEVEL SECURITY;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Disable RLS",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY
			);
			ALTER TABLE foobar ENABLE ROW LEVEL SECURITY;
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
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Force RLS",
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
			    id INT PRIMARY KEY
			);
			ALTER TABLE foobar FORCE ROW LEVEL SECURITY;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Unforce RLS",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY
			);
			ALTER TABLE foobar FORCE ROW LEVEL SECURITY;
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
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Alter table: New primary key, drop unique constraint, new unique constraint, change column types, delete unique index, delete FK's, new index, validate check constraint",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY CHECK (id > 0), CHECK (id < buzz),
				foo VARCHAR(255) COLLATE "POSIX" UNIQUE DEFAULT '' NOT NULL,
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    fizz BOOLEAN NOT NULL,
				buzz REAL,
				fizzbuzz TEXT
			);
			ALTER TABLE foobar ADD CONSTRAINT buzz_check CHECK (buzz IS NOT NULL) NOT VALID;
			CREATE INDEX normal_idx ON foobar(fizz);
			CREATE UNIQUE INDEX foobar_unique_idx ON foobar(foo, bar);

			CREATE TABLE foobar_fk(
			    bar TIMESTAMP,
			    foo VARCHAR(255)
			);
			CREATE UNIQUE INDEX foobar_fk_unique_idx ON foobar_fk(foo, bar);
			-- create a circular dependency of foreign keys (this is allowed)
			ALTER TABLE foobar_fk ADD CONSTRAINT foobar_fk_fk FOREIGN KEY (foo, bar) REFERENCES foobar(foo, bar);
			ALTER TABLE foobar ADD CONSTRAINT foobar_fk FOREIGN KEY (foo, bar) REFERENCES foobar_fk(foo, bar);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT CHECK (id > 0) UNIQUE,
				foo CHAR COLLATE "C" DEFAULT '5' NOT NULL PRIMARY KEY,
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL ,
			    fizz BOOLEAN NOT NULL,
			    buzz REAL,
				fizzbuzz TEXT COLLATE "POSIX"
			);
			ALTER TABLE foobar ADD CONSTRAINT buzz_check CHECK (buzz IS NOT NULL);
			CREATE INDEX normal_idx ON foobar(fizz);
			CREATE INDEX other_idx ON foobar(bar);

			CREATE TABLE foobar_fk(
			    bar TIMESTAMP,
			    foo VARCHAR(255)
			);
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
		name: "Alter table: New column, new primary key, new FK, drop FK, alter column to nullable, alter column types, drop column, drop index, drop check constraints, alter policies",
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

			CREATE POLICY foobar_policy ON foobar 
				AS PERMISSIVE 
				FOR INSERT 
				TO PUBLIC
				WITH CHECK (fizz > 0);

			CREATE POLICY some_policy_to_drop ON foobar
				AS RESTRICTIVE
				FOR SELECT
				TO PUBLIC
				USING (bar = CURRENT_TIMESTAMP AND fizz * 2 > 0);	


			CREATE TABLE foobar_fk(
			    bar TIMESTAMP,
			    foo VARCHAR(255)
			);
			-- create a circular dependency of foreign keys (this is allowed)
			ALTER TABLE foobar_fk ADD CONSTRAINT foobar_fk_fk FOREIGN KEY (foo, bar) REFERENCES foobar(foo, bar);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id SMALLSERIAL,
				foo CHAR DEFAULT '5',
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
			    new_fizz DECIMAL(65, 10) DEFAULT 5.25 NOT NULL PRIMARY KEY UNIQUE
			);

			CREATE INDEX other_idx ON foobar(bar);

			CREATE POLICY foobar_policy ON foobar 
				AS PERMISSIVE 
				FOR INSERT 
				TO PUBLIC
				WITH CHECK (new_fizz = 5.25);

			CREATE TABLE foobar_fk(
			    bar TIMESTAMP,
			    foo CHAR
			);
			CREATE UNIQUE INDEX foobar_fk_unique_idx ON foobar_fk(foo, bar);
			-- create a circular dependency of foreign keys (this is allowed)
			ALTER TABLE foobar ADD CONSTRAINT foobar_fk FOREIGN KEY (foo, bar) REFERENCES foobar_fk(foo, bar);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeAcquiresShareRowExclusiveLock,
			diff.MigrationHazardTypeAuthzUpdate,
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
			    fizz SERIAL NOT NULL UNIQUE,
			    buzz REAL CHECK (buzz IS NOT NULL)
			);
			CREATE INDEX normal_idx ON foobar USING hash (fizz);
			CREATE UNIQUE INDEX foobar_unique_idx ON foobar(foo, bar);

			CREATE POLICY foobar_policy ON foobar 
				AS PERMISSIVE 
				FOR INSERT 
				TO PUBLIC
				WITH CHECK (id > 1 AND foo = 'value');

			CREATE TABLE foobar_fk(
			    bar TIMESTAMP,
			    foo VARCHAR(255)
			);
			CREATE UNIQUE INDEX foobar_fk_unique_idx ON foobar_fk(foo, bar);
			-- create a circular dependency of foreign keys (this is allowed)
			ALTER TABLE foobar_fk ADD CONSTRAINT foobar_fk_fk FOREIGN KEY (foo, bar) REFERENCES foobar(foo, bar);
			ALTER TABLE foobar ADD CONSTRAINT foobar_fk FOREIGN KEY (foo, bar) REFERENCES foobar_fk(foo, bar);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    new_bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				new_buzz REAL CHECK (new_buzz IS NOT NULL),
			    new_fizz SERIAL NOT NULL UNIQUE,
				new_foo VARCHAR(255) COLLATE "POSIX" DEFAULT '' NOT NULL,
			    new_id INT PRIMARY KEY CHECK (new_id > 0), CHECK (new_id < new_buzz)
			);
			CREATE INDEX normal_idx ON foobar USING hash (new_fizz);
			CREATE UNIQUE INDEX foobar_unique_idx ON foobar(new_foo, new_bar);


			CREATE POLICY foobar_policy ON foobar 
				AS RESTRICTIVE 
				FOR INSERT 
				TO PUBLIC
				WITH CHECK (new_id > 0 AND new_foo = 'some_new_value');

			CREATE TABLE foobar_fk(
			    bar TIMESTAMP,
			    foo VARCHAR(255)
			);
			CREATE UNIQUE INDEX foobar_fk_unique_idx ON foobar_fk(foo, bar);
			-- create a circular dependency of foreign keys (this is allowed)
			ALTER TABLE foobar_fk ADD CONSTRAINT foobar_fk_fk FOREIGN KEY (foo, bar) REFERENCES foobar(new_foo, new_bar);
			ALTER TABLE foobar ADD CONSTRAINT foobar_fk FOREIGN KEY (new_foo, new_bar) REFERENCES foobar_fk(foo, bar);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeAcquiresShareRowExclusiveLock,
			diff.MigrationHazardTypeAuthzUpdate,
			diff.MigrationHazardTypeDeletesData,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Alter table: translate BIGINT type to TIMESTAMP, set to not null, set default",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
			    obj_attr__c_time BIGINT,
				obj_attr__m_time BIGINT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
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

func (suite *acceptanceTestSuite) TestTableTestCases() {
	suite.runTestCases(tableAcceptanceTestCases)
}
