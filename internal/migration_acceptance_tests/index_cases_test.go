package migration_acceptance_tests

import (
	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var indexAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "No-op",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar TEXT UNIQUE ,
                fizz INT
            );
            CREATE INDEX some_idx ON foobar USING hash (foo);
            CREATE UNIQUE INDEX some_other_idx ON foobar (bar DESC, fizz);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar TEXT UNIQUE ,
                fizz INT
            );
            CREATE INDEX some_idx ON foobar USING hash (foo);
            CREATE UNIQUE INDEX some_other_idx ON foobar (bar DESC, fizz);
			`,
		},

		expectEmptyPlan: true,
	},
	{
		name: "Add a normal index",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE TABLE schema_1.foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255)
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE TABLE schema_1.foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255)
            );
            CREATE INDEX some_idx ON schema_1.foobar(id DESC, foo);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a hash index",
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
                foo VARCHAR(255)
            );
            CREATE INDEX some_idx ON foobar USING hash (id);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a normal index with quoted names",
		oldSchemaDDL: []string{
			`
            CREATE TABLE "Foobar"(
                id INT PRIMARY KEY,
                "Foo" VARCHAR(255)
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE "Foobar"(
                id INT PRIMARY KEY,
                "Foo" VARCHAR(255)
            );
            CREATE INDEX "Some_idx" ON "Foobar"(id, "Foo");
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a unique index",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL
            );
            CREATE UNIQUE INDEX some_unique_idx ON foobar(foo);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a primary key",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE TABLE schema_1.foobar(
                id INT
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE TABLE schema_1.foobar(
                id INT PRIMARY KEY
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a unique constraint",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT UNIQUE
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a primary key on NOT NULL column",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT NOT NULL
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT NOT NULL PRIMARY KEY
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a primary key when the index already exists",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT
            );
            CREATE UNIQUE INDEX foobar_primary_key ON foobar(id);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT
            );
            CREATE UNIQUE INDEX foobar_primary_key ON foobar(id);
            ALTER TABLE foobar ADD CONSTRAINT foobar_primary_key PRIMARY KEY USING INDEX foobar_primary_key;
			`,
		},
	},
	{
		name: "Add a unique constraint when the index already exists",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT
            );
            CREATE UNIQUE INDEX foobar_unique_idx ON foobar(id);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT
            );
            CREATE UNIQUE INDEX foobar_unique_idx ON foobar(id);
            ALTER TABLE foobar ADD CONSTRAINT foobar_unique_idx UNIQUE USING INDEX foobar_unique_idx;
			`,
		},
	},
	{
		name: "Add a primary key when the index already exists but has a name different to the constraint",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT
            );
            CREATE UNIQUE INDEX foobar_idx ON foobar(id);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT
            );
            CREATE UNIQUE INDEX foobar_idx ON foobar(id);
            -- This renames the index
            ALTER TABLE foobar ADD CONSTRAINT foobar_primary_key PRIMARY KEY USING INDEX foobar_idx;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a unique constraint when the index already exists but has a name different to the constraint",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT
            );
            CREATE UNIQUE INDEX foobar_idx ON foobar(id);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT
            );
            CREATE UNIQUE INDEX foobar_idx ON foobar(id);
            -- This renames the index
            ALTER TABLE foobar ADD CONSTRAINT foobar_unique_constraint UNIQUE USING INDEX foobar_idx;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a primary key when the index already exists but is not unique",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT
            );
            CREATE INDEX foobar_idx ON foobar(id);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT
            );
            CREATE UNIQUE INDEX foobar_primary_key ON foobar(id);
            ALTER TABLE foobar ADD CONSTRAINT foobar_primary_key PRIMARY KEY USING INDEX foobar_primary_key;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a unique constraint when the index already exists but is not unique",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT
            );
            CREATE INDEX foobar_idx ON foobar(id);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT
            );
            CREATE UNIQUE INDEX foobar_idx ON foobar(id);
            ALTER TABLE foobar ADD CONSTRAINT foobar_unique_idx UNIQUE USING INDEX foobar_idx;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Delete a normal index",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE TABLE schema_1.foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL
            );
            CREATE INDEX some_inx ON schema_1.foobar(id, foo);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE TABLE schema_1.foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255)
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexDropped,
		},
	},
	{
		name: "Delete a normal index and columns (index dropped concurrently first)",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL
            );
            CREATE INDEX some_idx ON foobar(foo);
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
			diff.MigrationHazardTypeIndexDropped,
		},
		expectedPlanDDL: []string{
			"DROP INDEX CONCURRENTLY \"public\".\"some_idx\"",
			"ALTER TABLE \"public\".\"foobar\" DROP COLUMN \"foo\"",
		},
	},
	{
		name: "Delete a normal index with quoted names",
		oldSchemaDDL: []string{
			`
            CREATE TABLE "Foobar"(
                id INT PRIMARY KEY,
                "Foo" VARCHAR(255)
            );
            CREATE INDEX "Some_idx" ON "Foobar"(id, "Foo");
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE "Foobar"(
                id INT PRIMARY KEY,
                "Foo" VARCHAR(255) NOT NULL
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexDropped,
		},
	},
	{
		name: "Delete a unique index",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL
            );
            CREATE UNIQUE INDEX some_unique_idx ON foobar(foo);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexDropped,
		},
	},
	{
		name: "Delete a primary key",
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
                id INT
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
		},
	},
	{
		name: "Delete a unique constraint",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT UNIQUE
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
		},
	},
	{
		name: "Add and delete a normal index (conflicting schemas)",
		oldSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE TABLE schema_1.foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL
            );
            CREATE INDEX some_idx ON schema_1.foobar(id, foo);
            
            CREATE SCHEMA schema_2;
            CREATE TABLE schema_2.foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA schema_1;
            CREATE TABLE schema_1.foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL
            );
            
            CREATE SCHEMA schema_2;
            CREATE TABLE schema_2.foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL
            );
            CREATE INDEX some_idx ON schema_2.foobar(id, foo);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Change an index (with a really long name) columns",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo TEXT NOT NULL,
                bar BIGINT NOT NULL
            );
            CREATE UNIQUE INDEX some_idx_with_a_really_long_name_that_is_nearly_61_chars ON foobar(foo, bar)
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT,
                foo TEXT NOT NULL,
                bar BIGINT NOT NULL
            );
            CREATE UNIQUE INDEX some_idx_with_a_really_long_name_that_is_nearly_61_chars ON foobar(foo)
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Change indexes (conflicting schemas)",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo TEXT NOT NULL,
                bar BIGINT NOT NULL
            );
            CREATE UNIQUE INDEX some_idx ON foobar(foo, bar);

            CREATE SCHEMA schema_1;
            CREATE TABLE schema_1.foobar(
                id INT PRIMARY KEY,
                foo BIGINT NOT NULL,
                bar TEXt NOT NULL
            );
            CREATE UNIQUE INDEX some_idx ON schema_1.foobar(foo, bar)
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT,
                foo TEXT NOT NULL,
                bar BIGINT NOT NULL
            );
            CREATE UNIQUE INDEX some_idx ON foobar(foo);

            CREATE SCHEMA schema_1;
            CREATE TABLE schema_1.foobar(
                id INT PRIMARY KEY,
                foo BIGINT NOT NULL,
                bar TEXt NOT NULL
            );
            CREATE UNIQUE INDEX some_idx ON schema_1.foobar(foo);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Change an index type",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo TEXT NOT NULL,
                bar BIGINT NOT NULL
            );
            CREATE INDEX some_idx ON foobar (foo)
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT,
                foo TEXT NOT NULL,
                bar BIGINT NOT NULL
            );
            CREATE INDEX some_idx ON foobar USING hash (foo)
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Change an index column ordering",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo TEXT NOT NULL,
                bar BIGINT NOT NULL
            );
            CREATE INDEX some_idx ON foobar (foo, bar)
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT,
                foo TEXT NOT NULL,
                bar BIGINT NOT NULL
            );
            CREATE INDEX some_idx ON foobar (foo DESC, bar)
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Delete columns and associated index",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo TEXT NOT NULL,
                bar BIGINT NOT NULL
            );
            CREATE UNIQUE INDEX some_idx ON foobar(foo, bar);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT
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
		name: "Switch primary key and make old key nullable",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT NOT NULL PRIMARY KEY,
                foo TEXT NOT NULL
            );
            CREATE UNIQUE INDEX some_idx ON foobar(foo);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT,
                foo INT NOT NULL PRIMARY KEY
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
		name: "Switch primary key with quoted name",
		oldSchemaDDL: []string{
			`
            CREATE TABLE "Foobar"(
                "Id" INT NOT NULL PRIMARY KEY,
                foo TEXT NOT NULL
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE "Foobar"(
                "Id" INT,
                foo INT NOT NULL PRIMARY KEY
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
		name: "Switch primary key when the original primary key constraint has a non-default name",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT NOT NULL,
                foo TEXT NOT NULL
            );
            CREATE UNIQUE INDEX unique_idx ON foobar(id);
            ALTER TABLE foobar ADD CONSTRAINT non_default_primary_key PRIMARY KEY USING INDEX unique_idx;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT,
                foo INT NOT NULL PRIMARY KEY
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Alter primary key columns (name stays same)",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT NOT NULL,
                foo TEXT NOT NULL
            );
            CREATE UNIQUE INDEX unique_idx ON foobar(id);
            ALTER TABLE foobar ADD CONSTRAINT non_default_primary_key PRIMARY KEY USING INDEX unique_idx;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT,
                foo INT NOT NULL
            );
            CREATE UNIQUE INDEX unique_idx ON foobar(id, foo);
            ALTER TABLE foobar ADD CONSTRAINT non_default_primary_key PRIMARY KEY USING INDEX unique_idx;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Alter index columns (index replacement and prioritized builds)",
		oldSchemaDDL: []string{`
            CREATE TABLE foobar(
                foo TEXT,
                bar INT
            );
            CREATE INDEX some_idx_with_a_very_long_name ON foobar(foo);
            CREATE INDEX old_idx ON foobar(bar);
		`},
		newSchemaDDL: []string{`
            CREATE TABLE foobar(
                foo TEXT,
                bar INT
            );
            CREATE INDEX some_idx_with_a_very_long_name ON foobar(foo, bar);
            CREATE INDEX new_idx ON foobar(bar);
		`},
		expectedPlanDDL: []string{
			"ALTER INDEX \"public\".\"some_idx_with_a_very_long_name\" RENAME TO \"pgschemadiff_tmpidx_some_idx_with_a_very_EBESExQVRheYGRobHB0eHw\"",
			"CREATE INDEX CONCURRENTLY new_idx ON public.foobar USING btree (bar)",
			"CREATE INDEX CONCURRENTLY some_idx_with_a_very_long_name ON public.foobar USING btree (foo, bar)",
			"DROP INDEX CONCURRENTLY \"public\".\"old_idx\"",
			"DROP INDEX CONCURRENTLY \"public\".\"pgschemadiff_tmpidx_some_idx_with_a_very_EBESExQVRheYGRobHB0eHw\"",
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
			diff.MigrationHazardTypeIndexDropped,
		},
	},
}

func (suite *acceptanceTestSuite) TestIndexTestCases() {
	suite.runTestCases(indexAcceptanceTestCases)
}
