package migration_acceptance_tests

import (
	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var partitionedIndexAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "No-op",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
				bar TEXT,
				fizz INT,
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			CREATE INDEX some_idx ON foobar USING hash (foo);
			CREATE UNIQUE INDEX some_other_idx ON foobar(foo DESC, fizz);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
				bar TEXT,
				fizz INT,
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			CREATE INDEX some_idx ON foobar USING hash (foo);
			CREATE UNIQUE INDEX some_other_idx ON foobar(foo DESC, fizz);
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
		name: "Add a normal partitioned index",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			CREATE INDEX some_idx ON foobar(id DESC, foo);
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
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			CREATE INDEX some_idx ON foobar USING hash (foo);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a unique partitioned index",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			CREATE UNIQUE INDEX some_unique_idx ON foobar(foo, id);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a normal partitioned index with quotes names",
		oldSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id INT,
				"Foo" VARCHAR(255)
			) PARTITION BY LIST ("Foo");
			CREATE TABLE "FOOBAR_1" PARTITION OF "Foobar" FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF "Foobar" FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF "Foobar" FOR VALUES IN ('foo_3');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id INT,
				"Foo" VARCHAR(255)
			) PARTITION BY LIST ("Foo");
			CREATE TABLE "FOOBAR_1" PARTITION OF "Foobar" FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF "Foobar" FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF "Foobar" FOR VALUES IN ('foo_3');

			CREATE INDEX "SOME_IDX" ON "Foobar"(id, "Foo");
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
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeAcquiresShareLock,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a partitioned index that is used by a local primary key",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE UNIQUE INDEX some_idx ON ONLY foobar(foo, id);
			CREATE UNIQUE INDEX foobar_1_pkey ON foobar_1(foo, id);
			ALTER TABLE foobar_1 ADD CONSTRAINT foobar_1_pkey PRIMARY KEY USING INDEX foobar_1_pkey;
			ALTER INDEX some_idx ATTACH PARTITION foobar_1_pkey;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a primary key with quoted names",
		oldSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    "Id" INT,
				"FOO" VARCHAR(255)
			) PARTITION BY LIST ("FOO");
			CREATE TABLE foobar_1 PARTITION OF "Foobar" FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF "Foobar" FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF "Foobar" FOR VALUES IN ('foo_3');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    "Id" INT,
				"FOO" VARCHAR(255)
			) PARTITION BY LIST ("FOO");
			CREATE TABLE foobar_1 PARTITION OF "Foobar" FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF "Foobar" FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF "Foobar" FOR VALUES IN ('foo_3');
			ALTER TABLE "Foobar" ADD CONSTRAINT "FOOBAR_PK" PRIMARY KEY("FOO", "Id")
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexBuild,
			diff.MigrationHazardTypeAcquiresShareLock,
		},
	},
	{
		name: "Add a partitioned primary key when the local index already exists",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE UNIQUE INDEX foobar_1_unique_idx ON foobar_1(foo, id);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			ALTER TABLE foobar ADD CONSTRAINT foobar_pkey PRIMARY KEY (foo, id);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeAcquiresShareLock,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a unique index when the local index already exists",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE UNIQUE INDEX foobar_1_foo_id_idx ON foobar_1(foo, id);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			ALTER TABLE foobar ADD CONSTRAINT foobar_pkey PRIMARY KEY (foo, id);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE UNIQUE INDEX foobar_unique ON foobar(foo, id);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeAcquiresShareLock,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Delete a normal partitioned index",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			CREATE INDEX some_idx ON foobar(foo, id);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
		},
	},
	{
		name: "Delete a unique partitioned index",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			CREATE UNIQUE INDEX some_unique_idx ON foobar(foo, id);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
		},
	},
	{
		name: "Delete a primary key",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
		},
	},
	{
		name: "Change an index columns",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			   	bar INT
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			CREATE INDEX some_idx ON foobar(id, foo);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			   	bar INT
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			CREATE INDEX some_idx ON foobar(foo, bar);
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
			    id INT,
				foo VARCHAR(255),
			   	bar INT
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			CREATE INDEX some_idx ON foobar(foo);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			   	bar INT
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			CREATE INDEX some_idx ON foobar USING hash (foo);
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
			    id INT,
				foo VARCHAR(255),
			   	bar INT
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			CREATE INDEX some_idx ON foobar (foo, bar);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			   	bar INT
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			CREATE INDEX some_idx ON foobar (bar, foo);
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
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			CREATE INDEX some_idx ON foobar(id, foo);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeDeletesData,
			diff.MigrationHazardTypeIndexDropped,
		},
	},
	{
		name: "Switch primary key",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			    PRIMARY KEY (foo)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresShareLock,
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Attach an unnattached, invalid index",
		oldSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id INT,
				foo VARCHAR(255),
			    PRIMARY KEY (foo)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF "Foobar" FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF "Foobar" FOR VALUES IN ('foo_2');
			CREATE TABLE "Foobar_3" PARTITION OF "Foobar" FOR VALUES IN ('foo_3');

			CREATE INDEX "Partitioned_Idx" ON ONLY "Foobar"(foo);

			CREATE INDEX "foobar_1_part" ON foobar_1(foo);
			ALTER INDEX "Partitioned_Idx" ATTACH PARTITION "foobar_1_part";

			CREATE INDEX "foobar_2_part" ON foobar_2(foo);
			ALTER INDEX "Partitioned_Idx" ATTACH PARTITION "foobar_2_part";

			CREATE INDEX "Foobar_3_Part" ON "Foobar_3"(foo);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id INT,
				foo VARCHAR(255),
			    PRIMARY KEY (foo)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF "Foobar" FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF "Foobar" FOR VALUES IN ('foo_2');
			CREATE TABLE "Foobar_3" PARTITION OF "Foobar" FOR VALUES IN ('foo_3');

			CREATE INDEX "Partitioned_Idx" ON ONLY "Foobar"(foo);

			CREATE INDEX "foobar_1_part" ON foobar_1(foo);
			ALTER INDEX "Partitioned_Idx" ATTACH PARTITION "foobar_1_part";

			CREATE INDEX "foobar_2_part" ON foobar_2(foo);
			ALTER INDEX "Partitioned_Idx" ATTACH PARTITION "foobar_2_part";

			CREATE INDEX "Foobar_3_Part" ON "Foobar_3"(foo);
			ALTER INDEX "Partitioned_Idx" ATTACH PARTITION "Foobar_3_Part";
			`,
		},
	},
}

func (suite *acceptanceTestSuite) TestPartitionedIndexAcceptanceTestCases() {
	suite.runTestCases(partitionedIndexAcceptanceTestCases)
}
