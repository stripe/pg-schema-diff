package migration_acceptance_tests

import (
	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var localPartitionIndexAcceptanceTestCases = []acceptanceTestCase{
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

			CREATE INDEX foobar_1_some_idx ON foobar_1 (foo);
			CREATE UNIQUE INDEX foobar_2_some_unique_idx ON foobar_2 (foo);
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

			CREATE INDEX foobar_1_some_idx ON foobar_1 (foo);
			CREATE UNIQUE INDEX foobar_2_some_unique_idx ON foobar_2 (foo);
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
		name: "Add local indexes",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz BYTEA
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
			    bar TEXT,
			    fizz BYTEA
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			CREATE INDEX foobar_1_some_idx ON foobar_1(foo, id);
			CREATE INDEX foobar_2_some_idx ON foobar_2(foo, bar);
			CREATE INDEX foobar_3_some_idx ON foobar_3(foo, fizz);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a unique local index",
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

			CREATE UNIQUE INDEX foobar_1_some_idx ON foobar_1 (foo);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add local primary keys",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz bytea
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
			    bar TEXT,
			    fizz bytea
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar(
			    CONSTRAINT "foobar1_PRIMARY_KEY" PRIMARY KEY (foo, id)
			) FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar(
				CONSTRAINT "foobar2_PRIMARY_KEY" PRIMARY KEY (foo, bar)
			) FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar(
				CONSTRAINT "foobar3_PRIMARY_KEY" PRIMARY KEY (foo, fizz)
			) FOR VALUES IN ('foo_3');
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Delete a local index",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz BYTEA
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			CREATE INDEX foobar_1_some_idx ON foobar_1(foo, id);
			CREATE INDEX foobar_2_some_idx ON foobar_2(foo, bar);
			CREATE INDEX foobar_3_some_idx ON foobar_3(foo, fizz);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz BYTEA
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			CREATE INDEX foobar_1_some_idx ON foobar_1(foo, id);
			CREATE INDEX foobar_3_some_idx ON foobar_3(foo, fizz);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexDropped,
		},
	},
	{
		name: "Delete a unique local index",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			CREATE UNIQUE INDEX some_unique_idx ON foobar_1(foo, id);
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
			    bar TEXT,
			    fizz bytea
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar(
			    CONSTRAINT "foobar1_PRIMARY_KEY" PRIMARY KEY (foo, id)
			) FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar(
				CONSTRAINT "foobar2_PRIMARY_KEY" PRIMARY KEY (foo, bar)
			) FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar(
				CONSTRAINT "foobar3_PRIMARY_KEY" PRIMARY KEY (foo, fizz)
			) FOR VALUES IN ('foo_3');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz bytea
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar(
			    CONSTRAINT "foobar1_PRIMARY_KEY" PRIMARY KEY (foo, id)
			) FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar(
				CONSTRAINT "foobar2_PRIMARY_KEY" PRIMARY KEY (foo, bar)
			) FOR VALUES IN ('foo_2');
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
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			CREATE UNIQUE INDEX some_unique_idx ON foobar_1(foo, id);
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

			CREATE UNIQUE INDEX some_unique_idx ON foobar_1(id, foo);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
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

			CREATE UNIQUE INDEX some_unique_idx ON foobar_1(foo, id);
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
			    bar TEXT,
			    fizz bytea
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar(
			    CONSTRAINT "foobar1_PRIMARY_KEY" PRIMARY KEY (foo, id)
			) FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar(
				CONSTRAINT "foobar2_PRIMARY_KEY" PRIMARY KEY (foo, bar)
			) FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar(
				CONSTRAINT "foobar3_PRIMARY_KEY" PRIMARY KEY (foo, fizz)
			) FOR VALUES IN ('foo_3');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz bytea
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar(
			    CONSTRAINT "foobar1_PRIMARY_KEY" PRIMARY KEY (foo, id)
			) FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar(
				CONSTRAINT "foobar2_PRIMARY_KEY" PRIMARY KEY (foo, fizz)
			) FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar(
				CONSTRAINT "foobar3_PRIMARY_KEY" PRIMARY KEY (foo, fizz)
			) FOR VALUES IN ('foo_3');
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a primary key when the index already exists",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255) NOT NULL,
			    bar TEXT NOT NULL,
			    fizz bytea
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE UNIQUE INDEX "foobar1_PRIMARY_KEY" ON foobar_1(foo, id);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz bytea
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar(
			    CONSTRAINT "foobar1_PRIMARY_KEY" PRIMARY KEY (foo, id)
			) FOR VALUES IN ('foo_1');
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
		},
	},
	{
		name: "Switch partitioned index to local index",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz BYTEA
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			CREATE INDEX foobar_some_idx ON foobar(foo, id);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz BYTEA
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			CREATE INDEX foobar_1_foo_id_idx ON foobar_1(foo, id);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
}

func (suite *acceptanceTestSuite) TestLocalPartitionIndexAcceptanceTestCases() {
	suite.runTestCases(localPartitionIndexAcceptanceTestCases)
}
