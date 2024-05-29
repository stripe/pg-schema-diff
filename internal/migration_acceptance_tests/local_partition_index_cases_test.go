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
			CREATE SCHEMA schema_1;
			CREATE SCHEMA schema_2;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz BYTEA
			) PARTITION BY LIST (foo);
			CREATE TABLE schema_2.foobar_1 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_1');
			CREATE TABLE schema_2.foobar_2 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_2');
			CREATE TABLE schema_2.foobar_3 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_3');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE SCHEMA schema_2;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz BYTEA
			) PARTITION BY LIST (foo);
			CREATE TABLE schema_2.foobar_1 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_1');
			CREATE TABLE schema_2.foobar_2 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_2');
			CREATE TABLE schema_2.foobar_3 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_3');

			CREATE INDEX foobar_1_some_idx ON schema_2.foobar_1(foo, id);
			CREATE INDEX foobar_2_some_idx ON schema_2.foobar_2(foo, bar);
			CREATE INDEX foobar_3_some_idx ON schema_2.foobar_3(foo, fizz);
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
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz bytea
			) PARTITION BY LIST (foo);
			CREATE SCHEMA schema_2;
			CREATE TABLE schema_2.foobar_1 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_1');
			CREATE TABLE schema_2.foobar_2 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_2');
			CREATE TABLE schema_2.foobar_3 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_3');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz bytea
			) PARTITION BY LIST (foo);
			
			CREATE SCHEMA schema_2;
			CREATE TABLE schema_2.foobar_1 PARTITION OF schema_1.foobar(
			    CONSTRAINT "foobar1_PRIMARY_KEY" PRIMARY KEY (foo, id)
			) FOR VALUES IN ('foo_1');
			CREATE TABLE schema_2.foobar_2 PARTITION OF schema_1.foobar(
				CONSTRAINT "foobar2_PRIMARY_KEY" PRIMARY KEY (foo, bar)
			) FOR VALUES IN ('foo_2');
			CREATE TABLE schema_2.foobar_3 PARTITION OF schema_1.foobar(
				CONSTRAINT "foobar3_PRIMARY_KEY" PRIMARY KEY (foo, fizz)
			) FOR VALUES IN ('foo_3');
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
		},
	},
	{
		name: "Add local unique constraints",
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
			    CONSTRAINT "foobar1_PRIMARY_KEY" UNIQUE (foo, id)
			) FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar(
				CONSTRAINT "foobar2_PRIMARY_KEY" UNIQUE (foo, bar)
			) FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar(
				CONSTRAINT "foobar3_PRIMARY_KEY" UNIQUE (foo, fizz)
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
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz BYTEA
			) PARTITION BY LIST (foo);
			CREATE SCHEMA schema_2;
			CREATE TABLE schema_2.foobar_1 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_1');
			CREATE TABLE schema_2.foobar_2 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_2');
			CREATE TABLE schema_2.foobar_3 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_3');

			CREATE INDEX foobar_1_some_idx ON schema_2.foobar_1(foo, id);
			CREATE INDEX foobar_2_some_idx ON schema_2.foobar_2(foo, bar);
			CREATE INDEX foobar_3_some_idx ON schema_2.foobar_3(foo, fizz);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz BYTEA
			) PARTITION BY LIST (foo);
			CREATE SCHEMA schema_2;
			CREATE TABLE schema_2.foobar_1 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_1');
			CREATE TABLE schema_2.foobar_2 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_2');
			CREATE TABLE schema_2.foobar_3 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_3');

			CREATE INDEX foobar_1_some_idx ON schema_2.foobar_1(foo, id);
			CREATE INDEX foobar_3_some_idx ON schema_2.foobar_3(foo, fizz);
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
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz BYTEA
			) PARTITION BY LIST (foo);
			CREATE SCHEMA schema_2;
			CREATE TABLE schema_2.foobar_1 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_1');
			CREATE TABLE schema_2.foobar_2 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_2');
			CREATE TABLE schema_2.foobar_3 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_3');

			CREATE UNIQUE INDEX some_unique_idx ON schema_2.foobar_1(foo, id);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz BYTEA
			) PARTITION BY LIST (foo);
			CREATE SCHEMA schema_2;
			CREATE TABLE schema_2.foobar_1 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_1');
			CREATE TABLE schema_2.foobar_2 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_2');
			CREATE TABLE schema_2.foobar_3 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_3');
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
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz bytea
			) PARTITION BY LIST (foo);
			CREATE SCHEMA schema_2;
			CREATE TABLE schema_2.foobar_1 PARTITION OF schema_1.foobar(
			    CONSTRAINT "foobar1_PRIMARY_KEY" PRIMARY KEY (foo, id)
			) FOR VALUES IN ('foo_1');
			CREATE TABLE schema_2.foobar_2 PARTITION OF schema_1.foobar(
				CONSTRAINT "foobar2_PRIMARY_KEY" PRIMARY KEY (foo, bar)
			) FOR VALUES IN ('foo_2');
			CREATE TABLE schema_2.foobar_3 PARTITION OF schema_1.foobar(
				CONSTRAINT "foobar3_PRIMARY_KEY" PRIMARY KEY (foo, fizz)
			) FOR VALUES IN ('foo_3');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz bytea
			) PARTITION BY LIST (foo);
			CREATE SCHEMA schema_2;
			CREATE TABLE schema_2.foobar_1 PARTITION OF schema_1.foobar(
			    CONSTRAINT "foobar1_PRIMARY_KEY" PRIMARY KEY (foo, id)
			) FOR VALUES IN ('foo_1');
			CREATE TABLE schema_2.foobar_2 PARTITION OF schema_1.foobar(
				CONSTRAINT "foobar2_PRIMARY_KEY" PRIMARY KEY (foo, bar)
			) FOR VALUES IN ('foo_2');
			CREATE TABLE schema_2.foobar_3 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_3');
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
		},
	},
	{
		name: "Change an index columns (with conflicting schemas)",
		oldSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE SCHEMA schema_2;
			CREATE TABLE schema_2.foobar_1 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_1');
			CREATE TABLE schema_2.foobar_2 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_2');
			CREATE TABLE schema_2.foobar_3 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_3');

			CREATE UNIQUE INDEX some_unique_idx ON schema_2.foobar_1(foo, id);

			CREATE SCHEMA schema_3;
			CREATE TABLE schema_3.foobar(
			    id TEXT,
				foo INT
			) PARTITION BY LIST (foo);
			CREATE SCHEMA schema_4;
			CREATE TABLE schema_4.foobar_1 PARTITION OF schema_3.foobar FOR VALUES IN (1);
			CREATE TABLE schema_4.foobar_2 PARTITION OF schema_3.foobar FOR VALUES IN (2);
			CREATE TABLE schema_4.foobar_3 PARTITION OF schema_3.foobar FOR VALUES IN (3);

			CREATE UNIQUE INDEX some_unique_idx ON schema_4.foobar_1(foo, id);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			CREATE SCHEMA schema_2;
			CREATE TABLE schema_2.foobar_1 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_1');
			CREATE TABLE schema_2.foobar_2 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_2');
			CREATE TABLE schema_2.foobar_3 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_3');

			CREATE UNIQUE INDEX some_unique_idx ON schema_2.foobar_1(id, foo);

			CREATE SCHEMA schema_3;
			CREATE TABLE schema_3.foobar(
			    id TEXT,
				foo INT
			) PARTITION BY LIST (foo);
			CREATE SCHEMA schema_4;
			CREATE TABLE schema_4.foobar_1 PARTITION OF schema_3.foobar FOR VALUES IN (1);
			CREATE TABLE schema_4.foobar_2 PARTITION OF schema_3.foobar FOR VALUES IN (2);
			CREATE TABLE schema_4.foobar_3 PARTITION OF schema_3.foobar FOR VALUES IN (3);

			CREATE UNIQUE INDEX some_unique_idx ON schema_4.foobar_1(id, foo);
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
		name: "Switch primary key (add and drop) (conflicting schemas)",
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

			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT
			) PARTITION BY LIST (foo);
			CREATE TABLE schema_1.foobar_1 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_1');
			CREATE TABLE schema_1.foobar_2 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_2');
			CREATE TABLE schema_1.foobar_3 PARTITION OF schema_1.foobar FOR VALUES IN ('foo_3');
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

			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz bytea
			) PARTITION BY LIST (foo);
			CREATE TABLE schema_1.foobar_1 PARTITION OF schema_1.foobar(
			    CONSTRAINT "foobar1_PRIMARY_KEY" PRIMARY KEY (foo, id)
			) FOR VALUES IN ('foo_1');
			CREATE TABLE schema_1.foobar_2 PARTITION OF schema_1.foobar(
				CONSTRAINT "foobar2_PRIMARY_KEY" PRIMARY KEY (foo, bar)
			) FOR VALUES IN ('foo_2');
			CREATE TABLE schema_1.foobar_3 PARTITION OF schema_1.foobar(
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
			
			-- Create a table in a different schema where the index does not exist
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255) NOT NULL,
			    bar TEXT NOT NULL,
			    fizz bytea
			) PARTITION BY LIST (foo);
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

			-- Create a table in a different schema where the index does not exist
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    id INT,
				foo VARCHAR(255) NOT NULL,
			    bar TEXT NOT NULL,
			    fizz bytea
			) PARTITION BY LIST (foo);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
		},
	},
	{
		name: "Add a unique constraint when the index already exists",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			    bar TEXT,
			    fizz bytea
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE UNIQUE INDEX foobar_1_unique ON foobar_1(foo, id);
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
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			ALTER TABLE foobar_1 ADD CONSTRAINT foobar_1_unique UNIQUE (foo, id);

			`,
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

func (suite *acceptanceTestSuite) TestLocalPartitionIndexTestCases() {
	suite.runTestCases(localPartitionIndexAcceptanceTestCases)
}
