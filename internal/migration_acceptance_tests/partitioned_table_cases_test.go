package migration_acceptance_tests

import (
	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var partitionedTableAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "No-op",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
				bar TEXT COLLATE "POSIX",
				fizz SERIAL,
				CHECK ( fizz > 0 ),
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			-- partitioned indexes
			CREATE UNIQUE INDEX foobar_unique_idx ON foobar(foo, fizz);

			-- local indexes
			CREATE INDEX foobar_1_local_idx ON foobar_1(foo);
			CREATE UNIQUE INDEX foobar_2_local_unique_idx ON foobar_2(foo);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
				bar TEXT COLLATE "POSIX",
				fizz SERIAL,
				CHECK ( fizz > 0 ),
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			-- partitioned indexes
			CREATE UNIQUE INDEX foobar_unique_idx ON foobar(foo, fizz);

			-- local indexes
			CREATE INDEX foobar_1_local_idx ON foobar_1(foo);
			CREATE UNIQUE INDEX foobar_2_local_unique_idx ON foobar_2(foo);
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
		name:         "Create partitioned table with shared primary key",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id INT,
				foo VARCHAR(255),
				bar TEXT COLLATE "POSIX" NOT NULL DEFAULT 'some default',
				fizz SERIAL,
				CHECK ( fizz > 0 ),
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);

			CREATE TABLE "FOOBAR_1" PARTITION OF "Foobar"(
			    foo NOT NULL,
			    bar NOT NULL
			) FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF "Foobar" FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF "Foobar" FOR VALUES IN ('foo_3');

			-- partitioned indexes
			CREATE UNIQUE INDEX foobar_unique_idx ON "Foobar"(foo, fizz);

			-- local indexes
			CREATE INDEX foobar_1_local_idx ON "FOOBAR_1"(foo);
			CREATE UNIQUE INDEX foobar_2_local_unique_idx ON foobar_2(foo);
			`,
		},
		dataPackingExpectations: expectations{
			outputState: []string{
				`
				CREATE TABLE "Foobar"(
					id INT,
					fizz SERIAL,
					foo VARCHAR(255),
					bar TEXT COLLATE "POSIX" NOT NULL DEFAULT 'some default',
					CHECK ( fizz > 0 ),
					PRIMARY KEY (foo, id)
				) PARTITION BY LIST (foo);

				CREATE TABLE "FOOBAR_1" PARTITION OF "Foobar"(
					foo NOT NULL,
					bar NOT NULL
				) FOR VALUES IN ('foo_1');
				CREATE TABLE foobar_2 PARTITION OF "Foobar" FOR VALUES IN ('foo_2');
				CREATE TABLE foobar_3 PARTITION OF "Foobar" FOR VALUES IN ('foo_3');

				-- partitioned indexes
				CREATE UNIQUE INDEX foobar_unique_idx ON "Foobar"(foo, fizz);

				-- local indexes
				CREATE INDEX foobar_1_local_idx ON "FOOBAR_1"(foo);
				CREATE UNIQUE INDEX foobar_2_local_unique_idx ON foobar_2(foo);
				`,
			},
		},
	},
	{
		name:         "Create partitioned table with local primary keys",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id INT,
				foo VARCHAR(255),
				bar TEXT,
				fizz SERIAL,
				CHECK ( fizz > 0 )
			) PARTITION BY LIST (foo);
			CREATE TABLE "FOOBAR_1" PARTITION OF "Foobar"(
			    foo NOT NULL,
			    bar NOT NULL,
			    PRIMARY KEY (foo, id)
			) FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF "Foobar"(
			    PRIMARY KEY (foo, bar)
			) FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF "Foobar"(
			    PRIMARY KEY (foo, fizz)
			) FOR VALUES IN ('foo_3');

			-- partitioned indexes
			CREATE UNIQUE INDEX foobar_unique_idx ON "Foobar"(foo, fizz);

			-- local indexes
			CREATE INDEX foobar_1_local_idx ON "FOOBAR_1"(foo);
			CREATE UNIQUE INDEX foobar_2_local_unique_idx ON foobar_2(foo);
			`,
		},
		dataPackingExpectations: expectations{
			outputState: []string{
				`
				CREATE TABLE "Foobar"(
					id INT,
					fizz SERIAL,
					foo VARCHAR(255),
					bar TEXT,
					CHECK ( fizz > 0 )
				) PARTITION BY LIST (foo);

				CREATE TABLE "FOOBAR_1" PARTITION OF "Foobar"(
					foo NOT NULL,
					bar NOT NULL,
					PRIMARY KEY (foo, id)
				) FOR VALUES IN ('foo_1');
				CREATE TABLE foobar_2 PARTITION OF "Foobar"(
					PRIMARY KEY (foo, bar)
				) FOR VALUES IN ('foo_2');
				CREATE TABLE foobar_3 PARTITION OF "Foobar"(
					PRIMARY KEY (foo, fizz)
				) FOR VALUES IN ('foo_3');

				-- partitioned indexes
				CREATE UNIQUE INDEX foobar_unique_idx ON "Foobar"(foo, fizz);

				-- local indexes
				CREATE INDEX foobar_1_local_idx ON "FOOBAR_1"(foo);
				CREATE UNIQUE INDEX foobar_2_local_unique_idx ON foobar_2(foo);
				`,
			},
		},
	},
	{
		name: "Drop table",
		oldSchemaDDL: []string{
			`
			CREATE TABLE "Foobar"(
			    id INT,
				foo VARCHAR(255),
				bar TEXT,
				fizz SERIAL,
				CHECK ( fizz > 0 ),
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);

			CREATE TABLE "FOOBAR_1" PARTITION OF "Foobar"(
			    foo NOT NULL,
			    bar NOT NULL
			) FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF "Foobar" FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF "Foobar" FOR VALUES IN ('foo_3');

			-- partitioned indexes
			CREATE UNIQUE INDEX foobar_unique_idx ON "Foobar"(foo, fizz);

			-- local indexes
			CREATE INDEX foobar_1_local_idx ON "FOOBAR_1"(foo);
			CREATE UNIQUE INDEX foobar_2_local_unique_idx ON foobar_2(foo);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
		newSchemaDDL: nil,
	},
	{
		name: "Alter table: New primary key, change column types, delete unique partitioned index index, new partitioned index, delete local index, add local index, validate check constraint",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
				bar TEXT COLLATE "C",
				fizz SERIAL,
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);

			ALTER TABLE foobar ADD CONSTRAINT some_check_constraint CHECK ( fizz > 0 ) NOT VALID;

			CREATE TABLE foobar_1 PARTITION OF foobar(
			    foo NOT NULL,
			    bar NOT NULL
			) FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			-- partitioned indexes
			CREATE INDEX foobar_some_idx ON foobar(foo, bar);

			-- local indexes
			CREATE INDEX foobar_1_local_idx ON foobar_1(foo);
			CREATE UNIQUE INDEX foobar_2_local_unique_idx ON foobar_2(foo);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id TEXT,
				foo VARCHAR(255),
				bar TEXT COLLATE "POSIX",
				fizz TEXT,
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);
			ALTER TABLE foobar ADD CONSTRAINT some_check_constraint CHECK ( LENGTH(fizz) > 0 );

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar(
			    bar NOT NULL
			) FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');

			-- partitioned indexes
			CREATE UNIQUE INDEX foobar_unique_idx ON foobar(foo, fizz);

			-- local indexes
			CREATE UNIQUE INDEX foobar_2_local_unique_idx ON foobar_2(foo);
			CREATE INDEX foobar_3_local_idx ON foobar_3(foo, bar);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeDeletesData,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Changing partition key def errors",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
				bar TEXT,
				fizz INT
			) PARTITION BY LIST (foo);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
				bar TEXT,
				fizz INT
			) PARTITION BY LIST (bar);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
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
		name: "Unpartitioned to partitioned",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT
			);

			CREATE INDEX some_idx on foobar(id);


			CREATE FUNCTION increment_version() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = OLD.version + 1;
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_update_trigger
				BEFORE UPDATE ON foobar
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();

			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT
			) PARTITION BY LIST (foo);

			CREATE INDEX some_idx on foobar(id);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			CREATE FUNCTION increment_version() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = OLD.version + 1;
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_update_trigger
				BEFORE UPDATE ON foobar
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Unpartitioned to partitioned and child tables already exist",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT
			);

			CREATE INDEX some_idx on foobar(id);

			CREATE TABLE foobar_1(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT
			);

			CREATE INDEX foobar_1_id_idx on foobar(id);


			CREATE FUNCTION increment_version() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = OLD.version + 1;
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_update_trigger
				BEFORE UPDATE ON foobar
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();

			CREATE TRIGGER some_update_trigger
				BEFORE UPDATE ON foobar_1
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT
			) PARTITION BY LIST (foo);

			CREATE INDEX some_idx on foobar(id);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');


			CREATE FUNCTION increment_version() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = OLD.version + 1;
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_update_trigger
				BEFORE UPDATE ON foobar
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Partitioned to unpartitioned",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT
			) PARTITION BY LIST (foo);

			CREATE INDEX some_idx on foobar(id);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');


			CREATE FUNCTION increment_version() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = OLD.version + 1;
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_update_trigger
				BEFORE UPDATE ON foobar
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT
			);

			CREATE INDEX some_idx on foobar(id);


			CREATE FUNCTION increment_version() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = OLD.version + 1;
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_update_trigger
				BEFORE UPDATE ON foobar
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Partitioned to unpartitioned and child tables still exist",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT
			) PARTITION BY LIST (foo);

			CREATE INDEX some_idx on foobar(id);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');


			CREATE FUNCTION increment_version() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = OLD.version + 1;
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_update_trigger
				BEFORE UPDATE ON foobar
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT
			);

			CREATE INDEX some_idx on foobar(id);

			CREATE TABLE foobar_1(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT
			);

			CREATE INDEX foobar_1_id_idx on foobar(id);


			CREATE FUNCTION increment_version() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = OLD.version + 1;
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_update_trigger
				BEFORE UPDATE ON foobar
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();

			CREATE TRIGGER some_update_trigger
				BEFORE UPDATE ON foobar_1
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Adding a partition",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT,
				CHECK ( fizz > 0 ),
				PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);

			-- partitioned indexes
			CREATE UNIQUE INDEX some_partitioned_idx ON foobar(foo, bar);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT,
				CHECK ( fizz > 0 ),
				PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			-- partitioned indexes
			CREATE UNIQUE INDEX some_partitioned_idx ON foobar(foo, bar);
			`,
		},
	},
	{
		name: "Adding a partition with local primary key that can back the unique index",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT,
				CHECK ( fizz > 0 )
			) PARTITION BY LIST (foo);

			-- partitioned indexes
			CREATE UNIQUE INDEX some_partitioned_idx ON foobar(foo, bar);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT,
				CHECK ( fizz > 0 )
			) PARTITION BY LIST (foo);

			CREATE TABLE foobar_1 PARTITION OF foobar(
			    PRIMARY KEY (foo, bar)
			) FOR VALUES IN ('foo_1');

			-- partitioned indexes
			CREATE UNIQUE INDEX some_partitioned_idx ON foobar(foo, bar);
			`,
		},
	},
	{
		name: "Deleting a partitioning errors",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT
			) PARTITION BY LIST (foo);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				version INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT
			) PARTITION BY LIST (foo);
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
		name: "Altering a partition's 'FOR VALUES' errors",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
				bar TEXT,
				fizz INT
			) PARTITION BY LIST (foo);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
				bar TEXT,
				fizz INT
			) PARTITION BY LIST (foo);

			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_2');
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
		name: "Re-creating base table causes partitions to be re-created",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				fizz INT,
				foo VARCHAR(255),
				bar TEXT
			) PARTITION BY LIST (foo);

			CREATE TABLE foobar_1 PARTITION OF foobar(
			    PRIMARY KEY (foo, fizz)
			) FOR VALUES IN ('foo_1');

			-- partitioned indexes
			CREATE INDEX some_partitioned_idx ON foobar(foo, bar);
			-- local indexes
			CREATE INDEX some_local_idx ON foobar_1(foo, bar);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar_new(
			    id INT,
				fizz INT,
				foo VARCHAR(255),
				bar TEXT
			) PARTITION BY LIST (foo);

			CREATE TABLE foobar_1 PARTITION OF foobar_new(
			    PRIMARY KEY (foo, fizz)
			) FOR VALUES IN ('foo_1');

			-- partitioned indexes
			CREATE INDEX some_partitioned_idx ON foobar_new(foo, bar);
			-- local indexes
			CREATE INDEX some_local_idx ON foobar_1(foo, bar);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Can handle scenario where partition is not attached",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				fizz INT,
				foo VARCHAR(255),
				bar TEXT
			) PARTITION BY LIST (foo);

			CREATE TABLE foobar_1 (
			    id INT,
				fizz INT,
				foo VARCHAR(255),
				bar TEXT
			);

			-- partitioned indexes
			CREATE INDEX some_partitioned_idx ON foobar(foo, bar);
			-- local indexes
			CREATE INDEX some_local_idx ON foobar_1(foo, bar);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				fizz INT,
				foo VARCHAR(255),
				bar TEXT
			) PARTITION BY LIST (foo);

			CREATE TABLE foobar_1 (
			    id INT,
				fizz INT,
				foo VARCHAR(255),
				bar TEXT
			);
			ALTER TABLE foobar ATTACH PARTITION foobar_1 FOR VALUES IN ('foo_1');

			-- partitioned indexes
			CREATE INDEX some_partitioned_idx ON foobar(foo, bar);
			-- local indexes
			CREATE INDEX some_local_idx ON foobar_1(foo, bar);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
}

func (suite *acceptanceTestSuite) TestPartitionedTableAcceptanceTestCases() {
	suite.runTestCases(partitionedTableAcceptanceTestCases)
}
