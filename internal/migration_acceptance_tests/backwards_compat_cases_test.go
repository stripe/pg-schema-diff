package migration_acceptance_tests

import "github.com/stripe/pg-schema-diff/pkg/diff"

var backCompatAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "Drop partitioned table, Add partitioned table with local keys",
		oldSchemaDDL: []string{
			`
            -- Create a table in a different schema to validate it is being ignored (no delete operation).
            CREATE SCHEMA schema_filtered_1;
            CREATE TABLE schema_filtered_1.foo();    

            CREATE TABLE foobar(
                id INT,
                bar SERIAL NOT NULL,
                foo VARCHAR(255) DEFAULT 'some default' NOT NULL CHECK (LENGTH(foo) > 0),
                fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (foo, id),
                UNIQUE (foo, bar)
            ) PARTITION BY LIST(foo);

            CREATE TABLE foobar_1 PARTITION of foobar(
                fizz NOT NULL
            ) FOR VALUES IN ('foobar_1_val_1', 'foobar_1_val_2');

            -- partitioned indexes
            CREATE INDEX foobar_normal_idx ON foobar(foo, bar);
            CREATE UNIQUE INDEX foobar_unique_idx ON foobar(foo, fizz);
            -- local indexes
            CREATE INDEX foobar_1_local_idx ON foobar_1(foo, bar);

            CREATE table bar(
                id VARCHAR(255) PRIMARY KEY,
                foo VARCHAR(255),
                bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
                fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                buzz REAL NOT NULL CHECK (buzz IS NOT NULL),
                FOREIGN KEY (foo, fizz) REFERENCES foobar (foo, fizz)
            );
            CREATE INDEX bar_normal_idx ON bar(bar);
            CREATE INDEX bar_another_normal_id ON bar(bar, fizz);
            CREATE UNIQUE INDEX bar_unique_idx on bar(fizz, buzz);

            CREATE TABLE fizz(
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE new_foobar(
                id INT,
                bar TIMESTAMPTZ NOT NULL,
                foo VARCHAR(255) DEFAULT 'some default' NOT NULL CHECK (LENGTH(foo) > 0),
                fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (foo, bar)
            ) PARTITION BY LIST(foo);

            CREATE TABLE foobar_1 PARTITION of new_foobar(
                fizz NOT NULL,
                PRIMARY KEY (foo, bar)
            ) FOR VALUES IN ('foobar_1_val_1', 'foobar_1_val_2');

            -- local indexes
            CREATE INDEX foobar_1_local_idx ON foobar_1(foo, bar);
            -- partitioned indexes
            CREATE INDEX foobar_normal_idx ON new_foobar(foo, bar);
            CREATE UNIQUE INDEX foobar_unique_idx ON new_foobar(foo, fizz);

            CREATE table bar(
                id VARCHAR(255) PRIMARY KEY,
                foo VARCHAR(255),
                bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
                fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                buzz REAL NOT NULL CHECK (buzz IS NOT NULL),
                   FOREIGN KEY (foo, fizz) REFERENCES new_foobar (foo, fizz)
            );
            CREATE INDEX bar_normal_idx ON bar(bar);
            CREATE INDEX bar_another_normal_id ON bar(bar, fizz);
            CREATE UNIQUE INDEX bar_unique_idx on bar(fizz, buzz);

            CREATE TABLE fizz(
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresShareRowExclusiveLock,
			diff.MigrationHazardTypeDeletesData,
		},

		expectedDBSchemaDDL: []string{`
            -- Create a table in a different schema to validate it is being ignored (no delete operation).
            CREATE SCHEMA schema_filtered_1;
            CREATE TABLE schema_filtered_1.foo();    

            CREATE TABLE new_foobar(
                id INT,
                bar TIMESTAMPTZ NOT NULL,
                foo VARCHAR(255) DEFAULT 'some default' NOT NULL CHECK (LENGTH(foo) > 0),
                fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (foo, bar)
            ) PARTITION BY LIST(foo);

            CREATE TABLE foobar_1 PARTITION of new_foobar(
                fizz NOT NULL,
                PRIMARY KEY (foo, bar)
            ) FOR VALUES IN ('foobar_1_val_1', 'foobar_1_val_2');

            -- local indexes
            CREATE INDEX foobar_1_local_idx ON foobar_1(foo, bar);
            -- partitioned indexes
            CREATE INDEX foobar_normal_idx ON new_foobar(foo, bar);
            CREATE UNIQUE INDEX foobar_unique_idx ON new_foobar(foo, fizz);

            CREATE table bar(
                id VARCHAR(255) PRIMARY KEY,
                foo VARCHAR(255),
                bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
                fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                buzz REAL NOT NULL CHECK (buzz IS NOT NULL),
                   FOREIGN KEY (foo, fizz) REFERENCES new_foobar (foo, fizz)
            );
            CREATE INDEX bar_normal_idx ON bar(bar);
            CREATE INDEX bar_another_normal_id ON bar(bar, fizz);
            CREATE UNIQUE INDEX bar_unique_idx on bar(fizz, buzz);

            CREATE TABLE fizz(
            );
			`},

		// Ensure that we're maintaining backwards compatibility with the old generate plan func
		planFactory: diff.GeneratePlan,
	},
}

func (suite *acceptanceTestSuite) TestBackCompatTestCases() {
	suite.runTestCases(backCompatAcceptanceTestCases)
}
