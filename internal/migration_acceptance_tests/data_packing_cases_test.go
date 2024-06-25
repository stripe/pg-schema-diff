package migration_acceptance_tests

import "github.com/stripe/pg-schema-diff/pkg/diff"

var dataPackingCases = []acceptanceTestCase{
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

		expectedDBSchemaDDL: []string{`
			CREATE TABLE foobar(
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    id INT PRIMARY KEY CHECK (id > 0), CHECK (id < buzz),
			    fizz SERIAL NOT NULL UNIQUE,
				buzz REAL CHECK (buzz IS NOT NULL),
				foo VARCHAR(255) COLLATE "POSIX" DEFAULT '' NOT NULL
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
		name:         "Create partitioned table with shared primary key and RLS enabled globally",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1."Foobar"(
			    id INT,
				foo VARCHAR(255),
				bar TEXT COLLATE "POSIX" NOT NULL DEFAULT 'some default',
				fizz SERIAL,
				CHECK ( fizz > 0 ),
			    PRIMARY KEY (foo, id),
			    UNIQUE (foo, bar)
			) PARTITION BY LIST (foo);
			ALTER TABLE schema_1."Foobar" REPLICA IDENTITY FULL;
			
			ALTER TABLE schema_1."Foobar" ENABLE ROW LEVEL SECURITY;
			ALTER TABLE schema_1."Foobar" FORCE ROW LEVEL SECURITY;

			-- partitions
			CREATE SCHEMA schema_2;
			CREATE TABLE schema_2."FOOBAR_1" PARTITION OF schema_1."Foobar"(
			    foo NOT NULL,
			    bar NOT NULL
			) FOR VALUES IN ('foo_1');
			ALTER TABLE schema_2."FOOBAR_1" REPLICA IDENTITY NOTHING ;
			CREATE TABLE schema_2.foobar_2 PARTITION OF schema_1."Foobar" FOR VALUES IN ('foo_2');
			ALTER TABLE schema_2.foobar_2 REPLICA IDENTITY FULL;
			CREATE TABLE schema_2.foobar_3 PARTITION OF schema_1."Foobar" FOR VALUES IN ('foo_3');
			-- partitioned indexes
			CREATE UNIQUE INDEX foobar_unique_idx ON schema_1."Foobar"(foo, fizz);
			-- local indexes
			CREATE INDEX foobar_1_local_idx ON schema_2."FOOBAR_1"(foo);
			CREATE UNIQUE INDEX foobar_2_local_unique_idx ON schema_2.foobar_2(foo);
			`,
		},

		expectedDBSchemaDDL: []string{`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1."Foobar"(
			    id INT,
				fizz SERIAL,
				foo VARCHAR(255),
				bar TEXT COLLATE "POSIX" NOT NULL DEFAULT 'some default',
				CHECK ( fizz > 0 ),
			    PRIMARY KEY (foo, id),
			    UNIQUE (foo, bar)
			) PARTITION BY LIST (foo);
			ALTER TABLE schema_1."Foobar" REPLICA IDENTITY FULL;

			ALTER TABLE schema_1."Foobar" ENABLE ROW LEVEL SECURITY;
			ALTER TABLE schema_1."Foobar" FORCE ROW LEVEL SECURITY;

			-- partitions
			CREATE SCHEMA schema_2;
			CREATE TABLE schema_2."FOOBAR_1" PARTITION OF schema_1."Foobar"(
			    foo NOT NULL,
			    bar NOT NULL
			) FOR VALUES IN ('foo_1');
			ALTER TABLE schema_2."FOOBAR_1" REPLICA IDENTITY NOTHING ;
			CREATE TABLE schema_2.foobar_2 PARTITION OF schema_1."Foobar" FOR VALUES IN ('foo_2');
			ALTER TABLE schema_2.foobar_2 REPLICA IDENTITY FULL;
			CREATE TABLE schema_2.foobar_3 PARTITION OF schema_1."Foobar" FOR VALUES IN ('foo_3');
			-- partitioned indexes
			CREATE UNIQUE INDEX foobar_unique_idx ON schema_1."Foobar"(foo, fizz);
			-- local indexes
			CREATE INDEX foobar_1_local_idx ON schema_2."FOOBAR_1"(foo);
			CREATE UNIQUE INDEX foobar_2_local_unique_idx ON schema_2.foobar_2(foo);
				`,
		},
	},
}

func (suite *acceptanceTestSuite) TestDataPackingTestCases() {
	var tcs []acceptanceTestCase
	for _, tc := range dataPackingCases {
		tc.planOpts = append(tc.planOpts, diff.WithDataPackNewTables())
		tcs = append(tcs, tc)
	}
	suite.runTestCases(tcs)
}
