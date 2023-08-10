package migration_acceptance_tests

import "github.com/stripe/pg-schema-diff/pkg/diff"

var foreignKeyConstraintCases = []acceptanceTestCase{
	{
		name: "No-op",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id_part_1 INT,
			    id_part_2 VARCHAR(255),
			    PRIMARY KEY (id_part_1, id_part_2)
			);
			CREATE TABLE "foobar fk"(
			    fk_part_1 BIGINT,
			    fk_part_2 VARCHAR(255),
			    FOREIGN KEY (fk_part_1, fk_part_2) REFERENCES foobar(id_part_1, id_part_2)
					ON DELETE SET NULL
			        ON UPDATE SET NULL
			        NOT DEFERRABLE
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id_part_1 INT,
			    id_part_2 VARCHAR(255),
			    PRIMARY KEY (id_part_1, id_part_2)
			);
			CREATE TABLE "foobar fk"(
			    fk_part_1 BIGINT,
			    fk_part_2 VARCHAR(255),
			    FOREIGN KEY (fk_part_1, fk_part_2) REFERENCES foobar(id_part_1, id_part_2)
					ON DELETE SET NULL
			        ON UPDATE SET NULL
			        NOT DEFERRABLE
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
		name: "Add FK with most options",
		oldSchemaDDL: []string{
			`
			CREATE TABLE "foo bar"(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE "foo bar"(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT,
			    FOREIGN KEY (fk_id) REFERENCES "foo bar"(id)
					ON DELETE SET NULL
			        ON UPDATE SET NULL
			        NOT DEFERRABLE
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresShareRowExclusiveLock,
		},
	},
	{
		name: "Add FK (only referenced table is new)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT,
			    FOREIGN KEY (fk_id) REFERENCES foobar(id)
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresShareRowExclusiveLock,
		},
	},
	{
		name: "Add FK (owning table is not new)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE "foobar fk"(
			    fk_id INT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT,
			    FOREIGN KEY (fk_id) REFERENCES foobar(id)
					ON DELETE SET NULL
			        ON UPDATE SET NULL
			        NOT DEFERRABLE
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresShareRowExclusiveLock,
		},
	},
	{
		name: "Add FK (both tables new)",
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT,
			    FOREIGN KEY (fk_id) REFERENCES foobar(id)
					ON DELETE SET NULL
			        ON UPDATE SET NULL
			        NOT DEFERRABLE
			);
			`,
		},
	},
	{
		name: "Add not-valid FK (neither table is new)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT
			);
			ALTER TABLE "foobar fk" ADD CONSTRAINT some_foobar_fk
			    FOREIGN KEY (fk_id) REFERENCES foobar(id)
			        NOT VALID;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{},
	},
	{
		name: "Add FK (partitioned table)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    foo VARCHAR(255),
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('2');
			CREATE TABLE "foobar fk"(
			    fk_foo VARCHAR(255),
			    fk_id INT
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    foo VARCHAR(255),
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('2');
			CREATE TABLE "foobar fk"(
			    fk_foo VARCHAR(255),
			    fk_id INT,
			    FOREIGN KEY (fk_foo, fk_id) REFERENCES foobar(foo, id)
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresShareRowExclusiveLock,
		},
	},
	{
		name: "Drop FK",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT,
			    FOREIGN KEY (fk_id) REFERENCES foobar(id)
					ON DELETE SET NULL
			        ON UPDATE SET NULL
			        NOT DEFERRABLE
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT
			);
			`,
		},
	},
	{
		name: "Drop FK (partitioned table)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    foo VARCHAR(255),
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('2');
			CREATE TABLE "foobar fk"(
			    fk_foo VARCHAR(255),
			    fk_id INT,
			    FOREIGN KEY (fk_foo, fk_id) REFERENCES foobar(foo, id)
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    foo VARCHAR(255),
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('2');
			CREATE TABLE "foobar fk"(
			    fk_foo VARCHAR(255),
			    fk_id INT
			);
			`,
		},
	},
	{
		name: "Alter FK (not valid to valid)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT
			);
			ALTER TABLE "foobar fk" ADD CONSTRAINT some_fk
			    FOREIGN KEY (fk_id) REFERENCES foobar(id)
				NOT VALID;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT
			);
			ALTER TABLE "foobar fk" ADD CONSTRAINT some_fk
			    FOREIGN KEY (fk_id) REFERENCES foobar(id);
			`,
		},
	},
	{
		name: "Alter FK (valid to not valid)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT
			);
			ALTER TABLE "foobar fk" ADD CONSTRAINT some_fk
			    FOREIGN KEY (fk_id) REFERENCES foobar(id);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT
			);
			ALTER TABLE "foobar fk" ADD CONSTRAINT some_fk
			    FOREIGN KEY (fk_id) REFERENCES foobar(id)
				NOT VALID;
			`,
		},
	},
	{
		name: "Alter FK (columns)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    foo VARCHAR(255),
			    PRIMARY KEY (id)
			);
			CREATE UNIQUE INDEX foobar_unique ON foobar(foo);

			CREATE TABLE "foobar fk"(
			    fk_id INT,
			    fk_foo VARCHAR(255)
			);
			ALTER TABLE "foobar fk" ADD CONSTRAINT some_fk
			    FOREIGN KEY (fk_id) REFERENCES foobar(id)
				NOT VALID;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    foo VARCHAR(255),
			    PRIMARY KEY (id)
			);
			CREATE UNIQUE INDEX foobar_unique ON foobar(foo);

			CREATE TABLE "foobar fk"(
			    fk_id INT,
			    fk_foo VARCHAR(255)
			);
			ALTER TABLE "foobar fk" ADD CONSTRAINT some_fk
			    FOREIGN KEY (fk_foo) REFERENCES foobar(foo);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresShareRowExclusiveLock,
		},
	},
	{
		name: "Alter FK (on update)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT,
				FOREIGN KEY (fk_id) REFERENCES foobar(id)
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT,
				FOREIGN KEY (fk_id) REFERENCES foobar(id)
				  ON UPDATE CASCADE
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresShareRowExclusiveLock,
		},
	},
	{
		name: "Alter FK (on delete)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT,
			    FOREIGN KEY (fk_id) REFERENCES foobar(id)
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT,
			    FOREIGN KEY (fk_id) REFERENCES foobar(id)
					ON UPDATE CASCADE
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresShareRowExclusiveLock,
		},
	},
	{
		name: "Alter castable type change",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT,
			    FOREIGN KEY (fk_id) REFERENCES foobar(id)
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id BIGINT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT,
			    FOREIGN KEY (fk_id) REFERENCES foobar(id)
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "Alter non-castable type change",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id BIGINT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id BIGINT,
			    FOREIGN KEY (fk_id) REFERENCES foobar(id)
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id TIMESTAMP,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id TIMESTAMP,
			    FOREIGN KEY (fk_id) REFERENCES foobar(id)
			);
			`,
		},
		vanillaExpectations: expectations{
			planErrorContains: errValidatingPlan.Error(),
		},
		dataPackingExpectations: expectations{
			planErrorContains: errValidatingPlan.Error(),
		},
	},
	{
		name: "Switch FK owning table (to partitioned table)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT
			);
			ALTER TABLE "foobar fk" ADD CONSTRAINT some_fk
				FOREIGN KEY (fk_id) REFERENCES foobar(id);

			CREATE TABLE "foobar fk partitioned"(
			    foo varchar(255),
			    fk_id INT
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF "foobar fk partitioned"  FOR VALUES IN ('1');
			CREATE TABLE foobar_2 PARTITION OF "foobar fk partitioned"  FOR VALUES IN ('2');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar fk"(
			    fk_id INT
			);

			CREATE TABLE "foobar fk partitioned"(
			    foo varchar(255),
			    fk_id INT
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF "foobar fk partitioned" FOR VALUES IN ('1');
			CREATE TABLE foobar_2 PARTITION OF "foobar fk partitioned" FOR VALUES IN ('2');
			ALTER TABLE "foobar fk partitioned" ADD CONSTRAINT some_fk
				FOREIGN KEY (fk_id) REFERENCES foobar(id);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresShareRowExclusiveLock,
		},
	},
	{
		name: "Switch FK referenced table (to partitioned table with new unique index)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    foo varchar(255),
			    id INT,
			    PRIMARY KEY (id)
			);
			CREATE UNIQUE INDEX unique_idx ON foobar(foo, id);

			CREATE TABLE "foobar partitioned"(
			    foo varchar(255),
			    id INT
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF "foobar partitioned" FOR VALUES IN ('1');
			CREATE TABLE foobar_2 PARTITION OF "foobar partitioned" FOR VALUES IN ('2');

			CREATE TABLE "foobar fk"(
			    fk_foo VARCHAR(255),
			    fk_id INT
			);
			ALTER TABLE "foobar fk" ADD CONSTRAINT some_fk
				FOREIGN KEY (fk_foo, fk_id) REFERENCES foobar(foo, id) NOT VALID ;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    foo varchar(255),
			    id INT,
			    PRIMARY KEY (id)
			);

			CREATE TABLE "foobar partitioned"(
			    foo varchar(255),
			    id INT
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF "foobar partitioned" FOR VALUES IN ('1');
			CREATE TABLE foobar_2 PARTITION OF "foobar partitioned" FOR VALUES IN ('2');
			CREATE UNIQUE INDEX unique_idx ON "foobar partitioned"(foo, id);

			CREATE TABLE "foobar fk"(
			    fk_foo VARCHAR(255),
			    fk_id INT
			);
			ALTER TABLE "foobar fk" ADD CONSTRAINT some_fk
				FOREIGN KEY (fk_foo, fk_id) REFERENCES "foobar partitioned"(foo, id);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
			diff.MigrationHazardTypeAcquiresShareRowExclusiveLock,
		},
	},
}

func (suite *acceptanceTestSuite) TestForeignKeyConstraintTestCases() {
	suite.runTestCases(foreignKeyConstraintCases)
}
