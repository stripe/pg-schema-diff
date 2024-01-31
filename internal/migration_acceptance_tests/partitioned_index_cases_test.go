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
			    PRIMARY KEY (foo, id),
				UNIQUE (foo, bar)
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
			    PRIMARY KEY (foo, id),
			    UNIQUE (foo, bar)
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
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a unique constraint",
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
				UNIQUE (foo, id)
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');
			CREATE TABLE foobar_3 PARTITION OF foobar FOR VALUES IN ('foo_3');
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
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
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
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
			diff.MigrationHazardTypeIndexBuild,
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
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Add a partitioned unique constraint when the local index already exists",
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
			ALTER TABLE foobar ADD CONSTRAINT foobar_unique UNIQUE (foo, id);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
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
		name: "Delete a unique constraint",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
				foo VARCHAR(255),
			    UNIQUE (foo, id)
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
		name: "Local index and columns deleted (index dropped first)",
		oldSchemaDDL: []string{`
			CREATE TABLE foobar(
				id INT,
				bar INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			CREATE INDEX some_idx ON foobar(foo, id);
			CREATE INDEX foobar_1_some_local_idx ON foobar_1(foo, bar, id);
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foobar(
				bar INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
		`},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeDeletesData,
		},
		ddl: []string{
			"DROP INDEX CONCURRENTLY \"public\".\"foobar_1_some_local_idx\"",
			"DROP INDEX \"public\".\"some_idx\"",
			"ALTER TABLE \"public\".\"foobar\" DROP COLUMN \"id\"",
		},
	},
	{
		name: "Alter index columns (index replacement and prioritized builds)",
		oldSchemaDDL: []string{`
			CREATE TABLE foobar(
				id INT,
				bar INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');

			CREATE INDEX some_idx ON foobar(foo, id);
			CREATE INDEX old_idx ON foobar_1(foo, bar);

			CREATE INDEX foobar_1_some_local_idx ON foobar_1(foo, bar, id);
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foobar(
				id INT,
				bar INT,
				foo VARCHAR(255)
			) PARTITION BY LIST (foo);
			
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');
			CREATE TABLE foobar_2 PARTITION OF foobar FOR VALUES IN ('foo_2');

			ALTER TABLE foobar ADD CONSTRAINT some_idx PRIMARY KEY (foo, id);
			CREATE INDEX new_idx ON foobar_1(foo, bar);

			CREATE INDEX new_foobar_1_some_local_idx ON foobar_1(foo, bar, id);
		`},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
		ddl: []string{
			"ALTER INDEX \"public\".\"some_idx\" RENAME TO \"pgschemadiff_tmpidx_some_idx_MDEyMzQ1Rje4OTo7PD0$Pw\"",
			"ALTER TABLE \"public\".\"foobar\" ADD CONSTRAINT \"pgschemadiff_tmpnn_EBESExQVRheYGRobHB0eHw\" CHECK(\"id\" IS NOT NULL) NOT VALID",
			"ALTER TABLE \"public\".\"foobar\" VALIDATE CONSTRAINT \"pgschemadiff_tmpnn_EBESExQVRheYGRobHB0eHw\"",
			"ALTER TABLE \"public\".\"foobar\" ADD CONSTRAINT \"pgschemadiff_tmpnn_ICEiIyQlRieoKSorLC0uLw\" CHECK(\"foo\" IS NOT NULL) NOT VALID",
			"ALTER TABLE \"public\".\"foobar\" VALIDATE CONSTRAINT \"pgschemadiff_tmpnn_ICEiIyQlRieoKSorLC0uLw\"",
			"ALTER TABLE \"public\".\"foobar\" ALTER COLUMN \"foo\" SET NOT NULL",
			"ALTER TABLE \"public\".\"foobar\" ALTER COLUMN \"id\" SET NOT NULL",
			"ALTER TABLE \"public\".\"foobar\" DROP CONSTRAINT \"pgschemadiff_tmpnn_EBESExQVRheYGRobHB0eHw\"",
			"ALTER TABLE \"public\".\"foobar\" DROP CONSTRAINT \"pgschemadiff_tmpnn_ICEiIyQlRieoKSorLC0uLw\"",
			"ALTER TABLE ONLY \"public\".\"foobar\" ADD CONSTRAINT \"some_idx\" PRIMARY KEY (foo, id)",
			"CREATE UNIQUE INDEX CONCURRENTLY foobar_1_pkey ON public.foobar_1 USING btree (foo, id)",
			"ALTER TABLE \"public\".\"foobar_1\" ADD CONSTRAINT \"foobar_1_pkey\" PRIMARY KEY USING INDEX \"foobar_1_pkey\"",
			"ALTER INDEX \"public\".\"some_idx\" ATTACH PARTITION \"public\".\"foobar_1_pkey\"",
			"CREATE INDEX CONCURRENTLY new_foobar_1_some_local_idx ON public.foobar_1 USING btree (foo, bar, id)",
			"CREATE INDEX CONCURRENTLY new_idx ON public.foobar_1 USING btree (foo, bar)",
			"CREATE UNIQUE INDEX CONCURRENTLY foobar_2_pkey ON public.foobar_2 USING btree (foo, id)",
			"ALTER TABLE \"public\".\"foobar_2\" ADD CONSTRAINT \"foobar_2_pkey\" PRIMARY KEY USING INDEX \"foobar_2_pkey\"",
			"ALTER INDEX \"public\".\"some_idx\" ATTACH PARTITION \"public\".\"foobar_2_pkey\"",
			"DROP INDEX CONCURRENTLY \"public\".\"foobar_1_some_local_idx\"",
			"DROP INDEX CONCURRENTLY \"public\".\"old_idx\"",
			"DROP INDEX \"public\".\"pgschemadiff_tmpidx_some_idx_MDEyMzQ1Rje4OTo7PD0$Pw\"",
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
	{
		name: "Add primary key constraint with existing matching base-table index (matching child index does not exist)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			-- A unique index for foobar_1 exists, but it does not have the default name as the primary key constraint (foobar_1_pkey),
			-- so the primary key index effectively does not already exist when migrating
			CREATE UNIQUE INDEX foobar_pkey ON foobar(foo, id);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			ALTER TABLE foobar ADD CONSTRAINT foobar_pkey PRIMARY KEY (foo, id);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			// The parent table index is dropped and rebuilt
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
		},
	},
	{
		name: "Add unique constraint with existing matching base-table index (matching child index does not exist)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			-- A unique index for foobar_1 exists, but it does not have the default name as the unique constraint (foobar_1_foo_id_key),
			-- so the primary key index effectively does not already exist when migrating
			CREATE UNIQUE INDEX foobar_unique ON foobar(foo, id);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			ALTER TABLE foobar ADD CONSTRAINT foobar_unique UNIQUE (foo, id);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			// The parent table index is dropped and rebuilt
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
		},
	},
	{
		name: "Add primary key constraint with existing matching base-table index (local matching child index exists)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			CREATE UNIQUE INDEX foobar_pkey ON ONLY foobar(foo, id);
			CREATE UNIQUE INDEX foobar_1_pkey ON foobar_1(foo, id);
			ALTER INDEX foobar_pkey ATTACH PARTITION foobar_1_pkey;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			ALTER TABLE foobar ADD CONSTRAINT foobar_pkey PRIMARY KEY (foo, id);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			// The parent table index is dropped and rebuilt
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
		},
	},
	{
		name: "Add primary key constraint with existing matching base-table index (matching non-local index exists that backs local matching PK)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			CREATE UNIQUE INDEX foobar_pkey ON foobar(foo, id);
			ALTER TABLE foobar_1 ADD CONSTRAINT foobar_1_pkey PRIMARY KEY USING INDEX foobar_1_foo_id_idx;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			ALTER TABLE foobar ADD CONSTRAINT foobar_pkey PRIMARY KEY (foo, id);
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
		name: "Add unique constraint with existing matching base-table index (matching non-local index exists that backs local matching PK)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			CREATE UNIQUE INDEX foobar_foo_id_key ON ONLY foobar(foo, id);
			CREATE UNIQUE INDEX foobar_1_foo_id_key ON foobar_1(foo, id);
			ALTER INDEX foobar_foo_id_key ATTACH PARTITION foobar_1_foo_id_key;
			ALTER TABLE foobar_1 ADD CONSTRAINT foobar_1_pkey PRIMARY KEY USING INDEX foobar_1_foo_id_key;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			ALTER TABLE foobar ADD CONSTRAINT foobar_foo_id_key UNIQUE (foo, id);
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
		name: "Add unique constraint with existing matching base-table index (matching non-local index exists that backs local matching unique constraint)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			CREATE UNIQUE INDEX foobar_foo_id_key ON ONLY foobar(foo, id);
			CREATE UNIQUE INDEX foobar_1_foo_id_key ON foobar_1(foo, id);
			ALTER INDEX foobar_foo_id_key ATTACH PARTITION foobar_1_foo_id_key;
			ALTER TABLE foobar_1 ADD CONSTRAINT foobar_1_pkey UNIQUE USING INDEX foobar_1_foo_id_key;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			ALTER TABLE foobar ADD CONSTRAINT foobar_foo_id_key UNIQUE (foo, id);
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
		name: "Add primary key constraint with existing matching base-table index (matching local PK already exists backed by local index)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			CREATE UNIQUE INDEX foobar_pkey ON ONLY foobar(foo, id);
			ALTER TABLE foobar_1 ADD CONSTRAINT foobar_1_pkey PRIMARY KEY(foo, id);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			ALTER TABLE foobar ADD CONSTRAINT foobar_pkey PRIMARY KEY (foo, id);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			// Base table index is dropped
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
		},
	},
	{
		name: "Add unique constraint with existing matching base-table index (matching local PK already exists backed by local index)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			CREATE UNIQUE INDEX foobar_foo_id_key ON ONLY foobar(foo, id);
			ALTER TABLE foobar_1 ADD CONSTRAINT foobar_1_foo_id_key UNIQUE (foo, id);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			ALTER TABLE foobar ADD CONSTRAINT foobar_foo_id_key UNIQUE (foo, id);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			// Base table index is dropped
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
		},
	},
	{
		name: "Add primary key to partition using existing index",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			ALTER TABLE ONLY foobar ADD CONSTRAINT some_pkey PRIMARY KEY (foo, id);
			CREATE UNIQUE INDEX foobar_1_pkey ON foobar_1(foo, id);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			ALTER TABLE ONLY foobar ADD CONSTRAINT some_pkey PRIMARY KEY (foo, id);
			CREATE UNIQUE INDEX foobar_1_pkey ON foobar_1(foo, id);
			ALTER TABLE foobar_1 ADD CONSTRAINT foobar_1_pkey PRIMARY KEY USING INDEX foobar_1_pkey;
			ALTER INDEX some_pkey ATTACH PARTITION foobar_1_pkey;
			`,
		},
	},
	{
		name: "Add unique constraint to partition using existing index",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			ALTER TABLE ONLY foobar ADD CONSTRAINT foobar_foo_id_key UNIQUE (foo, id);
			CREATE UNIQUE INDEX foobar_1_foo_id_key ON foobar_1(foo, id);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT NOT NULL,
				foo VARCHAR(255) NOT NULL
			) PARTITION BY LIST (foo);
			CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('foo_1');

			ALTER TABLE ONLY foobar ADD CONSTRAINT foobar_foo_id_key UNIQUE (foo, id);
			CREATE UNIQUE INDEX foobar_1_foo_id_key ON foobar_1(foo, id);
			ALTER TABLE foobar_1 ADD CONSTRAINT foobar_1_foo_id_key UNIQUE USING INDEX foobar_1_foo_id_key;
			ALTER INDEX foobar_foo_id_key ATTACH PARTITION foobar_1_foo_id_key;
			`,
		},
	},
}

func (suite *acceptanceTestSuite) TestPartitionedIndexAcceptanceTestCases() {
	suite.runTestCases(partitionedIndexAcceptanceTestCases)
}
