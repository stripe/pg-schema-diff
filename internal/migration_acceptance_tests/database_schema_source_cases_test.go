package migration_acceptance_tests

import (
	"context"
	"fmt"

	"github.com/stripe/pg-schema-diff/pkg/diff"
	"github.com/stripe/pg-schema-diff/pkg/sqldb"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

func databaseSchemaSourcePlan(ctx context.Context, connPool sqldb.Queryable, tempDbFactory tempdb.Factory, newSchemaDDL []string, opts ...diff.PlanOpt) (_ diff.Plan, retErr error) {
	newSchemaDb, err := tempDbFactory.Create(ctx)
	if err != nil {
		return diff.Plan{}, fmt.Errorf("creating temp database: %w", err)
	}

	defer func() {
		tempDbErr := newSchemaDb.Close(ctx)
		if retErr == nil {
			retErr = tempDbErr
		}
	}()

	for _, stmt := range newSchemaDDL {
		if _, err := newSchemaDb.ConnPool.ExecContext(ctx, stmt); err != nil {
			return diff.Plan{}, fmt.Errorf("running DDL: %w", err)
		}
	}

	// Clone the opts so we don't modify the original.
	opts = append([]diff.PlanOpt(nil), opts...)
	opts = append(opts, diff.WithTempDbFactory(tempDbFactory))
	for _, o := range newSchemaDb.ExcludeMetadataOptions {
		opts = append(opts, diff.WithGetSchemaOpts(o))
	}

	return diff.Generate(ctx, diff.DBSchemaSource(connPool), diff.DBSchemaSource(newSchemaDb.ConnPool), opts...)
}

func dirSchemaSourcePlanFactory(schemaDirs []string) planFactory {
	return func(ctx context.Context, connPool sqldb.Queryable, tempDbFactory tempdb.Factory, newSchemaDDL []string, opts ...diff.PlanOpt) (_ diff.Plan, retErr error) {
		// Clone the opts so we don't modify the original.
		opts = append([]diff.PlanOpt(nil), opts...)
		opts = append(opts, diff.WithTempDbFactory(tempDbFactory))

		if len(newSchemaDDL) != 0 {
			panic("newSchemaDDL should be empty for dir schema sources")
		}

		schemaSource, err := diff.DirSchemaSource(schemaDirs)
		if err != nil {
			return diff.Plan{}, fmt.Errorf("creating schema source: %w", err)
		}

		connSource := diff.DBSchemaSource(connPool)

		return diff.Generate(ctx, connSource, schemaSource, opts...)
	}
}

var databaseSchemaSourceTestCases = []acceptanceTestCase{
	{
		planFactory: databaseSchemaSourcePlan,

		name: "Drop partitioned table, Add partitioned table with local keys",
		oldSchemaDDL: []string{
			`
            CREATE TABLE fizz();

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

			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE fizz();

            CREATE SCHEMA schema_1;
            CREATE TABLE schema_1.foobar(
                bar TIMESTAMPTZ NOT NULL,
                fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                id INT,
                foo VARCHAR(255) DEFAULT 'some default' NOT NULL CHECK (LENGTH(foo) > 0),
                UNIQUE (foo, bar)
            ) PARTITION BY LIST(foo);

            CREATE TABLE schema_1.foobar_1 PARTITION of schema_1.foobar(
                fizz NOT NULL,
                PRIMARY KEY (foo, bar)
            ) FOR VALUES IN ('foobar_1_val_1', 'foobar_1_val_2');

            -- local indexes
            CREATE INDEX foobar_1_local_idx ON schema_1.foobar_1(foo, bar);
            -- partitioned indexes
            CREATE INDEX foobar_normal_idx ON schema_1.foobar(foo, bar);
            CREATE UNIQUE INDEX foobar_unique_idx ON schema_1.foobar(foo, fizz);

            CREATE table bar(
                id VARCHAR(255) PRIMARY KEY,
                foo VARCHAR(255),
                bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
                fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                buzz REAL NOT NULL CHECK (buzz IS NOT NULL),
                   FOREIGN KEY (foo, fizz) REFERENCES schema_1.foobar (foo, fizz)
            );
            CREATE INDEX bar_normal_idx ON bar(bar);
            CREATE INDEX bar_another_normal_id ON bar(bar, fizz);
            CREATE UNIQUE INDEX bar_unique_idx on bar(fizz, buzz);

			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		planFactory: dirSchemaSourcePlanFactory([]string{"testdata/dirsrc_happy_path/schema_0", "testdata/dirsrc_happy_path/schema_1"}),

		name: "Dir src - happy path",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar( );
			`,
		},

		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
		expectedDBSchemaDDL: []string{
			`
                CREATE TYPE color AS ENUM ('red', 'green', 'blue');
                CREATE TABLE foobar(
                    color color,
                    id varchar(255) PRIMARY KEY
                );
                CREATE TABLE foobar_fk(
                    id TEXT REFERENCES foobar(id)
                );
                CREATE TABLE fizzbuzz(
                    id            TEXT,
                    primary_color color,
                    other_color   color
				);			`,
		},
	},
	{
		planFactory: dirSchemaSourcePlanFactory([]string{"testdata/dirsrc_invalid_sql/schema_0"}),

		name: "Dir src - invalid sql",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar( );
			`,
		},

		expectedPlanErrorContains: "testdata/dirsrc_invalid_sql/schema_0/1.sql",
	},
}

func (suite *acceptanceTestSuite) TestDatabaseSchemaSourceTestCases() {
	suite.runTestCases(databaseSchemaSourceTestCases)
}
