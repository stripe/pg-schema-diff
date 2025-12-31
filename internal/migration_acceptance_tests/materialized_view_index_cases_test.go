package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var materializedViewIndexAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "Add index to existing materialized view",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255)
            );

            CREATE MATERIALIZED VIEW foobar_view AS
                SELECT id, foo FROM foobar;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255)
            );

            CREATE MATERIALIZED VIEW foobar_view AS
                SELECT id, foo FROM foobar;

            CREATE INDEX foobar_view_foo_idx ON foobar_view(foo);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Drop index from materialized view",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255)
            );

            CREATE MATERIALIZED VIEW foobar_view AS
                SELECT id, foo FROM foobar;

            CREATE INDEX foobar_view_foo_idx ON foobar_view(foo);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255)
            );

            CREATE MATERIALIZED VIEW foobar_view AS
                SELECT id, foo FROM foobar;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexDropped,
		},
	},
	{
		name: "Add unique index to materialized view",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255)
            );

            CREATE MATERIALIZED VIEW foobar_view AS
                SELECT id, foo FROM foobar;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255)
            );

            CREATE MATERIALIZED VIEW foobar_view AS
                SELECT id, foo FROM foobar;

            CREATE UNIQUE INDEX foobar_view_id_idx ON foobar_view(id);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Change index columns on materialized view",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar VARCHAR(255)
            );

            CREATE MATERIALIZED VIEW foobar_view AS
                SELECT id, foo, bar FROM foobar;

            CREATE INDEX foobar_view_idx ON foobar_view(foo);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar VARCHAR(255)
            );

            CREATE MATERIALIZED VIEW foobar_view AS
                SELECT id, foo, bar FROM foobar;

            CREATE INDEX foobar_view_idx ON foobar_view(bar);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexDropped,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
}

func TestMaterializedViewIndexTestCases(t *testing.T) {
	runTestCases(t, materializedViewIndexAcceptanceTestCases)
}
