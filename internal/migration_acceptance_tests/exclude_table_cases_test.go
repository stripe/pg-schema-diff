package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var excludeTableAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "Excluded table only in old schema is not dropped",
		oldSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
			CREATE INDEX tmp_foo_idx ON tmp_foo(id);
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
		`},
		planOpts:        []diff.PlanOpt{diff.WithExcludeTablePatterns("tmp_.*")},
		expectEmptyPlan: true,
		expectedDBSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
			CREATE INDEX tmp_foo_idx ON tmp_foo(id);
		`},
	},
	{
		name: "Excluded table only in new schema is not created",
		oldSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
		`},
		planOpts:        []diff.PlanOpt{diff.WithExcludeTablePatterns("tmp_.*")},
		expectEmptyPlan: true,
		expectedDBSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
		`},
	},
	{
		name: "Changes to non-excluded tables are still planned",
		oldSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY, new_col TEXT);
		`},
		planOpts: []diff.PlanOpt{diff.WithExcludeTablePatterns("tmp_.*")},
		expectedDBSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY, new_col TEXT);
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
		`},
	},
	{
		name: "Schema-qualified pattern only excludes tables in that schema",
		oldSchemaDDL: []string{`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.tmp_foo(id INT PRIMARY KEY);
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
		`},
		newSchemaDDL: []string{`
			CREATE SCHEMA schema_1;
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
		`},
		planOpts:        []diff.PlanOpt{diff.WithExcludeTablePatterns(`schema_1\.tmp_foo`)},
		expectEmptyPlan: true,
		expectedDBSchemaDDL: []string{`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.tmp_foo(id INT PRIMARY KEY);
			CREATE TABLE tmp_foo(id INT PRIMARY KEY);
		`},
	},
	{
		name: "Partitions of an excluded partitioned table are also excluded",
		oldSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
			CREATE TABLE tmp_events(id INT) PARTITION BY RANGE (id);
			CREATE TABLE events_p1 PARTITION OF tmp_events FOR VALUES FROM (0) TO (100);
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
		`},
		planOpts:        []diff.PlanOpt{diff.WithExcludeTablePatterns("tmp_.*")},
		expectEmptyPlan: true,
		expectedDBSchemaDDL: []string{`
			CREATE TABLE foobar(id INT PRIMARY KEY);
			CREATE TABLE tmp_events(id INT) PARTITION BY RANGE (id);
			CREATE TABLE events_p1 PARTITION OF tmp_events FOR VALUES FROM (0) TO (100);
		`},
	},
}

func TestExcludeTableTestCases(t *testing.T) {
	runTestCases(t, excludeTableAcceptanceTestCases)
}
