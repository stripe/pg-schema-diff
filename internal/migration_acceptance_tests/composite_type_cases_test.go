package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var compositeTypeAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "no-op",
		oldSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text);
		`},
		newSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text);
		`},
		expectEmptyPlan: true,
	},
	{
		name:         "create composite type",
		oldSchemaDDL: []string{},
		newSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text);
		`},
	},
	{
		name: "drop composite type",
		oldSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text);
		`},
		newSchemaDDL: []string{},
	},
	{
		name: "drop nested composite types",
		oldSchemaDDL: []string{`
			CREATE TYPE inner_t AS (n int);
			CREATE TYPE outer_t AS (i inner_t, label text);
		`},
		newSchemaDDL: []string{},
	},
	{
		name:         "create composite type used by function",
		oldSchemaDDL: []string{},
		newSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text);
			CREATE FUNCTION mk_pair(x int, y text) RETURNS pair LANGUAGE sql AS 'SELECT (x, y)::pair';
		`},
	},
	{
		name: "drop composite type after dropping function that used it",
		oldSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text);
			CREATE FUNCTION mk_pair(x int, y text) RETURNS pair LANGUAGE sql AS 'SELECT (x, y)::pair';
		`},
		newSchemaDDL: []string{},
	},
	{
		name:         "create composite type with attributes that have collation",
		oldSchemaDDL: []string{},
		newSchemaDDL: []string{`
			CREATE TYPE labelled AS (id int, label text COLLATE "C");
		`},
	},
	{
		name:         "create nested composite types (one references the other)",
		oldSchemaDDL: []string{},
		newSchemaDDL: []string{`
			CREATE TYPE inner_t AS (n int);
			CREATE TYPE outer_t AS (i inner_t, label text);
		`},
	},
	// ─── Phase 2: drop+recreate cascade for function-only dependents ───
	{
		name: "alter composite type attrs - cascade through dependent function",
		oldSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text);
			CREATE FUNCTION mk_pair(x int, y text) RETURNS pair LANGUAGE sql AS 'SELECT (x, y)::pair';
		`},
		newSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text, c boolean);
			CREATE FUNCTION mk_pair(x int, y text, z boolean) RETURNS pair LANGUAGE sql AS 'SELECT (x, y, z)::pair';
		`},
	},
	{
		name: "alter composite type attrs - cascade through dependent procedure",
		oldSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text);
			CREATE PROCEDURE use_pair(p pair) LANGUAGE plpgsql AS $$ BEGIN END $$;
		`},
		newSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text, c boolean);
			CREATE PROCEDURE use_pair(p pair) LANGUAGE plpgsql AS $$ BEGIN END $$;
		`},
		// Procedures always carry the untrackable-deps hazard regardless of the
		// underlying composite-type recreation; pg-schema-diff cannot follow plpgsql
		// body references through pg_depend.
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeHasUntrackableDependencies,
		},
	},
	{
		name: "alter composite type attrs - cascade through multiple dependent functions",
		oldSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text);
			CREATE FUNCTION f_a(p pair) RETURNS int LANGUAGE sql AS 'SELECT (p).a';
			CREATE FUNCTION f_b(p pair) RETURNS text LANGUAGE sql AS 'SELECT (p).b';
		`},
		newSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text, c boolean);
			CREATE FUNCTION f_a(p pair) RETURNS int LANGUAGE sql AS 'SELECT (p).a';
			CREATE FUNCTION f_b(p pair) RETURNS text LANGUAGE sql AS 'SELECT (p).b';
		`},
	},
	{
		name: "alter composite type attrs - cascade through dependent function using array argument",
		oldSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text);
			CREATE FUNCTION f_items(p pair[]) RETURNS int LANGUAGE sql AS 'SELECT pg_catalog.cardinality(p)';
		`},
		newSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text, c boolean);
			CREATE FUNCTION f_items(p pair[]) RETURNS int LANGUAGE sql AS 'SELECT pg_catalog.cardinality(p)';
		`},
	},
	{
		name: "alter composite type attrs - cascade through dependent composite type and function",
		oldSchemaDDL: []string{`
			CREATE TYPE inner_t AS (n int);
			CREATE TYPE outer_t AS (i inner_t, label text);
			CREATE FUNCTION f_outer(p outer_t) RETURNS int LANGUAGE sql AS 'SELECT ((p).i).n';
		`},
		newSchemaDDL: []string{`
			CREATE TYPE inner_t AS (n int, extra text);
			CREATE TYPE outer_t AS (i inner_t, label text);
			CREATE FUNCTION f_outer(p outer_t) RETURNS int LANGUAGE sql AS 'SELECT ((p).i).n';
		`},
	},
	{
		name: "alter composite type attrs is unsupported when used by a table column",
		oldSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text);
			CREATE TABLE users (id int, attrs pair);
		`},
		newSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text, c boolean);
			CREATE TABLE users (id int, attrs pair);
		`},
		expectedPlanErrorIs: diff.ErrNotImplemented,
	},
	{
		name: "alter composite type attrs is unsupported when dependent composite type is used by a table column",
		oldSchemaDDL: []string{`
			CREATE TYPE inner_t AS (n int);
			CREATE TYPE outer_t AS (i inner_t, label text);
			CREATE TABLE users (id int, attrs outer_t);
		`},
		newSchemaDDL: []string{`
			CREATE TYPE inner_t AS (n int, extra text);
			CREATE TYPE outer_t AS (i inner_t, label text);
			CREATE TABLE users (id int, attrs outer_t);
		`},
		expectedPlanErrorIs: diff.ErrNotImplemented,
	},
	{
		name: "alter composite type attrs is unsupported when used by a table array column",
		oldSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text);
			CREATE TABLE users (id int, attrs pair[]);
		`},
		newSchemaDDL: []string{`
			CREATE TYPE pair AS (a int, b text, c boolean);
			CREATE TABLE users (id int, attrs pair[]);
		`},
		expectedPlanErrorIs: diff.ErrNotImplemented,
	},
}

func TestCompositeTypeTestCases(t *testing.T) {
	runTestCases(t, compositeTypeAcceptanceTestCases)
}
