package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

// Tests for body-text dependency detection (patch 02).
// These verify that pg-schema-diff correctly orders functions after the
// tables/views they reference in their bodies via FROM, JOIN, %ROWTYPE, [].
var functionBodyDepTestCases = []acceptanceTestCase{
	{
		name: "qualified FROM",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE public.orders(id INT PRIMARY KEY, total NUMERIC);
			CREATE FUNCTION public.sum_orders() RETURNS NUMERIC LANGUAGE sql AS
			$$ SELECT SUM(total) FROM public.orders; $$;
		`},
	},
	{
		name: "unqualified FROM",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE orders(id INT PRIMARY KEY, total NUMERIC);
			CREATE FUNCTION sum_orders() RETURNS NUMERIC LANGUAGE sql AS
			$$ SELECT SUM(total) FROM orders; $$;
		`},
	},
	{
		name: "qualified JOIN",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE public.customers(id INT PRIMARY KEY, name TEXT);
			CREATE TABLE public.orders(id INT PRIMARY KEY, customer_id INT, total NUMERIC);
			CREATE FUNCTION public.customer_totals() RETURNS TABLE(name TEXT, total NUMERIC) LANGUAGE sql AS
			$$ SELECT c.name, SUM(o.total) FROM public.orders o JOIN public.customers c ON c.id = o.customer_id GROUP BY c.name; $$;
		`},
	},
	{
		name: "unqualified JOIN",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE customers(id INT PRIMARY KEY, name TEXT);
			CREATE TABLE orders(id INT PRIMARY KEY, customer_id INT, total NUMERIC);
			CREATE FUNCTION customer_totals() RETURNS TABLE(name TEXT, total NUMERIC) LANGUAGE sql AS
			$$ SELECT c.name, SUM(o.total) FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.name; $$;
		`},
	},
	{
		name: "qualified %ROWTYPE",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE public.events(id INT PRIMARY KEY, event_type TEXT);
			CREATE FUNCTION public.process_events() RETURNS void LANGUAGE plpgsql AS $$
			DECLARE r public.events%ROWTYPE;
			BEGIN
				FOR r IN SELECT * FROM public.events LOOP
					RAISE NOTICE '%', r.event_type;
				END LOOP;
			END; $$;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "unqualified %ROWTYPE",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE events(id INT PRIMARY KEY, event_type TEXT);
			CREATE FUNCTION process_events() RETURNS void LANGUAGE plpgsql AS $$
			DECLARE r events%ROWTYPE;
			BEGIN
				FOR r IN SELECT * FROM events LOOP
					RAISE NOTICE '%', r.event_type;
				END LOOP;
			END; $$;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "qualified array of composite",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE public.items(id INT PRIMARY KEY, name TEXT, price NUMERIC);
			CREATE FUNCTION public.batch_items() RETURNS void LANGUAGE plpgsql AS $$
			DECLARE batch public.items[];
			BEGIN
				SELECT array_agg(i) INTO batch FROM public.items i;
			END; $$;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "unqualified array of composite",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE items(id INT PRIMARY KEY, name TEXT, price NUMERIC);
			CREATE FUNCTION batch_items() RETURNS void LANGUAGE plpgsql AS $$
			DECLARE batch items[];
			BEGIN
				SELECT array_agg(i) INTO batch FROM items i;
			END; $$;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "FROM in subquery",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE public.products(id INT PRIMARY KEY, category TEXT, price NUMERIC);
			CREATE FUNCTION public.expensive_categories() RETURNS SETOF TEXT LANGUAGE sql AS
			$$ SELECT DISTINCT category FROM public.products WHERE price > (SELECT AVG(price) FROM public.products); $$;
		`},
	},
	{
		name: "CTE referencing table",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE public.sales(id INT PRIMARY KEY, region TEXT, amount NUMERIC);
			CREATE FUNCTION public.top_regions() RETURNS TABLE(region TEXT, total NUMERIC) LANGUAGE sql AS
			$$ WITH regional AS (SELECT region, SUM(amount) as total FROM public.sales GROUP BY region)
			   SELECT region, total FROM regional ORDER BY total DESC LIMIT 5; $$;
		`},
	},
	{
		name: "function referencing view (qualified FROM)",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE public.logs(id INT PRIMARY KEY, level TEXT, msg TEXT, created_at TIMESTAMPTZ DEFAULT now());
			CREATE VIEW public.error_logs AS SELECT id, msg, created_at FROM public.logs WHERE level = 'ERROR';
			CREATE FUNCTION public.recent_errors() RETURNS BIGINT LANGUAGE sql AS
			$$ SELECT COUNT(*) FROM public.error_logs WHERE created_at > now() - interval '1 hour'; $$;
		`},
	},
	{
		name: "function referencing view (unqualified FROM)",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE logs(id INT PRIMARY KEY, level TEXT, msg TEXT, created_at TIMESTAMPTZ DEFAULT now());
			CREATE VIEW error_logs AS SELECT id, msg, created_at FROM logs WHERE level = 'ERROR';
			CREATE FUNCTION recent_errors() RETURNS BIGINT LANGUAGE sql AS
			$$ SELECT COUNT(*) FROM error_logs WHERE created_at > now() - interval '1 hour'; $$;
		`},
	},
}

func TestFunctionBodyDepTestCases(t *testing.T) {
	runTestCases(t, functionBodyDepTestCases)
}

var functionBodyDepGapTestCases = []acceptanceTestCase{
	{
		name: "Function with LEFT JOIN reference",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE public.a(id INT PRIMARY KEY, val TEXT);
			CREATE TABLE public.b(id INT PRIMARY KEY, a_id INT, extra TEXT);
			CREATE FUNCTION public.joined() RETURNS TABLE(val TEXT, extra TEXT) LANGUAGE sql AS
			$$ SELECT a.val, b.extra FROM public.a LEFT JOIN public.b ON b.a_id = a.id; $$;
		`},
	},
	{
		name: "Function with multiple FROM tables",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE public.t1(id INT PRIMARY KEY);
			CREATE TABLE public.t2(id INT PRIMARY KEY);
			CREATE TABLE public.t3(id INT PRIMARY KEY);
			CREATE FUNCTION public.multi() RETURNS BIGINT LANGUAGE sql AS
			$$ SELECT COUNT(*) FROM public.t1, public.t2, public.t3; $$;
		`},
		// Gap: comma-separated FROM list only detects first table.
		// t2 and t3 after commas are not matched by the regex.
		expectedPlanErrorContains: "does not exist",
	},
	{
		name: "Function with unqualified LEFT JOIN",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE a(id INT PRIMARY KEY, val TEXT);
			CREATE TABLE b(id INT PRIMARY KEY, a_id INT, extra TEXT);
			CREATE FUNCTION joined() RETURNS TABLE(val TEXT, extra TEXT) LANGUAGE sql AS
			$$ SELECT a.val, b.extra FROM a LEFT JOIN b ON b.a_id = a.id; $$;
		`},
	},
}

func TestFunctionBodyDepGapTestCases(t *testing.T) {
	runTestCases(t, functionBodyDepGapTestCases)
}
