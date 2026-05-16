package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var viewAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "No-op",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL,
                bar VARCHAR(255) UNIQUE,
                buzz BOOLEAN DEFAULT true
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE buzz = true;

            CREATE VIEW foobar_count AS
                SELECT COUNT(*) as total_count 
                FROM foobar;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL,
                bar VARCHAR(255) UNIQUE,
                buzz BOOLEAN DEFAULT true
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE buzz = true;

            CREATE VIEW foobar_count AS
                SELECT COUNT(*) as total_count 
                FROM foobar;
			`,
		},
		expectEmptyPlan: true,
	},
	{
		name: "Add view",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL,
                bar DECIMAL(10,2),
                buzz INT
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL,
                bar DECIMAL(10,2),
                buzz INT
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE bar > 100.00;
			`,
		},
	},
	{
		name: "Add recursive view",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL,
                parent_id INT REFERENCES foobar(id)
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL,
                parent_id INT REFERENCES foobar(id)
            );

            CREATE VIEW foobar_hierarchy AS
                WITH RECURSIVE hierarchy AS (
                    SELECT id, foo, parent_id, 0 as level
                    FROM foobar 
                    WHERE parent_id IS NULL
                    UNION ALL
                    SELECT f.id, f.foo, f.parent_id, h.level + 1
                    FROM foobar f
                    JOIN hierarchy h ON f.parent_id = h.id
                )
                SELECT * FROM hierarchy;
			`,
		},
	},
	{
		name:         "Add view with dependent table",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo INT NOT NULL,
                bar DECIMAL(10,2),
                buzz VARCHAR(50) DEFAULT 'pending'
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE buzz = 'pending';
			`,
		},
	},
	{
		name: "Add view with dependent column",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL,
                bar VARCHAR(100)
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL,
                bar VARCHAR(100),
                buzz DECIMAL(10,2)
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, bar, buzz 
                FROM foobar;
			`,
		},
	},
	{
		name: "Drop view",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar INT,
                buzz VARCHAR(100)
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE bar < 10;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar INT,
                buzz VARCHAR(100)
            );
			`,
		},
	},
	{
		name: "Drop view and underlying table",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE bar > CURRENT_DATE - INTERVAL '7 days';
			`,
		},
		newSchemaDDL: nil,
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Drop view and underlying column",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar INT,
                buzz VARCHAR(255),
                fizz TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, bar, buzz 
                FROM foobar;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar INT,
                fizz TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Recreate view due to table recreation (unpartitioned to partitioned)",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo INT,
                bar DECIMAL(10,2),
                buzz DATE
            );

            CREATE VIEW foobar_view AS
                SELECT DATE_TRUNC('month', buzz) as month, SUM(bar) as total
                FROM foobar 
                GROUP BY DATE_TRUNC('month', buzz);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT,
                foo INT,
                bar DECIMAL(10,2),
                buzz DATE,
                PRIMARY KEY (id, buzz)
            ) PARTITION BY RANGE (buzz);

            CREATE VIEW foobar_view AS
                SELECT DATE_TRUNC('month', buzz) as month, SUM(bar) as total
                FROM foobar 
                GROUP BY DATE_TRUNC('month', buzz);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Recreate view due dependent table changing",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar DECIMAL(10,2)
            );

            CREATE VIEW foobar_view AS
                SELECT foo, AVG(bar) as avg_bar
                FROM foobar 
                GROUP BY foo;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE bar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar DECIMAL(10,2)
            );

            CREATE VIEW foobar_view AS
                SELECT foo, AVG(bar) as avg_bar
                FROM bar 
                GROUP BY foo;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Recreate view due to dependent column changing",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                bar TIMESTAMP,
                foo VARCHAR(255),
                old_buzz VARCHAR(50)
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, old_buzz as buzz
                FROM foobar 
                WHERE old_buzz = 'active';
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                bar TIMESTAMP,
                foo VARCHAR(255),
                new_buzz VARCHAR(50)
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, new_buzz as buzz
                FROM foobar 
                WHERE new_buzz = 'active';
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "alter - add column and change condition",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                bar TIMESTAMP,
                foo VARCHAR(255)
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo
                FROM foobar 
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                bar TIMESTAMP,
                foo VARCHAR(255)
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, bar
                FROM foobar 
                WHERE bar < CURRENT_TIMESTAMP
			`,
		},
	},
	{
		name: "alter - removing select column",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar VARCHAR(255),
                buzz VARCHAR(255),
                fizz BOOLEAN DEFAULT true
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE fizz = true;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar VARCHAR(255),
                buzz VARCHAR(255),
                fizz BOOLEAN DEFAULT true
            );

            CREATE VIEW foobar_view AS
                SELECT foo, bar
                FROM foobar 
                WHERE fizz = true;
			`,
		},
	},
	{
		name: "alter - add security barrier",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar INT,
                buzz TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE VIEW foobar_view AS
                SELECT foo, bar, buzz 
                FROM foobar 
                WHERE buzz > CURRENT_DATE - INTERVAL '1 day';
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar INT,
                buzz TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE VIEW foobar_view WITH (security_barrier = true) AS
                SELECT foo, bar, buzz 
                FROM foobar 
                WHERE buzz > CURRENT_DATE - INTERVAL '1 day';
			`,
		},
	},
	{
		name: "alter - change check option",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo INT NOT NULL,
                bar VARCHAR(50) DEFAULT 'pending',
                buzz DECIMAL(10,2) CHECK (buzz > 0)
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, buzz 
                FROM foobar 
                WHERE bar = 'pending'
                WITH LOCAL CHECK OPTION;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo INT NOT NULL,
                bar VARCHAR(50) DEFAULT 'pending',
                buzz DECIMAL(10,2) CHECK (buzz > 0)
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, buzz 
                FROM foobar 
                WHERE bar = 'pending'
                WITH CASCADED CHECK OPTION;
			`,
		},
	},
	{
		name: "alter - add check option",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL,
                bar DECIMAL(10,2) CHECK (bar > 0),
                buzz BOOLEAN DEFAULT true
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE buzz = true;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL,
                bar DECIMAL(10,2) CHECK (bar > 0),
                buzz BOOLEAN DEFAULT true
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE buzz = true
                WITH CHECK OPTION;
			`,
		},
	},
	{
		name: "alter - remove check option",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar INT CHECK (bar >= 0),
                buzz BOOLEAN DEFAULT true
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE buzz = true
                WITH CHECK OPTION;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar INT CHECK (bar >= 0),
                buzz BOOLEAN DEFAULT true
            );

            CREATE VIEW foobar_view AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE buzz = true;
			`,
		},
	},
}

func TestViewTestCases(t *testing.T) {
	runTestCases(t, viewAcceptanceTestCases)
}

// Test cases for view-depends-on-view and view-depends-on-function scenarios.
// These reproduce the failures described in darryls-ai-dev-setup issue #5.
var viewDependencyTestCases = []acceptanceTestCase{
	{
		name: "No-op: view depends on another view (identical schemas)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE base_table(
				id INT PRIMARY KEY,
				name TEXT NOT NULL,
				active BOOLEAN DEFAULT true
			);

			CREATE VIEW active_items AS
				SELECT id, name FROM base_table WHERE active = true;

			CREATE VIEW active_item_count AS
				SELECT COUNT(*) as total FROM active_items;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE base_table(
				id INT PRIMARY KEY,
				name TEXT NOT NULL,
				active BOOLEAN DEFAULT true
			);

			CREATE VIEW active_items AS
				SELECT id, name FROM base_table WHERE active = true;

			CREATE VIEW active_item_count AS
				SELECT COUNT(*) as total FROM active_items;
			`,
		},
		expectEmptyPlan: true,
	},
	{
		name: "No-op: view calls a function (identical schemas)",
		oldSchemaDDL: []string{
			`
			CREATE TABLE orders(
				id INT PRIMARY KEY,
				amount NUMERIC NOT NULL
			);

			CREATE FUNCTION double_amount(val NUMERIC) RETURNS NUMERIC LANGUAGE sql AS
			$$ SELECT val * 2; $$;

			CREATE VIEW doubled_orders AS
				SELECT id, double_amount(amount) as doubled FROM orders;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE orders(
				id INT PRIMARY KEY,
				amount NUMERIC NOT NULL
			);

			CREATE FUNCTION double_amount(val NUMERIC) RETURNS NUMERIC LANGUAGE sql AS
			$$ SELECT val * 2; $$;

			CREATE VIEW doubled_orders AS
				SELECT id, double_amount(amount) as doubled FROM orders;
			`,
		},
		expectEmptyPlan: true,
	},
	{
		name: "No-op: cross-schema view depends on view",
		oldSchemaDDL: []string{
			`
			CREATE SCHEMA schema_a;
			CREATE SCHEMA schema_b;

			CREATE TABLE schema_a.items(
				id INT PRIMARY KEY,
				status TEXT NOT NULL
			);

			CREATE VIEW schema_a.pending_items AS
				SELECT id, status FROM schema_a.items WHERE status = 'pending';

			CREATE VIEW schema_b.pending_count AS
				SELECT COUNT(*) as total FROM schema_a.pending_items;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SCHEMA schema_a;
			CREATE SCHEMA schema_b;

			CREATE TABLE schema_a.items(
				id INT PRIMARY KEY,
				status TEXT NOT NULL
			);

			CREATE VIEW schema_a.pending_items AS
				SELECT id, status FROM schema_a.items WHERE status = 'pending';

			CREATE VIEW schema_b.pending_count AS
				SELECT COUNT(*) as total FROM schema_a.pending_items;
			`,
		},
		expectEmptyPlan: true,
	},
}

func TestViewDependencyTestCases(t *testing.T) {
	runTestCases(t, viewDependencyTestCases)
}

// Known issue: function→view ordering in from-scratch apply (issue #1).
// The function body references a view via FROM, but the apply path orders
// the function before the view. This is a plan validation failure, not a
// diff failure. Tracked separately from the view-depends-on-view fix.
var viewFunctionOrderingTestCases = []acceptanceTestCase{
	{
		name: "No-op: function references view via FROM (identical schemas) [KNOWN ISSUE #1]",
		oldSchemaDDL: []string{
			`
			CREATE TABLE events(
				id INT PRIMARY KEY,
				event_type TEXT NOT NULL,
				created_at TIMESTAMPTZ DEFAULT now()
			);

			CREATE VIEW recent_events AS
				SELECT id, event_type FROM events WHERE created_at > now() - interval '1 day';

			CREATE FUNCTION count_recent() RETURNS BIGINT LANGUAGE sql AS
			$$ SELECT COUNT(*) FROM recent_events; $$;

			CREATE VIEW event_summary AS
				SELECT count_recent() as recent_count;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE events(
				id INT PRIMARY KEY,
				event_type TEXT NOT NULL,
				created_at TIMESTAMPTZ DEFAULT now()
			);

			CREATE VIEW recent_events AS
				SELECT id, event_type FROM events WHERE created_at > now() - interval '1 day';

			CREATE FUNCTION count_recent() RETURNS BIGINT LANGUAGE sql AS
			$$ SELECT COUNT(*) FROM recent_events; $$;

			CREATE VIEW event_summary AS
				SELECT count_recent() as recent_count;
			`,
		},
		expectEmptyPlan: true,
	},
}

func TestViewFunctionOrderingTestCases(t *testing.T) {
	runTestCases(t, viewFunctionOrderingTestCases)
}

// Additional view dependency scenarios: adds, drops, and cross-object interactions.
var viewDepAdditionalTestCases = []acceptanceTestCase{
	{
		name: "Add view that depends on existing view",
		oldSchemaDDL: []string{`
			CREATE TABLE items(id INT PRIMARY KEY, active BOOLEAN DEFAULT true);
			CREATE VIEW active_items AS SELECT id FROM items WHERE active = true;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE items(id INT PRIMARY KEY, active BOOLEAN DEFAULT true);
			CREATE VIEW active_items AS SELECT id FROM items WHERE active = true;
			CREATE VIEW active_count AS SELECT COUNT(*) as total FROM active_items;
		`},
	},
	{
		name: "Add view that calls existing function",
		oldSchemaDDL: []string{`
			CREATE TABLE orders(id INT PRIMARY KEY, amount NUMERIC);
			CREATE FUNCTION total_orders() RETURNS NUMERIC LANGUAGE sql AS
			$$ SELECT SUM(amount) FROM orders; $$;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE orders(id INT PRIMARY KEY, amount NUMERIC);
			CREATE FUNCTION total_orders() RETURNS NUMERIC LANGUAGE sql AS
			$$ SELECT SUM(amount) FROM orders; $$;
			CREATE VIEW order_summary AS SELECT total_orders() as total;
		`},
	},
	{
		name: "Drop view that depends on another view",
		oldSchemaDDL: []string{`
			CREATE TABLE items(id INT PRIMARY KEY, active BOOLEAN DEFAULT true);
			CREATE VIEW active_items AS SELECT id FROM items WHERE active = true;
			CREATE VIEW active_count AS SELECT COUNT(*) as total FROM active_items;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE items(id INT PRIMARY KEY, active BOOLEAN DEFAULT true);
			CREATE VIEW active_items AS SELECT id FROM items WHERE active = true;
		`},
	},
	{
		name: "Drop view that calls a function",
		oldSchemaDDL: []string{`
			CREATE TABLE orders(id INT PRIMARY KEY, amount NUMERIC);
			CREATE FUNCTION total_orders() RETURNS NUMERIC LANGUAGE sql AS
			$$ SELECT SUM(amount) FROM orders; $$;
			CREATE VIEW order_summary AS SELECT total_orders() as total;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE orders(id INT PRIMARY KEY, amount NUMERIC);
			CREATE FUNCTION total_orders() RETURNS NUMERIC LANGUAGE sql AS
			$$ SELECT SUM(amount) FROM orders; $$;
		`},
	},
	{
		name: "Drop function while table dep persists",
		oldSchemaDDL: []string{`
			CREATE TABLE orders(id INT PRIMARY KEY, amount NUMERIC);
			CREATE FUNCTION sum_orders() RETURNS NUMERIC LANGUAGE sql AS
			$$ SELECT SUM(amount) FROM public.orders; $$;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE orders(id INT PRIMARY KEY, amount NUMERIC);
		`},
	},
	{
		name: "Function dep changes (old table removed, new table added)",
		oldSchemaDDL: []string{`
			CREATE TABLE old_data(id INT PRIMARY KEY, val TEXT);
			CREATE TABLE new_data(id INT PRIMARY KEY, val TEXT);
			CREATE FUNCTION get_val() RETURNS TEXT LANGUAGE sql AS
			$$ SELECT val FROM public.old_data LIMIT 1; $$;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE old_data(id INT PRIMARY KEY, val TEXT);
			CREATE TABLE new_data(id INT PRIMARY KEY, val TEXT);
			CREATE FUNCTION get_val() RETURNS TEXT LANGUAGE sql AS
			$$ SELECT val FROM public.new_data LIMIT 1; $$;
		`},
	},
	{
		name: "Recreate view_a when view_b is recreated due to table column type change",
		oldSchemaDDL: []string{`
			CREATE TABLE table_c(c1 INT PRIMARY KEY, c2_old TEXT);
			CREATE VIEW view_b AS SELECT c1, c2_old FROM table_c;
			CREATE VIEW view_a AS SELECT c1 FROM view_b;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE table_c(c1 INT PRIMARY KEY, c2_new TEXT);
			CREATE VIEW view_b AS SELECT c1, c2_new FROM table_c;
			CREATE VIEW view_a AS SELECT c1 FROM view_b;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Recreate view when dependent function signature changes",
		oldSchemaDDL: []string{
			`CREATE TABLE transactions(id INT PRIMARY KEY, amount NUMERIC);`,
			`CREATE FUNCTION calc_fee(val NUMERIC) RETURNS NUMERIC LANGUAGE sql AS
			$$ SELECT val * 0.1; $$;
			CREATE VIEW transaction_fees AS SELECT id, calc_fee(amount) as fee FROM transactions;`,
		},
		newSchemaDDL: []string{
			`CREATE TABLE transactions(id INT PRIMARY KEY, amount NUMERIC);`,
			`CREATE FUNCTION calc_fee(val NUMERIC) RETURNS TEXT LANGUAGE sql AS
			$$ SELECT (val * 0.1)::TEXT; $$;
			CREATE VIEW transaction_fees AS SELECT id, calc_fee(amount) as fee FROM transactions;`,
		},
		// Upstream limitation: pg-schema-diff doesn't handle function return type
		// changes as drop+create. It emits CREATE OR REPLACE which PG rejects.
		expectedPlanErrorContains: "cannot change return type of existing function",
	},
}

func TestViewDepAdditionalTestCases(t *testing.T) {
	runTestCases(t, viewDepAdditionalTestCases)
}

var viewGapTestCases = []acceptanceTestCase{
	{
		name: "From scratch: chain of 3 views (a -> b -> c -> table)",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE t(id INT PRIMARY KEY, val TEXT);
			CREATE VIEW vc AS SELECT id, val FROM t;
			CREATE VIEW vb AS SELECT id, val FROM vc;
			CREATE VIEW va AS SELECT id FROM vb;
		`},
	},
	{
		name: "Recreate cascades through 3-level view chain",
		oldSchemaDDL: []string{`
			CREATE TABLE t(id INT PRIMARY KEY, old_val TEXT);
			CREATE VIEW vc AS SELECT id, old_val FROM t;
			CREATE VIEW vb AS SELECT id, old_val FROM vc;
			CREATE VIEW va AS SELECT id FROM vb;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE t(id INT PRIMARY KEY, new_val TEXT);
			CREATE VIEW vc AS SELECT id, new_val FROM t;
			CREATE VIEW vb AS SELECT id, new_val FROM vc;
			CREATE VIEW va AS SELECT id FROM vb;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeDeletesData},
	},
	{
		name: "From scratch: view depends on function that depends on another function",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE t(id INT PRIMARY KEY, amount NUMERIC);
			CREATE FUNCTION helper(v NUMERIC) RETURNS NUMERIC LANGUAGE sql AS $$ SELECT v * 2; $$;
			CREATE FUNCTION calc(v NUMERIC) RETURNS NUMERIC LANGUAGE sql AS $$ SELECT public.helper(v) + 1; $$;
			CREATE VIEW v AS SELECT id, calc(amount) as result FROM t;
		`},
		// Gap: pg_depend doesn't track function→function deps for SQL functions.
		// Body-text regex only detects table/view refs, not function calls.
		expectedPlanErrorContains: "function public.helper(numeric) does not exist",
	},
	{
		name: "From scratch: two views depend on same function",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE t(id INT PRIMARY KEY, amount NUMERIC);
			CREATE FUNCTION fmt(v NUMERIC) RETURNS TEXT LANGUAGE sql AS $$ SELECT v::TEXT; $$;
			CREATE VIEW v1 AS SELECT id, fmt(amount) as formatted FROM t;
			CREATE VIEW v2 AS SELECT id, fmt(amount) as formatted FROM t WHERE amount > 0;
		`},
	},
	{
		name: "From scratch: view with cross-schema function dependency",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE SCHEMA lib;
			CREATE TABLE public.t(id INT PRIMARY KEY, val NUMERIC);
			CREATE FUNCTION lib.transform(v NUMERIC) RETURNS NUMERIC LANGUAGE sql AS $$ SELECT v * 100; $$;
			CREATE VIEW public.v AS SELECT id, lib.transform(val) as transformed FROM public.t;
		`},
	},
	{
		name: "From scratch: view depends on function with unqualified FROM referencing view",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE t(id INT PRIMARY KEY, active BOOLEAN DEFAULT true);
			CREATE VIEW active_items AS SELECT id FROM t WHERE active;
			CREATE FUNCTION count_active() RETURNS BIGINT LANGUAGE sql AS $$ SELECT COUNT(*) FROM active_items; $$;
			CREATE VIEW summary AS SELECT count_active() as total;
		`},
	},
}

func TestViewGapTestCases(t *testing.T) {
	runTestCases(t, viewGapTestCases)
}
