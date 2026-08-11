package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var materializedViewAcceptanceTestCases = []acceptanceTestCase{
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

            CREATE MATERIALIZED VIEW foobar_view AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE buzz = true;

            CREATE MATERIALIZED VIEW foobar_count AS
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

            CREATE MATERIALIZED VIEW foobar_view AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE buzz = true;

            CREATE MATERIALIZED VIEW foobar_count AS
                SELECT COUNT(*) as total_count 
                FROM foobar;
			`,
		},
		expectEmptyPlan: true,
	},
	{
		name: "Add materialized view",
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

            CREATE MATERIALIZED VIEW foobar_view AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE bar > 100.00;
			`,
		},
	},
	{
		name: "Add recursive materialized view",
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

            CREATE MATERIALIZED VIEW foobar_hierarchy AS
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
		name:         "Add materialized view with dependent table",
		oldSchemaDDL: nil,
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo INT NOT NULL,
                bar DECIMAL(10,2),
                buzz VARCHAR(50) DEFAULT 'pending'
            );

            CREATE MATERIALIZED VIEW foobar_view AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE buzz = 'pending';
			`,
		},
	},
	{
		name: "Add materialized view with dependent column",
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

            CREATE MATERIALIZED VIEW foobar_view AS
                SELECT id, foo, bar, buzz 
                FROM foobar;
			`,
		},
	},
	{
		name: "Drop materialized view",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar INT,
                buzz VARCHAR(100)
            );

            CREATE MATERIALIZED VIEW foobar_view AS
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
		name: "Drop materialized view and underlying table",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE MATERIALIZED VIEW foobar_view AS
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
		name: "Drop materialized view and underlying column",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar INT,
                buzz VARCHAR(255),
                fizz TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE MATERIALIZED VIEW foobar_view AS
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
		name: "Recreate materialized view due to table recreation (unpartitioned to partitioned)",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo INT,
                bar DECIMAL(10,2),
                buzz DATE
            );

            CREATE MATERIALIZED VIEW foobar_view AS
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

            CREATE MATERIALIZED VIEW foobar_view AS
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
		name: "Recreate materialized view due dependent table changing",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar DECIMAL(10,2)
            );

            CREATE MATERIALIZED VIEW foobar_view AS
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

            CREATE MATERIALIZED VIEW foobar_view AS
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
		name: "Recreate materialized view due to dependent column changing",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                bar TIMESTAMP,
                foo VARCHAR(255),
                old_buzz VARCHAR(50)
            );

            CREATE MATERIALIZED VIEW foobar_view AS
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

            CREATE MATERIALIZED VIEW foobar_view AS
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

            CREATE MATERIALIZED VIEW foobar_view AS
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

            CREATE MATERIALIZED VIEW foobar_view AS
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

            CREATE MATERIALIZED VIEW foobar_view AS
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

            CREATE MATERIALIZED VIEW foobar_view AS
                SELECT foo, bar
                FROM foobar 
                WHERE fizz = true;
			`,
		},
	},
	{
		name: "alter - add fill factor option",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar INT,
                buzz TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE MATERIALIZED VIEW foobar_view AS
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

            CREATE MATERIALIZED VIEW foobar_view WITH (fillfactor = 70) AS
                SELECT foo, bar, buzz 
                FROM foobar 
                WHERE buzz > CURRENT_DATE - INTERVAL '1 day';
			`,
		},
	},
	{
		name: "alter - change autovacuum option",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo INT NOT NULL,
                bar VARCHAR(50) DEFAULT 'pending',
                buzz DECIMAL(10,2) CHECK (buzz > 0)
            );

            CREATE MATERIALIZED VIEW foobar_view WITH (autovacuum_enabled = true) AS
                SELECT id, foo, buzz 
                FROM foobar 
                WHERE bar = 'pending';
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

            CREATE MATERIALIZED VIEW foobar_view WITH (autovacuum_enabled = false) AS
                SELECT id, foo, buzz 
                FROM foobar 
                WHERE bar = 'pending';
			`,
		},
	},
	{
		name: "alter - add log_autovacuum_min_duration option",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255) NOT NULL,
                bar DECIMAL(10,2) CHECK (bar > 0),
                buzz BOOLEAN DEFAULT true
            );

            CREATE MATERIALIZED VIEW foobar_view AS
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

            CREATE MATERIALIZED VIEW foobar_view WITH (log_autovacuum_min_duration = 1000) AS
                SELECT id, foo, bar 
                FROM foobar 
                WHERE buzz = true;
			`,
		},
	},
	{
		name: "alter - remove toast_tuple_target option",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255),
                bar INT CHECK (bar >= 0),
                buzz BOOLEAN DEFAULT true
            );

            CREATE MATERIALIZED VIEW foobar_view WITH (toast_tuple_target = 2048) AS
                SELECT id, foo, bar
                FROM foobar
                WHERE buzz = true;
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

            CREATE MATERIALIZED VIEW foobar_view AS
                SELECT id, foo, bar
                FROM foobar
                WHERE buzz = true;
			`,
		},
	},
	{
		name: "Add materialized view with index",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                foo VARCHAR(255)
            );
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
		// No hazards - index build hazard stripped when materialized view is new
	},
	{
		name: "Drop materialized view with index",
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
			`,
		},
		// No IndexDropped hazard - handled by DROP MATERIALIZED VIEW cascade
	},
	{
		name: "Drop materialized view with index and underlying table",
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
		newSchemaDDL: nil,
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
}

func TestMaterializedViewTestCases(t *testing.T) {
	runTestCases(t, materializedViewAcceptanceTestCases)
}

var materializedViewDepTestCases = []acceptanceTestCase{
	{
		name: "No-op: materialized view depends on a view (identical schemas)",
		oldSchemaDDL: []string{`
			CREATE TABLE events(id INT PRIMARY KEY, event_type TEXT, created_at TIMESTAMPTZ DEFAULT now());
			CREATE VIEW recent_events AS SELECT id, event_type FROM events WHERE created_at > now() - interval '7 days';
			CREATE MATERIALIZED VIEW event_stats AS SELECT event_type, COUNT(*) as cnt FROM recent_events GROUP BY event_type;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE events(id INT PRIMARY KEY, event_type TEXT, created_at TIMESTAMPTZ DEFAULT now());
			CREATE VIEW recent_events AS SELECT id, event_type FROM events WHERE created_at > now() - interval '7 days';
			CREATE MATERIALIZED VIEW event_stats AS SELECT event_type, COUNT(*) as cnt FROM recent_events GROUP BY event_type;
		`},
		expectEmptyPlan: true,
	},
	{
		name: "Recreate matview when dependent view is recreated due to table change",
		oldSchemaDDL: []string{`
			CREATE TABLE table_c(c1 INT PRIMARY KEY, c2_old TEXT);
			CREATE VIEW view_b AS SELECT c1, c2_old FROM table_c;
			CREATE MATERIALIZED VIEW matview_a AS SELECT c1 FROM view_b;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE table_c(c1 INT PRIMARY KEY, c2_new TEXT);
			CREATE VIEW view_b AS SELECT c1, c2_new FROM table_c;
			CREATE MATERIALIZED VIEW matview_a AS SELECT c1 FROM view_b;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Add matview that depends on existing view",
		oldSchemaDDL: []string{`
			CREATE TABLE items(id INT PRIMARY KEY, active BOOLEAN DEFAULT true);
			CREATE VIEW active_items AS SELECT id FROM items WHERE active = true;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE items(id INT PRIMARY KEY, active BOOLEAN DEFAULT true);
			CREATE VIEW active_items AS SELECT id FROM items WHERE active = true;
			CREATE MATERIALIZED VIEW active_count AS SELECT COUNT(*) as total FROM active_items;
		`},
	},
	{
		name: "Drop matview that depends on view",
		oldSchemaDDL: []string{`
			CREATE TABLE items(id INT PRIMARY KEY, active BOOLEAN DEFAULT true);
			CREATE VIEW active_items AS SELECT id FROM items WHERE active = true;
			CREATE MATERIALIZED VIEW active_count AS SELECT COUNT(*) as total FROM active_items;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE items(id INT PRIMARY KEY, active BOOLEAN DEFAULT true);
			CREATE VIEW active_items AS SELECT id FROM items WHERE active = true;
		`},
	},
	{
		name: "Matview depends on function - no-op identical schemas",
		oldSchemaDDL: []string{`
			CREATE TABLE orders(id INT PRIMARY KEY, amount NUMERIC);
			CREATE FUNCTION double_val(val NUMERIC) RETURNS NUMERIC LANGUAGE sql AS
			$$ SELECT val * 2; $$;
			CREATE MATERIALIZED VIEW order_doubles AS SELECT id, double_val(amount) as doubled FROM orders;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE orders(id INT PRIMARY KEY, amount NUMERIC);
			CREATE FUNCTION double_val(val NUMERIC) RETURNS NUMERIC LANGUAGE sql AS
			$$ SELECT val * 2; $$;
			CREATE MATERIALIZED VIEW order_doubles AS SELECT id, double_val(amount) as doubled FROM orders;
		`},
		expectEmptyPlan: true,
	},
	{
		name: "Recreate matview when dependent function signature changes",
		oldSchemaDDL: []string{
			`CREATE TABLE transactions(id INT PRIMARY KEY, amount NUMERIC);`,
			`CREATE FUNCTION calc_fee(val NUMERIC) RETURNS NUMERIC LANGUAGE sql AS
			$$ SELECT val * 0.1; $$;
			CREATE MATERIALIZED VIEW transaction_fees AS SELECT id, calc_fee(amount) as fee FROM transactions;`,
		},
		newSchemaDDL: []string{
			`CREATE TABLE transactions(id INT PRIMARY KEY, amount NUMERIC);`,
			`CREATE FUNCTION calc_fee(val NUMERIC) RETURNS TEXT LANGUAGE sql AS
			$$ SELECT (val * 0.1)::TEXT; $$;
			CREATE MATERIALIZED VIEW transaction_fees AS SELECT id, calc_fee(amount) as fee FROM transactions;`,
		},
		// Upstream limitation: pg-schema-diff doesn't handle function return type
		// changes as drop+create.
		expectedPlanErrorContains: "cannot change return type of existing function",
	},
}

func TestMaterializedViewDepTestCases(t *testing.T) {
	runTestCases(t, materializedViewDepTestCases)
}

// Matview function dependency ordering tests — from-scratch creation.
var materializedViewFuncDepTestCases = []acceptanceTestCase{
	{
		name: "From scratch: matview calls a function",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE orders(id INT PRIMARY KEY, amount NUMERIC);
			CREATE FUNCTION total_orders() RETURNS NUMERIC LANGUAGE sql AS
			$$ SELECT COALESCE(SUM(amount), 0) FROM public.orders; $$;
			CREATE MATERIALIZED VIEW order_summary AS SELECT total_orders() as total;
		`},
	},
}

func TestMaterializedViewFuncDepTestCases(t *testing.T) {
	runTestCases(t, materializedViewFuncDepTestCases)
}

var materializedViewGapTestCases = []acceptanceTestCase{
	{
		name: "From scratch: matview depends on view that calls a function",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE t(id INT PRIMARY KEY, val NUMERIC);
			CREATE FUNCTION double_val(v NUMERIC) RETURNS NUMERIC LANGUAGE sql AS $$ SELECT v * 2; $$;
			CREATE VIEW v AS SELECT id, double_val(val) as doubled FROM t;
			CREATE MATERIALIZED VIEW mv AS SELECT * FROM v;
		`},
	},
	{
		name: "From scratch: matview depends on another matview",
		oldSchemaDDL: []string{``},
		newSchemaDDL: []string{`
			CREATE TABLE t(id INT PRIMARY KEY, category TEXT, amount NUMERIC);
			CREATE MATERIALIZED VIEW mv_base AS SELECT category, SUM(amount) as total FROM t GROUP BY category;
			CREATE MATERIALIZED VIEW mv_top AS SELECT category FROM mv_base WHERE total > 1000;
		`},
		// vertex ID namespace, not table vertex IDs).
	},
	{
		name: "Recreate matview when dependent matview is recreated",
		oldSchemaDDL: []string{`
			CREATE TABLE t(id INT PRIMARY KEY, old_col TEXT, amount NUMERIC);
			CREATE MATERIALIZED VIEW mv_base AS SELECT old_col, SUM(amount) as total FROM t GROUP BY old_col;
			CREATE MATERIALIZED VIEW mv_top AS SELECT old_col FROM mv_base WHERE total > 0;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE t(id INT PRIMARY KEY, new_col TEXT, amount NUMERIC);
			CREATE MATERIALIZED VIEW mv_base AS SELECT new_col, SUM(amount) as total FROM t GROUP BY new_col;
			CREATE MATERIALIZED VIEW mv_top AS SELECT new_col FROM mv_base WHERE total > 0;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeDeletesData},
		expectedDBSchemaDDL: []string{`
			CREATE TABLE t(id INT PRIMARY KEY, amount NUMERIC, new_col TEXT);
			CREATE MATERIALIZED VIEW mv_base AS SELECT new_col, SUM(amount) as total FROM t GROUP BY new_col WITH NO DATA;
			CREATE MATERIALIZED VIEW mv_top AS SELECT new_col FROM mv_base WHERE total > 0 WITH NO DATA;
		`},
	},
}

func TestMaterializedViewGapTestCases(t *testing.T) {
	runTestCases(t, materializedViewGapTestCases)
}
