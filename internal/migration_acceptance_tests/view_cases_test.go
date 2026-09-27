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
	{
		name: "no-op - view definition with an unnamed union column",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY
            );

            CREATE VIEW foobar_view AS
                SELECT x.id, x.kind
                FROM (
                    SELECT id, 'credit'::text AS kind FROM foobar
                    UNION ALL
                    SELECT id, 'incentive' FROM foobar
                ) x;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY
            );

            CREATE VIEW foobar_view AS
                SELECT x.id, x.kind
                FROM (
                    SELECT id, 'credit'::text AS kind FROM foobar
                    UNION ALL
                    SELECT id, 'incentive' FROM foobar
                ) x;
			`,
		},
		// The second UNION branch is an untyped literal, so pg_get_viewdef
		// deparses the view without naming it, and re-creating the view from that
		// text names it "text". The two databases hold the same view under two
		// texts; validation must not read that as a change.
		expectEmptyPlan: true,
	},
	{
		name: "no-op - recursive view",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                parent_id INT REFERENCES foobar(id)
            );

            CREATE VIEW foobar_hierarchy AS
                WITH RECURSIVE hierarchy AS (
                    SELECT id, parent_id, 'root'::text AS kind
                    FROM foobar
                    WHERE parent_id IS NULL
                    UNION ALL
                    SELECT f.id, f.parent_id, 'child'
                    FROM foobar f
                    JOIN hierarchy h ON f.parent_id = h.id
                )
                SELECT * FROM hierarchy;
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE foobar(
                id INT PRIMARY KEY,
                parent_id INT REFERENCES foobar(id)
            );

            CREATE VIEW foobar_hierarchy AS
                WITH RECURSIVE hierarchy AS (
                    SELECT id, parent_id, 'root'::text AS kind
                    FROM foobar
                    WHERE parent_id IS NULL
                    UNION ALL
                    SELECT f.id, f.parent_id, 'child'
                    FROM foobar f
                    JOIN hierarchy h ON f.parent_id = h.id
                )
                SELECT * FROM hierarchy;
			`,
		},
		expectEmptyPlan: true,
	},
}

func TestViewTestCases(t *testing.T) {
	runTestCases(t, viewAcceptanceTestCases)
}
