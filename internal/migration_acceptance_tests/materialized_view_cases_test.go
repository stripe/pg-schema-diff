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
}

func TestMaterializedViewTestCases(t *testing.T) {
	runTestCases(t, materializedViewAcceptanceTestCases)
}
