package migration_acceptance_tests

import "testing"

// commentAcceptanceTestCases exercises COMMENT ON ... migrations across every
// object kind PostgreSQL allows comments on. Each case verifies one of three
// transitions per object: add a comment, change a comment, or remove a comment.
var commentAcceptanceTestCases = []acceptanceTestCase{
	// ─── Schema ───
	{
		name: "schema: add comment",
		oldSchemaDDL: []string{`
			CREATE SCHEMA app;
		`},
		newSchemaDDL: []string{`
			CREATE SCHEMA app;
			COMMENT ON SCHEMA app IS 'application schema';
		`},
	},
	{
		name: "schema: change comment",
		oldSchemaDDL: []string{`
			CREATE SCHEMA app;
			COMMENT ON SCHEMA app IS 'application schema';
		`},
		newSchemaDDL: []string{`
			CREATE SCHEMA app;
			COMMENT ON SCHEMA app IS 'app schema (renamed)';
		`},
	},
	{
		name: "schema: remove comment",
		oldSchemaDDL: []string{`
			CREATE SCHEMA app;
			COMMENT ON SCHEMA app IS 'application schema';
		`},
		newSchemaDDL: []string{`
			CREATE SCHEMA app;
		`},
	},
	{
		name: "schema: create with comment",
		oldSchemaDDL: []string{},
		newSchemaDDL: []string{`
			CREATE SCHEMA app;
			COMMENT ON SCHEMA app IS 'application schema';
		`},
	},

	// ─── Table + Column ───
	{
		name: "table: add comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			COMMENT ON TABLE foo IS '@interface mode:union';
		`},
	},
	{
		name: "table: change comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			COMMENT ON TABLE foo IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			COMMENT ON TABLE foo IS 'new';
		`},
	},
	{
		name: "table: remove comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			COMMENT ON TABLE foo IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
		`},
	},
	{
		name: "table: create with comment + column comment",
		oldSchemaDDL: []string{},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT, name TEXT);
			COMMENT ON TABLE foo IS '@behavior +select';
			COMMENT ON COLUMN foo.id IS 'primary id';
			COMMENT ON COLUMN foo.name IS 'display name';
		`},
	},
	{
		name: "column: add comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			COMMENT ON COLUMN foo.id IS 'primary id';
		`},
	},
	{
		name: "column: change comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			COMMENT ON COLUMN foo.id IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			COMMENT ON COLUMN foo.id IS 'new';
		`},
	},
	{
		name: "column: remove comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			COMMENT ON COLUMN foo.id IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
		`},
	},
	{
		name: "column: comment with single-quote literal",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			COMMENT ON COLUMN foo.id IS 'this isn''t escaped easily';
		`},
	},

	// ─── Enum (type) ───
	{
		name: "enum: add comment",
		oldSchemaDDL: []string{`
			CREATE TYPE color AS ENUM ('red', 'green');
		`},
		newSchemaDDL: []string{`
			CREATE TYPE color AS ENUM ('red', 'green');
			COMMENT ON TYPE color IS 'color codes';
		`},
	},
	{
		name: "enum: change comment",
		oldSchemaDDL: []string{`
			CREATE TYPE color AS ENUM ('red', 'green');
			COMMENT ON TYPE color IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE TYPE color AS ENUM ('red', 'green');
			COMMENT ON TYPE color IS 'new';
		`},
	},
	{
		name: "enum: create with comment",
		oldSchemaDDL: []string{},
		newSchemaDDL: []string{`
			CREATE TYPE color AS ENUM ('red', 'green');
			COMMENT ON TYPE color IS 'color codes';
		`},
	},

	// ─── Function ───
	{
		name: "function: add comment",
		oldSchemaDDL: []string{`
			CREATE FUNCTION add(a int, b int) RETURNS int LANGUAGE sql AS 'SELECT a + b';
		`},
		newSchemaDDL: []string{`
			CREATE FUNCTION add(a int, b int) RETURNS int LANGUAGE sql AS 'SELECT a + b';
			COMMENT ON FUNCTION add(int, int) IS '@name addition';
		`},
	},
	{
		name: "function: change comment only (no body change)",
		oldSchemaDDL: []string{`
			CREATE FUNCTION add(a int, b int) RETURNS int LANGUAGE sql AS 'SELECT a + b';
			COMMENT ON FUNCTION add(int, int) IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE FUNCTION add(a int, b int) RETURNS int LANGUAGE sql AS 'SELECT a + b';
			COMMENT ON FUNCTION add(int, int) IS 'new';
		`},
	},
	{
		name: "function: remove comment",
		oldSchemaDDL: []string{`
			CREATE FUNCTION add(a int, b int) RETURNS int LANGUAGE sql AS 'SELECT a + b';
			COMMENT ON FUNCTION add(int, int) IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE FUNCTION add(a int, b int) RETURNS int LANGUAGE sql AS 'SELECT a + b';
		`},
	},
	{
		name: "function: create with comment",
		oldSchemaDDL: []string{},
		newSchemaDDL: []string{`
			CREATE FUNCTION add(a int, b int) RETURNS int LANGUAGE sql AS 'SELECT a + b';
			COMMENT ON FUNCTION add(int, int) IS '@name addition';
		`},
	},

	// ─── Procedure ───
	{
		name: "procedure: add comment",
		oldSchemaDDL: []string{`
			CREATE PROCEDURE noop() LANGUAGE plpgsql AS $$ BEGIN END $$;
		`},
		newSchemaDDL: []string{`
			CREATE PROCEDURE noop() LANGUAGE plpgsql AS $$ BEGIN END $$;
			COMMENT ON PROCEDURE noop() IS 'a procedure';
		`},
	},
	{
		name: "procedure: change comment only",
		oldSchemaDDL: []string{`
			CREATE PROCEDURE noop() LANGUAGE plpgsql AS $$ BEGIN END $$;
			COMMENT ON PROCEDURE noop() IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE PROCEDURE noop() LANGUAGE plpgsql AS $$ BEGIN END $$;
			COMMENT ON PROCEDURE noop() IS 'new';
		`},
	},
	{
		name: "procedure: remove comment",
		oldSchemaDDL: []string{`
			CREATE PROCEDURE noop() LANGUAGE plpgsql AS $$ BEGIN END $$;
			COMMENT ON PROCEDURE noop() IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE PROCEDURE noop() LANGUAGE plpgsql AS $$ BEGIN END $$;
		`},
	},

	// ─── Trigger ───
	{
		name: "trigger: add comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE FUNCTION trg_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
			CREATE TRIGGER trg BEFORE INSERT ON foo FOR EACH ROW EXECUTE FUNCTION trg_fn();
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE FUNCTION trg_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
			CREATE TRIGGER trg BEFORE INSERT ON foo FOR EACH ROW EXECUTE FUNCTION trg_fn();
			COMMENT ON TRIGGER trg ON foo IS 'audit trigger';
		`},
	},
	{
		name: "trigger: change comment only",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE FUNCTION trg_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
			CREATE TRIGGER trg BEFORE INSERT ON foo FOR EACH ROW EXECUTE FUNCTION trg_fn();
			COMMENT ON TRIGGER trg ON foo IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE FUNCTION trg_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
			CREATE TRIGGER trg BEFORE INSERT ON foo FOR EACH ROW EXECUTE FUNCTION trg_fn();
			COMMENT ON TRIGGER trg ON foo IS 'new';
		`},
	},
	{
		name: "trigger: remove comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE FUNCTION trg_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
			CREATE TRIGGER trg BEFORE INSERT ON foo FOR EACH ROW EXECUTE FUNCTION trg_fn();
			COMMENT ON TRIGGER trg ON foo IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE FUNCTION trg_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
			CREATE TRIGGER trg BEFORE INSERT ON foo FOR EACH ROW EXECUTE FUNCTION trg_fn();
		`},
	},

	// ─── View ───
	{
		name: "view: add comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE VIEW v AS SELECT id FROM foo;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE VIEW v AS SELECT id FROM foo;
			COMMENT ON VIEW v IS '@behavior +select';
		`},
	},
	{
		name: "view: change comment only",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE VIEW v AS SELECT id FROM foo;
			COMMENT ON VIEW v IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE VIEW v AS SELECT id FROM foo;
			COMMENT ON VIEW v IS 'new';
		`},
	},
	{
		name: "view: remove comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE VIEW v AS SELECT id FROM foo;
			COMMENT ON VIEW v IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE VIEW v AS SELECT id FROM foo;
		`},
	},

	// ─── Materialized view ───
	{
		name: "materialized view: add comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE MATERIALIZED VIEW mv AS SELECT id FROM foo;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE MATERIALIZED VIEW mv AS SELECT id FROM foo;
			COMMENT ON MATERIALIZED VIEW mv IS 'mv comment';
		`},
	},
	{
		name: "materialized view: change comment only",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE MATERIALIZED VIEW mv AS SELECT id FROM foo;
			COMMENT ON MATERIALIZED VIEW mv IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			CREATE MATERIALIZED VIEW mv AS SELECT id FROM foo;
			COMMENT ON MATERIALIZED VIEW mv IS 'new';
		`},
	},

	// ─── Sequence ───
	{
		name: "sequence: add comment",
		oldSchemaDDL: []string{`
			CREATE SEQUENCE s;
		`},
		newSchemaDDL: []string{`
			CREATE SEQUENCE s;
			COMMENT ON SEQUENCE s IS 'sequence';
		`},
	},
	{
		name: "sequence: change comment",
		oldSchemaDDL: []string{`
			CREATE SEQUENCE s;
			COMMENT ON SEQUENCE s IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE SEQUENCE s;
			COMMENT ON SEQUENCE s IS 'new';
		`},
	},

	// ─── Index ───
	{
		name: "index: add comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT, name TEXT);
			CREATE INDEX foo_name_idx ON foo (name);
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT, name TEXT);
			CREATE INDEX foo_name_idx ON foo (name);
			COMMENT ON INDEX foo_name_idx IS 'lookup by name';
		`},
	},
	{
		name: "index: change comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT, name TEXT);
			CREATE INDEX foo_name_idx ON foo (name);
			COMMENT ON INDEX foo_name_idx IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT, name TEXT);
			CREATE INDEX foo_name_idx ON foo (name);
			COMMENT ON INDEX foo_name_idx IS 'new';
		`},
	},

	// ─── Constraint (CHECK) ───
	{
		name: "check constraint: add comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT, CONSTRAINT foo_id_pos CHECK (id > 0));
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT, CONSTRAINT foo_id_pos CHECK (id > 0));
			COMMENT ON CONSTRAINT foo_id_pos ON foo IS 'positive';
		`},
	},
	{
		name: "check constraint: change comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT, CONSTRAINT foo_id_pos CHECK (id > 0));
			COMMENT ON CONSTRAINT foo_id_pos ON foo IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT, CONSTRAINT foo_id_pos CHECK (id > 0));
			COMMENT ON CONSTRAINT foo_id_pos ON foo IS 'new';
		`},
	},

	// ─── Constraint (FOREIGN KEY) ───
	{
		name: "fk constraint: add comment",
		oldSchemaDDL: []string{`
			CREATE TABLE parent (id INT PRIMARY KEY);
			CREATE TABLE child (id INT, parent_id INT, CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES parent(id));
		`},
		newSchemaDDL: []string{`
			CREATE TABLE parent (id INT PRIMARY KEY);
			CREATE TABLE child (id INT, parent_id INT, CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES parent(id));
			COMMENT ON CONSTRAINT child_parent_fk ON child IS 'fk to parent';
		`},
	},

	// ─── Policy ───
	{
		name: "policy: add comment",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			ALTER TABLE foo ENABLE ROW LEVEL SECURITY;
			CREATE POLICY p ON foo FOR SELECT USING (true);
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			ALTER TABLE foo ENABLE ROW LEVEL SECURITY;
			CREATE POLICY p ON foo FOR SELECT USING (true);
			COMMENT ON POLICY p ON foo IS 'allow all reads';
		`},
	},
	{
		name: "policy: change comment only",
		oldSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			ALTER TABLE foo ENABLE ROW LEVEL SECURITY;
			CREATE POLICY p ON foo FOR SELECT USING (true);
			COMMENT ON POLICY p ON foo IS 'old';
		`},
		newSchemaDDL: []string{`
			CREATE TABLE foo (id INT);
			ALTER TABLE foo ENABLE ROW LEVEL SECURITY;
			CREATE POLICY p ON foo FOR SELECT USING (true);
			COMMENT ON POLICY p ON foo IS 'new';
		`},
	},
}

func TestCommentTestCases(t *testing.T) {
	runTestCases(t, commentAcceptanceTestCases)
}
