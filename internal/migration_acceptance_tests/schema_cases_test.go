package migration_acceptance_tests

import (
	"github.com/stripe/pg-schema-diff/pkg/diff"
)

// These are tests for "public" schema" alterations (full migrations)
var schemaAcceptanceTests = []acceptanceTestCase{
	{
		name: "No-op",
		oldSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE EXTENSION amcheck;

			CREATE TABLE fizz(
			);

			CREATE TYPE schema_1.color AS ENUM ('red', 'green', 'blue');

			CREATE SEQUENCE schema_1.foobar_sequence
				AS BIGINT
				INCREMENT BY 2
				MINVALUE 5 MAXVALUE 100
				START WITH 10 CACHE 5 CYCLE
				OWNED BY NONE;

			CREATE FUNCTION schema_1.add(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN a + b;

			CREATE FUNCTION increment(i integer) RETURNS integer AS $$
					BEGIN
							RETURN i + 1;
					END;
			$$ LANGUAGE plpgsql;

			CREATE FUNCTION function_with_dependencies(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN schema_1.add(a, b) + increment(a);

			CREATE TABLE schema_1.foobar(
				id INT,
				foo VARCHAR(255) DEFAULT 'some default' NOT NULL CHECK (LENGTH(foo) > 0),
				bar SERIAL NOT NULL,
				fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
				color schema_1.color DEFAULT 'green',
				PRIMARY KEY (foo, id),
				UNIQUE (foo, bar)
			) PARTITION BY LIST(foo);
			ALTER TABLE schema_1.foobar ENABLE ROW LEVEL SECURITY;
			ALTER TABLE schema_1.foobar FORCE ROW LEVEL SECURITY;

			CREATE TABLE foobar_1 PARTITION of schema_1.foobar(
				fizz NOT NULL
			) FOR VALUES IN ('foobar_1_val_1', 'foobar_1_val_2');

			-- partitioned indexes
			CREATE INDEX foobar_normal_idx ON schema_1.foobar(foo DESC, bar);
			CREATE INDEX foobar_hash_idx ON schema_1.foobar USING hash (foo);
			CREATE UNIQUE INDEX foobar_unique_idx ON schema_1.foobar(foo, fizz);
			-- local indexes
			CREATE INDEX foobar_1_local_idx ON foobar_1(foo, fizz);

			CREATE POLICY foobar_foo_policy ON schema_1.foobar FOR SELECT TO PUBLIC USING (foo = current_user);

			CREATE table bar(
				id  INT PRIMARY KEY,
				foo VARCHAR(255),
				bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
				fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
				buzz REAL NOT NULL CHECK (buzz IS NOT NULL),
				FOREIGN KEY (foo, fizz) REFERENCES schema_1.foobar (foo, fizz)
			);
			ALTER TABLE bar REPLICA IDENTITY FULL;
			CREATE INDEX bar_normal_idx ON bar(bar);
			CREATE INDEX bar_another_normal_id ON bar(bar, fizz);
			CREATE UNIQUE INDEX bar_unique_idx on bar(foo, buzz);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE EXTENSION amcheck;

			CREATE TABLE fizz(
			);

			CREATE TYPE schema_1.color AS ENUM ('red', 'green', 'blue');

			CREATE SEQUENCE schema_1.foobar_sequence
				AS BIGINT
				INCREMENT BY 2
				MINVALUE 5 MAXVALUE 100
				START WITH 10 CACHE 5 CYCLE
				OWNED BY NONE;

			CREATE FUNCTION schema_1.add(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN a + b;

			CREATE FUNCTION increment(i integer) RETURNS integer AS $$
					BEGIN
							RETURN i + 1;
					END;
			$$ LANGUAGE plpgsql;

			CREATE FUNCTION function_with_dependencies(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN schema_1.add(a, b) + increment(a);

			CREATE TABLE schema_1.foobar(
				id INT,
				foo VARCHAR(255) DEFAULT 'some default' NOT NULL CHECK (LENGTH(foo) > 0),
				bar SERIAL NOT NULL,
				fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
				color schema_1.color DEFAULT 'green',
				PRIMARY KEY (foo, id),
				UNIQUE (foo, bar)
			) PARTITION BY LIST(foo);
			ALTER TABLE schema_1.foobar ENABLE ROW LEVEL SECURITY;
			ALTER TABLE schema_1.foobar FORCE ROW LEVEL SECURITY;

			CREATE TABLE foobar_1 PARTITION of schema_1.foobar(
				fizz NOT NULL
			) FOR VALUES IN ('foobar_1_val_1', 'foobar_1_val_2');

			-- partitioned indexes
			CREATE INDEX foobar_normal_idx ON schema_1.foobar(foo DESC, bar);
			CREATE INDEX foobar_hash_idx ON schema_1.foobar USING hash (foo);
			CREATE UNIQUE INDEX foobar_unique_idx ON schema_1.foobar(foo, fizz);
			-- local indexes
			CREATE INDEX foobar_1_local_idx ON foobar_1(foo, fizz);

			CREATE POLICY foobar_foo_policy ON schema_1.foobar FOR SELECT TO PUBLIC USING (foo = current_user);

			CREATE table bar(
				id  INT PRIMARY KEY,
				foo VARCHAR(255),
				bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
				fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
				buzz REAL NOT NULL CHECK (buzz IS NOT NULL),
				FOREIGN KEY (foo, fizz) REFERENCES schema_1.foobar (foo, fizz)
			);
			ALTER TABLE bar REPLICA IDENTITY FULL;
			CREATE INDEX bar_normal_idx ON bar(bar);
			CREATE INDEX bar_another_normal_id ON bar(bar, fizz);
			CREATE UNIQUE INDEX bar_unique_idx on bar(foo, buzz);
			`,
		},
		expectEmptyPlan: true,
	},
	{
		name:  "Add schema, drop schema, Add enum, Drop enum, Drop table, Add Table, Drop Seq, Add Seq, Drop Funcs, Add Funcs, Drop Triggers, Add Triggers, Create Extension, Drop Extension, Create Index Using Extension, Add policies, Drop policies",
		roles: []string{"role_1"},
		oldSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE EXTENSION amcheck WITH SCHEMA schema_1;
			
			CREATE SCHEMA schema_2;

			CREATE TABLE fizz(
			);

			CREATE TYPE schema_1.color AS ENUM ('red', 'green', 'blue');

			CREATE SEQUENCE schema_2.foobar_sequence
				AS BIGINT
				INCREMENT BY 2
				MINVALUE 5 MAXVALUE 100
				START WITH 10 CACHE 5 CYCLE
				OWNED BY NONE;

			CREATE FUNCTION schema_2.add(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN a + b;

			CREATE FUNCTION increment(i integer) RETURNS integer AS $$
					BEGIN
							RETURN i + 1;
					END;
			$$ LANGUAGE plpgsql;

			CREATE FUNCTION function_with_dependencies(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN schema_2.add(a, b) + increment(a);

			CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = increment(OLD.version);
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TABLE foobar(
				id INT PRIMARY KEY,
				bar SERIAL NOT NULL,
				foo VARCHAR(255) DEFAULT 'some default' NOT NULL,
				fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
				color schema_1.color DEFAULT 'green',
				UNIQUE (foo, bar)
			);
			CREATE INDEX foobar_normal_idx ON foobar USING hash (fizz);
			CREATE UNIQUE INDEX foobar_unique_idx ON foobar(foo, fizz DESC);

			CREATE POLICY foobar_foo_policy ON foobar FOR SELECT TO PUBLIC USING (foo = current_user);

			CREATE TRIGGER "some trigger"
				BEFORE UPDATE ON foobar
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE "increment version"();

			CREATE table schema_2.bar(
				id VARCHAR(255) PRIMARY KEY,
				foo VARCHAR(255),
				bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
				fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
				buzz REAL NOT NULL,
				FOREIGN KEY (foo, fizz) REFERENCES foobar (foo, fizz)
			);
			ALTER TABLE schema_2.bar ADD CONSTRAINT "FOO_CHECK" CHECK (LENGTH(foo) < bar) NOT VALID;
			CREATE INDEX bar_normal_idx ON schema_2.bar(bar);
			CREATE INDEX bar_another_normal_id ON schema_2.bar(bar DESC, fizz DESC);
			CREATE UNIQUE INDEX bar_unique_idx on schema_2.bar(fizz, buzz);

			CREATE POLICY bar_bar_policy ON schema_2.bar FOR INSERT TO role_1 WITH CHECK (bar > 5.1);
			CREATE POLICY bar_foo_policy ON schema_2.bar FOR SELECT TO PUBLIC USING (foo = 'some_foo');
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE EXTENSION pg_trgm;
			
			CREATE SCHEMA schema_2;
			CREATE SCHEMA schema_3;

			CREATE TABLE fizz(
			);

			CREATE TYPE new_color AS ENUM ('yellow', 'orange', 'cyan');

			CREATE SEQUENCE schema_3.new_foobar_sequence
				AS SMALLINT
				INCREMENT BY 4
				MINVALUE 10 MAXVALUE 200
				START WITH 20 CACHE 10 NO CYCLE
				OWNED BY NONE;

			CREATE FUNCTION schema_3."new add"(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN a + b + a;

			CREATE FUNCTION "new increment"(i integer) RETURNS integer AS $$
					BEGIN
							RETURN i + 2;
					END;
			$$ LANGUAGE plpgsql;

			CREATE FUNCTION "new function with dependencies"(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN schema_3."new add"(a, b) + "new increment"(a);

			CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = "new increment"(OLD.version);
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TABLE "New_table"(
				new_fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
				id INT PRIMARY KEY,
				version INT NOT NULL DEFAULT 0,
				new_color new_color DEFAULT 'cyan',
				new_bar SMALLSERIAL NOT NULL,
				new_foo VARCHAR(255) DEFAULT '' NOT NULL CHECK ( new_foo IS NOT NULL),
				UNIQUE (new_foo, new_bar)
			);
			ALTER TABLE "New_table" ADD CONSTRAINT "new_fzz_check" CHECK ( new_fizz < CURRENT_TIMESTAMP - interval '1 month' ) NO INHERIT NOT VALID;
			CREATE UNIQUE INDEX foobar_unique_idx ON "New_table"(new_foo, new_fizz);

			CREATE POLICY "New_table_foo_policy" ON "New_table" FOR DELETE TO PUBLIC USING (version > 0);

			CREATE TRIGGER "some trigger"
				BEFORE UPDATE ON "New_table"
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE "increment version"();

			CREATE TABLE schema_2.bar(
				id VARCHAR(255) PRIMARY KEY,
				foo VARCHAR(255),
				bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
				fizz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
				buzz REAL NOT NULL,
				quux TEXT,
				FOREIGN KEY (foo, fizz) REFERENCES "New_table" (new_foo, new_fizz)
			);
			ALTER TABLE schema_2.bar ADD CONSTRAINT "FOO_CHECK" CHECK ( LENGTH(foo) < bar );
			CREATE INDEX bar_normal_idx ON schema_2.bar(bar);
			CREATE INDEX bar_another_normal_id ON schema_2.bar(bar DESC, fizz DESC);
			CREATE UNIQUE INDEX bar_unique_idx ON schema_2.bar(fizz, buzz);
			CREATE INDEX gin_index ON schema_2.bar USING gin (quux gin_trgm_ops);

			CREATE POLICY bar_foo_policy ON schema_2.bar FOR SELECT TO role_1 USING (foo = 'some_foo' AND quux = 'some_quux');

			CREATE FUNCTION check_content() RETURNS TRIGGER AS $$
				BEGIN
					IF LENGTH(NEW.id) == 0 THEN
						RAISE EXCEPTION 'content is empty';
					END IF;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_check_trigger
				BEFORE UPDATE ON schema_2.bar
				FOR EACH ROW
				EXECUTE FUNCTION check_content();
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresShareRowExclusiveLock,
			diff.MigrationHazardTypeAuthzUpdate,
			diff.MigrationHazardTypeDeletesData,
			diff.MigrationHazardTypeHasUntrackableDependencies,
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
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
			diff.MigrationHazardTypeAcquiresShareRowExclusiveLock,
			diff.MigrationHazardTypeDeletesData,
		},
	},
}

func (suite *acceptanceTestSuite) TestSchemaTestCases() {
	suite.runTestCases(schemaAcceptanceTests)
}
