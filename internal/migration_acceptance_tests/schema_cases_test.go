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
			CREATE TABLE fizz(
			);

			CREATE SEQUENCE foobar_sequence
			    AS BIGINT
				INCREMENT BY 2
				MINVALUE 5 MAXVALUE 100
				START WITH 10 CACHE 5 CYCLE
				OWNED BY NONE;

			CREATE FUNCTION add(a integer, b integer) RETURNS integer
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
				RETURN add(a, b) + increment(a);

			CREATE TABLE foobar(
			    id INT,
			    fizz SERIAL NOT NULL,
				foo VARCHAR(255) DEFAULT 'some default' NOT NULL CHECK (LENGTH(foo) > 0),
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST(foo);

			CREATE TABLE foobar_1 PARTITION of foobar(
			    fizz NOT NULL
			) FOR VALUES IN ('foobar_1_val_1', 'foobar_1_val_2');

			-- partitioned indexes
			CREATE INDEX foobar_normal_idx ON foobar(foo DESC, fizz);
			CREATE INDEX foobar_hash_idx ON foobar USING hash (foo);
			CREATE UNIQUE INDEX foobar_unique_idx ON foobar(foo, bar);
			-- local indexes
			CREATE INDEX foobar_1_local_idx ON foobar_1(foo, fizz);

			CREATE table bar(
			    id VARCHAR(255) PRIMARY KEY,
			    foo INT DEFAULT 0 CHECK (foo > 0 AND foo > bar),
			    bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
			    fizz timestamptz DEFAULT CURRENT_TIMESTAMP,
			    buzz REAL NOT NULL CHECK (buzz IS NOT NULL)
			);
			CREATE INDEX bar_normal_idx ON bar(bar);
			CREATE INDEX bar_another_normal_id ON bar(bar, fizz);
			CREATE UNIQUE INDEX bar_unique_idx on bar(fizz, buzz);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE fizz(
			);

			CREATE SEQUENCE foobar_sequence
			    AS BIGINT
				INCREMENT BY 2
				MINVALUE 5 MAXVALUE 100
				START WITH 10 CACHE 5 CYCLE
				OWNED BY NONE;

			CREATE FUNCTION add(a integer, b integer) RETURNS integer
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
				RETURN add(a, b) + increment(a);

			CREATE TABLE foobar(
				id INT,
			    fizz SERIAL NOT NULL,
				foo VARCHAR(255) DEFAULT 'some default' NOT NULL CHECK (LENGTH(foo) > 0),
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST(foo);

			CREATE TABLE foobar_1 PARTITION of foobar(
			    fizz NOT NULL
			) FOR VALUES IN ('foobar_1_val_1', 'foobar_1_val_2');

			-- partitioned indexes
			CREATE INDEX foobar_normal_idx ON foobar(foo DESC, fizz);
			CREATE INDEX foobar_hash_idx ON foobar USING hash (foo);
			CREATE UNIQUE INDEX foobar_unique_idx ON foobar(foo, bar);
			-- local indexes
			CREATE INDEX foobar_1_local_idx ON foobar_1(foo, fizz);

			CREATE table bar(
			    id VARCHAR(255) PRIMARY KEY,
			    foo INT DEFAULT 0 CHECK (foo > 0 AND foo > bar),
			    bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
			    fizz timestamptz DEFAULT CURRENT_TIMESTAMP,
			    buzz REAL NOT NULL CHECK (buzz IS NOT NULL)
			);
			CREATE INDEX bar_normal_idx ON bar(bar);
			CREATE INDEX bar_another_normal_id ON bar(bar, fizz);
			CREATE UNIQUE INDEX bar_unique_idx on bar(fizz, buzz);
			`,
		},
		vanillaExpectations: expectations{
			empty: true,
		},
		dataPackingExpectations: expectations{
			empty: true,
		},
	},
	{
		name: "Drop table, Add Table, Drop seq, Add Seq, Drop Funcs, Add Funcs, Drop Triggers, Add Triggers",
		oldSchemaDDL: []string{
			`
			CREATE TABLE fizz(
			);

			CREATE SEQUENCE foobar_sequence
			    AS BIGINT
				INCREMENT BY 2
				MINVALUE 5 MAXVALUE 100
				START WITH 10 CACHE 5 CYCLE
				OWNED BY NONE;

			CREATE FUNCTION add(a integer, b integer) RETURNS integer
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
				RETURN add(a, b) + increment(a);

			CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = increment(OLD.version);
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TABLE foobar(
			    id INT PRIMARY KEY,
			    fizz SERIAL NOT NULL,
				foo VARCHAR(255) DEFAULT 'some default' NOT NULL,
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);
			CREATE INDEX foobar_normal_idx ON foobar USING hash (fizz);
			CREATE UNIQUE INDEX foobar_unique_idx ON foobar(foo, bar DESC);

			CREATE TRIGGER "some trigger"
				BEFORE UPDATE ON foobar
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE "increment version"();

			CREATE table bar(
			    id VARCHAR(255) PRIMARY KEY,
			    foo INT DEFAULT 0,
			    bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
			    fizz timestamptz DEFAULT CURRENT_TIMESTAMP,
			    buzz REAL NOT NULL
			);
			ALTER TABLE bar ADD CONSTRAINT "FOO_CHECK" CHECK (foo < bar) NOT VALID;
			CREATE INDEX bar_normal_idx ON bar(bar);
			CREATE INDEX bar_another_normal_id ON bar(bar DESC, fizz DESC);
			CREATE UNIQUE INDEX bar_unique_idx on bar(fizz, buzz);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE fizz(
			);

			CREATE SEQUENCE new_foobar_sequence
			    AS SMALLINT
				INCREMENT BY 4
				MINVALUE 10 MAXVALUE 200
				START WITH 20 CACHE 10 NO CYCLE
				OWNED BY NONE;

			CREATE FUNCTION "new add"(a integer, b integer) RETURNS integer
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
				RETURN "new add"(a, b) + "new increment"(a);

			CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = "new increment"(OLD.version);
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TABLE "New_table"(
			    id INT PRIMARY KEY,
			    new_fizz SMALLSERIAL NOT NULL,
				new_foo VARCHAR(255) DEFAULT '' NOT NULL CHECK ( new_foo IS NOT NULL),
			    new_bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    version INT NOT NULL DEFAULT 0
			);
			ALTER TABLE "New_table" ADD CONSTRAINT "new_bar_check" CHECK ( new_bar < CURRENT_TIMESTAMP - interval '1 month' ) NO INHERIT NOT VALID;
			CREATE UNIQUE INDEX foobar_unique_idx ON "New_table"(new_foo, new_bar);

			CREATE TRIGGER "some trigger"
				BEFORE UPDATE ON "New_table"
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE "increment version"();

			CREATE TABLE bar(
			    id VARCHAR(255) PRIMARY KEY,
			    foo INT,
			    bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
			    fizz timestamptz DEFAULT CURRENT_TIMESTAMP,
			    buzz REAL NOT NULL
			);
			ALTER TABLE bar ADD CONSTRAINT "FOO_CHECK" CHECK ( foo < bar );
			CREATE INDEX bar_normal_idx ON bar(bar);
			CREATE INDEX bar_another_normal_id ON bar(bar DESC, fizz DESC);
			CREATE UNIQUE INDEX bar_unique_idx on bar(fizz, buzz);

			CREATE FUNCTION check_content() RETURNS TRIGGER AS $$
				BEGIN
				    IF LENGTH(NEW.id) == 0 THEN
				        RAISE EXCEPTION 'content is empty';
				    END IF;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_check_trigger
				BEFORE UPDATE ON bar
				FOR EACH ROW
				EXECUTE FUNCTION check_content();
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
			diff.MigrationHazardTypeHasUntrackableDependencies,
		},
		dataPackingExpectations: expectations{
			outputState: []string{
				`
			CREATE TABLE fizz(
			);

			CREATE SEQUENCE new_foobar_sequence
			    AS SMALLINT
				INCREMENT BY 4
				MINVALUE 10 MAXVALUE 200
				START WITH 20 CACHE 10 NO CYCLE
				OWNED BY NONE;

			CREATE FUNCTION "new add"(a integer, b integer) RETURNS integer
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
				RETURN "new add"(a, b) + "new increment"(a);

			CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = "new increment"(OLD.version);
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TABLE "New_table"(
				new_bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				id INT PRIMARY KEY,
				version INT NOT NULL DEFAULT 0,
				new_fizz SMALLSERIAL NOT NULL,
				new_foo VARCHAR(255) DEFAULT '' NOT NULL CHECK (new_foo IS NOT NULL)
			);
			ALTER TABLE "New_table" ADD CONSTRAINT "new_bar_check" CHECK ( new_bar < CURRENT_TIMESTAMP - interval '1 month' ) NO INHERIT NOT VALID;
			CREATE UNIQUE INDEX foobar_unique_idx ON "New_table"(new_foo, new_bar);

			CREATE TRIGGER "some trigger"
				BEFORE UPDATE ON "New_table"
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE "increment version"();

			CREATE TABLE bar(
				id VARCHAR(255) PRIMARY KEY,
				foo INT,
				bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
				fizz timestamptz DEFAULT CURRENT_TIMESTAMP,
				buzz REAL NOT NULL
			);
			ALTER TABLE bar ADD CONSTRAINT "FOO_CHECK" CHECK ( foo < bar );
			CREATE INDEX bar_normal_idx ON bar(bar);
			CREATE INDEX bar_another_normal_id ON bar(bar DESC, fizz DESC);
			CREATE UNIQUE INDEX bar_unique_idx on bar(fizz, buzz);

			CREATE FUNCTION check_content() RETURNS TRIGGER AS $$
				BEGIN
				    IF LENGTH(NEW.id) == 0 THEN
				        RAISE EXCEPTION 'content is empty';
				    END IF;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_check_trigger
				BEFORE UPDATE ON bar
				FOR EACH ROW
				EXECUTE FUNCTION check_content();
				`,
			},
		},
	},
	{
		name: "Drop partitioned table, Add partitioned table with local keys",
		oldSchemaDDL: []string{
			`
			CREATE TABLE foobar(
			    id INT,
			    fizz SERIAL NOT NULL,
				foo VARCHAR(255) DEFAULT 'some default' NOT NULL CHECK (LENGTH(foo) > 0),
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			    PRIMARY KEY (foo, id)
			) PARTITION BY LIST(foo);

			CREATE TABLE foobar_1 PARTITION of foobar(
			    fizz NOT NULL
			) FOR VALUES IN ('foobar_1_val_1', 'foobar_1_val_2');

			-- partitioned indexes
			CREATE INDEX foobar_normal_idx ON foobar(foo, fizz);
			CREATE UNIQUE INDEX foobar_unique_idx ON foobar(foo, bar);
			-- local indexes
			CREATE INDEX foobar_1_local_idx ON foobar_1(foo, fizz);

			CREATE table bar(
			    id VARCHAR(255) PRIMARY KEY,
			    foo INT DEFAULT 0 CHECK (foo > 0 AND foo > bar),
			    bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
			    fizz timestamptz DEFAULT CURRENT_TIMESTAMP,
			    buzz REAL NOT NULL CHECK (buzz IS NOT NULL)
			);
			CREATE INDEX bar_normal_idx ON bar(bar);
			CREATE INDEX bar_another_normal_id ON bar(bar, fizz);
			CREATE UNIQUE INDEX bar_unique_idx on bar(fizz, buzz);

			CREATE TABLE fizz(
			);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE new_foobar(
			    id INT,
				fizz SERIAL NOT NULL,
				foo VARCHAR(255) DEFAULT 'some default' NOT NULL CHECK (LENGTH(foo) > 0),
			    bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			) PARTITION BY LIST(foo);

			CREATE TABLE foobar_1 PARTITION of new_foobar(
			    fizz NOT NULL,
			    PRIMARY KEY (foo, bar)
			) FOR VALUES IN ('foobar_1_val_1', 'foobar_1_val_2');

			-- local indexes
			CREATE INDEX foobar_1_local_idx ON foobar_1(foo, fizz);
			-- partitioned indexes
			CREATE INDEX foobar_normal_idx ON new_foobar(foo, fizz);
			CREATE UNIQUE INDEX foobar_unique_idx ON new_foobar(foo, bar);

			CREATE table bar(
			    id VARCHAR(255) PRIMARY KEY,
			    foo INT DEFAULT 0 CHECK (foo > 0 AND foo > bar),
			    bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
			    fizz timestamptz DEFAULT CURRENT_TIMESTAMP,
			    buzz REAL NOT NULL CHECK (buzz IS NOT NULL)
			);
			CREATE INDEX bar_normal_idx ON bar(bar);
			CREATE INDEX bar_another_normal_id ON bar(bar, fizz);
			CREATE UNIQUE INDEX bar_unique_idx on bar(fizz, buzz);

			CREATE TABLE fizz(
			);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
		dataPackingExpectations: expectations{
			outputState: []string{
				`
				CREATE TABLE new_foobar(
					bar TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					id INT,
					fizz SERIAL NOT NULL,
					foo VARCHAR(255) DEFAULT 'some default' NOT NULL CHECK (LENGTH(foo) > 0)
				) PARTITION BY LIST(foo);

				CREATE TABLE foobar_1 PARTITION of new_foobar(
					fizz NOT NULL,
					PRIMARY KEY (foo, bar)
				) FOR VALUES IN ('foobar_1_val_1', 'foobar_1_val_2');

				-- local indexes
				CREATE INDEX foobar_1_local_idx ON foobar_1(foo, fizz);
				-- partitioned indexes
				CREATE INDEX foobar_normal_idx ON new_foobar(foo, fizz);
				CREATE UNIQUE INDEX foobar_unique_idx ON new_foobar(foo, bar);

				CREATE table bar(
					id VARCHAR(255) PRIMARY KEY,
					foo INT DEFAULT 0 CHECK (foo > 0 AND foo > bar),
					bar DOUBLE PRECISION NOT NULL DEFAULT 8.8,
					fizz timestamptz DEFAULT CURRENT_TIMESTAMP,
					buzz REAL NOT NULL CHECK (buzz IS NOT NULL)
				);
				CREATE INDEX bar_normal_idx ON bar(bar);
				CREATE INDEX bar_another_normal_id ON bar(bar, fizz);
				CREATE UNIQUE INDEX bar_unique_idx on bar(fizz, buzz);

				CREATE TABLE fizz(
				);
				`,
			},
		},
	},
}

func (suite *acceptanceTestSuite) TestSchemaAcceptanceTestCases() {
	suite.runTestCases(schemaAcceptanceTests)
}
