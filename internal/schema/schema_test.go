package schema_test

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/pgengine"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type testCase struct {
	name           string
	ddl            []string
	expectedSchema schema.Schema
	expectedHash   string
	expectedErrIs  error
}

var (
	defaultCollation = schema.SchemaQualifiedName{
		EscapedName: `"default"`,
		SchemaName:  "pg_catalog",
	}
	cCollation = schema.SchemaQualifiedName{
		EscapedName: `"C"`,
		SchemaName:  "pg_catalog",
	}

	testCases = []*testCase{
		{
			name: "Simple test",
			ddl: []string{`
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

			CREATE TABLE foo (
				id INTEGER PRIMARY KEY,
				author TEXT COLLATE "C",
				content TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP CHECK (created_at > CURRENT_TIMESTAMP - interval '1 month') NO INHERIT,
				version INT NOT NULL DEFAULT 0,
				CHECK ( function_with_dependencies(id, id) > 0)
			);

			ALTER TABLE foo ADD CONSTRAINT author_check CHECK (author IS NOT NULL AND LENGTH(author) > 0) NO INHERIT NOT VALID;
			CREATE INDEX some_idx ON foo USING hash (content);
			CREATE UNIQUE INDEX some_unique_idx ON foo (created_at DESC, author ASC);

			CREATE FUNCTION increment_version() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = OLD.version + 1;
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_trigger
				BEFORE UPDATE ON foo
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();
		`},
			expectedHash: "9648c294aed76ef6",
			expectedSchema: schema.Schema{
				Name: "public",
				Tables: []schema.Table{
					{
						Name: "foo",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Size: 4},
							{Name: "author", Type: "text", IsNullable: true, Size: -1, Collation: cCollation},
							{Name: "content", Type: "text", Default: "''::text", Size: -1, Collation: defaultCollation},
							{Name: "created_at", Type: "timestamp without time zone", Default: "CURRENT_TIMESTAMP", Size: 8},
							{Name: "version", Type: "integer", Default: "0", Size: 4},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "author_check", Expression: "((author IS NOT NULL) AND (length(author) > 0))"},
							{Name: "foo_created_at_check", Expression: "(created_at > (CURRENT_TIMESTAMP - '1 mon'::interval))", IsValid: true},
							{
								Name:          "foo_id_check",
								Expression:    "(function_with_dependencies(id, id) > 0)",
								IsValid:       true,
								IsInheritable: true,
								DependsOnFunctions: []schema.SchemaQualifiedName{
									{EscapedName: "\"function_with_dependencies\"(a integer, b integer)", SchemaName: "public"},
								},
							},
						},
					},
				},
				Indexes: []schema.Index{
					{
						TableName: "foo",
						Name:      "foo_pkey", Columns: []string{"id"}, IsPk: true, IsUnique: true, ConstraintName: "foo_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_pkey ON public.foo USING btree (id)",
					},
					{
						TableName: "foo",
						Name:      "some_idx", Columns: []string{"content"},
						GetIndexDefStmt: "CREATE INDEX some_idx ON public.foo USING hash (content)",
					},
					{
						TableName: "foo",
						Name:      "some_unique_idx", Columns: []string{"created_at", "author"}, IsPk: false, IsUnique: true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX some_unique_idx ON public.foo USING btree (created_at DESC, author)",
					},
				},
				Functions: []schema.Function{
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{EscapedName: "\"add\"(a integer, b integer)", SchemaName: "public"},
						FunctionDef:         "CREATE OR REPLACE FUNCTION public.add(a integer, b integer)\n RETURNS integer\n LANGUAGE sql\n IMMUTABLE STRICT\nRETURN (a + b)\n",
						Language:            "sql",
					},
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{EscapedName: "\"function_with_dependencies\"(a integer, b integer)", SchemaName: "public"},
						FunctionDef:         "CREATE OR REPLACE FUNCTION public.function_with_dependencies(a integer, b integer)\n RETURNS integer\n LANGUAGE sql\n IMMUTABLE STRICT\nRETURN (add(a, b) + increment(a))\n",
						Language:            "sql",
						DependsOnFunctions: []schema.SchemaQualifiedName{
							{EscapedName: "\"add\"(a integer, b integer)", SchemaName: "public"},
							{EscapedName: "\"increment\"(i integer)", SchemaName: "public"},
						},
					},
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{EscapedName: "\"increment\"(i integer)", SchemaName: "public"},
						FunctionDef:         "CREATE OR REPLACE FUNCTION public.increment(i integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$\n\t\t\t\t\tBEGIN\n\t\t\t\t\t\t\tRETURN i + 1;\n\t\t\t\t\tEND;\n\t\t\t$function$\n",
						Language:            "plpgsql",
					},
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{EscapedName: "\"increment_version\"()", SchemaName: "public"},
						FunctionDef:         "CREATE OR REPLACE FUNCTION public.increment_version()\n RETURNS trigger\n LANGUAGE plpgsql\nAS $function$\n\t\t\t\tBEGIN\n\t\t\t\t\tNEW.version = OLD.version + 1;\n\t\t\t\t\tRETURN NEW;\n\t\t\t\tEND;\n\t\t\t$function$\n",
						Language:            "plpgsql",
					},
				},
				Triggers: []schema.Trigger{
					{
						EscapedName:              "\"some_trigger\"",
						OwningTable:              schema.SchemaQualifiedName{EscapedName: "\"foo\"", SchemaName: "public"},
						OwningTableUnescapedName: "foo",
						Function:                 schema.SchemaQualifiedName{EscapedName: "\"increment_version\"()", SchemaName: "public"},
						GetTriggerDefStmt:        "CREATE TRIGGER some_trigger BEFORE UPDATE ON public.foo FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION increment_version()",
					},
				},
			},
		},
		{
			name: "Simple partition test",
			ddl: []string{`
			CREATE TABLE foo (
				id INTEGER CHECK (id > 0),
				author TEXT COLLATE "C",
				content TEXT DEFAULT '',
				genre VARCHAR(256) NOT NULL,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP CHECK (created_at > CURRENT_TIMESTAMP - interval '1 month'),
				PRIMARY KEY (author, id)
			) PARTITION BY LIST (author);
			ALTER TABLE foo ADD CONSTRAINT author_check CHECK (author IS NOT NULL AND LENGTH(author) > 0) NOT VALID;

			CREATE FUNCTION increment_version() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = OLD.version + 1;
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_trigger
				BEFORE UPDATE ON foo
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();

			CREATE TABLE foo_1 PARTITION OF foo(
			    content NOT NULL
			) FOR VALUES IN ('some author 1');
			CREATE TABLE foo_2 PARTITION OF foo FOR VALUES IN ('some author 2');
			CREATE TABLE foo_3 PARTITION OF foo FOR VALUES IN ('some author 3');

			-- partitioned indexes
			CREATE INDEX some_partitioned_idx ON foo USING hash(author);
			CREATE UNIQUE INDEX some_unique_partitioned_idx ON foo(author, created_at DESC);
			CREATE INDEX some_invalid_idx ON ONLY foo(author, genre);

			-- local indexes
			CREATE UNIQUE INDEX foo_1_local_idx ON foo_1(author DESC, id);
			CREATE UNIQUE INDEX foo_2_local_idx ON foo_2(author, content);
			CREATE UNIQUE INDEX foo_3_local_idx ON foo_3(author, created_at);

			CREATE TRIGGER some_partition_trigger
				BEFORE UPDATE ON foo_1
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();

		`},
			expectedHash: "651c9229cd8373f0",
			expectedSchema: schema.Schema{
				Name: "public",
				Tables: []schema.Table{
					{
						Name: "foo",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Size: 4},
							{Name: "author", Type: "text", Size: -1, Collation: cCollation},
							{Name: "content", Type: "text", Default: "''::text", IsNullable: true, Size: -1, Collation: defaultCollation},
							{Name: "genre", Type: "character varying(256)", Size: -1, Collation: defaultCollation},
							{Name: "created_at", Type: "timestamp without time zone", Default: "CURRENT_TIMESTAMP", Size: 8},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "author_check", Expression: "((author IS NOT NULL) AND (length(author) > 0))", IsInheritable: true},
							{Name: "foo_created_at_check", Expression: "(created_at > (CURRENT_TIMESTAMP - '1 mon'::interval))", IsValid: true, IsInheritable: true},
							{Name: "foo_id_check", Expression: "(id > 0)", IsValid: true, IsInheritable: true},
						},
						PartitionKeyDef: "LIST (author)",
					},
					{
						ParentTableName: "foo",
						Name:            "foo_1",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Size: 4},
							{Name: "author", Type: "text", Size: -1, Collation: cCollation},
							{Name: "content", Type: "text", Default: "''::text", Size: -1, Collation: defaultCollation},
							{Name: "genre", Type: "character varying(256)", Size: -1, Collation: defaultCollation},
							{Name: "created_at", Type: "timestamp without time zone", Default: "CURRENT_TIMESTAMP", Size: 8},
						},
						CheckConstraints: nil,
						ForValues:        "FOR VALUES IN ('some author 1')",
					},
					{
						ParentTableName: "foo",
						Name:            "foo_2",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Size: 4},
							{Name: "author", Type: "text", Size: -1, Collation: cCollation},
							{Name: "content", Type: "text", Default: "''::text", IsNullable: true, Size: -1, Collation: defaultCollation},
							{Name: "genre", Type: "character varying(256)", Size: -1, Collation: defaultCollation},
							{Name: "created_at", Type: "timestamp without time zone", Default: "CURRENT_TIMESTAMP", Size: 8},
						},
						CheckConstraints: nil,
						ForValues:        "FOR VALUES IN ('some author 2')",
					},
					{
						ParentTableName: "foo",
						Name:            "foo_3",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Size: 4},
							{Name: "author", Type: "text", Size: -1, Collation: cCollation},
							{Name: "content", Type: "text", Default: "''::text", IsNullable: true, Size: -1, Collation: defaultCollation},
							{Name: "genre", Type: "character varying(256)", Size: -1, Collation: defaultCollation},
							{Name: "created_at", Type: "timestamp without time zone", Default: "CURRENT_TIMESTAMP", Size: 8},
						},
						CheckConstraints: nil,
						ForValues:        "FOR VALUES IN ('some author 3')",
					},
				},
				Indexes: []schema.Index{
					{
						TableName: "foo",
						Name:      "foo_pkey", Columns: []string{"author", "id"}, IsPk: true, IsUnique: true, ConstraintName: "foo_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_pkey ON ONLY public.foo USING btree (author, id)",
					},
					{
						TableName: "foo",
						Name:      "some_partitioned_idx", Columns: []string{"author"},
						GetIndexDefStmt: "CREATE INDEX some_partitioned_idx ON ONLY public.foo USING hash (author)",
					},
					{
						TableName: "foo",
						Name:      "some_unique_partitioned_idx", Columns: []string{"author", "created_at"}, IsPk: false, IsUnique: true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX some_unique_partitioned_idx ON ONLY public.foo USING btree (author, created_at DESC)",
					},
					{
						TableName: "foo",
						Name:      "some_invalid_idx", Columns: []string{"author", "genre"}, IsInvalid: true, IsPk: false, IsUnique: false,
						GetIndexDefStmt: "CREATE INDEX some_invalid_idx ON ONLY public.foo USING btree (author, genre)",
					},
					// foo_1 indexes
					{
						TableName: "foo_1",
						Name:      "foo_1_author_idx", Columns: []string{"author"}, ParentIdxName: "some_partitioned_idx",
						GetIndexDefStmt: "CREATE INDEX foo_1_author_idx ON public.foo_1 USING hash (author)",
					},
					{
						TableName: "foo_1",
						Name:      "foo_1_author_created_at_idx", Columns: []string{"author", "created_at"}, IsPk: false, IsUnique: true, ParentIdxName: "some_unique_partitioned_idx",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_1_author_created_at_idx ON public.foo_1 USING btree (author, created_at DESC)",
					},
					{
						TableName: "foo_1",
						Name:      "foo_1_local_idx", Columns: []string{"author", "id"}, IsUnique: true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_1_local_idx ON public.foo_1 USING btree (author DESC, id)",
					},
					{
						TableName: "foo_1",
						Name:      "foo_1_pkey", Columns: []string{"author", "id"}, IsPk: true, IsUnique: true, ConstraintName: "foo_1_pkey", ParentIdxName: "foo_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_1_pkey ON public.foo_1 USING btree (author, id)",
					},
					// foo_2 indexes
					{
						TableName: "foo_2",
						Name:      "foo_2_author_idx", Columns: []string{"author"}, ParentIdxName: "some_partitioned_idx",
						GetIndexDefStmt: "CREATE INDEX foo_2_author_idx ON public.foo_2 USING hash (author)",
					},
					{
						TableName: "foo_2",
						Name:      "foo_2_author_created_at_idx", Columns: []string{"author", "created_at"}, IsPk: false, IsUnique: true, ParentIdxName: "some_unique_partitioned_idx",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_2_author_created_at_idx ON public.foo_2 USING btree (author, created_at DESC)",
					},
					{
						TableName: "foo_2",
						Name:      "foo_2_local_idx", Columns: []string{"author", "content"}, IsUnique: true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_2_local_idx ON public.foo_2 USING btree (author, content)",
					},
					{
						TableName: "foo_2",
						Name:      "foo_2_pkey", Columns: []string{"author", "id"}, IsPk: true, IsUnique: true, ConstraintName: "foo_2_pkey", ParentIdxName: "foo_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_2_pkey ON public.foo_2 USING btree (author, id)",
					},
					// foo_3 indexes
					{
						TableName: "foo_3",
						Name:      "foo_3_author_idx", Columns: []string{"author"}, ParentIdxName: "some_partitioned_idx",
						GetIndexDefStmt: "CREATE INDEX foo_3_author_idx ON public.foo_3 USING hash (author)",
					},
					{
						TableName: "foo_3",
						Name:      "foo_3_author_created_at_idx", Columns: []string{"author", "created_at"}, IsPk: false, IsUnique: true, ParentIdxName: "some_unique_partitioned_idx",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_3_author_created_at_idx ON public.foo_3 USING btree (author, created_at DESC)",
					},
					{
						TableName: "foo_3",
						Name:      "foo_3_local_idx", Columns: []string{"author", "created_at"}, IsUnique: true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_3_local_idx ON public.foo_3 USING btree (author, created_at)",
					},
					{
						TableName: "foo_3",
						Name:      "foo_3_pkey", Columns: []string{"author", "id"}, IsPk: true, IsUnique: true, ConstraintName: "foo_3_pkey", ParentIdxName: "foo_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_3_pkey ON public.foo_3 USING btree (author, id)",
					},
				},

				Functions: []schema.Function{
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{EscapedName: "\"increment_version\"()", SchemaName: "public"},
						FunctionDef:         "CREATE OR REPLACE FUNCTION public.increment_version()\n RETURNS trigger\n LANGUAGE plpgsql\nAS $function$\n\t\t\t\tBEGIN\n\t\t\t\t\tNEW.version = OLD.version + 1;\n\t\t\t\t\tRETURN NEW;\n\t\t\t\tEND;\n\t\t\t$function$\n",
						Language:            "plpgsql",
					},
				},
				Triggers: []schema.Trigger{
					{
						EscapedName:              "\"some_trigger\"",
						OwningTable:              schema.SchemaQualifiedName{EscapedName: "\"foo\"", SchemaName: "public"},
						OwningTableUnescapedName: "foo",
						Function:                 schema.SchemaQualifiedName{EscapedName: "\"increment_version\"()", SchemaName: "public"},
						GetTriggerDefStmt:        "CREATE TRIGGER some_trigger BEFORE UPDATE ON public.foo FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION increment_version()",
					},
					{
						EscapedName:              "\"some_partition_trigger\"",
						OwningTable:              schema.SchemaQualifiedName{EscapedName: "\"foo_1\"", SchemaName: "public"},
						OwningTableUnescapedName: "foo_1",
						Function:                 schema.SchemaQualifiedName{EscapedName: "\"increment_version\"()", SchemaName: "public"},
						GetTriggerDefStmt:        "CREATE TRIGGER some_partition_trigger BEFORE UPDATE ON public.foo_1 FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION increment_version()",
					},
				},
			},
		},
		{
			name: "Partition test local primary key",
			ddl: []string{`
			CREATE TABLE foo (
				id INTEGER,
				author TEXT
			) PARTITION BY LIST (author);

			CREATE TABLE foo_1 PARTITION OF foo(
			    PRIMARY KEY (author, id)
			) FOR VALUES IN ('some author 1');
		`},
			expectedHash: "6976f3d0ada49b66",
			expectedSchema: schema.Schema{
				Name: "public",
				Tables: []schema.Table{
					{
						Name: "foo",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", IsNullable: true, Size: 4},
							{Name: "author", Type: "text", IsNullable: true, Size: -1, Collation: defaultCollation},
						},
						CheckConstraints: nil,
						PartitionKeyDef:  "LIST (author)",
					},
					{
						ParentTableName: "foo",
						Name:            "foo_1",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Size: 4},
							{Name: "author", Type: "text", Size: -1, Collation: defaultCollation},
						},
						CheckConstraints: nil,
						ForValues:        "FOR VALUES IN ('some author 1')",
					},
				},
				Indexes: []schema.Index{
					{
						TableName: "foo_1",
						Name:      "foo_1_pkey", Columns: []string{"author", "id"}, IsPk: true, IsUnique: true, ConstraintName: "foo_1_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_1_pkey ON public.foo_1 USING btree (author, id)",
					},
				},
			},
		},
		{
			name: "Common Data Types",
			ddl: []string{`
			CREATE TABLE foo (
				"varchar" VARCHAR(128) NOT NULL DEFAULT '',
				"text" TEXT NOT NULL DEFAULT '',
			    "bool" BOOLEAN NOT NULL DEFAULT False,
			    "blob" BYTEA NOT NULL DEFAULT '',
			    "smallint" SMALLINT NOT NULL DEFAULT 0,
			    "real" REAL NOT NULL DEFAULT 0.0,
			    "double_precision" DOUBLE PRECISION NOT NULL DEFAULT 0.0,
			    "integer" INTEGER NOT NULL DEFAULT 0,
			    "big_integer" BIGINT NOT NULL DEFAULT 0,
				"decimal" DECIMAL(65, 10) NOT NULL DEFAULT 0.0
			);
		`},
			expectedHash: "2181b2da75bb74f7",
			expectedSchema: schema.Schema{
				Name: "public",
				Tables: []schema.Table{
					{
						Name: "foo",
						Columns: []schema.Column{
							{Name: "varchar", Type: "character varying(128)", Default: "''::character varying", Size: -1, Collation: defaultCollation},
							{Name: "text", Type: "text", Default: "''::text", Size: -1, Collation: defaultCollation},
							{Name: "bool", Type: "boolean", Default: "false", Size: 1},
							{Name: "blob", Type: "bytea", Default: `'\x'::bytea`, Size: -1},
							{Name: "smallint", Type: "smallint", Default: "0", Size: 2},
							{Name: "real", Type: "real", Default: "0.0", Size: 4},
							{Name: "double_precision", Type: "double precision", Default: "0.0", Size: 8},
							{Name: "integer", Type: "integer", Default: "0", Size: 4},
							{Name: "big_integer", Type: "bigint", Default: "0", Size: 8},
							{Name: "decimal", Type: "numeric(65,10)", Default: "0.0", Size: -1},
						},
						CheckConstraints: nil,
					},
				},
			},
		},
		{
			name: "Multi-Table",
			ddl: []string{`
			CREATE TABLE foo (
				id INTEGER PRIMARY KEY CHECK (id > 0) NO INHERIT,
				content TEXT DEFAULT 'some default'
			);
			CREATE INDEX foo_idx ON foo(id, content);
			CREATE TABLE bar(
				id INTEGER PRIMARY KEY CHECK (id > 0),
				content TEXT NOT NULL
			);
			CREATE INDEX bar_idx ON bar(content, id);
			CREATE TABLE foobar(
			    id INTEGER PRIMARY KEY,
			    content BIGINT NOT NULL
			);
			ALTER TABLE foobar ADD CONSTRAINT foobar_id_check CHECK (id > 0) NOT VALID;
			CREATE UNIQUE INDEX foobar_idx ON foobar(content);
		`},
			expectedHash: "6518bbfe220d4f16",
			expectedSchema: schema.Schema{
				Name: "public",
				Tables: []schema.Table{
					{
						Name: "foo",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Size: 4},
							{Name: "content", Type: "text", IsNullable: true, Default: "'some default'::text", Size: -1, Collation: defaultCollation},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "foo_id_check", Expression: "(id > 0)", IsValid: true},
						},
					},
					{
						Name: "bar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Size: 4},
							{Name: "content", Type: "text", Size: -1, Collation: defaultCollation},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "bar_id_check", Expression: "(id > 0)", IsValid: true, IsInheritable: true},
						},
					},
					{
						Name: "foobar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Size: 4},
							{Name: "content", Type: "bigint", Size: 8},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "foobar_id_check", Expression: "(id > 0)", IsInheritable: true},
						},
					},
				},
				Indexes: []schema.Index{
					// foo indexes
					{
						TableName: "foo",
						Name:      "foo_pkey", Columns: []string{"id"}, IsPk: true, IsUnique: true, ConstraintName: "foo_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_pkey ON public.foo USING btree (id)",
					},
					{
						TableName: "foo",
						Name:      "foo_idx", Columns: []string{"id", "content"},
						GetIndexDefStmt: "CREATE INDEX foo_idx ON public.foo USING btree (id, content)",
					},
					// bar indexes
					{
						TableName: "bar",
						Name:      "bar_pkey", Columns: []string{"id"}, IsPk: true, IsUnique: true, ConstraintName: "bar_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX bar_pkey ON public.bar USING btree (id)",
					},
					{
						TableName: "bar",
						Name:      "bar_idx", Columns: []string{"content", "id"},
						GetIndexDefStmt: "CREATE INDEX bar_idx ON public.bar USING btree (content, id)",
					},
					// foobar indexes
					{
						TableName: "foobar",
						Name:      "foobar_pkey", Columns: []string{"id"}, IsPk: true, IsUnique: true, ConstraintName: "foobar_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foobar_pkey ON public.foobar USING btree (id)",
					},
					{
						TableName: "foobar",
						Name:      "foobar_idx", Columns: []string{"content"}, IsUnique: true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX foobar_idx ON public.foobar USING btree (content)",
					},
				},
			},
		},
		{
			name: "Multi-Schema",
			ddl: []string{`
			CREATE TABLE foo (
				id INTEGER PRIMARY KEY CHECK (id > 0),
				version INTEGER NOT NULL DEFAULT 0,
				content TEXT
			);

			CREATE FUNCTION dup(in int, out f1 int, out f2 text)
				AS $$ SELECT $1, CAST($1 AS text) || ' is text' $$
				LANGUAGE SQL;


			CREATE FUNCTION increment_version() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = OLD.version + 1;
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_trigger
				BEFORE UPDATE ON foo
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();

			CREATE SCHEMA test;

			CREATE TABLE test.foo(
				test_schema_id INTEGER PRIMARY KEY,
				test_schema_version INTEGER NOT NULL DEFAULT 0,
				test_schema_content TEXT CHECK (LENGTH(test_schema_content) > 0)
			);

			CREATE FUNCTION test.dup(in int, out f1 int, out f2 text)
				AS $$ SELECT $1, ' is int' $$
				LANGUAGE SQL;

			CREATE FUNCTION test.increment_version() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = OLD.version + 1;
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_trigger
				BEFORE UPDATE ON test.foo
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();

			CREATE COLLATION test."some collation" (locale = 'en_US');

			CREATE TABLE bar (
				id INTEGER CHECK (id > 0),
				author TEXT COLLATE test."some collation",
				PRIMARY KEY (author, id)
			) PARTITION BY LIST (author);
			CREATE INDEX some_partitioned_idx ON bar(author, id);
			CREATE TABLE bar_1 PARTITION OF bar FOR VALUES IN ('some author 1');

			CREATE TABLE test.bar (
				test_id INTEGER CHECK (test_id > 0),
				test_author TEXT,
				PRIMARY KEY (test_author, test_id)
			) PARTITION BY LIST (test_author);
			CREATE INDEX some_partitioned_idx ON test.bar(test_author, test_id);
			CREATE TABLE test.bar_1 PARTITION OF test.bar FOR VALUES IN ('some author 1');

			-- create a trigger on the original schema using a function from the other schema
			CREATE TRIGGER some_trigger_using_other_schema_function
				BEFORE UPDATE ON foo
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE test.increment_version();
		`},
			expectedHash: "a6a845ad846dc362",
			expectedSchema: schema.Schema{
				Name: "public",
				Tables: []schema.Table{
					{
						Name: "foo",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Size: 4},
							{Name: "version", Type: "integer", Default: "0", Size: 4},
							{Name: "content", Type: "text", IsNullable: true, Size: -1, Collation: defaultCollation},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "foo_id_check", Expression: "(id > 0)", IsValid: true, IsInheritable: true},
						},
					},
					{
						Name: "bar",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Default: "", Size: 4},
							{Name: "author", Type: "text", Default: "", Size: -1, Collation: schema.SchemaQualifiedName{SchemaName: "test", EscapedName: `"some collation"`}},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "bar_id_check", Expression: "(id > 0)", IsValid: true, IsInheritable: true},
						},
						PartitionKeyDef: "LIST (author)",
					},
					{
						ParentTableName: "bar",
						Name:            "bar_1",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Default: "", Size: 4},
							{Name: "author", Type: "text", Default: "", Size: -1, Collation: schema.SchemaQualifiedName{SchemaName: "test", EscapedName: `"some collation"`}},
						},
						CheckConstraints: nil,
						ForValues:        "FOR VALUES IN ('some author 1')",
					},
				},
				Indexes: []schema.Index{
					// foo indexes
					{
						TableName: "foo",
						Name:      "foo_pkey", Columns: []string{"id"}, IsPk: true, IsUnique: true, ConstraintName: "foo_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_pkey ON public.foo USING btree (id)",
					},
					// bar indexes
					{
						TableName: "bar",
						Name:      "bar_pkey", Columns: []string{"author", "id"}, IsPk: true, IsUnique: true, ConstraintName: "bar_pkey", ParentIdxName: "",
						GetIndexDefStmt: "CREATE UNIQUE INDEX bar_pkey ON ONLY public.bar USING btree (author, id)",
					},
					{
						TableName: "bar",
						Name:      "some_partitioned_idx", Columns: []string{"author", "id"}, IsPk: false, IsUnique: false, ConstraintName: "", ParentIdxName: "",
						GetIndexDefStmt: "CREATE INDEX some_partitioned_idx ON ONLY public.bar USING btree (author, id)",
					},
					// bar_1 indexes
					{
						TableName: "bar_1",
						Name:      "bar_1_author_id_idx", Columns: []string{"author", "id"}, IsPk: false, IsUnique: false, ConstraintName: "", ParentIdxName: "some_partitioned_idx",
						GetIndexDefStmt: "CREATE INDEX bar_1_author_id_idx ON public.bar_1 USING btree (author, id)",
					},
					{
						TableName: "bar_1",
						Name:      "bar_1_pkey", Columns: []string{"author", "id"}, IsPk: true, IsUnique: true, ConstraintName: "bar_1_pkey", ParentIdxName: "bar_pkey",
						GetIndexDefStmt: "CREATE UNIQUE INDEX bar_1_pkey ON public.bar_1 USING btree (author, id)",
					},
				},
				Functions: []schema.Function{
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{EscapedName: "\"dup\"(integer, OUT f1 integer, OUT f2 text)", SchemaName: "public"},
						FunctionDef:         "CREATE OR REPLACE FUNCTION public.dup(integer, OUT f1 integer, OUT f2 text)\n RETURNS record\n LANGUAGE sql\nAS $function$ SELECT $1, CAST($1 AS text) || ' is text' $function$\n",
						Language:            "sql",
					},
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{EscapedName: "\"increment_version\"()", SchemaName: "public"},
						FunctionDef:         "CREATE OR REPLACE FUNCTION public.increment_version()\n RETURNS trigger\n LANGUAGE plpgsql\nAS $function$\n\t\t\t\tBEGIN\n\t\t\t\t\tNEW.version = OLD.version + 1;\n\t\t\t\t\tRETURN NEW;\n\t\t\t\tEND;\n\t\t\t$function$\n",
						Language:            "plpgsql",
					},
				},
				Triggers: []schema.Trigger{
					{
						EscapedName:              "\"some_trigger\"",
						OwningTable:              schema.SchemaQualifiedName{EscapedName: "\"foo\"", SchemaName: "public"},
						OwningTableUnescapedName: "foo",
						Function:                 schema.SchemaQualifiedName{EscapedName: "\"increment_version\"()", SchemaName: "public"},
						GetTriggerDefStmt:        "CREATE TRIGGER some_trigger BEFORE UPDATE ON public.foo FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION increment_version()",
					},
				},
			},
		},
		{
			name:         "Empty Schema",
			ddl:          nil,
			expectedHash: "660be155e4c39f8b",
			expectedSchema: schema.Schema{
				Name:   "public",
				Tables: nil,
			},
		},
		{
			name: "No Indexes or constraints",
			ddl: []string{`
			CREATE TABLE foo (
				value TEXT
			);
		`},
			expectedHash: "9db57cf969f0a509",
			expectedSchema: schema.Schema{
				Name: "public",
				Tables: []schema.Table{
					{
						Name: "foo",
						Columns: []schema.Column{
							{Name: "value", Type: "text", IsNullable: true, Size: -1, Collation: defaultCollation},
						},
						CheckConstraints: nil,
					},
				},
			},
		},
	}
)

func TestSchemaTestCases(t *testing.T) {
	engine, err := pgengine.StartEngine()
	require.NoError(t, err)
	defer engine.Close()

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db, err := engine.CreateDatabase()
			require.NoError(t, err)
			conn, err := sql.Open("pgx", db.GetDSN())
			require.NoError(t, err)

			for _, stmt := range testCase.ddl {
				_, err := conn.Exec(stmt)
				require.NoError(t, err)
			}

			fetchedSchema, err := schema.GetPublicSchema(context.TODO(), conn)
			if testCase.expectedErrIs != nil {
				require.ErrorIs(t, err, testCase.expectedErrIs)
				return
			} else {
				require.NoError(t, err)
			}

			expectedNormalized := testCase.expectedSchema.Normalize()
			fetchedNormalized := fetchedSchema.Normalize()
			assert.Equal(t, expectedNormalized, fetchedNormalized, "expected=\n%# v \n fetched=%# v\n", pretty.Formatter(expectedNormalized), pretty.Formatter(fetchedNormalized))

			fetchedSchemaHash, err := fetchedSchema.Hash()
			require.NoError(t, err)
			expectedSchemaHash, err := testCase.expectedSchema.Hash()
			require.NoError(t, err)
			assert.Equal(t, testCase.expectedHash, fetchedSchemaHash)
			// same schemas should have the same hashes
			assert.Equal(t, expectedSchemaHash, fetchedSchemaHash, "hash of expected schema should match fetched hash")

			require.NoError(t, conn.Close())
			require.NoError(t, db.DropDB())
		})
	}
}

func TestIdxDefStmtToCreateIdxConcurrently(t *testing.T) {
	for _, tc := range []struct {
		name      string
		defStmt   string
		out       string
		expectErr bool
	}{
		{
			name:    "simple index",
			defStmt: `CREATE INDEX foobar ON public.foobar USING btree (foo)`,
			out:     `CREATE INDEX CONCURRENTLY foobar ON public.foobar USING btree (foo)`,
		},
		{
			name:    "unique index",
			defStmt: `CREATE UNIQUE INDEX foobar ON public.foobar USING btree (foo)`,
			out:     `CREATE UNIQUE INDEX CONCURRENTLY foobar ON public.foobar USING btree (foo)`,
		},
		{
			name:    "malicious name index",
			defStmt: `CREATE UNIQUE INDEX "CREATE INDEX ON" ON public.foobar USING btree (foo)`,
			out:     `CREATE UNIQUE INDEX CONCURRENTLY "CREATE INDEX ON" ON public.foobar USING btree (foo)`,
		},
		{
			name:      "case sensitive",
			defStmt:   `CREATE uNIQUE INDEX foobar ON public.foobar USING btree (foo)`,
			expectErr: true,
		},
		{
			name:      "errors with random start character",
			defStmt:   `ALTER TABLE CREATE UNIQUE INDEX foobar ON public.foobar USING btree (foo)`,
			expectErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := schema.GetIndexDefStatement(tc.defStmt).ToCreateIndexConcurrently()
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.out, out)
			}
		})
	}
}

func TestTriggerDefStmtToCreateOrReplace(t *testing.T) {
	for _, tc := range []struct {
		name      string
		defStmt   string
		out       string
		expectErr bool
	}{
		{
			name:    "simple trigger",
			defStmt: "CREATE TRIGGER some_trigger BEFORE UPDATE ON public.foo FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION increment_version()",
			out:     "CREATE OR REPLACE TRIGGER some_trigger BEFORE UPDATE ON public.foo FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION increment_version()",
		},
		{
			name:    "malicious name trigger",
			defStmt: `CREATE TRIGGER "CREATE TRIGGER" BEFORE UPDATE ON public.foo FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION increment_version()`,
			out:     `CREATE OR REPLACE TRIGGER "CREATE TRIGGER" BEFORE UPDATE ON public.foo FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION increment_version()`,
		},
		{
			name:      "case sensitive",
			defStmt:   "cREATE TRIGGER some_trigger BEFORE UPDATE ON public.foo FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION increment_version()",
			expectErr: true,
		},
		{
			name:      "errors with random start character",
			defStmt:   "ALTER TRIGGER CREATE TRIGGER some_trigger BEFORE UPDATE ON public.foo FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION increment_version()",
			expectErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := schema.GetTriggerDefStatement(tc.defStmt).ToCreateOrReplace()
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.out, out)
			}
		})
	}
}
