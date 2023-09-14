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
			CREATE EXTENSION pg_trgm WITH VERSION '1.6';

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

			CREATE TABLE foo (
				id SERIAL,
				author TEXT COLLATE "C",
				content TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP CHECK (created_at > CURRENT_TIMESTAMP - interval '1 month') NO INHERIT,
				version INT NOT NULL DEFAULT 0,
				PRIMARY KEY(id, version),
				CHECK ( function_with_dependencies(id, id) > 0)
			);

			ALTER TABLE foo ADD CONSTRAINT author_check CHECK (author IS NOT NULL AND LENGTH(author) > 0) NO INHERIT NOT VALID;
			CREATE INDEX some_idx ON foo (created_at DESC, author ASC);
			CREATE UNIQUE INDEX some_unique_idx ON foo (content);
			CREATE INDEX some_gin_idx ON foo USING GIN (author gin_trgm_ops);

			ALTER TABLE foo REPLICA IDENTITY USING INDEX some_unique_idx;

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

			CREATE TABLE foo_fk(
				id INT,
				version INT
			);
			ALTER TABLE foo_fk ADD CONSTRAINT foo_fk_fk FOREIGN KEY (id, version) REFERENCES foo (id, version)
				ON UPDATE CASCADE
				ON DELETE CASCADE
				NOT VALID;
		`},
			expectedHash: "2ebe255a8e21e27",
			expectedSchema: schema.Schema{
				Extensions: []schema.Extension{
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{
							EscapedName: schema.EscapeIdentifier("pg_trgm"),
							SchemaName:  "public",
						},
						Version: "1.6",
					},
				},
				Tables: []schema.Table{
					{
						Name: "foo",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Size: 4, Default: "nextval('foo_id_seq'::regclass)"},
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
						ReplicaIdentity: schema.ReplicaIdentityIndex,
					},
					{
						Name: "foo_fk",
						Columns: []schema.Column{
							{
								Name:       "id",
								Type:       "integer",
								Collation:  schema.SchemaQualifiedName{},
								Default:    "",
								IsNullable: true,
								Size:       4,
							},
							{
								Name:       "version",
								Type:       "integer",
								Collation:  schema.SchemaQualifiedName{},
								Default:    "",
								IsNullable: true,
								Size:       4,
							},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
						PartitionKeyDef:  "",
						ParentTableName:  "",
						ForValues:        "",
					},
				},
				ForeignKeyConstraints: []schema.ForeignKeyConstraint{
					{
						EscapedName: "\"foo_fk_fk\"",
						OwningTable: schema.SchemaQualifiedName{
							SchemaName:  "public",
							EscapedName: "\"foo_fk\"",
						},
						OwningTableUnescapedName: "foo_fk",
						ForeignTable: schema.SchemaQualifiedName{
							SchemaName:  "public",
							EscapedName: "\"foo\"",
						},
						ForeignTableUnescapedName: "foo",
						ConstraintDef:             "FOREIGN KEY (id, version) REFERENCES foo(id, version) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID",
						IsValid:                   false,
					},
				},
				Sequences: []schema.Sequence{
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_id_seq\""},
						Owner: &schema.SequenceOwner{
							TableName:          schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
							TableUnescapedName: "foo",
							ColumnName:         "id",
						},
						Type:       "integer",
						StartValue: 1,
						Increment:  1,
						MaxValue:   2147483647,
						MinValue:   1,
						CacheSize:  1,
						Cycle:      false,
					},
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar_sequence\""},
						Type:                "bigint",
						StartValue:          10,
						Increment:           2,
						MaxValue:            100,
						MinValue:            5,
						CacheSize:           5,
						Cycle:               true,
					},
				},
				Indexes: []schema.Index{
					{
						TableName:       "foo",
						Name:            "foo_pkey",
						Columns:         []string{"id", "version"},
						IsUnique:        true,
						Constraint:      &schema.IndexConstraint{Type: schema.PkIndexConstraintType, EscapedConstraintName: "\"foo_pkey\"", ConstraintDef: "PRIMARY KEY (id, version)", IsLocal: true},
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_pkey ON public.foo USING btree (id, version)",
					},
					{
						TableName:       "foo",
						Name:            "some_idx",
						Columns:         []string{"created_at", "author"},
						GetIndexDefStmt: "CREATE INDEX some_idx ON public.foo USING btree (created_at DESC, author)",
					},
					{
						TableName:       "foo",
						Name:            "some_unique_idx",
						Columns:         []string{"content"},
						IsUnique:        true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX some_unique_idx ON public.foo USING btree (content)",
					},
					{
						TableName:       "foo",
						Name:            "some_gin_idx",
						Columns:         []string{"author"},
						GetIndexDefStmt: "CREATE INDEX some_gin_idx ON public.foo USING gin (author gin_trgm_ops)",
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
				id SERIAL CHECK (id > 0),
				author TEXT COLLATE "C",
				content TEXT DEFAULT '',
				genre VARCHAR(256) NOT NULL,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP CHECK (created_at > CURRENT_TIMESTAMP - interval '1 month'),
				version INT NOT NULL DEFAULT 0,
				PRIMARY KEY (author, id)
			) PARTITION BY LIST (author);
			ALTER TABLE foo ADD CONSTRAINT author_check CHECK (author IS NOT NULL AND LENGTH(author) > 0) NOT VALID;
			ALTER TABLE foo REPLICA IDENTITY FULL;

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
			ALTER TABLE foo_1 REPLICA IDENTITY NOTHING;
			CREATE TABLE foo_2 PARTITION OF foo FOR VALUES IN ('some author 2');
			CREATE TABLE foo_3 PARTITION OF foo FOR VALUES IN ('some author 3');

			-- partitioned indexes
			CREATE INDEX some_partitioned_idx ON foo USING hash(author);
			CREATE UNIQUE INDEX some_unique_partitioned_idx ON foo(author, created_at DESC);
			CREATE INDEX some_invalid_idx ON ONLY foo(author, genre);

			-- local indexes
			CREATE UNIQUE INDEX foo_1_local_idx ON foo_1(author, content);
			CREATE UNIQUE INDEX foo_2_local_idx ON foo_2(author DESC, id);
			CREATE UNIQUE INDEX foo_3_local_idx ON foo_3(author, created_at);

			CREATE TRIGGER some_partition_trigger
				BEFORE UPDATE ON foo_1
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				EXECUTE PROCEDURE increment_version();


			CREATE TABLE foo_fk(
				author TEXT,
				id INT,
				content TEXT DEFAULT ''
			) PARTITION BY LIST (author);
			CREATE TABLE foo_fk_1 PARTITION OF foo_fk FOR VALUES IN ('some author 1');
			ALTER TABLE foo_fk ADD CONSTRAINT foo_fk_fk FOREIGN KEY (author, id) REFERENCES foo (author, id)
				ON UPDATE CASCADE
				ON DELETE CASCADE;
			ALTER TABLE foo_fk_1 ADD CONSTRAINT foo_fk_1_fk FOREIGN KEY (author, content) REFERENCES foo_1 (author, content)
				NOT VALID;
		`},
			expectedHash: "7cc1abf755b2ec27",
			expectedSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foo",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Size: 4, Default: "nextval('foo_id_seq'::regclass)"},
							{Name: "author", Type: "text", Size: -1, Collation: cCollation},
							{Name: "content", Type: "text", Default: "''::text", IsNullable: true, Size: -1, Collation: defaultCollation},
							{Name: "genre", Type: "character varying(256)", Size: -1, Collation: defaultCollation},
							{Name: "created_at", Type: "timestamp without time zone", Default: "CURRENT_TIMESTAMP", Size: 8},
							{Name: "version", Type: "integer", Default: "0", Size: 4},
						},
						CheckConstraints: []schema.CheckConstraint{
							{Name: "author_check", Expression: "((author IS NOT NULL) AND (length(author) > 0))", IsInheritable: true},
							{Name: "foo_created_at_check", Expression: "(created_at > (CURRENT_TIMESTAMP - '1 mon'::interval))", IsValid: true, IsInheritable: true},
							{Name: "foo_id_check", Expression: "(id > 0)", IsValid: true, IsInheritable: true},
						},
						ReplicaIdentity: schema.ReplicaIdentityFull,
						PartitionKeyDef: "LIST (author)",
					},
					{
						ParentTableName: "foo",
						Name:            "foo_1",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Size: 4, Default: "nextval('foo_id_seq'::regclass)"},
							{Name: "author", Type: "text", Size: -1, Collation: cCollation},
							{Name: "content", Type: "text", Default: "''::text", Size: -1, Collation: defaultCollation},
							{Name: "genre", Type: "character varying(256)", Size: -1, Collation: defaultCollation},
							{Name: "created_at", Type: "timestamp without time zone", Default: "CURRENT_TIMESTAMP", Size: 8},
							{Name: "version", Type: "integer", Default: "0", Size: 4},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityNothing,
						ForValues:        "FOR VALUES IN ('some author 1')",
					},
					{
						ParentTableName: "foo",
						Name:            "foo_2",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Size: 4, Default: "nextval('foo_id_seq'::regclass)"},
							{Name: "author", Type: "text", Size: -1, Collation: cCollation},
							{Name: "content", Type: "text", Default: "''::text", IsNullable: true, Size: -1, Collation: defaultCollation},
							{Name: "genre", Type: "character varying(256)", Size: -1, Collation: defaultCollation},
							{Name: "created_at", Type: "timestamp without time zone", Default: "CURRENT_TIMESTAMP", Size: 8},
							{Name: "version", Type: "integer", Default: "0", Size: 4},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
						ForValues:        "FOR VALUES IN ('some author 2')",
					},
					{
						ParentTableName: "foo",
						Name:            "foo_3",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", Size: 4, Default: "nextval('foo_id_seq'::regclass)"},
							{Name: "author", Type: "text", Size: -1, Collation: cCollation},
							{Name: "content", Type: "text", Default: "''::text", IsNullable: true, Size: -1, Collation: defaultCollation},
							{Name: "genre", Type: "character varying(256)", Size: -1, Collation: defaultCollation},
							{Name: "created_at", Type: "timestamp without time zone", Default: "CURRENT_TIMESTAMP", Size: 8},
							{Name: "version", Type: "integer", Default: "0", Size: 4},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
						ForValues:        "FOR VALUES IN ('some author 3')",
					},
					{
						Name: "foo_fk",
						Columns: []schema.Column{
							{
								Name:       "author",
								Type:       "text",
								Collation:  schema.SchemaQualifiedName{SchemaName: "pg_catalog", EscapedName: "\"default\""},
								Default:    "",
								IsNullable: true,
								Size:       -1,
							},
							{
								Name:       "id",
								Type:       "integer",
								Collation:  schema.SchemaQualifiedName{},
								Default:    "",
								IsNullable: true,
								Size:       4,
							},
							{
								Name:       "content",
								Type:       "text",
								Collation:  schema.SchemaQualifiedName{SchemaName: "pg_catalog", EscapedName: "\"default\""},
								Default:    "''::text",
								IsNullable: true,
								Size:       -1,
							},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
						PartitionKeyDef:  "LIST (author)",
						ParentTableName:  "",
						ForValues:        "",
					},
					{
						Name: "foo_fk_1",
						Columns: []schema.Column{
							{
								Name:       "author",
								Type:       "text",
								Collation:  schema.SchemaQualifiedName{SchemaName: "pg_catalog", EscapedName: "\"default\""},
								Default:    "",
								IsNullable: true,
								Size:       -1,
							},
							{
								Name:       "id",
								Type:       "integer",
								Collation:  schema.SchemaQualifiedName{},
								Default:    "",
								IsNullable: true,
								Size:       4,
							},
							{
								Name:       "content",
								Type:       "text",
								Collation:  schema.SchemaQualifiedName{SchemaName: "pg_catalog", EscapedName: "\"default\""},
								Default:    "''::text",
								IsNullable: true,
								Size:       -1,
							},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
						PartitionKeyDef:  "",
						ParentTableName:  "foo_fk",
						ForValues:        "FOR VALUES IN ('some author 1')",
					},
				},
				Indexes: []schema.Index{
					{
						TableName: "foo",
						Name:      "foo_pkey", Columns: []string{"author", "id"}, IsUnique: true,
						Constraint:      &schema.IndexConstraint{Type: schema.PkIndexConstraintType, EscapedConstraintName: "\"foo_pkey\"", ConstraintDef: "PRIMARY KEY (author, id)", IsLocal: true},
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_pkey ON ONLY public.foo USING btree (author, id)",
					},
					{
						TableName: "foo",
						Name:      "some_partitioned_idx", Columns: []string{"author"},
						GetIndexDefStmt: "CREATE INDEX some_partitioned_idx ON ONLY public.foo USING hash (author)",
					},
					{
						TableName: "foo",
						Name:      "some_unique_partitioned_idx", Columns: []string{"author", "created_at"}, IsUnique: true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX some_unique_partitioned_idx ON ONLY public.foo USING btree (author, created_at DESC)",
					},
					{
						TableName: "foo",
						Name:      "some_invalid_idx", Columns: []string{"author", "genre"}, IsInvalid: true, IsUnique: false,
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
						Name:      "foo_1_author_created_at_idx", Columns: []string{"author", "created_at"}, IsUnique: true, ParentIdxName: "some_unique_partitioned_idx",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_1_author_created_at_idx ON public.foo_1 USING btree (author, created_at DESC)",
					},
					{
						TableName: "foo_1",
						Name:      "foo_1_local_idx", Columns: []string{"author", "content"}, IsUnique: true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_1_local_idx ON public.foo_1 USING btree (author, content)",
					},
					{
						TableName: "foo_1",
						Name:      "foo_1_pkey", Columns: []string{"author", "id"}, ParentIdxName: "foo_pkey", IsUnique: true,
						Constraint:      &schema.IndexConstraint{Type: schema.PkIndexConstraintType, EscapedConstraintName: "\"foo_1_pkey\"", ConstraintDef: "PRIMARY KEY (author, id)"},
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
						Name:      "foo_2_author_created_at_idx", Columns: []string{"author", "created_at"}, IsUnique: true, ParentIdxName: "some_unique_partitioned_idx",
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_2_author_created_at_idx ON public.foo_2 USING btree (author, created_at DESC)",
					},
					{
						TableName: "foo_2",
						Name:      "foo_2_local_idx", Columns: []string{"author", "id"}, IsUnique: true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_2_local_idx ON public.foo_2 USING btree (author DESC, id)",
					},
					{
						TableName: "foo_2",
						Name:      "foo_2_pkey", Columns: []string{"author", "id"}, ParentIdxName: "foo_pkey", IsUnique: true,
						Constraint:      &schema.IndexConstraint{Type: schema.PkIndexConstraintType, EscapedConstraintName: "\"foo_2_pkey\"", ConstraintDef: "PRIMARY KEY (author, id)"},
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
						Name:      "foo_3_author_created_at_idx", Columns: []string{"author", "created_at"}, ParentIdxName: "some_unique_partitioned_idx", IsUnique: true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_3_author_created_at_idx ON public.foo_3 USING btree (author, created_at DESC)",
					},
					{
						TableName: "foo_3",
						Name:      "foo_3_local_idx", Columns: []string{"author", "created_at"}, IsUnique: true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_3_local_idx ON public.foo_3 USING btree (author, created_at)",
					},
					{
						TableName: "foo_3",
						Name:      "foo_3_pkey", Columns: []string{"author", "id"}, ParentIdxName: "foo_pkey", IsUnique: true,
						Constraint:      &schema.IndexConstraint{Type: schema.PkIndexConstraintType, EscapedConstraintName: "\"foo_3_pkey\"", ConstraintDef: "PRIMARY KEY (author, id)"},
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_3_pkey ON public.foo_3 USING btree (author, id)",
					},
				},
				ForeignKeyConstraints: []schema.ForeignKeyConstraint{
					{
						EscapedName:               "\"foo_fk_fk\"",
						OwningTable:               schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_fk\""},
						OwningTableUnescapedName:  "foo_fk",
						ForeignTable:              schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						ForeignTableUnescapedName: "foo",
						ConstraintDef:             "FOREIGN KEY (author, id) REFERENCES foo(author, id) ON UPDATE CASCADE ON DELETE CASCADE",
						IsValid:                   true,
					},
					{
						EscapedName:               "\"foo_fk_1_fk\"",
						OwningTable:               schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_fk_1\""},
						OwningTableUnescapedName:  "foo_fk_1",
						ForeignTable:              schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_1\""},
						ForeignTableUnescapedName: "foo_1",
						ConstraintDef:             "FOREIGN KEY (author, content) REFERENCES foo_1(author, content) NOT VALID",
						IsValid:                   false,
					},
				},
				Sequences: []schema.Sequence{
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_id_seq\""},
						Owner: &schema.SequenceOwner{
							TableName:          schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
							TableUnescapedName: "foo",
							ColumnName:         "id",
						},
						Type:       "integer",
						StartValue: 1,
						Increment:  1,
						MaxValue:   2147483647,
						MinValue:   1,
						CacheSize:  1,
						Cycle:      false,
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
			expectedHash: "a79f6f70a0439aea",
			expectedSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foo",
						Columns: []schema.Column{
							{Name: "id", Type: "integer", IsNullable: true, Size: 4},
							{Name: "author", Type: "text", IsNullable: true, Size: -1, Collation: defaultCollation},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
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
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
						ForValues:        "FOR VALUES IN ('some author 1')",
					},
				},
				Indexes: []schema.Index{
					{
						TableName: "foo_1",
						Name:      "foo_1_pkey", Columns: []string{"author", "id"}, IsUnique: true,
						Constraint:      &schema.IndexConstraint{Type: schema.PkIndexConstraintType, EscapedConstraintName: "\"foo_1_pkey\"", ConstraintDef: "PRIMARY KEY (author, id)", IsLocal: true},
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
				"decimal" DECIMAL(65, 10) NOT NULL DEFAULT 0.0,
				"serial" SERIAL NOT NULL
			);
		`},
			expectedHash: "b9bb73829e790b8e",
			expectedSchema: schema.Schema{
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
							{Name: "serial", Type: "integer", Collation: schema.SchemaQualifiedName{}, Default: "nextval('foo_serial_seq'::regclass)", IsNullable: false, Size: 4},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
					},
				},
				Sequences: []schema.Sequence{
					{
						SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_serial_seq\""},
						Owner: &schema.SequenceOwner{
							TableName:          schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
							TableUnescapedName: "foo",
							ColumnName:         "serial",
						},
						Type:       "integer",
						StartValue: 1,
						Increment:  1,
						MaxValue:   2147483647,
						MinValue:   1,
						CacheSize:  1,
						Cycle:      false,
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
				test_id SERIAL CHECK (test_id > 0),
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

			-- create foreign keys
			ALTER TABLE foo ADD CONSTRAINT foo_fk FOREIGN KEY (id) REFERENCES test.foo(test_schema_id);
			ALTER TABLE test.foo ADD CONSTRAINT foo_fk FOREIGN KEY (test_schema_id) REFERENCES foo(id);
		`},
			expectedHash: "e6bd150cfcc499e9",
			expectedSchema: schema.Schema{
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
						ReplicaIdentity: schema.ReplicaIdentityDefault,
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
						ReplicaIdentity: schema.ReplicaIdentityDefault,
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
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
						ForValues:        "FOR VALUES IN ('some author 1')",
					},
				},
				Indexes: []schema.Index{
					// foo indexes
					{
						TableName: "foo",
						Name:      "foo_pkey", Columns: []string{"id"}, IsUnique: true,
						Constraint:      &schema.IndexConstraint{Type: schema.PkIndexConstraintType, EscapedConstraintName: "\"foo_pkey\"", ConstraintDef: "PRIMARY KEY (id)", IsLocal: true},
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_pkey ON public.foo USING btree (id)",
					},
					// bar indexes
					{
						TableName: "bar",
						Name:      "bar_pkey", Columns: []string{"author", "id"}, IsUnique: true,
						Constraint:      &schema.IndexConstraint{Type: schema.PkIndexConstraintType, EscapedConstraintName: "\"bar_pkey\"", ConstraintDef: "PRIMARY KEY (author, id)", IsLocal: true},
						GetIndexDefStmt: "CREATE UNIQUE INDEX bar_pkey ON ONLY public.bar USING btree (author, id)",
					},
					{
						TableName: "bar",
						Name:      "some_partitioned_idx", Columns: []string{"author", "id"},
						GetIndexDefStmt: "CREATE INDEX some_partitioned_idx ON ONLY public.bar USING btree (author, id)",
					},
					// bar_1 indexes
					{
						TableName: "bar_1",
						Name:      "bar_1_author_id_idx", Columns: []string{"author", "id"}, ParentIdxName: "some_partitioned_idx",
						GetIndexDefStmt: "CREATE INDEX bar_1_author_id_idx ON public.bar_1 USING btree (author, id)",
					},
					{
						TableName: "bar_1",
						Name:      "bar_1_pkey", Columns: []string{"author", "id"}, ParentIdxName: "bar_pkey", IsUnique: true,
						Constraint:      &schema.IndexConstraint{Type: schema.PkIndexConstraintType, EscapedConstraintName: "\"bar_1_pkey\"", ConstraintDef: "PRIMARY KEY (author, id)"},
						GetIndexDefStmt: "CREATE UNIQUE INDEX bar_1_pkey ON public.bar_1 USING btree (author, id)",
					},
				},
				ForeignKeyConstraints: []schema.ForeignKeyConstraint{
					{
						EscapedName:               "\"foo_fk\"",
						OwningTable:               schema.SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						OwningTableUnescapedName:  "foo",
						ForeignTable:              schema.SchemaQualifiedName{SchemaName: "test", EscapedName: "\"foo\""},
						ForeignTableUnescapedName: "foo",
						ConstraintDef:             "FOREIGN KEY (id) REFERENCES test.foo(test_schema_id)",
						IsValid:                   true,
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
			expectedHash: "aebac19ebacc31e8",
			expectedSchema: schema.Schema{
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
			expectedHash: "5fa049e3f5b044a5",
			expectedSchema: schema.Schema{
				Tables: []schema.Table{
					{
						Name: "foo",
						Columns: []schema.Column{
							{Name: "value", Type: "text", IsNullable: true, Size: -1, Collation: defaultCollation},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  schema.ReplicaIdentityDefault,
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
