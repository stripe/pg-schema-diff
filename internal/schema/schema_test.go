package schema

import (
	"context"
	"database/sql"
	"io"
	"testing"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/pgengine"
	"github.com/stripe/pg-schema-diff/internal/queries"
)

type testCase struct {
	name           string
	opts           []GetSchemaOpt
	ddl            []string
	expectedSchema Schema
	// expectedHash is the expected hash of the schema. If it is not provided, the test will not validate the hash.
	expectedHash        string
	expectedErrIs       error
	expectedErrContains string
}

var (
	defaultCollation = SchemaQualifiedName{
		EscapedName: `"default"`,
		SchemaName:  "pg_catalog",
	}
	cCollation = SchemaQualifiedName{
		EscapedName: `"C"`,
		SchemaName:  "pg_catalog",
	}

	testCases = []*testCase{
		{
			name: "Simple schema (validate all schema objects and schema name filters)",
			opts: []GetSchemaOpt{
				WithIncludeSchemas("public", "schema_1", "schema_2"),
			},
			ddl: []string{`
			CREATE SCHEMA schema_1;
			CREATE SCHEMA schema_2;
			-- Validate schemas are filtered out
			CREATE SCHEMA schema_filtered_1;

			CREATE EXTENSION pg_trgm WITH SCHEMA schema_1 VERSION '1.6';
			CREATE EXTENSION pg_visibility WITH SCHEMA schema_filtered_1 VERSION '1.2';
			CREATE EXTENSION pg_stat_statements VERSION '1.9';

			CREATE TYPE foobar_enum AS ENUM ('foobar_1', 'foobar_2', 'foobar_3');
			CREATE TYPE schema_1.foobar_enum AS ENUM ('foobar_1', 'foobar_2');
			-- Validate types are filtered out
			CREATE TYPE schema_filtered_1.foobar_enum AS ENUM ('foobar_1', 'foobar_2');		

			CREATE SEQUENCE schema_1.foobar_sequence
			    AS BIGINT
				INCREMENT BY 2
				MINVALUE 5 MAXVALUE 100
				START WITH 10 CACHE 5 CYCLE
				OWNED BY NONE;

			-- Validate sequences are filtered out
			CREATE SEQUENCE schema_filtered_1.foobar_sequence
			    AS BIGINT
				INCREMENT BY 2	
				MINVALUE 5 MAXVALUE 100
				START WITH 10 CACHE 5 CYCLE
				OWNED BY NONE;

			CREATE FUNCTION schema_1.increment(i integer) RETURNS integer AS $$
					BEGIN
							RETURN i + 1;
					END;
			$$ LANGUAGE plpgsql;

			-- Validate functions are filtered out
			CREATE FUNCTION schema_filtered_1.add(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN a + b;

			CREATE FUNCTION function_with_dependencies(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN schema_filtered_1.add(a, b) + schema_1.increment(a);

			CREATE TABLE schema_2.foo (
				id SERIAL,
				author TEXT COLLATE "C",
				content TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP CHECK (created_at > CURRENT_TIMESTAMP - interval '1 month') NO INHERIT,
				version INT NOT NULL DEFAULT 0,
				PRIMARY KEY(id, version),
				CHECK ( function_with_dependencies(id, id) > 0)
			);
			ALTER TABLE schema_2.foo ENABLE ROW LEVEL SECURITY;
			ALTER TABLE schema_2.foo ADD CONSTRAINT author_content_check CHECK ( LENGTH(content) > 0 AND LENGTH(author) > 0 ) NO INHERIT NOT VALID;
			CREATE INDEX some_idx ON schema_2.foo (created_at DESC, author ASC);
			CREATE UNIQUE INDEX some_unique_idx ON schema_2.foo (content);
			CREATE INDEX some_gin_idx ON schema_2.foo USING GIN (author schema_1.gin_trgm_ops);
			ALTER TABLE schema_2.foo REPLICA IDENTITY USING INDEX some_unique_idx;

			CREATE POLICY foo_policy_1 ON schema_2.foo
				AS PERMISSIVE
				FOR ALL
				TO PUBLIC
				USING (author = current_user)
				WITH CHECK (version > 0);
			CREATE ROLE some_role_1;
			CREATE ROLE some_role_2;
			CREATE POLICY foo_policy_2 ON schema_2.foo
				AS RESTRICTIVE
				FOR INSERT
				TO some_role_2, some_role_1
				WITH CHECK (version > 0);


			CREATE FUNCTION increment_version() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = OLD.version + 1;
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE FUNCTION schema_filtered_1.increment_version() RETURNS TRIGGER AS $$
				BEGIN
					NEW.version = OLD.version + 1;
					RETURN NEW;
				END;
			$$ language 'plpgsql';

			CREATE TRIGGER some_trigger
				BEFORE UPDATE ON schema_2.foo
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				-- Reference a function in a filtered out schema. The trigger should still be included.
				EXECUTE PROCEDURE schema_filtered_1.increment_version();

			-- Create table with conflicting name that has check constraints
			CREATE TABLE schema_1.foo(
				id INT NOT NULL,
				CHECK (id > 0)
			);
			ALTER TABLE schema_1.foo ENABLE ROW LEVEL SECURITY;
			ALTER TABLE schema_1.foo FORCE ROW LEVEL SECURITY;
			CREATE POLICY foo_policy_1 ON schema_1.foo
				AS RESTRICTIVE
				FOR UPDATE
				TO PUBLIC
				WITH CHECK (id > 0);

			CREATE TABLE schema_1.foo_fk(
				id INT,
				version INT
			);
			CREATE INDEX some_idx on schema_1.foo_fk (id, version);
			ALTER TABLE schema_1.foo_fk ADD CONSTRAINT foo_fk_fk FOREIGN KEY (id, version) REFERENCES schema_2.foo (id, version)
				ON UPDATE CASCADE
				ON DELETE CASCADE
				NOT VALID;

			-- Validate tables are filtered out
			CREATE TABLE schema_filtered_1.foo_fk(
				id INT,
				version INT CHECK (version > 0) -- Validate check constraints are filtered out
			);
			-- Validate indexes are filtered out
			CREATE INDEX some_idx on schema_filtered_1.foo_fk (id, version);
			-- Validate FK are filtered out
			ALTER TABLE schema_filtered_1.foo_fk ADD CONSTRAINT foo_fk_fk FOREIGN KEY (id, version) REFERENCES schema_2.foo (id, version)
				ON UPDATE CASCADE
				ON DELETE CASCADE
				NOT VALID;
			-- Validate triggers are filtered out
			CREATE TRIGGER some_trigger
				BEFORE UPDATE ON schema_filtered_1.foo_fk
				FOR EACH ROW
				WHEN (OLD.* IS DISTINCT FROM NEW.*)
				-- Reference a function in a filtered out schema. The trigger should still be included.
				EXECUTE PROCEDURE public.increment_version();
			-- Validate policies are filtered out
			CREATE POLICY foo_policy_1 ON schema_filtered_1.foo_fk
				AS PERMISSIVE
				FOR SELECT
				TO PUBLIC
				USING (version > 0);
		`},
			expectedHash: "ffcf26204e89f536",
			expectedSchema: Schema{
				NamedSchemas: []NamedSchema{
					{Name: "public"},
					{Name: "schema_1"},
					{Name: "schema_2"},
				},
				Extensions: []Extension{
					{
						SchemaQualifiedName: SchemaQualifiedName{
							SchemaName:  "schema_1",
							EscapedName: EscapeIdentifier("pg_trgm"),
						},
						Version: "1.6",
					},
					{
						SchemaQualifiedName: SchemaQualifiedName{
							SchemaName:  "public",
							EscapedName: EscapeIdentifier("pg_stat_statements"),
						},
						Version: "1.9",
					},
				},
				Enums: []Enum{
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar_enum\""},
						Labels:              []string{"foobar_1", "foobar_2", "foobar_3"},
					},
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "schema_1", EscapedName: "\"foobar_enum\""},
						Labels:              []string{"foobar_1", "foobar_2"},
					},
				},
				Tables: []Table{
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "schema_2", EscapedName: "\"foo\""},
						Columns: []Column{
							{Name: "id", Type: "integer", Size: 4, Default: "nextval('schema_2.foo_id_seq'::regclass)"},
							{Name: "author", Type: "text", IsNullable: true, Size: -1, Collation: cCollation},
							{Name: "content", Type: "text", Default: "''::text", Size: -1, Collation: defaultCollation},
							{Name: "created_at", Type: "timestamp without time zone", Default: "CURRENT_TIMESTAMP", Size: 8},
							{Name: "version", Type: "integer", Default: "0", Size: 4},
						},
						CheckConstraints: []CheckConstraint{
							{Name: "author_content_check", Expression: "((length(content) > 0) AND (length(author) > 0))", KeyColumns: []string{"author", "content"}},
							{Name: "foo_created_at_check", Expression: "(created_at > (CURRENT_TIMESTAMP - '1 mon'::interval))", IsValid: true, KeyColumns: []string{"created_at"}},
							{
								Name:          "foo_id_check",
								Expression:    "(function_with_dependencies(id, id) > 0)",
								IsValid:       true,
								IsInheritable: true,
								DependsOnFunctions: []SchemaQualifiedName{
									{EscapedName: "\"function_with_dependencies\"(a integer, b integer)", SchemaName: "public"},
								},
								KeyColumns: []string{"id"},
							},
						},
						Policies: []Policy{
							{
								EscapedName:     "\"foo_policy_1\"",
								IsPermissive:    true,
								AppliesTo:       []string{"PUBLIC"},
								Cmd:             AllPolicyCmd,
								UsingExpression: "(author = CURRENT_USER)",
								CheckExpression: "(version > 0)",
								Columns:         []string{"author", "version"},
							},
							{
								EscapedName:     "\"foo_policy_2\"",
								AppliesTo:       []string{"some_role_1", "some_role_2"},
								Cmd:             InsertPolicyCmd,
								CheckExpression: "(version > 0)",
								Columns:         []string{"version"},
							},
						},
						ReplicaIdentity: ReplicaIdentityIndex,
						RLSEnabled:      true,
					},
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "schema_1", EscapedName: "\"foo\""},
						Columns: []Column{
							{Name: "id", Type: "integer", Size: 4, Default: ""},
						},
						CheckConstraints: []CheckConstraint{
							{Name: "foo_id_check", Expression: "(id > 0)", IsValid: true, IsInheritable: true, KeyColumns: []string{"id"}},
						},
						Policies: []Policy{
							{
								EscapedName:     "\"foo_policy_1\"",
								IsPermissive:    false,
								AppliesTo:       []string{"PUBLIC"},
								Cmd:             UpdatePolicyCmd,
								CheckExpression: "(id > 0)",
								Columns:         []string{"id"},
							},
						},
						ReplicaIdentity: ReplicaIdentityDefault,
						RLSEnabled:      true,
						RLSForced:       true,
					},
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "schema_1", EscapedName: "\"foo_fk\""},
						Columns: []Column{
							{
								Name:       "id",
								Type:       "integer",
								Collation:  SchemaQualifiedName{},
								Default:    "",
								IsNullable: true,
								Size:       4,
							},
							{
								Name:       "version",
								Type:       "integer",
								Collation:  SchemaQualifiedName{},
								Default:    "",
								IsNullable: true,
								Size:       4,
							},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  ReplicaIdentityDefault,
						PartitionKeyDef:  "",
						ForValues:        "",
					},
				},
				ForeignKeyConstraints: []ForeignKeyConstraint{
					{
						EscapedName: "\"foo_fk_fk\"",
						OwningTable: SchemaQualifiedName{
							SchemaName:  "schema_1",
							EscapedName: "\"foo_fk\"",
						},
						ForeignTable: SchemaQualifiedName{
							SchemaName:  "schema_2",
							EscapedName: "\"foo\"",
						},
						ConstraintDef: "FOREIGN KEY (id, version) REFERENCES schema_2.foo(id, version) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID",
						IsValid:       false,
					},
				},
				Sequences: []Sequence{
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "schema_2", EscapedName: "\"foo_id_seq\""},
						Owner: &SequenceOwner{
							TableName:  SchemaQualifiedName{SchemaName: "schema_2", EscapedName: "\"foo\""},
							ColumnName: "id",
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
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "schema_1", EscapedName: "\"foobar_sequence\""},
						Type:                "bigint",
						StartValue:          10,
						Increment:           2,
						MaxValue:            100,
						MinValue:            5,
						CacheSize:           5,
						Cycle:               true,
					},
				},
				Indexes: []Index{
					{
						OwningTable:     SchemaQualifiedName{SchemaName: "schema_2", EscapedName: "\"foo\""},
						Name:            "foo_pkey",
						Columns:         []string{"id", "version"},
						IsUnique:        true,
						Constraint:      &IndexConstraint{Type: PkIndexConstraintType, EscapedConstraintName: "\"foo_pkey\"", ConstraintDef: "PRIMARY KEY (id, version)", IsLocal: true},
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_pkey ON schema_2.foo USING btree (id, version)",
					},
					{
						OwningTable:     SchemaQualifiedName{SchemaName: "schema_2", EscapedName: "\"foo\""},
						Name:            "some_idx",
						Columns:         []string{"created_at", "author"},
						GetIndexDefStmt: "CREATE INDEX some_idx ON schema_2.foo USING btree (created_at DESC, author)",
					},
					{
						OwningTable:     SchemaQualifiedName{SchemaName: "schema_2", EscapedName: "\"foo\""},
						Name:            "some_unique_idx",
						Columns:         []string{"content"},
						IsUnique:        true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX some_unique_idx ON schema_2.foo USING btree (content)",
					},
					{
						OwningTable:     SchemaQualifiedName{SchemaName: "schema_2", EscapedName: "\"foo\""},
						Name:            "some_gin_idx",
						Columns:         []string{"author"},
						GetIndexDefStmt: "CREATE INDEX some_gin_idx ON schema_2.foo USING gin (author schema_1.gin_trgm_ops)",
					},
					{
						Name: "some_idx",
						OwningTable: SchemaQualifiedName{
							SchemaName:  "schema_1",
							EscapedName: "\"foo_fk\"",
						},
						Columns: []string{
							"id",
							"version",
						},
						GetIndexDefStmt: "CREATE INDEX some_idx ON schema_1.foo_fk USING btree (id, version)",
					},
				},
				Functions: []Function{
					{
						SchemaQualifiedName: SchemaQualifiedName{EscapedName: "\"function_with_dependencies\"(a integer, b integer)", SchemaName: "public"},
						FunctionDef:         "CREATE OR REPLACE FUNCTION public.function_with_dependencies(a integer, b integer)\n RETURNS integer\n LANGUAGE sql\n IMMUTABLE STRICT\nRETURN (schema_filtered_1.add(a, b) + schema_1.increment(a))\n",
						Language:            "sql",
						DependsOnFunctions: []SchemaQualifiedName{
							{EscapedName: "\"add\"(a integer, b integer)", SchemaName: "schema_filtered_1"},
							{EscapedName: "\"increment\"(i integer)", SchemaName: "schema_1"},
						},
					},
					{
						SchemaQualifiedName: SchemaQualifiedName{EscapedName: "\"increment\"(i integer)", SchemaName: "schema_1"},
						FunctionDef:         "CREATE OR REPLACE FUNCTION schema_1.increment(i integer)\n RETURNS integer\n LANGUAGE plpgsql\nAS $function$\n\t\t\t\t\tBEGIN\n\t\t\t\t\t\t\tRETURN i + 1;\n\t\t\t\t\tEND;\n\t\t\t$function$\n",
						Language:            "plpgsql",
					},
					{
						SchemaQualifiedName: SchemaQualifiedName{EscapedName: "\"increment_version\"()", SchemaName: "public"},
						FunctionDef:         "CREATE OR REPLACE FUNCTION public.increment_version()\n RETURNS trigger\n LANGUAGE plpgsql\nAS $function$\n\t\t\t\tBEGIN\n\t\t\t\t\tNEW.version = OLD.version + 1;\n\t\t\t\t\tRETURN NEW;\n\t\t\t\tEND;\n\t\t\t$function$\n",
						Language:            "plpgsql",
					},
				},
				Triggers: []Trigger{
					{
						EscapedName:       "\"some_trigger\"",
						OwningTable:       SchemaQualifiedName{EscapedName: "\"foo\"", SchemaName: "schema_2"},
						Function:          SchemaQualifiedName{EscapedName: "\"increment_version\"()", SchemaName: "schema_filtered_1"},
						GetTriggerDefStmt: "CREATE TRIGGER some_trigger BEFORE UPDATE ON schema_2.foo FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION schema_filtered_1.increment_version()",
					},
				},
			},
		},
		{
			name: "Partition test",
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
			expectedHash: "481b62a68155716d",
			expectedSchema: Schema{
				NamedSchemas: []NamedSchema{
					{Name: "public"},
				},
				Tables: []Table{
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						Columns: []Column{
							{Name: "id", Type: "integer", Size: 4, Default: "nextval('foo_id_seq'::regclass)"},
							{Name: "author", Type: "text", Size: -1, Collation: cCollation},
							{Name: "content", Type: "text", Default: "''::text", IsNullable: true, Size: -1, Collation: defaultCollation},
							{Name: "genre", Type: "character varying(256)", Size: -1, Collation: defaultCollation},
							{Name: "created_at", Type: "timestamp without time zone", Default: "CURRENT_TIMESTAMP", Size: 8},
							{Name: "version", Type: "integer", Default: "0", Size: 4},
						},
						CheckConstraints: []CheckConstraint{
							{Name: "author_check", Expression: "((author IS NOT NULL) AND (length(author) > 0))", IsInheritable: true, KeyColumns: []string{"author"}},
							{Name: "foo_created_at_check", Expression: "(created_at > (CURRENT_TIMESTAMP - '1 mon'::interval))", IsValid: true, IsInheritable: true, KeyColumns: []string{"created_at"}},
							{Name: "foo_id_check", Expression: "(id > 0)", IsValid: true, IsInheritable: true, KeyColumns: []string{"id"}},
						},
						ReplicaIdentity: ReplicaIdentityFull,
						PartitionKeyDef: "LIST (author)",
					},
					{
						ParentTable:         &SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_1\""},
						Columns: []Column{
							{Name: "id", Type: "integer", Size: 4, Default: "nextval('foo_id_seq'::regclass)"},
							{Name: "author", Type: "text", Size: -1, Collation: cCollation},
							{Name: "content", Type: "text", Default: "''::text", Size: -1, Collation: defaultCollation},
							{Name: "genre", Type: "character varying(256)", Size: -1, Collation: defaultCollation},
							{Name: "created_at", Type: "timestamp without time zone", Default: "CURRENT_TIMESTAMP", Size: 8},
							{Name: "version", Type: "integer", Default: "0", Size: 4},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  ReplicaIdentityNothing,
						ForValues:        "FOR VALUES IN ('some author 1')",
					},
					{
						ParentTable:         &SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_2\""},
						Columns: []Column{
							{Name: "id", Type: "integer", Size: 4, Default: "nextval('foo_id_seq'::regclass)"},
							{Name: "author", Type: "text", Size: -1, Collation: cCollation},
							{Name: "content", Type: "text", Default: "''::text", IsNullable: true, Size: -1, Collation: defaultCollation},
							{Name: "genre", Type: "character varying(256)", Size: -1, Collation: defaultCollation},
							{Name: "created_at", Type: "timestamp without time zone", Default: "CURRENT_TIMESTAMP", Size: 8},
							{Name: "version", Type: "integer", Default: "0", Size: 4},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  ReplicaIdentityDefault,
						ForValues:        "FOR VALUES IN ('some author 2')",
					},
					{
						ParentTable:         &SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_3\""},
						Columns: []Column{
							{Name: "id", Type: "integer", Size: 4, Default: "nextval('foo_id_seq'::regclass)"},
							{Name: "author", Type: "text", Size: -1, Collation: cCollation},
							{Name: "content", Type: "text", Default: "''::text", IsNullable: true, Size: -1, Collation: defaultCollation},
							{Name: "genre", Type: "character varying(256)", Size: -1, Collation: defaultCollation},
							{Name: "created_at", Type: "timestamp without time zone", Default: "CURRENT_TIMESTAMP", Size: 8},
							{Name: "version", Type: "integer", Default: "0", Size: 4},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  ReplicaIdentityDefault,
						ForValues:        "FOR VALUES IN ('some author 3')",
					},
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_fk\""},
						Columns: []Column{
							{
								Name:       "author",
								Type:       "text",
								Collation:  SchemaQualifiedName{SchemaName: "pg_catalog", EscapedName: "\"default\""},
								Default:    "",
								IsNullable: true,
								Size:       -1,
							},
							{
								Name:       "id",
								Type:       "integer",
								Collation:  SchemaQualifiedName{},
								Default:    "",
								IsNullable: true,
								Size:       4,
							},
							{
								Name:       "content",
								Type:       "text",
								Collation:  SchemaQualifiedName{SchemaName: "pg_catalog", EscapedName: "\"default\""},
								Default:    "''::text",
								IsNullable: true,
								Size:       -1,
							},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  ReplicaIdentityDefault,
						PartitionKeyDef:  "LIST (author)",
						ForValues:        "",
					},
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_fk_1\""},
						ParentTable:         &SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_fk\""},
						Columns: []Column{
							{
								Name:       "author",
								Type:       "text",
								Collation:  SchemaQualifiedName{SchemaName: "pg_catalog", EscapedName: "\"default\""},
								Default:    "",
								IsNullable: true,
								Size:       -1,
							},
							{
								Name:       "id",
								Type:       "integer",
								Collation:  SchemaQualifiedName{},
								Default:    "",
								IsNullable: true,
								Size:       4,
							},
							{
								Name:       "content",
								Type:       "text",
								Collation:  SchemaQualifiedName{SchemaName: "pg_catalog", EscapedName: "\"default\""},
								Default:    "''::text",
								IsNullable: true,
								Size:       -1,
							},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  ReplicaIdentityDefault,
						PartitionKeyDef:  "",
						ForValues:        "FOR VALUES IN ('some author 1')",
					},
				},
				Indexes: []Index{
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						Name:        "foo_pkey", Columns: []string{"author", "id"}, IsUnique: true,
						Constraint:      &IndexConstraint{Type: PkIndexConstraintType, EscapedConstraintName: "\"foo_pkey\"", ConstraintDef: "PRIMARY KEY (author, id)", IsLocal: true},
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_pkey ON ONLY public.foo USING btree (author, id)",
					},
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						Name:        "some_partitioned_idx", Columns: []string{"author"},
						GetIndexDefStmt: "CREATE INDEX some_partitioned_idx ON ONLY public.foo USING hash (author)",
					},
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						Name:        "some_unique_partitioned_idx", Columns: []string{"author", "created_at"}, IsUnique: true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX some_unique_partitioned_idx ON ONLY public.foo USING btree (author, created_at DESC)",
					},
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						Name:        "some_invalid_idx", Columns: []string{"author", "genre"}, IsInvalid: true, IsUnique: false,
						GetIndexDefStmt: "CREATE INDEX some_invalid_idx ON ONLY public.foo USING btree (author, genre)",
					},
					// foo_1 indexes
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_1\""},
						Name:        "foo_1_author_idx", Columns: []string{"author"},
						ParentIdx:       &SchemaQualifiedName{SchemaName: "public", EscapedName: "\"some_partitioned_idx\""},
						GetIndexDefStmt: "CREATE INDEX foo_1_author_idx ON public.foo_1 USING hash (author)",
					},
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_1\""},
						Name:        "foo_1_author_created_at_idx", Columns: []string{"author", "created_at"}, IsUnique: true,
						ParentIdx:       &SchemaQualifiedName{SchemaName: "public", EscapedName: "\"some_unique_partitioned_idx\""},
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_1_author_created_at_idx ON public.foo_1 USING btree (author, created_at DESC)",
					},
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_1\""},
						Name:        "foo_1_local_idx", Columns: []string{"author", "content"}, IsUnique: true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_1_local_idx ON public.foo_1 USING btree (author, content)",
					},
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_1\""},
						Name:        "foo_1_pkey", Columns: []string{"author", "id"}, IsUnique: true,
						ParentIdx:       &SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_pkey\""},
						Constraint:      &IndexConstraint{Type: PkIndexConstraintType, EscapedConstraintName: "\"foo_1_pkey\"", ConstraintDef: "PRIMARY KEY (author, id)"},
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_1_pkey ON public.foo_1 USING btree (author, id)",
					},
					// foo_2 indexes
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_2\""},
						Name:        "foo_2_author_idx", Columns: []string{"author"},
						ParentIdx:       &SchemaQualifiedName{SchemaName: "public", EscapedName: "\"some_partitioned_idx\""},
						GetIndexDefStmt: "CREATE INDEX foo_2_author_idx ON public.foo_2 USING hash (author)",
					},
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_2\""},
						Name:        "foo_2_author_created_at_idx", Columns: []string{"author", "created_at"}, IsUnique: true,
						ParentIdx:       &SchemaQualifiedName{SchemaName: "public", EscapedName: "\"some_unique_partitioned_idx\""},
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_2_author_created_at_idx ON public.foo_2 USING btree (author, created_at DESC)",
					},
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_2\""},
						Name:        "foo_2_local_idx", Columns: []string{"author", "id"}, IsUnique: true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_2_local_idx ON public.foo_2 USING btree (author DESC, id)",
					},
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_2\""},
						Name:        "foo_2_pkey", Columns: []string{"author", "id"}, IsUnique: true,
						ParentIdx:       &SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_pkey\""},
						Constraint:      &IndexConstraint{Type: PkIndexConstraintType, EscapedConstraintName: "\"foo_2_pkey\"", ConstraintDef: "PRIMARY KEY (author, id)"},
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_2_pkey ON public.foo_2 USING btree (author, id)",
					},
					// foo_3 indexes
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_3\""},
						Name:        "foo_3_author_idx", Columns: []string{"author"},
						ParentIdx:       &SchemaQualifiedName{SchemaName: "public", EscapedName: "\"some_partitioned_idx\""},
						GetIndexDefStmt: "CREATE INDEX foo_3_author_idx ON public.foo_3 USING hash (author)",
					},
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_3\""},
						Name:        "foo_3_author_created_at_idx", Columns: []string{"author", "created_at"}, IsUnique: true,
						ParentIdx:       &SchemaQualifiedName{SchemaName: "public", EscapedName: "\"some_unique_partitioned_idx\""},
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_3_author_created_at_idx ON public.foo_3 USING btree (author, created_at DESC)",
					},
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_3\""},
						Name:        "foo_3_local_idx", Columns: []string{"author", "created_at"}, IsUnique: true,
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_3_local_idx ON public.foo_3 USING btree (author, created_at)",
					},
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_3\""},
						Name:        "foo_3_pkey", Columns: []string{"author", "id"}, IsUnique: true,
						ParentIdx:       &SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_pkey\""},
						Constraint:      &IndexConstraint{Type: PkIndexConstraintType, EscapedConstraintName: "\"foo_3_pkey\"", ConstraintDef: "PRIMARY KEY (author, id)"},
						GetIndexDefStmt: "CREATE UNIQUE INDEX foo_3_pkey ON public.foo_3 USING btree (author, id)",
					},
				},
				ForeignKeyConstraints: []ForeignKeyConstraint{
					{
						EscapedName:   "\"foo_fk_fk\"",
						OwningTable:   SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_fk\""},
						ForeignTable:  SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						ConstraintDef: "FOREIGN KEY (author, id) REFERENCES foo(author, id) ON UPDATE CASCADE ON DELETE CASCADE",
						IsValid:       true,
					},
					{
						EscapedName:   "\"foo_fk_1_fk\"",
						OwningTable:   SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_fk_1\""},
						ForeignTable:  SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_1\""},
						ConstraintDef: "FOREIGN KEY (author, content) REFERENCES foo_1(author, content) NOT VALID",
						IsValid:       false,
					},
				},
				Sequences: []Sequence{
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_id_seq\""},
						Owner: &SequenceOwner{
							TableName:  SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
							ColumnName: "id",
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
				Functions: []Function{
					{
						SchemaQualifiedName: SchemaQualifiedName{EscapedName: "\"increment_version\"()", SchemaName: "public"},
						FunctionDef:         "CREATE OR REPLACE FUNCTION public.increment_version()\n RETURNS trigger\n LANGUAGE plpgsql\nAS $function$\n\t\t\t\tBEGIN\n\t\t\t\t\tNEW.version = OLD.version + 1;\n\t\t\t\t\tRETURN NEW;\n\t\t\t\tEND;\n\t\t\t$function$\n",
						Language:            "plpgsql",
					},
				},
				Triggers: []Trigger{
					{
						EscapedName:       "\"some_trigger\"",
						OwningTable:       SchemaQualifiedName{EscapedName: "\"foo\"", SchemaName: "public"},
						Function:          SchemaQualifiedName{EscapedName: "\"increment_version\"()", SchemaName: "public"},
						GetTriggerDefStmt: "CREATE TRIGGER some_trigger BEFORE UPDATE ON public.foo FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION increment_version()",
					},
					{
						EscapedName:       "\"some_partition_trigger\"",
						OwningTable:       SchemaQualifiedName{EscapedName: "\"foo_1\"", SchemaName: "public"},
						Function:          SchemaQualifiedName{EscapedName: "\"increment_version\"()", SchemaName: "public"},
						GetTriggerDefStmt: "CREATE TRIGGER some_partition_trigger BEFORE UPDATE ON public.foo_1 FOR EACH ROW WHEN ((old.* IS DISTINCT FROM new.*)) EXECUTE FUNCTION increment_version()",
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
			expectedSchema: Schema{
				NamedSchemas: []NamedSchema{
					{Name: "public"},
				},
				Tables: []Table{
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						Columns: []Column{
							{Name: "id", Type: "integer", IsNullable: true, Size: 4},
							{Name: "author", Type: "text", IsNullable: true, Size: -1, Collation: defaultCollation},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  ReplicaIdentityDefault,
						PartitionKeyDef:  "LIST (author)",
					},
					{
						ParentTable:         &SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_1\""},
						Columns: []Column{
							{Name: "id", Type: "integer", Size: 4},
							{Name: "author", Type: "text", Size: -1, Collation: defaultCollation},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  ReplicaIdentityDefault,
						ForValues:        "FOR VALUES IN ('some author 1')",
					},
				},
				Indexes: []Index{
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_1\""},
						Name:        "foo_1_pkey", Columns: []string{"author", "id"}, IsUnique: true,
						Constraint:      &IndexConstraint{Type: PkIndexConstraintType, EscapedConstraintName: "\"foo_1_pkey\"", ConstraintDef: "PRIMARY KEY (author, id)", IsLocal: true},
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
			expectedSchema: Schema{
				NamedSchemas: []NamedSchema{
					{Name: "public"},
				},
				Tables: []Table{
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						Columns: []Column{
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
							{Name: "serial", Type: "integer", Collation: SchemaQualifiedName{}, Default: "nextval('foo_serial_seq'::regclass)", IsNullable: false, Size: 4},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  ReplicaIdentityDefault,
					},
				},
				Sequences: []Sequence{
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo_serial_seq\""},
						Owner: &SequenceOwner{
							TableName:  SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
							ColumnName: "serial",
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
			name: "Column rename",
			ddl: []string{`
			CREATE TABLE foo (
				value TEXT
			);
			CREATE INDEX foo_value_idx ON foo (value);
			ALTER TABLE foo RENAME COLUMN value TO renamed_value;
		`},
			expectedSchema: Schema{
				NamedSchemas: []NamedSchema{
					{Name: "public"},
				},
				Tables: []Table{
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						Columns: []Column{
							{Name: "renamed_value", Type: "text", IsNullable: true, Size: -1, Collation: defaultCollation},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  ReplicaIdentityDefault,
					},
				},
				Indexes: []Index{
					{
						OwningTable: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						Name:        "foo_value_idx", Columns: []string{"renamed_value"},
						GetIndexDefStmt: "CREATE INDEX foo_value_idx ON public.foo USING btree (renamed_value)",
					},
				},
			},
		},
		{
			name: "Filtering - filtering out the base table",
			opts: []GetSchemaOpt{WithIncludeSchemas("public")},
			ddl: []string{`
				CREATE SCHEMA schema_filtered_1;
				CREATE TABLE schema_filtered_1.foobar(	
				    id VARCHAR(255) NOT NULL
				) PARTITION BY LIST (id);
				CREATE TABLE foobar_1 PARTITION OF schema_filtered_1.foobar FOR VALUES IN ('1');
		   `},
			expectedSchema: Schema{
				NamedSchemas: []NamedSchema{
					{Name: "public"},
				},
				Tables: []Table{
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar_1\""},
						Columns: []Column{
							{Name: "id", Type: "character varying(255)", IsNullable: false, Size: -1, Collation: defaultCollation},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  "d",
						ParentTable:      &SchemaQualifiedName{SchemaName: "schema_filtered_1", EscapedName: "\"foobar\""},
						ForValues:        "FOR VALUES IN ('1')",
					},
				},
			},
		},
		{
			name: "Filtering - filtering out partition",
			opts: []GetSchemaOpt{WithIncludeSchemas("public")},
			ddl: []string{`
				CREATE SCHEMA schema_filtered_1;
				CREATE TABLE foobar(	
				    id VARCHAR(255) NOT NULL
				) PARTITION BY LIST (id);
				CREATE TABLE schema_filtered_1.foobar_1 PARTITION OF foobar FOR VALUES IN ('1');
		   `},
			expectedSchema: Schema{
				NamedSchemas: []NamedSchema{
					{Name: "public"},
				},
				Tables: []Table{
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						Columns: []Column{
							{Name: "id", Type: "character varying(255)", IsNullable: false, Size: -1, Collation: defaultCollation},
						},
						ReplicaIdentity: "d",
						PartitionKeyDef: "LIST (id)",
					},
				},
			},
		},
		{
			name: "Filtering - no filtering fetches everything",
			ddl: []string{`
				CREATE SCHEMA schema_1;
				CREATE TABLE schema_1.foobar(	
				    id VARCHAR(255) NOT NULL
				) PARTITION BY LIST (id);
				CREATE TABLE foobar_1 PARTITION OF schema_1.foobar FOR VALUES IN ('1');
		   `},
			expectedSchema: Schema{
				NamedSchemas: []NamedSchema{
					{Name: "public"},
					{Name: "schema_1"},
				},
				Tables: []Table{
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "schema_1", EscapedName: "\"foobar\""},
						Columns: []Column{
							{Name: "id", Type: "character varying(255)", IsNullable: false, Size: -1, Collation: defaultCollation},
						},
						ReplicaIdentity: "d",
						PartitionKeyDef: "LIST (id)",
					},
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar_1\""},
						Columns: []Column{
							{Name: "id", Type: "character varying(255)", IsNullable: false, Size: -1, Collation: defaultCollation},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  "d",
						ParentTable:      &SchemaQualifiedName{SchemaName: "schema_1", EscapedName: "\"foobar\""},
						ForValues:        "FOR VALUES IN ('1')",
					},
				},
			},
		},
		{
			name: "Empty Schema (aside from public schema)",
			ddl: []string{`
				-- Create temporary objects to ensure they are excluded
				CREATE TEMP TABLE temp_table (
					id SERIAL PRIMARY KEY,
					description TEXT
				);
				CREATE TEMP SEQUENCE temp_seq;
				CREATE OR REPLACE FUNCTION pg_temp.temp_func()
				RETURNS integer AS $$
				BEGIN
				RETURN 1;
				END;
				$$ LANGUAGE plpgsql;
				CREATE TYPE pg_temp.color AS ENUM ('red', 'green', 'blue');
			`},
			// Assert empty schema hash, since we want to validate specifically that this hash is deterministic
			expectedHash: "e63f48c273376e85",
			expectedSchema: Schema{
				NamedSchemas: []NamedSchema{
					{Name: "public"},
				},
			},
		},
		{
			name: "No Indexes or constraints",
			ddl: []string{`
			CREATE TABLE foo (
				value TEXT
			);
		`},
			expectedSchema: Schema{
				NamedSchemas: []NamedSchema{
					{Name: "public"},
				},
				Tables: []Table{
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foo\""},
						Columns: []Column{
							{Name: "value", Type: "text", IsNullable: true, Size: -1, Collation: defaultCollation},
						},
						CheckConstraints: nil,
						ReplicaIdentity:  ReplicaIdentityDefault,
					},
				},
			},
		},
		{
			name: "Filters - exclude schemas",
			opts: []GetSchemaOpt{
				WithExcludeSchemas("schema_1"),
			},
			ddl: []string{`
				CREATE TABLE foobar();
				CREATE SCHEMA schema_1;
				CREATE TABLE schema_1.foobar();
				CREATE SCHEMA schema_2;
				CREATE TABLE schema_2.foobar();
			`},
			expectedSchema: Schema{
				NamedSchemas: []NamedSchema{
					{Name: "public"},
					{Name: "schema_2"},
				},
				Tables: []Table{
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: "\"foobar\""},
						ReplicaIdentity:     ReplicaIdentityDefault,
					},
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "schema_2", EscapedName: "\"foobar\""},
						ReplicaIdentity:     ReplicaIdentityDefault,
					},
				},
			},
		},
		{
			name: "Filters - include and exclude schemas",
			opts: []GetSchemaOpt{
				WithIncludeSchemas("schema_1"),
				// schema_3 is inherently excluded since it is not included
				WithExcludeSchemas("schema_2"),
			},
			ddl: []string{`
				CREATE TABLE foobar();
				CREATE SCHEMA schema_1;	
				CREATE TABLE schema_1.foobar();
				CREATE SCHEMA schema_2;
				CREATE TABLE schema_2.foobar();
			`},
			expectedSchema: Schema{
				NamedSchemas: []NamedSchema{
					{Name: "schema_1"},
				},
				Tables: []Table{
					{
						SchemaQualifiedName: SchemaQualifiedName{SchemaName: "schema_1", EscapedName: "\"foobar\""},
						ReplicaIdentity:     ReplicaIdentityDefault,
					},
				},
			},
		},
		{
			name: "Filter - include and exclude the same schema",
			opts: []GetSchemaOpt{
				WithIncludeSchemas("schema_1"),

				WithIncludeSchemas("schema_2"),
				WithExcludeSchemas("schema_2"),

				WithExcludeSchemas("schema_3"),

				WithExcludeSchemas("schema_4"),
				WithExcludeSchemas("schema_4"),
			},
			expectedErrContains: "are both included and excluded",
		},
	}
)

func TestSchemaTestCases(t *testing.T) {
	engine, err := pgengine.StartEngine()
	require.NoError(t, err)
	defer engine.Close()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("Conn pool", func(t *testing.T) {
				runTestCase(t, engine, tc, func(db *sql.DB) (queries.DBTX, io.Closer) {
					return db, nil
				})
			})
			t.Run("Connection", func(t *testing.T) {
				runTestCase(t, engine, tc, func(db *sql.DB) (queries.DBTX, io.Closer) {
					conn, err := db.Conn(context.Background())
					require.NoError(t, err)
					return conn, conn
				})
			})
		})
	}
}

func runTestCase(t *testing.T, engine *pgengine.Engine, testCase *testCase, getDBTX func(db *sql.DB) (queries.DBTX, io.Closer)) {
	defer func() {
		db, err := sql.Open("pgx", engine.GetPostgresDatabaseDSN())
		require.NoError(t, err)
		defer db.Close()
		require.NoError(t, pgengine.ResetInstance(context.Background(), db))
	}()

	db, err := engine.CreateDatabase()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.DropDB())
	}()

	connPool, err := sql.Open("pgx", db.GetDSN())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, connPool.Close())
	}()

	for _, stmt := range testCase.ddl {
		_, err := connPool.Exec(stmt)
		require.NoError(t, err)
	}

	dbtx, closer := getDBTX(connPool)
	if closer != nil {
		defer func() {
			require.NoError(t, closer.Close())
		}()
	}

	fetchedSchema, err := GetSchema(context.TODO(), dbtx, testCase.opts...)
	if testCase.expectedErrIs != nil {
		require.ErrorIs(t, err, testCase.expectedErrIs)
		return
	}
	if testCase.expectedErrContains != "" {
		require.ErrorContains(t, err, testCase.expectedErrContains)
		return
	}
	if testCase.expectedErrIs == nil && testCase.expectedErrContains == "" {
		require.NoError(t, err)
	}

	expectedNormalized := testCase.expectedSchema.Normalize()
	fetchedNormalized := fetchedSchema.Normalize()
	assert.Equal(t, expectedNormalized, fetchedNormalized, "expected=\n%# v \n fetched=%# v\n", pretty.Formatter(expectedNormalized), pretty.Formatter(fetchedNormalized))

	fetchedSchemaHash, err := fetchedSchema.Hash()
	require.NoError(t, err)
	expectedSchemaHash, err := testCase.expectedSchema.Hash()
	require.NoError(t, err)
	// same schemas should have the same hashes
	assert.Equal(t, expectedSchemaHash, fetchedSchemaHash, "hash of expected schema should match fetched hash")
	if testCase.expectedHash != "" {
		// Optionally assert that the hash matches the expected hash
		assert.Equal(t, testCase.expectedHash, fetchedSchemaHash)
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
			out, err := GetIndexDefStatement(tc.defStmt).ToCreateIndexConcurrently()
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
			out, err := GetTriggerDefStatement(tc.defStmt).ToCreateOrReplace()
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.out, out)
			}
		})
	}
}
