package schema_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/stripe/pg-schema-diff/internal/pgengine"
	internalschema "github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/pkg/schema"
)

type schemaTestSuite struct {
	suite.Suite
	pgEngine *pgengine.Engine
}

func (suite *schemaTestSuite) SetupSuite() {
	engine, err := pgengine.StartEngine()
	suite.Require().NoError(err)
	suite.pgEngine = engine
}

func (suite *schemaTestSuite) TearDownSuite() {
	suite.pgEngine.Close()
}

func (suite *schemaTestSuite) TestGetPublicSchemaHash() {
	const (
		ddl = `
			CREATE EXTENSION pg_trgm WITH VERSION '1.6';

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
			
	        CREATE SCHEMA schema_filtered_1;
			CREATE TABLE schema_filtered_1.bar()
	`
	)
	db, err := suite.pgEngine.CreateDatabase()
	suite.Require().NoError(err)
	defer db.DropDB()

	connPool, err := sql.Open("pgx", db.GetDSN())
	suite.Require().NoError(err)
	defer connPool.Close()

	_, err = connPool.ExecContext(context.Background(), ddl)
	suite.Require().NoError(err)

	conn, err := connPool.Conn(context.Background())
	suite.Require().NoError(err)
	defer conn.Close()

	hash, err := schema.GetPublicSchemaHash(context.Background(), conn)
	suite.Require().NoError(err)

	schema, err := internalschema.GetSchema(context.Background(), conn, internalschema.WithIncludeSchemas("public"))
	suite.Require().NoError(err)
	expectedHash, err := schema.Hash()
	suite.Require().NoError(err)
	suite.Equal(expectedHash, hash)
}

func TestSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(schemaTestSuite))
}
