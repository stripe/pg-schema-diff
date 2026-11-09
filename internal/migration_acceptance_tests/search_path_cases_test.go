package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var searchPathAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "Create expression index on a function in public",
		oldSchemaDDL: []string{
			`
			CREATE FUNCTION normalize_email(text) RETURNS text LANGUAGE sql IMMUTABLE
				AS $$ SELECT lower($1) $$;
			CREATE TABLE users (email text);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE FUNCTION normalize_email(text) RETURNS text LANGUAGE sql IMMUTABLE
				AS $$ SELECT lower($1) $$;
			CREATE TABLE users (email text);
			CREATE INDEX users_normalized_email_idx ON users (normalize_email(email));
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
	{
		name: "Create GIN index with an operator class from an extension in public",
		oldSchemaDDL: []string{
			`
			CREATE EXTENSION pg_trgm;
			CREATE TABLE users (name text);
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE EXTENSION pg_trgm;
			CREATE TABLE users (name text);
			CREATE INDEX users_name_trgm_idx ON users USING gin (name gin_trgm_ops);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeIndexBuild,
		},
	},
}

func TestSearchPathTestCases(t *testing.T) {
	runTestCases(t, searchPathAcceptanceTestCases)
}
