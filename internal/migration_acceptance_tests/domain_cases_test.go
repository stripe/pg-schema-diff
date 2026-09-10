package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var domainAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "no-op",
		oldSchemaDDL: []string{
			`
            CREATE DOMAIN email AS TEXT DEFAULT 'nobody@example.com' NOT NULL
                CONSTRAINT email_check CHECK (VALUE ~ '^[^@]+@[^@]+$');
            CREATE TABLE foo(
                email email
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE DOMAIN email AS TEXT DEFAULT 'nobody@example.com' NOT NULL
                CONSTRAINT email_check CHECK (VALUE ~ '^[^@]+@[^@]+$');
            CREATE TABLE foo(
                email email
            );
			`,
		},
		expectEmptyPlan: true,
	},
	{
		name: "create domain and a table using it in the same migration",
		oldSchemaDDL: []string{
			`
            CREATE TABLE foo();
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE SCHEMA types;
            CREATE DOMAIN types.euro_amount AS NUMERIC(14, 2) DEFAULT 0 NOT NULL
                CONSTRAINT euro_amount_check CHECK (VALUE >= 0)
                CONSTRAINT euro_amount_scale_check CHECK (scale(VALUE) <= 2);
            CREATE TABLE foo(
                price types.euro_amount
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{},
	},
	{
		name: "drop domain with the table using it",
		oldSchemaDDL: []string{
			`
            CREATE DOMAIN email AS TEXT CONSTRAINT email_check CHECK (VALUE ~ '^[^@]+@[^@]+$');
            CREATE TABLE foo(
                email email
            );
			`,
		},
		newSchemaDDL: []string{
			`
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "add, change and drop domain constraints, default and not null",
		oldSchemaDDL: []string{
			`
            CREATE DOMAIN email AS TEXT DEFAULT 'nobody@example.com'
                CONSTRAINT email_check CHECK (VALUE ~ '^[^@]+@[^@]+$')
                CONSTRAINT email_length_check CHECK (length(VALUE) <= 320);
            CREATE TABLE foo(
                email email
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE DOMAIN email AS TEXT NOT NULL
                CONSTRAINT email_check CHECK (VALUE ~ '^[^@[:space:]]+@[^@[:space:]]+$')
                CONSTRAINT email_not_blank_check CHECK (btrim(VALUE) <> '');
            CREATE TABLE foo(
                email email
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{},
	},
	{
		name: "column moves from a text with a check to a domain, and the old domain is dropped",
		oldSchemaDDL: []string{
			`
            CREATE DOMAIN old_email AS TEXT CONSTRAINT old_email_check CHECK (VALUE ~ '^[^@]+@[^@]+$');
            CREATE TABLE foo(
                email old_email,
                title TEXT CONSTRAINT foo_title_check CHECK (btrim(title) <> '')
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE DOMAIN email AS TEXT CONSTRAINT email_check CHECK (VALUE ~ '^[^@]+@[^@]+$');
            CREATE DOMAIN non_blank_text AS TEXT CONSTRAINT non_blank_text_check CHECK (btrim(VALUE) <> '');
            CREATE TABLE foo(
                email email,
                title non_blank_text
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeImpactsDatabasePerformance,
		},
	},
	{
		name: "changing the base type of a domain is not supported",
		oldSchemaDDL: []string{
			`
            CREATE DOMAIN amount AS INTEGER CONSTRAINT amount_check CHECK (VALUE >= 0);
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE DOMAIN amount AS BIGINT CONSTRAINT amount_check CHECK (VALUE >= 0);
			`,
		},
		expectedPlanErrorIs: diff.ErrNotImplemented,
	},
}

func TestDomainTestCases(t *testing.T) {
	runTestCases(t, domainAcceptanceTestCases)
}
