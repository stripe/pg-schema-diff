package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var objectOwnerAcceptanceTestCases = []acceptanceTestCase{
	{
		name:  "Change table owner",
		roles: []string{"owner_a", "owner_b"},
		oldSchemaDDL: []string{`
			CREATE TABLE owner_test(id int);
			ALTER TABLE owner_test OWNER TO owner_a;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE owner_test(id int);
			ALTER TABLE owner_test OWNER TO owner_b;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeAuthzUpdate},
		expectedPlanDDL:     []string{`ALTER TABLE "public"."owner_test" OWNER TO "owner_b"`},
	},
	{
		name:  "Change function owner",
		roles: []string{"owner_a", "owner_b"},
		oldSchemaDDL: []string{`
			CREATE FUNCTION owner_add(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN a + b;
			ALTER FUNCTION owner_add(a integer, b integer) OWNER TO owner_a;
		`},
		newSchemaDDL: []string{`
			CREATE FUNCTION owner_add(a integer, b integer) RETURNS integer
				LANGUAGE SQL
				IMMUTABLE
				RETURNS NULL ON NULL INPUT
				RETURN a + b;
			ALTER FUNCTION owner_add(a integer, b integer) OWNER TO owner_b;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeAuthzUpdate},
		expectedPlanDDL:     []string{`ALTER FUNCTION "public"."owner_add"(a integer, b integer) OWNER TO "owner_b"`},
	},
	{
		name:  "Change procedure owner",
		roles: []string{"owner_a", "owner_b"},
		oldSchemaDDL: []string{`
			CREATE PROCEDURE owner_work()
				LANGUAGE plpgsql
				AS $$
				BEGIN
				END;
				$$;
			ALTER PROCEDURE owner_work() OWNER TO owner_a;
		`},
		newSchemaDDL: []string{`
			CREATE PROCEDURE owner_work()
				LANGUAGE plpgsql
				AS $$
				BEGIN
				END;
				$$;
			ALTER PROCEDURE owner_work() OWNER TO owner_b;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeAuthzUpdate},
		expectedPlanDDL:     []string{`ALTER PROCEDURE "public"."owner_work"() OWNER TO "owner_b"`},
	},
	{
		name:  "Change enum owner",
		roles: []string{"owner_a", "owner_b"},
		oldSchemaDDL: []string{`
			CREATE TYPE owner_color AS ENUM ('red', 'blue');
			ALTER TYPE owner_color OWNER TO owner_a;
		`},
		newSchemaDDL: []string{`
			CREATE TYPE owner_color AS ENUM ('red', 'blue');
			ALTER TYPE owner_color OWNER TO owner_b;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeAuthzUpdate},
		expectedPlanDDL:     []string{`ALTER TYPE "public"."owner_color" OWNER TO "owner_b"`},
	},
	{
		name:  "Change sequence owner",
		roles: []string{"owner_a", "owner_b"},
		oldSchemaDDL: []string{`
			CREATE SEQUENCE owner_seq OWNED BY NONE;
			ALTER SEQUENCE owner_seq OWNER TO owner_a;
		`},
		newSchemaDDL: []string{`
			CREATE SEQUENCE owner_seq OWNED BY NONE;
			ALTER SEQUENCE owner_seq OWNER TO owner_b;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeAuthzUpdate},
		expectedPlanDDL:     []string{`ALTER SEQUENCE "public"."owner_seq" OWNER TO "owner_b"`},
	},
	{
		name:  "Change view owner",
		roles: []string{"owner_a", "owner_b"},
		oldSchemaDDL: []string{`
			CREATE TABLE owner_base(id int);
			CREATE VIEW owner_view AS SELECT id FROM owner_base;
			ALTER VIEW owner_view OWNER TO owner_a;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE owner_base(id int);
			CREATE VIEW owner_view AS SELECT id FROM owner_base;
			ALTER VIEW owner_view OWNER TO owner_b;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeAuthzUpdate},
		expectedPlanDDL:     []string{`ALTER VIEW "public"."owner_view" OWNER TO "owner_b"`},
	},
	{
		name:  "Change materialized view owner",
		roles: []string{"owner_a", "owner_b"},
		oldSchemaDDL: []string{`
			CREATE TABLE owner_base(id int);
			CREATE MATERIALIZED VIEW owner_mview AS SELECT id FROM owner_base;
			ALTER MATERIALIZED VIEW owner_mview OWNER TO owner_a;
		`},
		newSchemaDDL: []string{`
			CREATE TABLE owner_base(id int);
			CREATE MATERIALIZED VIEW owner_mview AS SELECT id FROM owner_base;
			ALTER MATERIALIZED VIEW owner_mview OWNER TO owner_b;
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeAuthzUpdate},
		expectedPlanDDL:     []string{`ALTER MATERIALIZED VIEW "public"."owner_mview" OWNER TO "owner_b"`},
	},
}

func TestObjectOwnerTestCases(t *testing.T) {
	runTestCases(t, objectOwnerAcceptanceTestCases)
}
