package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var indexNoConcurrentAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "Add",
		oldSchemaDDL: []string{
			`
            CREATE TABLE "Foobar"(
                id INT PRIMARY KEY,
                "Foo" VARCHAR(255)
            );
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE "Foobar"(
                id INT PRIMARY KEY,
                "Foo" VARCHAR(255)
            );
            CREATE INDEX "Some_idx" ON "Foobar"(id, "Foo");
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresShareLock,
		},
		planOpts: []diff.PlanOpt{diff.WithNoConcurrentIndexOps()},
	},
	{
		name: "Delete",
		oldSchemaDDL: []string{
			`
            CREATE TABLE "Foobar"(
                id INT PRIMARY KEY,
                "Foo" VARCHAR(255)
            );
            CREATE INDEX "Some_idx" ON "Foobar"(id, "Foo");
			`,
		},
		newSchemaDDL: []string{
			`
            CREATE TABLE "Foobar"(
                id INT PRIMARY KEY,
                "Foo" VARCHAR(255) NOT NULL
            );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAcquiresAccessExclusiveLock,
			diff.MigrationHazardTypeIndexDropped,
		},
		planOpts: []diff.PlanOpt{diff.WithNoConcurrentIndexOps()},
	},
}

func TestIndexNoConcurrentTestCases(t *testing.T) {
	runTestCases(t, indexNoConcurrentAcceptanceTestCases)
}
