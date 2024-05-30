package migration_acceptance_tests

import "github.com/stripe/pg-schema-diff/pkg/diff"

var policyAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "no-op",
		roles: []string{
			"role_1",
			"role_2",
		},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE
					FOR ALL 
					TO role_1, role_2 
					USING (true)
					WITH CHECK (true);
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE
					FOR ALL 
					TO role_1, role_2 
					USING (true)
					WITH CHECK (true);
			`,
		},
		expectEmptyPlan: true,
	},
	{
		name: "Add permissive ALL policy target on non-public schema",
		roles: []string{
			"role_1",
			"role_2",
		},
		oldSchemaDDL: []string{
			`
				CREATE SCHEMA schema_1;
				CREATE TABLE schema_1.foobar();
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE SCHEMA schema_1;
				CREATE TABLE schema_1.foobar();
				CREATE POLICY foobar_policy ON schema_1.foobar 
					AS PERMISSIVE
					FOR ALL 
					TO role_1, role_2
					USING (true)
					WITH CHECK (true);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Create SELECT policy",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar();
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS RESTRICTIVE
					FOR SELECT 
					TO PUBLIC
					USING (true);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Create INSERT policy",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar();
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS RESTRICTIVE
					FOR INSERT 
					TO PUBLIC
					WITH CHECK (true);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Create UPDATE policy",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar();
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS RESTRICTIVE
					FOR UPDATE 
					TO PUBLIC
					WITH CHECK (true);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Create DELETE policy",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar();
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS RESTRICTIVE
					FOR SELECT 
					TO PUBLIC
					USING (true);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Add policy on new table",
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS RESTRICTIVE
					FOR SELECT 
					TO PUBLIC
					USING (true);
			`,
		},
	},
	{
		name: "Add policy then enable RLS",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar();
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				ALTER TABLE foobar ENABLE ROW LEVEL SECURITY;
				ALTER TABLE foobar FORCE ROW LEVEL SECURITY;
				CREATE POLICY foobar_policy ON foobar 
					AS RESTRICTIVE
					FOR SELECT 
					TO PUBLIC
					USING (true);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
		expectedPlanDDL: []string{
			// Ensure that the policy is created before enabling RLS.
			"CREATE POLICY \"foobar_policy\" ON \"public\".\"foobar\"\n\tAS RESTRICTIVE\n\tFOR SELECT\n\tTO PUBLIC\n\tUSING (true)",
			"ALTER TABLE \"public\".\"foobar\" ENABLE ROW LEVEL SECURITY",
			"ALTER TABLE \"public\".\"foobar\" FORCE ROW LEVEL SECURITY",
		},
	},
	{
		name: "Drop non-public schema policy",
		oldSchemaDDL: []string{
			`
				CREATE SCHEMA schema_1;
				CREATE TABLE schema_1.foobar();
				CREATE POLICY foobar_policy ON schema_1.foobar 
					AS RESTRICTIVE
					FOR SELECT 
					TO PUBLIC
					USING (true);
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE SCHEMA schema_1;
				CREATE TABLE schema_1.foobar();
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Drop policy and table",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS RESTRICTIVE
					FOR SELECT 
					TO PUBLIC
					USING (true);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Disable RLS then drop policy",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS RESTRICTIVE
					FOR SELECT 
					TO PUBLIC
					USING (true);
				ALTER TABLE foobar ENABLE ROW LEVEL SECURITY;
				ALTER TABLE foobar FORCE ROW LEVEL SECURITY;
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
		expectedPlanDDL: []string{
			"ALTER TABLE \"public\".\"foobar\" DISABLE ROW LEVEL SECURITY",
			"ALTER TABLE \"public\".\"foobar\" NO FORCE ROW LEVEL SECURITY",
			"DROP POLICY \"foobar_policy\" ON \"public\".\"foobar\"",
		},
	},
	{
		name: "Drop policy and columns",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(
						category TEXT,
						val TEXT
				);
				CREATE POLICY foobar_policy ON foobar 
					AS RESTRICTIVE
					FOR SELECT 
					TO PUBLIC
					USING (category = 'category' AND val = 'value');
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Restrictive to permissive policy",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE 
					FOR SELECT 
					TO PUBLIC
					USING (true);
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS RESTRICTIVE 
					FOR SELECT 
					TO PUBLIC
					USING (true);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Alter policy target",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE 
					FOR SELECT 
					TO PUBLIC
					USING (true);
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE 
					FOR DELETE 
					TO PUBLIC
					USING (true);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name:  "Alter policy applies to",
		roles: []string{"role_1", "role_2"},
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE 
					FOR SELECT 
					TO PUBLIC
					USING (true);
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE 
					FOR SELECT 
					TO role_1, role_2
					USING (true);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Alter policy using",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE 
					FOR SELECT 
					TO PUBLIC
					USING (true);
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE 
					FOR SELECT 
					TO PUBLIC
					USING (false);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Alter policy check",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE 
					FOR INSERT 
					TO PUBLIC
					WITH CHECK (true);
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE 
					FOR INSERT 
					TO PUBLIC
					WITH CHECK (false);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Remove using check for ALL policy",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE 
					FOR ALL 
					TO PUBLIC
					USING (true)
					WITH CHECK (true);
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE 
					FOR ALL 
					TO PUBLIC
					WITH CHECK (true);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Remove check for ALL policy",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE 
					FOR ALL 
					TO PUBLIC
					USING (true)
					WITH CHECK (true);
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar();
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE 
					FOR ALL 
					TO PUBLIC
					USING (true);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Alter all alterable attributes on non-public schema",
		roles: []string{
			"role_1",
			"role_2",
		},
		oldSchemaDDL: []string{
			`
				CREATE SCHEMA schema_1;
				CREATE TABLE schema_1.foobar();
				CREATE POLICY foobar_policy ON schema_1.foobar 
					AS PERMISSIVE 
					FOR ALL 
					TO role_1, role_2
					USING (true)
					WITH CHECK (true);
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE SCHEMA schema_1;
				CREATE TABLE schema_1.foobar();
				CREATE POLICY foobar_policy ON schema_1.foobar 
					AS PERMISSIVE 
					FOR ALL 
					TO PUBLIC
					USING (false)
					WITH CHECK (false);
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
		},
	},
	{
		name: "Alter policy that references deleted columns and new columns (non-public schema)",
		oldSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    category TEXT,
			    val TEXT
			);
			CREATE POLICY foobar_policy ON schema_1.foobar 
				AS PERMISSIVE 
				FOR ALL 
				TO PUBLIC
				USING (val = 'value' AND category = 'category')
				WITH CHECK (val = 'value');
		`,
		},
		newSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    category TEXT NOT NULL,
			    new_val TEXT
			);
			CREATE POLICY foobar_policy ON schema_1.foobar 
				AS PERMISSIVE 
				FOR ALL 
				TO PUBLIC
				USING (new_val = 'value' AND category = 'category')
				WITH CHECK (new_val = 'value');
		`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Re-create policy that references deleted columns and new columns (non-public schema)",
		oldSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    category TEXT,
			    val TEXT
			);
			CREATE POLICY foobar_policy ON schema_1.foobar 
				AS PERMISSIVE 
				FOR ALL 
				TO PUBLIC
				USING (val = 'value' AND category = 'category')
				WITH CHECK (val = 'value');
		`,
		},
		newSchemaDDL: []string{
			`
			CREATE SCHEMA schema_1;
			CREATE TABLE schema_1.foobar(
			    category TEXT NOT NULL,
			    new_val TEXT
			);
			CREATE POLICY foobar_policy ON schema_1.foobar 
				AS RESTRICTIVE -- force-recreate the policy 
				FOR ALL 
				TO PUBLIC
				USING (new_val = 'value' AND category = 'category')
				WITH CHECK (new_val = 'value');
		`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeAuthzUpdate,
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Alter policy (table is re-created)",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(
				    category TEXT
				) partition by list (category);
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE 
					FOR INSERT 
					TO PUBLIC
					WITH CHECK (true);
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(
				    category TEXT,
					some_new_column TEXT
				); -- Re-create by removing partitioning
				CREATE POLICY foobar_policy ON foobar 
					AS PERMISSIVE 
					FOR INSERT 
					TO PUBLIC
					WITH CHECK (category = 'category' AND some_new_column = 'value');
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "Policy on new partition (not implemented)",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(
				    category TEXT
				) partition by list (category);
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(
				    category TEXT
				) partition by list (category);
				CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('category');
				CREATE POLICY foobar_1_policy ON foobar_1 
					AS PERMISSIVE 
					FOR INSERT 
					TO PUBLIC
					WITH CHECK (true);
			`,
		},

		expectedPlanErrorIs: diff.ErrNotImplemented,
	},
	{
		name: "Add policy on existing partition (not implemented)",
		oldSchemaDDL: []string{
			`
				CREATE TABLE foobar(
				    category TEXT
				) partition by list (category);
				CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('category');
			`,
		},
		newSchemaDDL: []string{
			`
				CREATE TABLE foobar(
				    category TEXT
				) partition by list (category);
				CREATE TABLE foobar_1 PARTITION OF foobar FOR VALUES IN ('category');
				CREATE POLICY foobar_1_policy ON foobar_1 
					AS PERMISSIVE 
					FOR INSERT 
					TO PUBLIC
					WITH CHECK (true);
			`,
		},

		expectedPlanErrorIs: diff.ErrNotImplemented,
	},
}

func (suite *acceptanceTestSuite) TestPolicyCases() {
	suite.runTestCases(policyAcceptanceTestCases)
}
