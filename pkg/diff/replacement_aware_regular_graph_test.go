package diff

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

func TestTableDispositionDeleteBehavior(t *testing.T) {
	t.Parallel()

	table := replacementAwareTestTable(false)
	for _, tc := range []struct {
		name     string
		kind     tableDispositionKind
		wantDrop bool
	}{
		{name: "physical delete", kind: tableDispositionKindPhysicalDelete, wantDrop: true},
		{name: "archival move", kind: tableDispositionKindArchivalMove},
		{name: "cleanup only", kind: tableDispositionKindCleanupOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			generator := tableSQLVertexGenerator{tableDispositions: tableDispositions{
				table.GetName(): {Kind: tc.kind},
			}}
			statements, err := generator.deleteStatements(table)
			require.NoError(t, err)
			if tc.wantDrop {
				require.Len(t, statements, 1)
				assert.Equal(t, `DROP TABLE "public"."accounts"`, statements[0].DDL)
				assert.Equal(t, []MigrationHazard{{
					Type:    MigrationHazardTypeDeletesData,
					Message: "Deletes all rows in the table (and the table itself)",
				}}, statements[0].Hazards)
			} else {
				assert.Empty(t, statements)
			}
		})
	}
}

func TestResolveTableDispositionsRejectsInvalidArchivalCombinations(t *testing.T) {
	t.Parallel()

	table := replacementAwareTestTable(false)
	request := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_ABCDEFGH", 10, "public", "accounts",
	)
	groups, err := preparePlainTableArchivalGroups(request)
	require.NoError(t, err)
	group := groups[0]
	group.members = append([]preparedArchivalMember{}, group.members...)

	for _, tc := range []struct {
		name         string
		deletes      []schema.Table
		dispositions tableDispositions
		groups       []preparedArchivalGroup
		errorText    string
	}{
		{
			name: "missing disposition", deletes: []schema.Table{table},
			dispositions: tableDispositions{}, groups: groups,
			errorText: "missing disposition",
		},
		{
			name: "move without group", deletes: []schema.Table{table},
			dispositions: tableDispositions{table.GetName(): {
				Kind: tableDispositionKindArchivalMove, GroupID: group.id,
			}},
			errorText: "missing group",
		},
		{
			name: "physical delete with group", deletes: []schema.Table{table},
			dispositions: tableDispositions{table.GetName(): {
				Kind: tableDispositionKindPhysicalDelete,
			}}, groups: groups,
			errorText: "conflicts with archival group",
		},
		{
			name: "cleanup before move", deletes: []schema.Table{table},
			dispositions: tableDispositions{table.GetName(): {
				Kind: tableDispositionKindCleanupOnly, GroupID: group.id,
			}}, groups: groups,
			errorText: "still requires an ordinary table move",
		},
		{
			name: "unknown disposition", deletes: []schema.Table{table},
			dispositions: tableDispositions{table.GetName(): {
				Kind: tableDispositionKindUnknown,
			}},
			errorText: "unknown disposition",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := resolveTableDispositions(tc.deletes, tc.dispositions, tc.groups)
			require.ErrorContains(t, err, tc.errorText)
		})
	}

	group.members[0].remainingMove = nil
	resolved, err := resolveTableDispositions(nil, tableDispositions{table.GetName(): {
		Kind: tableDispositionKindCleanupOnly, GroupID: group.id,
	}}, []preparedArchivalGroup{group})
	require.NoError(t, err)
	assert.Equal(t, tableDispositionKindCleanupOnly, resolved[table.GetName()].Kind)
}

func TestReplacementAwareGraphSuppressesMovedTableChildDeletes(t *testing.T) {
	t.Parallel()

	oldSchema := replacementAwareTestSchema(false)
	diff, _, err := buildSchemaDiff(oldSchema, schema.Schema{})
	require.NoError(t, err)
	request := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_ABCDEFGH", 10, "public", "accounts",
	)
	_, err = generateReplacementAwareSchemaSQL(
		newSchemaSQLGenerator(&deterministicRandReader{}, &planOptions{}),
		diff,
		tableDispositions{},
		request,
	)
	require.ErrorContains(t, err, "missing disposition")

	statements, err := generateReplacementAwareSchemaSQL(
		newSchemaSQLGenerator(&deterministicRandReader{}, &planOptions{}),
		diff,
		tableDispositions{oldSchema.Tables[0].GetName(): {
			Kind: tableDispositionKindArchivalMove, GroupID: request.DependencyClosure.ValidatedGroupIDs[0],
		}},
		request,
	)
	require.NoError(t, err)

	ddl := replacementAwareDDL(statements)
	assert.Contains(t, ddl, `ALTER TABLE "public"."accounts" SET SCHEMA`)
	for _, unwanted := range []string{
		"DROP TABLE", "DROP INDEX", "DROP SEQUENCE", "DROP TRIGGER", "DROP POLICY", "DROP CONSTRAINT",
	} {
		assert.NotContains(t, ddl, unwanted)
	}
	assertReplacementAwareStatementsAreNonDestructive(t, statements)
	assert.Contains(t, statements[len(statements)-1].DDL, "archival marker mismatch")
}

func TestReplacementAwareGraphOrdersRecreationAndTargetKindsAfterMove(t *testing.T) {
	t.Parallel()

	oldSchema := replacementAwareTestSchema(false)
	newSchema := replacementAwareTestSchema(true)
	newSchema.Enums = []schema.Enum{{
		SchemaQualifiedName: replacementAwareName("status"), Labels: []string{"active"},
	}}
	newSchema.Sequences = append(newSchema.Sequences, schema.Sequence{
		SchemaQualifiedName: replacementAwareName("standalone_sequence"),
		Type:                "bigint", StartValue: 1, Increment: 1, MinValue: 1,
		MaxValue: 9223372036854775807, CacheSize: 1,
	})
	newSchema.Functions = append(newSchema.Functions, schema.Function{
		SchemaQualifiedName: replacementAwareName("account_id"),
		FunctionDef:         `CREATE FUNCTION public.account_id(public.accounts) RETURNS bigint LANGUAGE sql RETURN $1.id`,
		Language:            "sql",
	})
	newSchema.Procedures = []schema.Procedure{{
		SchemaQualifiedName: replacementAwareName("refresh_accounts"),
		Def:                 `CREATE PROCEDURE public.refresh_accounts() LANGUAGE sql AS 'SELECT 1'`,
	}}
	newSchema.Views = []schema.View{{
		SchemaQualifiedName: replacementAwareName("accounts_view"),
		ViewDefinition:      `SELECT id FROM public.accounts`,
		TableDependencies: []schema.TableDependency{{
			SchemaQualifiedName: replacementAwareName("accounts"), Columns: []string{"id"},
		}},
	}}
	newSchema.MaterializedViews = []schema.MaterializedView{{
		SchemaQualifiedName: replacementAwareName("accounts_materialized"),
		ViewDefinition:      `SELECT id FROM public.accounts`,
		TableDependencies: []schema.TableDependency{{
			SchemaQualifiedName: replacementAwareName("accounts"), Columns: []string{"id"},
		}},
	}}

	diff, _, err := buildSchemaDiff(oldSchema, newSchema)
	require.NoError(t, err)
	request := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_ABCDEFGH", 10, "public", "accounts",
	)
	statements, err := generateReplacementAwareSchemaSQL(
		newSchemaSQLGenerator(&deterministicRandReader{}, &planOptions{}),
		diff,
		tableDispositions{oldSchema.Tables[0].GetName(): {
			Kind: tableDispositionKindArchivalMove, GroupID: request.DependencyClosure.ValidatedGroupIDs[0],
		}},
		request,
	)
	require.NoError(t, err)

	move := statementIndexContaining(t, statements, `ALTER TABLE "public"."accounts" SET SCHEMA`)
	targetTable := statementIndexContaining(t, statements, `CREATE TABLE "public"."accounts"`)
	assert.Less(t, move, statementIndexContaining(t, statements, `CREATE TYPE "public"."status"`))
	assert.Less(t, move, statementIndexContaining(t, statements,
		`CREATE SEQUENCE "public"."accounts_id_seq"`))
	assert.Less(t, move, statementIndexContaining(t, statements,
		`CREATE SEQUENCE "public"."standalone_sequence"`))
	assert.Less(t, move, targetTable)
	assert.Less(t, targetTable, statementIndexContaining(t, statements, "CREATE INDEX accounts_payload_idx"))
	assert.Less(t, targetTable, statementIndexContaining(t, statements, "CREATE TRIGGER accounts_trigger"))
	assert.Less(t, targetTable, statementIndexContaining(t, statements,
		`CREATE VIEW "public"."accounts_view"`))
	assert.Less(t, targetTable, statementIndexContaining(t, statements,
		`CREATE MATERIALIZED VIEW "public"."accounts_materialized"`))
	assert.Less(t, targetTable, statementIndexContaining(t, statements, "CREATE FUNCTION public.account_id"))
	assert.Less(t, targetTable, statementIndexContaining(t, statements,
		"CREATE PROCEDURE public.refresh_accounts"))
	assert.NotContains(t, replacementAwareDDL(statements), "ALTER INDEX")
	assert.NotContains(t, replacementAwareDDL(statements), "DROP ")
	assertReplacementAwareStatementsAreNonDestructive(t, statements)
	assert.Contains(t, statements[len(statements)-1].DDL, "archival marker mismatch")
}

func TestReplacementAwareGraphRejectsUnrepresentedStage14ForeignKeyWork(t *testing.T) {
	t.Parallel()

	oldSchema := replacementAwareTestSchema(false)
	oldSchema.ForeignKeyConstraints = []schema.ForeignKeyConstraint{{
		EscapedName:   `"accounts_parent_fk"`,
		OwningTable:   oldSchema.Tables[0].SchemaQualifiedName,
		ForeignTable:  oldSchema.Tables[0].SchemaQualifiedName,
		ConstraintDef: `FOREIGN KEY (id) REFERENCES public.accounts(id)`,
	}}
	diff, _, err := buildSchemaDiff(oldSchema, schema.Schema{})
	require.NoError(t, err)
	request := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_ABCDEFGH", 10, "public", "accounts",
	)
	_, err = generateReplacementAwareSchemaSQL(
		newSchemaSQLGenerator(&deterministicRandReader{}, &planOptions{}),
		diff,
		tableDispositions{oldSchema.Tables[0].GetName(): {
			Kind: tableDispositionKindArchivalMove, GroupID: request.DependencyClosure.ValidatedGroupIDs[0],
		}},
		request,
	)
	require.ErrorContains(t, err, "not fully represented by Stage 14")
}

func TestExplicitPhysicalDispositionMatchesLegacyTableRemoval(t *testing.T) {
	t.Parallel()

	oldSchema := schema.Schema{Tables: []schema.Table{replacementAwareTestTable(false)}}
	diff, _, err := buildSchemaDiff(oldSchema, schema.Schema{})
	require.NoError(t, err)
	legacy, err := newSchemaSQLGenerator(&deterministicRandReader{}, &planOptions{}).Alter(diff)
	require.NoError(t, err)
	explicit, err := generateReplacementAwareSchemaSQL(
		newSchemaSQLGenerator(&deterministicRandReader{}, &planOptions{}),
		diff,
		physicalTableDispositions(diff.tableDiffs.deletes),
		plainTableArchivalRequest{},
	)
	require.NoError(t, err)
	assert.Equal(t, legacy, explicit)
	require.Len(t, legacy, 1)
	assert.Equal(t, `DROP TABLE "public"."accounts"`, legacy[0].DDL)
	assert.Equal(t, MigrationHazardTypeDeletesData, legacy[0].Hazards[0].Type)

	unchangedDiff, _, err := buildSchemaDiff(oldSchema, oldSchema)
	require.NoError(t, err)
	unchanged, err := generateReplacementAwareSchemaSQL(
		newSchemaSQLGenerator(&deterministicRandReader{}, &planOptions{}),
		unchangedDiff,
		tableDispositions{},
		plainTableArchivalRequest{},
	)
	require.NoError(t, err)
	assert.Empty(t, unchanged)
}

func replacementAwareTestSchema(recreated bool) schema.Schema {
	table := replacementAwareTestTable(recreated)
	return schema.Schema{
		Tables: []schema.Table{table},
		Indexes: []schema.Index{{
			Name: "accounts_payload_idx", OwningRelName: table.SchemaQualifiedName,
			OwningRelKind: schema.RelKindOrdinaryTable, Columns: []string{"payload"},
			GetIndexDefStmt: schema.GetIndexDefStatement("CREATE INDEX accounts_payload_idx ON " +
				map[bool]string{false: "public.accounts", true: "ONLY public.accounts"}[recreated] +
				" USING btree (payload)"),
		}},
		Sequences: []schema.Sequence{{
			SchemaQualifiedName: replacementAwareName("accounts_id_seq"),
			Owner:               &schema.SequenceOwner{TableName: table.SchemaQualifiedName, ColumnName: "id"},
			Type:                "bigint", StartValue: 1, Increment: 1, MinValue: 1,
			MaxValue: 9223372036854775807, CacheSize: 1,
		}},
		Functions: []schema.Function{{
			SchemaQualifiedName: replacementAwareName("accounts_trigger_fn"),
			FunctionDef:         `CREATE FUNCTION public.accounts_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'`,
			Language:            "plpgsql",
		}},
		Triggers: []schema.Trigger{{
			EscapedName: `"accounts_trigger"`, OwningTable: table.SchemaQualifiedName,
			Function: replacementAwareName("accounts_trigger_fn"),
			GetTriggerDefStmt: `CREATE TRIGGER accounts_trigger BEFORE INSERT ON public.accounts ` +
				`FOR EACH ROW EXECUTE FUNCTION public.accounts_trigger_fn()`,
		}},
	}
}

func replacementAwareTestTable(recreated bool) schema.Table {
	table := schema.Table{
		SchemaQualifiedName: replacementAwareName("accounts"),
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Default: `nextval('public.accounts_id_seq'::regclass)`},
			{Name: "payload", Type: "text"},
		},
		CheckConstraints: []schema.CheckConstraint{{
			Name: "accounts_payload_check", Expression: "length(payload) > 0", IsValid: true, IsInheritable: true,
		}},
		Policies: []schema.Policy{{
			EscapedName: `"accounts_policy"`, IsPermissive: true, AppliesTo: []string{"PUBLIC"},
			Cmd: schema.SelectPolicyCmd, UsingExpression: "id > 0",
		}},
		RLSEnabled:      true,
		ReplicaIdentity: schema.ReplicaIdentityDefault,
	}
	if recreated {
		table.PartitionKeyDef = "HASH (id)"
	}
	return table
}

func replacementAwareName(name string) schema.SchemaQualifiedName {
	return schema.SchemaQualifiedName{
		SchemaName:  "public",
		EscapedName: schema.EscapeIdentifier(name),
	}
}

func replacementAwareDDL(statements []Statement) string {
	var ddl []string
	for _, statement := range statements {
		ddl = append(ddl, statement.DDL)
	}
	return strings.Join(ddl, "\n")
}

func statementIndexContaining(t *testing.T, statements []Statement, fragment string) int {
	t.Helper()
	for idx, statement := range statements {
		if strings.Contains(statement.DDL, fragment) {
			return idx
		}
	}
	require.Failf(t, "statement not found", "missing DDL fragment %q in:\n%s", fragment,
		replacementAwareDDL(statements))
	return -1
}

func assertReplacementAwareStatementsAreNonDestructive(t *testing.T, statements []Statement) {
	t.Helper()
	for _, statement := range statements {
		upperDDL := strings.ToUpper(statement.DDL)
		assert.False(t, strings.HasPrefix(strings.TrimSpace(upperDDL), "DROP TABLE"))
		assert.NotContains(t, upperDDL, "TRUNCATE")
		assert.NotContains(t, upperDDL, "DELETE FROM")
		for _, hazard := range statement.Hazards {
			assert.NotEqual(t, MigrationHazardTypeDeletesData, hazard.Type)
			assert.NotEqual(t, MigrationHazardType("TABLE_REMOVAL"), hazard.Type)
		}
	}
}
