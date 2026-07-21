package diff_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

func TestPlan_InsertStatement(t *testing.T) {
	statementToInsert := diff.Statement{
		DDL: "some DDL",
		Hazards: []diff.MigrationHazard{
			{Type: diff.MigrationHazardTypeIsUserGenerated, Message: "user-generated"},
			{Type: diff.MigrationHazardTypeAcquiresShareLock, Message: "acquires share lock"},
		},
	}

	for _, tc := range []struct {
		name  string
		plan  diff.Plan
		index int

		expectedPlan        diff.Plan
		expectedErrContains string
	}{
		{
			name: "empty plan",
			plan: diff.Plan{
				Statements:        nil,
				CurrentSchemaHash: "some-hash",
			},
			index: 0,

			expectedPlan: diff.Plan{
				Statements: []diff.Statement{
					statementToInsert,
				},
				CurrentSchemaHash: "some-hash",
			},
		},
		{
			name: "insert at start",
			plan: diff.Plan{
				Statements: []diff.Statement{
					{DDL: "statement 1"},
					{DDL: "statement 2"},
					{DDL: "statement 3"},
				},
				CurrentSchemaHash: "some-hash",
			},
			index: 0,

			expectedPlan: diff.Plan{
				Statements: []diff.Statement{
					statementToInsert,
					{DDL: "statement 1"},
					{DDL: "statement 2"},
					{DDL: "statement 3"},
				},
				CurrentSchemaHash: "some-hash",
			},
		},
		{
			name: "insert after first statement",
			plan: diff.Plan{
				Statements: []diff.Statement{
					{DDL: "statement 1"},
					{DDL: "statement 2"},
					{DDL: "statement 3"},
				},
				CurrentSchemaHash: "some-hash",
			},
			index: 1,

			expectedPlan: diff.Plan{
				Statements: []diff.Statement{
					{DDL: "statement 1"},
					statementToInsert,
					{DDL: "statement 2"},
					{DDL: "statement 3"},
				},
				CurrentSchemaHash: "some-hash",
			},
		},
		{
			name: "insert after last statement statement",
			plan: diff.Plan{
				Statements: []diff.Statement{
					{DDL: "statement 1"},
					{DDL: "statement 2"},
					{DDL: "statement 3"},
				},
				CurrentSchemaHash: "some-hash",
			},
			index: 3,

			expectedPlan: diff.Plan{
				Statements: []diff.Statement{
					{DDL: "statement 1"},
					{DDL: "statement 2"},
					{DDL: "statement 3"},
					statementToInsert,
				},
				CurrentSchemaHash: "some-hash",
			},
		},
		{
			name: "errors on negative index",
			plan: diff.Plan{
				Statements:        nil,
				CurrentSchemaHash: "some-hash",
			},
			index: -1,

			expectedErrContains: "index must be",
		},
		{
			name: "errors on index greater than len",
			plan: diff.Plan{
				Statements:        nil,
				CurrentSchemaHash: "some-hash",
			},
			index: 1,

			expectedErrContains: "index must be",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resultingPlan, err := tc.plan.InsertStatement(tc.index, statementToInsert)
			if len(tc.expectedErrContains) > 0 {
				assert.ErrorContains(t, err, tc.expectedErrContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expectedPlan, resultingPlan)
		})
	}
}

func TestPlan_InsertStatementPreservesCleanupStatements(t *testing.T) {
	cleanupStatements := []diff.Statement{
		{
			DDL: "cleanup DDL",
			Hazards: []diff.MigrationHazard{
				{Type: diff.MigrationHazardTypeDeletesData, Message: "deletes data"},
			},
		},
	}
	plan := diff.Plan{
		Statements:        []diff.Statement{{DDL: "ordinary DDL"}},
		CleanupStatements: cleanupStatements,
		CurrentSchemaHash: "some-hash",
	}

	resultingPlan, err := plan.InsertStatement(0, diff.Statement{DDL: "inserted DDL"})
	require.NoError(t, err)
	assert.Equal(t, []diff.Statement{
		{DDL: "inserted DDL"},
		{DDL: "ordinary DDL"},
	}, resultingPlan.Statements)
	assert.Equal(t, cleanupStatements, resultingPlan.CleanupStatements)
}

func TestPlanJSONRoundTripWithCleanupStatements(t *testing.T) {
	plan := diff.Plan{
		Statements: []diff.Statement{
			{DDL: "ALTER TABLE accounts ADD COLUMN active boolean"},
		},
		CleanupStatements: []diff.Statement{
			{
				DDL: "DROP TABLE archived_accounts",
				Hazards: []diff.MigrationHazard{
					{Type: diff.MigrationHazardTypeDeletesData, Message: "deletes archived data"},
				},
			},
		},
		CurrentSchemaHash: "some-hash",
	}

	encoded, err := json.Marshal(plan)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"cleanup_statements"`)

	var decoded diff.Plan
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, plan, decoded)
}

func TestPlanJSONOmitsEmptyCleanupStatements(t *testing.T) {
	plan := diff.Plan{
		Statements:        []diff.Statement{},
		CleanupStatements: []diff.Statement{},
		CurrentSchemaHash: "some-hash",
	}

	encoded, err := json.Marshal(plan)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"statements": [],
		"current_schema_hash": "some-hash"
	}`, string(encoded))
}
