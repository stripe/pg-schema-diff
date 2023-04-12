package diff_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

func TestPlan_ApplyStatementTimeoutModifier(t *testing.T) {
	for _, tc := range []struct {
		name         string
		regex        string
		timeout      time.Duration
		plan         diff.Plan
		expectedPlan diff.Plan
	}{
		{
			name:    "no matches",
			regex:   "^.*x.*y$",
			timeout: 2 * time.Hour,
			plan: diff.Plan{
				Statements: []diff.Statement{
					{
						DDL:     "does-not-match-1",
						Timeout: 3 * time.Second,
					},
					{
						DDL:     "does-not-match-2",
						Timeout: time.Second,
					},
					{
						DDL:     "does-not-match-3",
						Timeout: 2 * time.Second,
					},
				},
				CurrentSchemaHash: "some-hash",
			},
			expectedPlan: diff.Plan{
				Statements: []diff.Statement{
					{
						DDL:     "does-not-match-1",
						Timeout: 3 * time.Second,
					},
					{
						DDL:     "does-not-match-2",
						Timeout: time.Second,
					},
					{
						DDL:     "does-not-match-3",
						Timeout: 2 * time.Second,
					},
				},
				CurrentSchemaHash: "some-hash",
			},
		},
		{
			name:    "some match",
			regex:   "^.*x.*y$",
			timeout: 2 * time.Hour,
			plan: diff.Plan{
				Statements: []diff.Statement{
					{
						DDL:     "some-letters-than-an-x--end-in-y",
						Timeout: 3 * time.Second,
					},
					{
						DDL:     "does-not-match",
						Timeout: time.Second,
					},
					{
						DDL:     "other-letters xerox but ends in finally",
						Timeout: 2 * time.Second,
					},
				},
				CurrentSchemaHash: "some-hash",
			},
			expectedPlan: diff.Plan{
				Statements: []diff.Statement{
					{
						DDL:     "some-letters-than-an-x--end-in-y",
						Timeout: 2 * time.Hour,
					},
					{
						DDL:     "does-not-match",
						Timeout: time.Second,
					},
					{
						DDL:     "other-letters xerox but ends in finally",
						Timeout: 2 * time.Hour,
					},
				},
				CurrentSchemaHash: "some-hash",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			regex := regexp.MustCompile(tc.regex)
			resultingPlan := tc.plan.ApplyStatementTimeoutModifier(regex, tc.timeout)
			assert.Equal(t, tc.expectedPlan, resultingPlan)
		})
	}
}

func TestPlan_InsertStatement(t *testing.T) {
	var statementToInsert = diff.Statement{
		DDL:     "some DDL",
		Timeout: 3 * time.Second,
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
					{DDL: "statement 1", Timeout: time.Second},
					{DDL: "statement 2", Timeout: 2 * time.Second},
					{DDL: "statement 3", Timeout: 3 * time.Second},
				},
				CurrentSchemaHash: "some-hash",
			},
			index: 0,

			expectedPlan: diff.Plan{
				Statements: []diff.Statement{
					statementToInsert,
					{DDL: "statement 1", Timeout: time.Second},
					{DDL: "statement 2", Timeout: 2 * time.Second},
					{DDL: "statement 3", Timeout: 3 * time.Second},
				},
				CurrentSchemaHash: "some-hash",
			},
		},
		{
			name: "insert after first statement",
			plan: diff.Plan{
				Statements: []diff.Statement{
					{DDL: "statement 1", Timeout: time.Second},
					{DDL: "statement 2", Timeout: 2 * time.Second},
					{DDL: "statement 3", Timeout: 3 * time.Second},
				},
				CurrentSchemaHash: "some-hash",
			},
			index: 1,

			expectedPlan: diff.Plan{
				Statements: []diff.Statement{

					{DDL: "statement 1", Timeout: time.Second},
					statementToInsert,
					{DDL: "statement 2", Timeout: 2 * time.Second},
					{DDL: "statement 3", Timeout: 3 * time.Second},
				},
				CurrentSchemaHash: "some-hash",
			},
		},
		{
			name: "insert after last statement statement",
			plan: diff.Plan{
				Statements: []diff.Statement{
					{DDL: "statement 1", Timeout: time.Second},
					{DDL: "statement 2", Timeout: 2 * time.Second},
					{DDL: "statement 3", Timeout: 3 * time.Second},
				},
				CurrentSchemaHash: "some-hash",
			},
			index: 3,

			expectedPlan: diff.Plan{
				Statements: []diff.Statement{
					{DDL: "statement 1", Timeout: time.Second},
					{DDL: "statement 2", Timeout: 2 * time.Second},
					{DDL: "statement 3", Timeout: 3 * time.Second},
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
