package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

func TestEscapeRoleNames(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "PUBLIC is not escaped",
			input:    []string{"PUBLIC"},
			expected: []string{"PUBLIC"},
		},
		{
			name:     "simple role is escaped",
			input:    []string{"my_role"},
			expected: []string{`"my_role"`},
		},
		{
			name:     "mixed PUBLIC and regular roles",
			input:    []string{"PUBLIC", "role_1", "role_2"},
			expected: []string{"PUBLIC", `"role_1"`, `"role_2"`},
		},
		{
			name:     "role with embedded double quote",
			input:    []string{`evil"role`},
			expected: []string{`"evil""role"`},
		},
		{
			name:     "injection attempt via role name",
			input:    []string{`x"; DROP TABLE foo; --`},
			expected: []string{`"x""; DROP TABLE foo; --"`},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, escapeRoleNames(tc.input))
		})
	}
}

func TestPolicySQLGenerator_Add_EscapesRoleNames(t *testing.T) {
	table := schema.Table{
		SchemaQualifiedName: schema.SchemaQualifiedName{
			SchemaName:  "public",
			EscapedName: `"foo"`,
		},
	}
	psg, err := newPolicySQLVertexGenerator(nil, table)
	require.NoError(t, err)

	for _, tc := range []struct {
		name        string
		appliesTo   []string
		expectedSub string
	}{
		{
			name:        "PUBLIC unquoted",
			appliesTo:   []string{"PUBLIC"},
			expectedSub: "\n\tTO PUBLIC",
		},
		{
			name:        "regular role quoted",
			appliesTo:   []string{"my_role"},
			expectedSub: "\n\tTO \"my_role\"",
		},
		{
			name:        "role with embedded double quote",
			appliesTo:   []string{`evil"role`},
			expectedSub: "\n\tTO \"evil\"\"role\"",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			graph, err := psg.Add(schema.Policy{
				EscapedName:  `"test_policy"`,
				IsPermissive: true,
				AppliesTo:    tc.appliesTo,
				Cmd:          schema.AllPolicyCmd,
			})
			require.NoError(t, err)
			require.NotEmpty(t, graph.vertices)
			require.NotEmpty(t, graph.vertices[0].statements)
			ddl := graph.vertices[0].statements[0].DDL
			assert.Contains(t, ddl, tc.expectedSub)
		})
	}
}
