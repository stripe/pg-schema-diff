package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

func TestEnumSQLGenerator_Add_EscapesLabels(t *testing.T) {
	gen := &enumSQLGenerator{}

	for _, tc := range []struct {
		name        string
		labels      []string
		expectedDDL string
	}{
		{
			name:        "simple labels",
			labels:      []string{"red", "green", "blue"},
			expectedDDL: `CREATE TYPE "public"."color" AS ENUM ('red', 'green', 'blue')`,
		},
		{
			name:        "label with single quote",
			labels:      []string{"it's", "fine"},
			expectedDDL: `CREATE TYPE "public"."color" AS ENUM ('it''s', 'fine')`,
		},
		{
			name:        "SQL injection attempt",
			labels:      []string{"x'); DROP TABLE foo; --"},
			expectedDDL: `CREATE TYPE "public"."color" AS ENUM ('x''); DROP TABLE foo; --')`,
		},
		{
			name:        "COPY TO PROGRAM injection attempt",
			labels:      []string{"x'); COPY(SELECT 1)TO PROGRAM 'id'; --"},
			expectedDDL: `CREATE TYPE "public"."color" AS ENUM ('x''); COPY(SELECT 1)TO PROGRAM ''id''; --')`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := gen.Add(schema.Enum{
				SchemaQualifiedName: schema.SchemaQualifiedName{
					SchemaName:  "public",
					EscapedName: `"color"`,
				},
				Labels: tc.labels,
			})
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			assert.Equal(t, tc.expectedDDL, stmts[0].DDL)
		})
	}
}

func TestEnumSQLGenerator_Alter_EscapesLabels(t *testing.T) {
	gen := &enumSQLGenerator{}

	stmts, err := gen.Alter(enumDiff{
		oldAndNew: oldAndNew[schema.Enum]{
			old: schema.Enum{
				SchemaQualifiedName: schema.SchemaQualifiedName{
					SchemaName:  "public",
					EscapedName: `"status"`,
				},
				Labels: []string{"active"},
			},
			new: schema.Enum{
				SchemaQualifiedName: schema.SchemaQualifiedName{
					SchemaName:  "public",
					EscapedName: `"status"`,
				},
				Labels: []string{"active", "it's complicated"},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, `ALTER TYPE "public"."status" ADD VALUE 'it''s complicated'`, stmts[0].DDL)
}

func TestEnumSQLGenerator_Alter_EscapesBEFORE(t *testing.T) {
	gen := &enumSQLGenerator{}

	stmts, err := gen.Alter(enumDiff{
		oldAndNew: oldAndNew[schema.Enum]{
			old: schema.Enum{
				SchemaQualifiedName: schema.SchemaQualifiedName{
					SchemaName:  "public",
					EscapedName: `"status"`,
				},
				Labels: []string{"active", "it's complicated"},
			},
			new: schema.Enum{
				SchemaQualifiedName: schema.SchemaQualifiedName{
					SchemaName:  "public",
					EscapedName: `"status"`,
				},
				Labels: []string{"active", "new'val", "it's complicated"},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, `ALTER TYPE "public"."status" ADD VALUE 'new''val' BEFORE 'it''s complicated'`, stmts[0].DDL)
}
