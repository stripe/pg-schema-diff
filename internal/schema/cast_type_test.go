package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQualifyTypeForCast(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "integer maps to pg_catalog typname int4",
			input:    "integer",
			expected: "pg_catalog.int4",
		},
		{
			name:     "character varying with typmod maps to varchar",
			input:    "character varying(255)",
			expected: "pg_catalog.varchar(255)",
		},
		{
			name:     "timestamp without time zone maps to timestamp",
			input:    "timestamp without time zone",
			expected: "pg_catalog.timestamp",
		},
		{
			name:     "double precision maps to float8",
			input:    "double precision",
			expected: "pg_catalog.float8",
		},
		{
			name:     "numeric with typmod unchanged typname",
			input:    "numeric(65,10)",
			expected: "pg_catalog.numeric(65,10)",
		},
		{
			name:     "text maps to pg_catalog typname text",
			input:    "text",
			expected: "pg_catalog.text",
		},
		{
			name:     "uuid maps to pg_catalog typname uuid",
			input:    "uuid",
			expected: "pg_catalog.uuid",
		},
		{
			name:     "jsonb maps to pg_catalog typname jsonb",
			input:    "jsonb",
			expected: "pg_catalog.jsonb",
		},
		{
			name:     "user type already schema qualified",
			input:    `"public"."myenum"`,
			expected: `"public"."myenum"`,
		},
		{
			name:     "user type schema qualified without quotes",
			input:    "myschema.mytype",
			expected: "myschema.mytype",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, QualifyTypeForCast(tc.input))
		})
	}
}
