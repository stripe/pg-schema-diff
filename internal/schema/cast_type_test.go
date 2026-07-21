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
			name:     "integer builtin",
			input:    "integer",
			expected: "pg_catalog.integer",
		},
		{
			name:     "character varying with typmod",
			input:    "character varying(255)",
			expected: `"pg_catalog"."character varying"(255)`,
		},
		{
			name:     "timestamp without time zone",
			input:    "timestamp without time zone",
			expected: `"pg_catalog"."timestamp without time zone"`,
		},
		{
			name:     "double precision",
			input:    "double precision",
			expected: `"pg_catalog"."double precision"`,
		},
		{
			name:     "numeric with typmod",
			input:    "numeric(65,10)",
			expected: "pg_catalog.numeric(65,10)",
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
