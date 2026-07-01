package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEscapeLiteral(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple value",
			input:    "red",
			expected: "'red'",
		},
		{
			name:     "value with single quote",
			input:    "it's",
			expected: "'it''s'",
		},
		{
			name:     "value with multiple single quotes",
			input:    "it's a 'test'",
			expected: "'it''s a ''test'''",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "''",
		},
		{
			name:     "value with backslash",
			input:    `back\slash`,
			expected: `'back\slash'`,
		},
		{
			name:     "value with null byte stripped",
			input:    "null\x00byte",
			expected: "'nullbyte'",
		},
		{
			name:     "SQL injection attempt via single quote",
			input:    "x'); DROP TABLE foo; --",
			expected: "'x''); DROP TABLE foo; --'",
		},
		{
			name:     "COPY TO PROGRAM injection attempt",
			input:    "x'); COPY(SELECT 1)TO PROGRAM 'id>/tmp/pwned'; --",
			expected: "'x''); COPY(SELECT 1)TO PROGRAM ''id>/tmp/pwned''; --'",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, EscapeLiteral(tc.input))
		})
	}
}
