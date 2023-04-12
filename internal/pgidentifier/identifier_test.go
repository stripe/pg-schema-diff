package pgidentifier

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_IsSimpleIdentifier(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "starts with letter",
			input:    "foo",
			expected: true,
		},
		{
			name:     "starts with underscore",
			input:    "_foo",
			expected: true,
		},
		{
			name:     "start with number",
			input:    "1foo",
			expected: false,
		},
		{
			name:     "contains all possible characters",
			input:    "some_1119$_",
			expected: true,
		},
		{
			name:     "empty",
			input:    "",
			expected: false,
		},
		{
			name:     "contains upper case letter",
			input:    "fooBar",
			expected: false,
		},
		{
			name:     "contains spaces",
			input:    "foo bar",
			expected: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := IsSimpleIdentifier(tc.input)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
