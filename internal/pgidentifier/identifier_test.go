package pgidentifier

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestRandomIdentifierSuffix(t *testing.T) {
	suffix, err := RandomIdentifierSuffix(bytes.NewReader([]byte{0, 25, 26, 51, 52, 61, 62, 63}), 8)
	require.NoError(t, err)
	assert.Equal(t, "AZaz09$_", suffix)

	suffix, err = RandomIdentifierSuffix(bytes.NewReader(make([]byte, 7)), 8)
	assert.Empty(t, suffix)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	suffix, err = RandomIdentifierSuffix(bytes.NewReader(nil), 0)
	assert.Empty(t, suffix)
	assert.ErrorContains(t, err, "length must be positive")
}
