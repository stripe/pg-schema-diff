package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsNotNullCCRegex(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    string
		expected bool
	}{
		{name: "Matching simple string without space that is not null", input: `foo IS NOT NULL`, expected: true},
		{name: "Matching quotes string", input: `"foo bar" IS NOT NULL`, expected: true},
		{name: "Matching string with open and closed parenthesis", input: `("foo" IS NOT NULL)`, expected: true},
		{name: "Not matching on multi-part check constraint", input: `foo IS NOT NULL AND LENGTH(foo) > 0`, expected: false},
		{name: "Not matching on other check constraint", input: `foo IS NULL`, expected: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := isNotNullCCRegex.MatchString(tc.input)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
