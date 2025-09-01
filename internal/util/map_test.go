package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeys(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]int
		expected []string
	}{
		{
			name:     "empty map",
			input:    map[string]int{},
			expected: []string{},
		},
		{
			name:     "multiple keys",
			input:    map[string]int{"foo": 1, "bar": 2, "baz": 3},
			expected: []string{"bar", "baz", "foo"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Keys(tt.input)
			// Sort both slices since map iteration order is not guaranteed
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}
