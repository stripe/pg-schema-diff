package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stripe/pg-schema-diff/internal/schema"
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

func TestBuildColumnDefinition(t *testing.T) {
	for _, tc := range []struct {
		name     string
		column   schema.Column
		expected string
	}{
		{
			name: "Regular column with default",
			column: schema.Column{
				Name:       "name",
				Type:       "text",
				Default:    "'default value'",
				IsNullable: true,
			},
			expected: `"name" text DEFAULT 'default value'`,
		},
		{
			name: "Generated column",
			column: schema.Column{
				Name:                 "search_vector",
				Type:                 "tsvector",
				IsGenerated:          true,
				GenerationExpression: "to_tsvector('simple', title || ' ' || coalesce(artist, ''))",
				IsNullable:           true,
			},
			expected: `"search_vector" tsvector GENERATED ALWAYS AS (to_tsvector('simple', title || ' ' || coalesce(artist, ''))) STORED`,
		},
		{
			name: "Generated column with NOT NULL",
			column: schema.Column{
				Name:                 "price_with_tax",
				Type:                 "numeric(10,2)",
				IsGenerated:          true,
				GenerationExpression: "price * 1.1",
				IsNullable:           false,
			},
			expected: `"price_with_tax" numeric(10,2) GENERATED ALWAYS AS (price * 1.1) STORED NOT NULL`,
		},
		{
			name: "Regular column with NOT NULL",
			column: schema.Column{
				Name:       "email",
				Type:       "text",
				IsNullable: false,
			},
			expected: `"email" text NOT NULL`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := buildColumnDefinition(tc.column)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}
