package diff

import (
	"testing"
)

func TestGetTypeCoercionInfo(t *testing.T) {
	tests := []struct {
		name                  string
		oldType              string
		newType              string
		expectedCoercible    bool
		expectedBinaryCoercible bool
	}{
		// Same types
		{
			name:                    "same type",
			oldType:                 "integer",
			newType:                 "integer",
			expectedCoercible:       true,
			expectedBinaryCoercible: true,
		},
		// Type aliases
		{
			name:                    "int to integer alias",
			oldType:                 "int",
			newType:                 "integer",
			expectedCoercible:       true,
			expectedBinaryCoercible: true,
		},
		// Binary coercible conversions
		{
			name:                    "smallint to integer",
			oldType:                 "smallint",
			newType:                 "integer",
			expectedCoercible:       true,
			expectedBinaryCoercible: true,
		},
		{
			name:                    "integer to bigint",
			oldType:                 "integer",
			newType:                 "bigint",
			expectedCoercible:       true,
			expectedBinaryCoercible: true,
		},
		{
			name:                    "varchar to text",
			oldType:                 "varchar",
			newType:                 "text",
			expectedCoercible:       true,
			expectedBinaryCoercible: true,
		},
		// Coercible but requires rewrite
		{
			name:                    "bigint to integer (downsize)",
			oldType:                 "bigint",
			newType:                 "integer",
			expectedCoercible:       true,
			expectedBinaryCoercible: false,
		},
		{
			name:                    "text to integer",
			oldType:                 "text",
			newType:                 "integer",
			expectedCoercible:       true,
			expectedBinaryCoercible: false,
		},
		// Incompatible conversions (the main issue #179 case)
		{
			name:                    "integer to timestamp",
			oldType:                 "integer",
			newType:                 "timestamp",
			expectedCoercible:       false,
			expectedBinaryCoercible: false,
		},
		{
			name:                    "integer to timestamptz",
			oldType:                 "integer",
			newType:                 "timestamptz",
			expectedCoercible:       false,
			expectedBinaryCoercible: false,
		},
		{
			name:                    "timestamp to integer",
			oldType:                 "timestamp",
			newType:                 "integer",
			expectedCoercible:       false,
			expectedBinaryCoercible: false,
		},
		{
			name:                    "text to bytea",
			oldType:                 "text",
			newType:                 "bytea",
			expectedCoercible:       false,
			expectedBinaryCoercible: false,
		},
		{
			name:                    "boolean to integer",
			oldType:                 "boolean",
			newType:                 "integer",
			expectedCoercible:       false,
			expectedBinaryCoercible: false,
		},
		// Special case: bigint to timestamp (should be coercible with special handling)
		{
			name:                    "bigint to timestamp",
			oldType:                 "bigint",
			newType:                 "timestamp",
			expectedCoercible:       true,
			expectedBinaryCoercible: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := GetTypeCoercionInfo(tt.oldType, tt.newType)
			
			if info.IsCoercible != tt.expectedCoercible {
				t.Errorf("GetTypeCoercionInfo(%q, %q).IsCoercible = %v, want %v", 
					tt.oldType, tt.newType, info.IsCoercible, tt.expectedCoercible)
			}
			
			if info.IsBinaryCoercible != tt.expectedBinaryCoercible {
				t.Errorf("GetTypeCoercionInfo(%q, %q).IsBinaryCoercible = %v, want %v", 
					tt.oldType, tt.newType, info.IsBinaryCoercible, tt.expectedBinaryCoercible)
			}
		})
	}
}

func TestIsTypeConversionCompatible(t *testing.T) {
	tests := []struct {
		name     string
		oldType  string
		newType  string
		expected bool
	}{
		{
			name:     "compatible types",
			oldType:  "integer",
			newType:  "bigint",
			expected: true,
		},
		{
			name:     "incompatible types - integer to timestamp",
			oldType:  "integer",
			newType:  "timestamp",
			expected: false,
		},
		{
			name:     "incompatible types - timestamp to integer",
			oldType:  "timestamp",
			newType:  "integer",
			expected: false,
		},
		{
			name:     "special case - bigint to timestamp",
			oldType:  "bigint",
			newType:  "timestamp",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsTypeConversionCompatible(tt.oldType, tt.newType)
			if result != tt.expected {
				t.Errorf("IsTypeConversionCompatible(%q, %q) = %v, want %v", 
					tt.oldType, tt.newType, result, tt.expected)
			}
		})
	}
}