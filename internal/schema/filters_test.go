package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type (
	fakeNameFilterMock struct {
		expectedInput SchemaQualifiedName
		returnValue   bool
	}
	fakeNameFilter struct {
		t    *testing.T
		mock fakeNameFilterMock
	}
)

func newFakeNameFilter(t *testing.T, mock fakeNameFilterMock) fakeNameFilter {
	return fakeNameFilter{
		t:    t,
		mock: mock,
	}
}

func (f fakeNameFilter) filter(input SchemaQualifiedName) bool {
	assert.Equal(f.t, f.mock.expectedInput, input)
	return f.mock.returnValue
}

func TestOrNameFilters(t *testing.T) {
	someName1 := SchemaQualifiedName{
		SchemaName:  "some_schema",
		EscapedName: "some_name",
	}
	for _, tc := range []struct {
		name        string
		input       SchemaQualifiedName
		filters     []fakeNameFilterMock
		expectedOut bool
	}{
		{
			name:        "empty",
			input:       someName1,
			expectedOut: false,
		},
		{
			name:  "one filter (true)",
			input: someName1,
			filters: []fakeNameFilterMock{
				{
					expectedInput: someName1,
					returnValue:   true,
				},
			},
			expectedOut: true,
		},
		{
			name:  "one filter (false)",
			input: someName1,
			filters: []fakeNameFilterMock{
				{expectedInput: someName1, returnValue: false},
			},
			expectedOut: false,
		},
		{
			name:  "two filters (false, true)",
			input: someName1,
			filters: []fakeNameFilterMock{
				{expectedInput: someName1, returnValue: false},
				{expectedInput: someName1, returnValue: true},
			},
			expectedOut: true,
		},
		{
			name:  "two filters (false, false)",
			input: someName1,
			filters: []fakeNameFilterMock{
				{expectedInput: someName1, returnValue: false},
				{expectedInput: someName1, returnValue: false},
			},
			expectedOut: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var filters []nameFilter
			for _, filter := range tc.filters {
				filters = append(filters, newFakeNameFilter(t, filter).filter)
			}
			assert.Equal(t, tc.expectedOut, orNameFilter(filters...)(tc.input))
		})
	}
}

func TestAndNameFilters(t *testing.T) {
	someName1 := SchemaQualifiedName{
		SchemaName:  "some_schema",
		EscapedName: "some_name",
	}
	for _, tc := range []struct {
		name        string
		input       SchemaQualifiedName
		filters     []fakeNameFilterMock
		expectedOut bool
	}{
		{
			name:        "empty",
			input:       someName1,
			expectedOut: false,
		},
		{
			name:  "one filter (true)",
			input: someName1,
			filters: []fakeNameFilterMock{
				{
					expectedInput: someName1,
					returnValue:   true,
				},
			},
			expectedOut: true,
		},
		{
			name:  "one filter (false)",
			input: someName1,
			filters: []fakeNameFilterMock{
				{expectedInput: someName1, returnValue: false},
			},
			expectedOut: false,
		},
		{
			name:  "two filters (false, true)",
			input: someName1,
			filters: []fakeNameFilterMock{
				{expectedInput: someName1, returnValue: false},
				{expectedInput: someName1, returnValue: true},
			},
			expectedOut: false,
		},
		{
			name:  "two filters (true, true)",
			input: someName1,
			filters: []fakeNameFilterMock{
				{expectedInput: someName1, returnValue: true},
				{expectedInput: someName1, returnValue: true},
			},
			expectedOut: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var filters []nameFilter
			for _, filter := range tc.filters {
				filters = append(filters, newFakeNameFilter(t, filter).filter)
			}
			assert.Equal(t, tc.expectedOut, andNameFilter(filters...)(tc.input))
		})
	}
}

func TestSchemaNamePatternFilters(t *testing.T) {
	for _, tc := range []struct {
		name        string
		pattern     string
		schemaName  string
		exclude     bool
		expectedOut bool
	}{
		{
			name:        "matching expression",
			pattern:     "pgschemadiff_archive.*",
			schemaName:  "pgschemadiff_archive_public_foo",
			expectedOut: true,
		},
		{
			name:        "full name anchoring",
			pattern:     "public",
			schemaName:  "not_public",
			expectedOut: false,
		},
		{
			name:        "exclude matching expression",
			pattern:     "pgschemadiff_archive.*",
			schemaName:  "pgschemadiff_archive_public_foo",
			exclude:     true,
			expectedOut: false,
		},
		{
			name:        "exclude non-matching expression",
			pattern:     "pgschemadiff_archive.*",
			schemaName:  "application",
			exclude:     true,
			expectedOut: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pattern, err := compileSchemaNamePattern(tc.pattern)
			assert.NoError(t, err)
			filter := schemaNamePatternFilter(pattern)
			if tc.exclude {
				filter = notSchemaNamePatternFilter(pattern)
			}
			assert.Equal(t, tc.expectedOut, filter(SchemaQualifiedName{
				SchemaName: tc.schemaName,
			}))
		})
	}
}
