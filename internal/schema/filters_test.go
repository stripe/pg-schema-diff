package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestUnescapeIdentifier(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    string
		expected string
	}{
		{name: "quoted", input: `"foobar"`, expected: "foobar"},
		{name: "quoted with inner quotes", input: `"foo""bar"`, expected: `foo"bar`},
		{name: "unquoted", input: "foobar", expected: "foobar"},
		{name: "empty", input: "", expected: ""},
		{name: "single quote char", input: `"`, expected: `"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, unescapeIdentifier(tc.input))
		})
	}
}

func TestBuildExcludeTablesFilter(t *testing.T) {
	for _, tc := range []struct {
		name     string
		patterns []string
		input    SchemaQualifiedName
		// expectedKeep is whether the filter should keep (true) or exclude (false) the input.
		expectedKeep bool
	}{
		{
			name:         "bare name match",
			patterns:     []string{"tmp_.*"},
			input:        SchemaQualifiedName{SchemaName: "public", EscapedName: `"tmp_foo"`},
			expectedKeep: false,
		},
		{
			name:         "bare name match in non-public schema",
			patterns:     []string{"tmp_.*"},
			input:        SchemaQualifiedName{SchemaName: "schema_1", EscapedName: `"tmp_foo"`},
			expectedKeep: false,
		},
		{
			name:         "no match",
			patterns:     []string{"tmp_.*"},
			input:        SchemaQualifiedName{SchemaName: "public", EscapedName: `"foobar"`},
			expectedKeep: true,
		},
		{
			name:         "anchored: no substring match",
			patterns:     []string{"tmp_.*"},
			input:        SchemaQualifiedName{SchemaName: "public", EscapedName: `"my_tmp_foo"`},
			expectedKeep: true,
		},
		{
			name:         "anchored: plain name does not match as prefix",
			patterns:     []string{"users"},
			input:        SchemaQualifiedName{SchemaName: "public", EscapedName: `"users_audit"`},
			expectedKeep: true,
		},
		{
			name:         "qualified name match",
			patterns:     []string{`schema_1\.tmp_.*`},
			input:        SchemaQualifiedName{SchemaName: "schema_1", EscapedName: `"tmp_foo"`},
			expectedKeep: false,
		},
		{
			name:         "qualified pattern does not match other schemas",
			patterns:     []string{`schema_1\.tmp_.*`},
			input:        SchemaQualifiedName{SchemaName: "public", EscapedName: `"tmp_foo"`},
			expectedKeep: true,
		},
		{
			name:         "multiple patterns",
			patterns:     []string{"foo", "bar"},
			input:        SchemaQualifiedName{SchemaName: "public", EscapedName: `"bar"`},
			expectedKeep: false,
		},
		{
			name:         "identifier with special characters",
			patterns:     []string{"some table"},
			input:        SchemaQualifiedName{SchemaName: "public", EscapedName: `"some table"`},
			expectedKeep: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter, err := buildExcludeTablesFilter(tc.patterns)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedKeep, filter(tc.input))
		})
	}
}

func TestBuildExcludeTablesFilterInvalidPattern(t *testing.T) {
	_, err := buildExcludeTablesFilter([]string{"["})
	require.ErrorContains(t, err, "compiling exclude table pattern")
}

func TestBuildExcludeTablesFilterEmpty(t *testing.T) {
	filter, err := buildExcludeTablesFilter(nil)
	require.NoError(t, err)
	require.Nil(t, filter)
}
