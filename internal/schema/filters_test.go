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

func TestExcludeTables(t *testing.T) {
	fooTable := SchemaQualifiedName{SchemaName: "public", EscapedName: `"foo"`}
	tmpTable := SchemaQualifiedName{SchemaName: "public", EscapedName: `"tmp_bar"`}
	partitionedTable := SchemaQualifiedName{SchemaName: "public", EscapedName: `"tmp_events"`}
	// The partition and sub-partition names do not match the exclude pattern; they must be excluded because their
	// (transitive) parent is excluded.
	partition := SchemaQualifiedName{SchemaName: "public", EscapedName: `"events_p1"`}
	subPartition := SchemaQualifiedName{SchemaName: "public", EscapedName: `"events_p1_sub"`}

	input := Schema{
		// Children are listed before their parents so the fixed-point loop must take multiple passes to exclude the
		// transitive partition chain. This guards against a regression that "optimizes" excludeTables to a single pass.
		Tables: []Table{
			{SchemaQualifiedName: subPartition, ParentTable: &partition},
			{SchemaQualifiedName: partition, ParentTable: &partitionedTable, PartitionKeyDef: "RANGE (id)"},
			{SchemaQualifiedName: partitionedTable, PartitionKeyDef: "RANGE (id)"},
			{SchemaQualifiedName: fooTable},
			{SchemaQualifiedName: tmpTable},
		},
		Indexes: []Index{
			{Name: "foo_idx", OwningRelName: fooTable},
			{Name: "tmp_bar_idx", OwningRelName: tmpTable},
			{Name: "events_p1_idx", OwningRelName: partition},
		},
		ForeignKeyConstraints: []ForeignKeyConstraint{
			{EscapedName: `"foo_fk"`, OwningTable: fooTable, ForeignTable: tmpTable},
			{EscapedName: `"tmp_bar_fk"`, OwningTable: tmpTable, ForeignTable: fooTable},
		},
		Triggers: []Trigger{
			{EscapedName: `"foo_trigger"`, OwningTable: fooTable},
			{EscapedName: `"tmp_bar_trigger"`, OwningTable: tmpTable},
		},
	}

	filter, err := buildExcludeTablesFilter([]string{"tmp_.*"})
	require.NoError(t, err)
	output := excludeTables(input, filter)

	var tableNames []string
	for _, table := range output.Tables {
		tableNames = append(tableNames, table.GetFQEscapedName())
	}
	assert.ElementsMatch(t, []string{fooTable.GetFQEscapedName()}, tableNames)

	var indexNames []string
	for _, idx := range output.Indexes {
		indexNames = append(indexNames, idx.Name)
	}
	assert.ElementsMatch(t, []string{"foo_idx"}, indexNames)

	// The FK owned by the kept table is kept even though it references an excluded table. This is consistent with
	// how cross-schema FKs behave with WithExcludeSchemas.
	var fkNames []string
	for _, fk := range output.ForeignKeyConstraints {
		fkNames = append(fkNames, fk.EscapedName)
	}
	assert.ElementsMatch(t, []string{`"foo_fk"`}, fkNames)

	var triggerNames []string
	for _, trigger := range output.Triggers {
		triggerNames = append(triggerNames, trigger.EscapedName)
	}
	assert.ElementsMatch(t, []string{`"foo_trigger"`}, triggerNames)
}
