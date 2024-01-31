package diff

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

func TestTransformDiffDataPackNewTables(t *testing.T) {
	tcs := []transformDiffTestCase{
		{
			name: "No new tables",
			in: schemaDiff{
				oldAndNew:     oldAndNew[schema.Schema]{},
				tableDiffs:    listDiff[schema.Table, tableDiff]{},
				indexDiffs:    listDiff[schema.Index, indexDiff]{},
				functionDiffs: listDiff[schema.Function, functionDiff]{},
				triggerDiffs:  listDiff[schema.Trigger, triggerDiff]{},
			},
			expectedOut: schemaDiff{
				oldAndNew:     oldAndNew[schema.Schema]{},
				tableDiffs:    listDiff[schema.Table, tableDiff]{},
				indexDiffs:    listDiff[schema.Index, indexDiff]{},
				functionDiffs: listDiff[schema.Function, functionDiff]{},
				triggerDiffs:  listDiff[schema.Trigger, triggerDiff]{},
			},
		},
		{
			name: "New table with no columns",
			in: schemaDiff{
				oldAndNew: oldAndNew[schema.Schema]{},
				tableDiffs: listDiff[schema.Table, tableDiff]{
					adds: []schema.Table{
						{
							Columns:          nil,
							CheckConstraints: nil,
						},
					},
				},
				indexDiffs:    listDiff[schema.Index, indexDiff]{},
				functionDiffs: listDiff[schema.Function, functionDiff]{},
				triggerDiffs:  listDiff[schema.Trigger, triggerDiff]{},
			},
			expectedOut: schemaDiff{
				oldAndNew: oldAndNew[schema.Schema]{},
				tableDiffs: listDiff[schema.Table, tableDiff]{
					adds: []schema.Table{
						{
							Columns:          nil,
							CheckConstraints: nil,
						},
					},
				},
				indexDiffs:    listDiff[schema.Index, indexDiff]{},
				functionDiffs: listDiff[schema.Function, functionDiff]{},
				triggerDiffs:  listDiff[schema.Trigger, triggerDiff]{},
			},
		},
		{
			name: "Standard",
			in: schemaDiff{
				oldAndNew: oldAndNew[schema.Schema]{},
				tableDiffs: listDiff[schema.Table, tableDiff]{
					alters: []tableDiff{
						buildTableDiffWithColDiffs("foo", []columnDiff{
							buildColumnDiff(schema.Column{Name: "genre", Type: "some type", Size: 3}, 0, 0),
							buildColumnDiff(schema.Column{Name: "content", Type: "some type", Size: 2}, 1, 1),
							buildColumnDiff(schema.Column{Name: "title", Type: "some type", Size: 10}, 2, 2),
						}),
					},
					adds: []schema.Table{
						{
							Columns: []schema.Column{
								{Name: "coffee", Type: "some type", Size: 3},
								{Name: "mocha", Type: "some type", Size: 2},
								{Name: "latte", Type: "some type", Size: 10},
							},
							CheckConstraints: nil,
						},
						{
							Columns: []schema.Column{
								{Name: "dog", Type: "some type", Size: 1},
								{Name: "cat", Type: "some type", Size: 2},
								{Name: "rabbit", Type: "some type", Size: 3},
							},
							CheckConstraints: nil,
						},
					},
					deletes: []schema.Table{
						{
							Columns: []schema.Column{
								{Name: "croissant", Type: "some type", Size: 3},
								{Name: "bagel", Type: "some type", Size: 2},
								{Name: "pastry", Type: "some type", Size: 10},
							},
							CheckConstraints: nil,
						},
					},
				},
				indexDiffs:    listDiff[schema.Index, indexDiff]{},
				functionDiffs: listDiff[schema.Function, functionDiff]{},
				triggerDiffs:  listDiff[schema.Trigger, triggerDiff]{},
			},
			expectedOut: schemaDiff{
				oldAndNew: oldAndNew[schema.Schema]{},
				tableDiffs: listDiff[schema.Table, tableDiff]{
					alters: []tableDiff{
						buildTableDiffWithColDiffs("foo", []columnDiff{
							buildColumnDiff(schema.Column{Name: "genre", Type: "some type", Size: 3}, 0, 0),
							buildColumnDiff(schema.Column{Name: "content", Type: "some type", Size: 2}, 1, 1),
							buildColumnDiff(schema.Column{Name: "title", Type: "some type", Size: 10}, 2, 2),
						}),
					},
					adds: []schema.Table{
						{
							Columns: []schema.Column{
								{Name: "latte", Type: "some type", Size: 10},
								{Name: "coffee", Type: "some type", Size: 3},
								{Name: "mocha", Type: "some type", Size: 2},
							},
							CheckConstraints: nil,
						},
						{
							Columns: []schema.Column{
								{Name: "rabbit", Type: "some type", Size: 3},
								{Name: "cat", Type: "some type", Size: 2},
								{Name: "dog", Type: "some type", Size: 1},
							},
							CheckConstraints: nil,
						},
					},
					deletes: []schema.Table{
						{
							Columns: []schema.Column{
								{Name: "croissant", Type: "some type", Size: 3},
								{Name: "bagel", Type: "some type", Size: 2},
								{Name: "pastry", Type: "some type", Size: 10},
							},
							CheckConstraints: nil,
						},
					},
				},
				indexDiffs:    listDiff[schema.Index, indexDiff]{},
				functionDiffs: listDiff[schema.Function, functionDiff]{},
				triggerDiffs:  listDiff[schema.Trigger, triggerDiff]{},
			},
		},
	}
	runTestCases(t, dataPackNewTables, tcs)
}

func TestTransformDiffRemoveChangesToColumnOrdering(t *testing.T) {
	tcs := []transformDiffTestCase{
		{
			name: "No altered tables",
			in: schemaDiff{
				oldAndNew:     oldAndNew[schema.Schema]{},
				tableDiffs:    listDiff[schema.Table, tableDiff]{},
				indexDiffs:    listDiff[schema.Index, indexDiff]{},
				functionDiffs: listDiff[schema.Function, functionDiff]{},
				triggerDiffs:  listDiff[schema.Trigger, triggerDiff]{},
			},
			expectedOut: schemaDiff{
				oldAndNew:     oldAndNew[schema.Schema]{},
				tableDiffs:    listDiff[schema.Table, tableDiff]{},
				indexDiffs:    listDiff[schema.Index, indexDiff]{},
				functionDiffs: listDiff[schema.Function, functionDiff]{},
				triggerDiffs:  listDiff[schema.Trigger, triggerDiff]{},
			},
		},
		{
			name: "Altered table with no columns",
			in: schemaDiff{
				oldAndNew: oldAndNew[schema.Schema]{},
				tableDiffs: listDiff[schema.Table, tableDiff]{
					alters: []tableDiff{buildTableDiffWithColDiffs("foobar", nil)},
				},
				indexDiffs:    listDiff[schema.Index, indexDiff]{},
				functionDiffs: listDiff[schema.Function, functionDiff]{},
				triggerDiffs:  listDiff[schema.Trigger, triggerDiff]{},
			},
			expectedOut: schemaDiff{
				oldAndNew: oldAndNew[schema.Schema]{},
				tableDiffs: listDiff[schema.Table, tableDiff]{
					alters: []tableDiff{buildTableDiffWithColDiffs("foobar", nil)},
				},
				indexDiffs:    listDiff[schema.Index, indexDiff]{},
				functionDiffs: listDiff[schema.Function, functionDiff]{},
				triggerDiffs:  listDiff[schema.Trigger, triggerDiff]{},
			},
		},
		{
			name: "Standard",
			in: schemaDiff{
				oldAndNew: oldAndNew[schema.Schema]{},
				tableDiffs: listDiff[schema.Table, tableDiff]{
					alters: []tableDiff{
						buildTableDiffWithColDiffs("foo", []columnDiff{
							buildColumnDiff(schema.Column{Name: "genre", Type: "some type", Size: 3}, 0, 1),
							buildColumnDiff(schema.Column{Name: "content", Type: "some type", Size: 2}, 1, 2),
							buildColumnDiff(schema.Column{Name: "title", Type: "some type", Size: 10}, 2, 0),
						}),
						buildTableDiffWithColDiffs("bar", []columnDiff{
							buildColumnDiff(schema.Column{Name: "item", Type: "some type", Size: 3}, 0, 2),
							buildColumnDiff(schema.Column{Name: "type", Type: "some type", Size: 2}, 1, 1),
							buildColumnDiff(schema.Column{Name: "color", Type: "some type", Size: 10}, 2, 0),
						}),
					},
					adds: []schema.Table{
						{
							Columns: []schema.Column{
								{Name: "cold brew", Type: "some type", Size: 2},
								{Name: "coffee", Type: "some type", Size: 3},
								{Name: "mocha", Type: "some type", Size: 2},
								{Name: "latte", Type: "some type", Size: 10},
							},
							CheckConstraints: nil,
						},
					},
					deletes: []schema.Table{
						{
							Columns: []schema.Column{
								{Name: "croissant", Type: "some type", Size: 3},
								{Name: "bagel", Type: "some type", Size: 2},
								{Name: "pastry", Type: "some type", Size: 10},
							},
							CheckConstraints: nil,
						},
					},
				},
				indexDiffs:    listDiff[schema.Index, indexDiff]{},
				functionDiffs: listDiff[schema.Function, functionDiff]{},
				triggerDiffs:  listDiff[schema.Trigger, triggerDiff]{},
			},
			expectedOut: schemaDiff{
				oldAndNew: oldAndNew[schema.Schema]{},
				tableDiffs: listDiff[schema.Table, tableDiff]{
					alters: []tableDiff{
						buildTableDiffWithColDiffs("foo", []columnDiff{
							buildColumnDiff(schema.Column{Name: "genre", Type: "some type", Size: 3}, 1, 1),
							buildColumnDiff(schema.Column{Name: "content", Type: "some type", Size: 2}, 2, 2),
							buildColumnDiff(schema.Column{Name: "title", Type: "some type", Size: 10}, 0, 0),
						}),
						buildTableDiffWithColDiffs("bar", []columnDiff{
							buildColumnDiff(schema.Column{Name: "item", Type: "some type", Size: 3}, 2, 2),
							buildColumnDiff(schema.Column{Name: "type", Type: "some type", Size: 2}, 1, 1),
							buildColumnDiff(schema.Column{Name: "color", Type: "some type", Size: 10}, 0, 0),
						}),
					},
					adds: []schema.Table{
						{
							Columns: []schema.Column{
								{Name: "cold brew", Type: "some type", Size: 2},
								{Name: "coffee", Type: "some type", Size: 3},
								{Name: "mocha", Type: "some type", Size: 2},
								{Name: "latte", Type: "some type", Size: 10},
							},
							CheckConstraints: nil,
						},
					},
					deletes: []schema.Table{
						{
							Columns: []schema.Column{
								{Name: "croissant", Type: "some type", Size: 3},
								{Name: "bagel", Type: "some type", Size: 2},
								{Name: "pastry", Type: "some type", Size: 10},
							},
							CheckConstraints: nil,
						},
					},
				},
				indexDiffs:    listDiff[schema.Index, indexDiff]{},
				functionDiffs: listDiff[schema.Function, functionDiff]{},
				triggerDiffs:  listDiff[schema.Trigger, triggerDiff]{},
			},
		},
	}
	runTestCases(t, removeChangesToColumnOrdering, tcs)
}

type transformDiffTestCase struct {
	name        string
	in          schemaDiff
	expectedOut schemaDiff
}

func runTestCases(t *testing.T, transform func(diff schemaDiff) schemaDiff, tcs []transformDiffTestCase) {
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedOut, transform(tc.in))
		})
	}
}

func buildTableDiffWithColDiffs(name string, columnDiffs []columnDiff) tableDiff {
	var oldColumns, newColumns []schema.Column
	copiedColumnDiffs := append([]columnDiff(nil), columnDiffs...)
	sort.Slice(copiedColumnDiffs, func(i, j int) bool {
		return copiedColumnDiffs[i].oldOrdering < copiedColumnDiffs[j].oldOrdering
	})
	for _, colDiff := range copiedColumnDiffs {
		oldColumns = append(oldColumns, colDiff.old)
	}
	sort.Slice(copiedColumnDiffs, func(i, j int) bool {
		return copiedColumnDiffs[i].newOrdering < copiedColumnDiffs[j].newOrdering
	})
	for _, colDiff := range copiedColumnDiffs {
		oldColumns = append(newColumns, colDiff.new)
	}

	return tableDiff{
		oldAndNew: oldAndNew[schema.Table]{
			old: schema.Table{
				Columns:          oldColumns,
				CheckConstraints: nil,
			},
			new: schema.Table{
				Columns:          newColumns,
				CheckConstraints: nil,
			},
		},
		columnsDiff: listDiff[schema.Column, columnDiff]{
			alters: columnDiffs,
		},
		checkConstraintDiff: listDiff[schema.CheckConstraint, checkConstraintDiff]{},
	}
}

func buildColumnDiff(col schema.Column, oldOrdering, newOrdering int) columnDiff {
	return columnDiff{
		oldAndNew: oldAndNew[schema.Column]{
			old: col,
			new: col,
		},
		oldOrdering: oldOrdering,
		newOrdering: newOrdering,
	}
}
