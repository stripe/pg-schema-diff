package diff

import (
	"sort"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

// dataPackNewTables packs the columns in new tables to minimize the space they occupy
//
// Note: We need to copy all arrays we modify because otherwise those arrays (effectively pointers)
// will still exist in the original structs, leading to mutation. Go is copy-by-value, but all slices
// are pointers. If we don't copy the arrays we change, then changing a struct in the array will mutate the
// original SchemaDiff struct
func dataPackNewTables(s schemaDiff) schemaDiff {
	copiedNewTables := append([]schema.Table(nil), s.tableDiffs.adds...)
	for i, _ := range copiedNewTables {
		copiedColumns := append([]schema.Column(nil), copiedNewTables[i].Columns...)
		copiedNewTables[i].Columns = copiedColumns
		sort.Slice(copiedColumns, func(i, j int) bool {
			// Sort in descending order of size
			return copiedColumns[i].Size > copiedColumns[j].Size
		})
	}
	s.tableDiffs.adds = copiedNewTables

	return s
}

// removeChangesToColumnOrdering removes any changes to column ordering. In effect, it tells the SQL
// generator to ignore changes to column ordering
func removeChangesToColumnOrdering(s schemaDiff) schemaDiff {
	copiedTableDiffs := append([]tableDiff(nil), s.tableDiffs.alters...)
	for i, _ := range copiedTableDiffs {
		copiedColDiffs := append([]columnDiff(nil), copiedTableDiffs[i].columnsDiff.alters...)
		for i, _ := range copiedColDiffs {
			copiedColDiffs[i].oldOrdering = copiedColDiffs[i].newOrdering
		}
		copiedTableDiffs[i].columnsDiff.alters = copiedColDiffs
	}
	s.tableDiffs.alters = copiedTableDiffs

	return s
}
