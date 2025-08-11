package diff

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type viewDiff struct {
	oldAndNew[schema.View]
}

func buildViewDiff(
	deletedTablesByName map[string]schema.Table,
	tableDiffsByName map[string]tableDiff,
	old, new schema.View) (viewDiff, bool, error) {
	// Assuming the view's outputted columns do not change, there are few situations where the view
	// needs to be totally recreated (delete then re-add):
	//- One of its dependent columns is deleted then added. As in, a column that it depends on in the old and new is recreated.
	//- Same as above but for the table itself.
	//- "Outputted" columns of the view change (remove or type altered)
	//
	// It does not need to be recreated in the following situations:
	// - The recreated column/table is only just becoming a dependency: In this case, it can rely on being altered.
	// --> A column "foobar" is added to the table, and "foobar" is being added to the view.
	// - The recreated column/table is no longer a dependency: In this case, it can rely on being altered.
	// --> A column "foobar" is removed to the table, and "foobar" is being removed from the view.
	//
	// For now, we will go with the simplest behavior and always recreate the view if a dependent column/table,
	// and that column/table is deleted/recreated. In part, this is because we cannot depend on individual column
	// changes...all added and removes columns are combined into the same SQL vertex.
	// - See https://github.com/stripe/pg-schema-diff/issues/135#issuecomment-2357382217 for details.
	// - For some table X, it is currently not possible to create a SQL statement outside the table sql genreator
	// that comes before a column Y's delete statement but after a column Z's add statement.
	for _, t := range old.TableDependencies {
		if _, ok := deletedTablesByName[t.GetName()]; ok {
			// Recreate if a dependent table was deleted (or recreated).
			return viewDiff{}, true, nil
		}
		// It's possible a dependent column was deleted (or recreated).
		td, ok := tableDiffsByName[t.GetName()]
		if !ok {
			return viewDiff{}, false, fmt.Errorf("processing view table dependencies: expected a table diff to exist for %q. have=\n%s", t.GetName(), slices.Sorted(maps.Keys(tableDiffsByName)))
		}
		deletedColumnsByName := buildSchemaObjByNameMap(td.columnsDiff.deletes)
		for _, c := range t.Columns {
			if _, ok := deletedColumnsByName[c]; ok {
				// Recreate if a dependent column was deleted (or recreated).
				return viewDiff{}, true, nil
			}
		}
	}

	// Recreate if the view SQL generator cannot alter the view.
	d := viewDiff{oldAndNew: oldAndNew[schema.View]{old: old, new: new}}
	if _, err := newViewSQLVertexGenerator().Alter(d); err != nil {
		if errors.Is(err, ErrNotImplemented) {
			// The SQL generator cannot alter the view, so add and delete it.
			return viewDiff{}, true, nil
		}
		return viewDiff{}, false, fmt.Errorf("generating view alter statements: %w", err)
	}
	return d, false, nil
}

type viewSQLGenerator struct {
}

func newViewSQLVertexGenerator() sqlVertexGenerator[schema.View, viewDiff] {
	return &viewSQLGenerator{}
}

func (vsg *viewSQLGenerator) Add(v schema.View) (partialSQLGraph, error) {
	viewSb := strings.Builder{}
	viewSb.WriteString(fmt.Sprintf("CREATE VIEW %s", v.GetFQEscapedName()))
	if len(v.Options) > 0 {
		var kvs []string
		for k, v := range v.Options {
			kvs = append(kvs, fmt.Sprintf("%s=%s", k, v))
		}
		// Sort kvs so the generated DDL is deterministic.
		slices.Sort(kvs)
		viewSb.WriteString(fmt.Sprintf(" WITH (%s)", strings.Join(kvs, ", ")))
	}
	viewSb.WriteString(" AS\n")
	viewSb.WriteString(v.ViewDefinition)

	addVertexId := buildTableVertexId(v.SchemaQualifiedName, diffTypeAddAlter)

	var deps []dependency

	// Run after re-create (if recreated).
	deps = append(deps, mustRun(addVertexId).after(buildViewVertexId(v.SchemaQualifiedName, diffTypeDelete)))

	// Run after any dependent tables are added/altered.
	for _, t := range v.TableDependencies {
		deps = append(deps, mustRun(addVertexId).after(buildTableVertexId(t.SchemaQualifiedName, diffTypeDelete)))
		deps = append(deps, mustRun(addVertexId).after(buildTableVertexId(t.SchemaQualifiedName, diffTypeAddAlter)))
	}

	return partialSQLGraph{
		vertices: []sqlVertex{{
			id:       addVertexId,
			priority: sqlPrioritySooner,
			statements: []Statement{{
				DDL:         viewSb.String(),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
			}},
		}},
		dependencies: deps,
	}, nil
}

func (vsg *viewSQLGenerator) Delete(v schema.View) (partialSQLGraph, error) {
	deleteVertexId := buildViewVertexId(v.SchemaQualifiedName, diffTypeDelete)

	// Run before any dependent tables are deleted or added/altered.
	var deps []dependency
	for _, t := range v.TableDependencies {
		deps = append(deps, mustRun(deleteVertexId).before(buildTableVertexId(t.SchemaQualifiedName, diffTypeDelete)))
		deps = append(deps, mustRun(deleteVertexId).before(buildTableVertexId(t.SchemaQualifiedName, diffTypeAddAlter)))
	}

	return partialSQLGraph{
		vertices: []sqlVertex{{
			id:       deleteVertexId,
			priority: sqlPriorityLater,
			statements: []Statement{{
				DDL:         fmt.Sprintf("DROP VIEW %s", v.GetFQEscapedName()),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
			}},
		}},
		dependencies: deps,
	}, nil
}

func (vsg *viewSQLGenerator) Alter(vd viewDiff) (partialSQLGraph, error) {
	// In the initial MVP, we will not support altering.
	if !cmp.Equal(vd.old, vd.new) {
		return partialSQLGraph{}, ErrNotImplemented
	}
	return partialSQLGraph{}, nil
}

func buildViewVertexId(n schema.SchemaQualifiedName, d diffType) sqlVertexId {
	return buildSchemaObjVertexId("view", n.GetFQEscapedName(), d)
}
