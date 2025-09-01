package diff

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/internal/util"
)

type materializedViewDiff struct {
	oldAndNew[schema.MaterializedView]
}

func buildMaterializedViewDiff(
	deletedTablesByName map[string]schema.Table,
	tableDiffsByName map[string]tableDiff,
	old, new schema.MaterializedView) (materializedViewDiff, bool, error) {
	// Assuming the materialized view's outputted columns do not change, there are few situations where the materialized view
	// needs to be totally recreated (delete then re-add):
	//- One of its dependent columns is deleted then added. As in, a column that it depends on in the old and new is recreated.
	//- Same as above but for the table itself.
	//- "Outputted" columns of the materialized view change (remove or type altered)
	//
	// It does not need to be recreated in the following situations:
	// - The recreated column/table is only just becoming a dependency: In this case, it can rely on being altered.
	// --> A column "foobar" is added to the table, and "foobar" is being added to the materialized view.
	// - The recreated column/table is no longer a dependency: In this case, it can rely on being altered.
	// --> A column "foobar" is removed to the table, and "foobar" is being removed from the materialized view.
	//
	// For now, we will go with the simplest behavior and always recreate the materialized view if a dependent column/table,
	// and that column/table is deleted/recreated. In part, this is because we cannot depend on individual column
	// changes...all added and removes columns are combined into the same SQL vertex.
	// - See https://github.com/stripe/pg-schema-diff/issues/135#issuecomment-2357382217 for details.
	// - For some table X, it is currently not possible to create a SQL statement outside the table sql genreator
	// that comes before a column Y's delete statement but after a column Z's add statement.
	for _, t := range old.TableDependencies {
		if _, ok := deletedTablesByName[t.GetName()]; ok {
			// Recreate if a dependent table was deleted (or recreated).
			return materializedViewDiff{}, true, nil
		}
		// It's possible a dependent column was deleted (or recreated).
		td, ok := tableDiffsByName[t.GetName()]
		if !ok {
			return materializedViewDiff{}, false, fmt.Errorf("processing materialized view table dependencies: expected a table diff to exist for %q. have=\n%s", t.GetName(), util.Keys(tableDiffsByName))
		}
		deletedColumnsByName := buildSchemaObjByNameMap(td.columnsDiff.deletes)
		for _, c := range t.Columns {
			if _, ok := deletedColumnsByName[c]; ok {
				// Recreate if a dependent column was deleted (or recreated).
				return materializedViewDiff{}, true, nil
			}
		}
	}

	// Recreate if the materialized view SQL generator cannot alter the materialized view.
	d := materializedViewDiff{oldAndNew: oldAndNew[schema.MaterializedView]{old: old, new: new}}
	if _, err := newMaterializedViewSQLVertexGenerator().Alter(d); err != nil {
		if errors.Is(err, ErrNotImplemented) {
			// The SQL generator cannot alter the materialized view, so add and delete it.
			return materializedViewDiff{}, true, nil
		}
		return materializedViewDiff{}, false, fmt.Errorf("generating materialized view alter statements: %w", err)
	}
	return d, false, nil
}

type materializedViewSQLGenerator struct {
}

func newMaterializedViewSQLVertexGenerator() sqlVertexGenerator[schema.MaterializedView, materializedViewDiff] {
	return &materializedViewSQLGenerator{}
}

func (mvsg *materializedViewSQLGenerator) Add(mv schema.MaterializedView) (partialSQLGraph, error) {
	materializedViewSb := strings.Builder{}
	materializedViewSb.WriteString(fmt.Sprintf("CREATE MATERIALIZED VIEW %s", mv.GetFQEscapedName()))
	if len(mv.Options) > 0 {
		var kvs []string
		for k, v := range mv.Options {
			kvs = append(kvs, fmt.Sprintf("%s=%s", k, v))
		}
		// Sort kvs so the generated DDL is deterministic. This is unnecessarily verbose because the slices
		// package is not yet available.
		// // TODO(https://github.com/stripe/pg-schema-diff/issues/227) - Remove this
		sort.Strings(kvs)
		materializedViewSb.WriteString(fmt.Sprintf(" WITH (%s)", strings.Join(kvs, ", ")))
	}
	if len(mv.Tablespace) > 0 {
		materializedViewSb.WriteString(fmt.Sprintf(" TABLESPACE %s", schema.EscapeIdentifier(mv.Tablespace)))
	}
	materializedViewSb.WriteString(" AS\n")
	materializedViewSb.WriteString(mv.ViewDefinition)

	addVertexId := buildMaterializedViewVertexId(mv.SchemaQualifiedName, diffTypeAddAlter)

	var deps []dependency

	// Run after re-create (if recreated).
	deps = append(deps, mustRun(addVertexId).after(buildMaterializedViewVertexId(mv.SchemaQualifiedName, diffTypeDelete)))

	// Run after any dependent tables are added/altered.
	for _, t := range mv.TableDependencies {
		deps = append(deps, mustRun(addVertexId).after(buildTableVertexId(t.SchemaQualifiedName, diffTypeDelete)))
		deps = append(deps, mustRun(addVertexId).after(buildTableVertexId(t.SchemaQualifiedName, diffTypeAddAlter)))
	}

	return partialSQLGraph{
		vertices: []sqlVertex{{
			id:       addVertexId,
			priority: sqlPrioritySooner,
			statements: []Statement{{
				DDL:         materializedViewSb.String(),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
			}},
		}},
		dependencies: deps,
	}, nil
}

func (mvsg *materializedViewSQLGenerator) Delete(mv schema.MaterializedView) (partialSQLGraph, error) {
	deleteVertexId := buildMaterializedViewVertexId(mv.SchemaQualifiedName, diffTypeDelete)

	// Run before any dependent tables are deleted or added/altered.
	var deps []dependency
	for _, t := range mv.TableDependencies {
		deps = append(deps, mustRun(deleteVertexId).before(buildTableVertexId(t.SchemaQualifiedName, diffTypeDelete)))
		deps = append(deps, mustRun(deleteVertexId).before(buildTableVertexId(t.SchemaQualifiedName, diffTypeAddAlter)))
	}

	return partialSQLGraph{
		vertices: []sqlVertex{{
			id:       deleteVertexId,
			priority: sqlPriorityLater,
			statements: []Statement{{
				DDL:         fmt.Sprintf("DROP MATERIALIZED VIEW %s", mv.GetFQEscapedName()),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
			}},
		}},
		dependencies: deps,
	}, nil
}

func (mvsg *materializedViewSQLGenerator) Alter(mvd materializedViewDiff) (partialSQLGraph, error) {
	// In the initial MVP, we will not support altering.
	if !cmp.Equal(mvd.old, mvd.new) {
		return partialSQLGraph{}, ErrNotImplemented
	}
	return partialSQLGraph{}, nil
}

func buildMaterializedViewVertexId(n schema.SchemaQualifiedName, d diffType) sqlVertexId {
	return buildSchemaObjVertexId("materialized_view", n.GetFQEscapedName(), d)
}
