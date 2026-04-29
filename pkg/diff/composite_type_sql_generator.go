package diff

import (
	"fmt"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

// compositeTypeSQLVertexGenerator handles `CREATE TYPE foo AS (...)` and
// `DROP TYPE foo` for user-defined composite types. It is a SQLVertexGenerator
// (rather than a plain SQLGenerator) so it can carry explicit ordering
// dependencies relative to the consumers of a composite type — namely tables,
// functions, procedures, and triggers, which may reference the type in column
// types, parameter types, or return types.
//
// Phase 1 scope: Add and Drop only. ALTER (when attributes change) is
// returned as ErrNotImplemented so the diff machinery fails loudly rather
// than silently dropping the change. A future phase can extend Alter to
// drop+recreate cascade for the function-only-dependents case.
type compositeTypeSQLVertexGenerator struct {
	// newSchema and oldSchema are used to set up dependency edges from every
	// table/function/procedure/trigger that may reference a composite type to
	// the type's add/delete vertices. We do not parse signatures to know
	// which functions actually reference a given composite — instead we
	// take a blanket approach and let topo-sort untangle it. This matches
	// how procedureSQLVertexGenerator handles its own untrackable deps.
	newSchema schema.Schema
	oldSchema schema.Schema

	recreatedCompositeTypes map[string]bool
}

func newCompositeTypeSQLVertexGenerator(oldSchema, newSchema schema.Schema, recreatedCompositeTypes map[string]bool) sqlVertexGenerator[schema.CompositeType, compositeTypeDiff] {
	return &compositeTypeSQLVertexGenerator{
		newSchema:               newSchema,
		oldSchema:               oldSchema,
		recreatedCompositeTypes: recreatedCompositeTypes,
	}
}

func (c *compositeTypeSQLVertexGenerator) Add(ct schema.CompositeType) (partialSQLGraph, error) {
	addVertexId := buildCompositeTypeVertexId(ct.SchemaQualifiedName, diffTypeAddAlter)

	stmts := []Statement{{
		DDL:         buildCreateCompositeTypeDDL(ct),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}

	// The type must exist before any consumer (table/function/procedure/trigger)
	// is added or altered. We add blanket "after-this" dependencies on the type
	// from every such consumer in the new schema; the topo sort will pick the
	// correct order.
	deps := c.consumerDepsForAddAlter(ct)
	deps = append(deps, c.compositeTypeDepsForAddAlter(ct)...)
	// Run after re-create (if recreated). Mirrors the view/mview pattern.
	deps = append(deps, mustRun(addVertexId).after(buildCompositeTypeVertexId(ct.SchemaQualifiedName, diffTypeDelete)))

	return partialSQLGraph{
		vertices: []sqlVertex{{
			id:         addVertexId,
			priority:   sqlPrioritySooner,
			statements: stmts,
		}},
		dependencies: deps,
	}, nil
}

func (c *compositeTypeSQLVertexGenerator) Delete(ct schema.CompositeType) (partialSQLGraph, error) {
	deleteVertexId := buildCompositeTypeVertexId(ct.SchemaQualifiedName, diffTypeDelete)

	// The type must be dropped after every consumer (in the OLD schema) is
	// dropped or altered to no longer reference it. Add blanket
	// "before-this" dependencies on the type's delete from every consumer's
	// delete and add/alter vertices.
	deps := c.consumerDepsForDelete(ct)
	deps = append(deps, c.compositeTypeDepsForDelete(ct)...)

	return partialSQLGraph{
		vertices: []sqlVertex{{
			id:       deleteVertexId,
			priority: sqlPriorityLater,
			statements: []Statement{{
				DDL:         fmt.Sprintf("DROP TYPE %s", ct.GetFQEscapedName()),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
			}},
		}},
		dependencies: deps,
	}, nil
}

func (c *compositeTypeSQLVertexGenerator) Alter(d compositeTypeDiff) (partialSQLGraph, error) {
	if cmp.Equal(d.old, d.new) {
		return partialSQLGraph{}, nil
	}

	// Anything beyond a description change requires altering the type's
	// attribute list. Phase 2 (drop dependent functions → drop type → recreate
	// type → recreate functions) is not implemented yet. For now we surface
	// ErrNotImplemented so the user gets an explicit error rather than a
	// silent drop.
	return partialSQLGraph{}, fmt.Errorf("altering composite type attributes: %w", ErrNotImplemented)
}

func buildCompositeTypeVertexId(name schema.SchemaQualifiedName, d diffType) sqlVertexId {
	return buildSchemaObjVertexId("composite_type", name.GetFQEscapedName(), d)
}

func buildCreateCompositeTypeDDL(ct schema.CompositeType) string {
	if len(ct.Attributes) == 0 {
		// PostgreSQL does not allow `CREATE TYPE foo AS ()` with zero columns at
		// creation time, but it does allow ALTER TYPE ... DROP ATTRIBUTE down
		// to zero. Such a state is not reachable from a declarative schema
		// (which always describes what it wants from scratch), so emitting an
		// empty parens is fine — apply will fail with a clear PG error if it
		// ever happens.
		return fmt.Sprintf("CREATE TYPE %s AS ()", ct.GetFQEscapedName())
	}
	var attrDefs []string
	for _, a := range ct.Attributes {
		def := fmt.Sprintf("\t%s %s", schema.EscapeIdentifier(a.Name), a.Type)
		if !a.Collation.IsEmpty() {
			def += fmt.Sprintf(" COLLATE %s", a.Collation.GetFQEscapedName())
		}
		attrDefs = append(attrDefs, def)
	}
	return fmt.Sprintf("CREATE TYPE %s AS (\n%s\n)", ct.GetFQEscapedName(), strings.Join(attrDefs, ",\n"))
}

// dependsOnAnyRecreatedType reports whether any of the given composite-type
// references is in the set of types being recreated (their attribute list is
// changing). Used to force-recreate functions and procedures so PostgreSQL
// resolves their argument/return types against the new layout.
func dependsOnAnyRecreatedType(deps []schema.SchemaQualifiedName, recreated map[string]bool) bool {
	for _, d := range deps {
		if recreated[d.GetName()] {
			return true
		}
	}
	return false
}

// consumerDepsForAddAlter returns dependency edges that force the
// composite type's CREATE to run before any consumer's CREATE/ALTER in
// the new schema.
func (c *compositeTypeSQLVertexGenerator) consumerDepsForAddAlter(ct schema.CompositeType) []dependency {
	addVertexId := buildCompositeTypeVertexId(ct.SchemaQualifiedName, diffTypeAddAlter)

	var deps []dependency
	for _, t := range c.newSchema.Tables {
		deps = append(deps, mustRun(addVertexId).before(buildTableVertexId(t.SchemaQualifiedName, diffTypeAddAlter)))
	}
	for _, f := range c.newSchema.Functions {
		deps = append(deps, mustRun(addVertexId).before(buildFunctionVertexId(f.SchemaQualifiedName, diffTypeAddAlter)))
	}
	for _, p := range c.newSchema.Procedures {
		deps = append(deps, mustRun(addVertexId).before(buildProcedureVertexId(p.SchemaQualifiedName, diffTypeAddAlter)))
	}
	return deps
}

func (c *compositeTypeSQLVertexGenerator) compositeTypeDepsForAddAlter(ct schema.CompositeType) []dependency {
	addVertexId := buildCompositeTypeVertexId(ct.SchemaQualifiedName, diffTypeAddAlter)

	var deps []dependency
	for _, dep := range ct.DependsOnCompositeTypes {
		deps = append(deps, mustRun(addVertexId).after(buildCompositeTypeVertexId(dep, diffTypeAddAlter)))
	}
	return deps
}

// consumerDepsForDelete returns dependency edges that force the
// composite type's DROP to run after every consumer in the old schema is
// dropped or (in the case of a pure delete) altered to no longer reference
// the type.
//
// When a consumer (function/procedure) STILL references this type in the
// new schema — i.e. the type is being recreated because its attributes
// changed and the consumer is force-recreated alongside it — we must NOT
// add the `typeDelete > consumerAddAlter` edge: doing so would force the
// consumer to be created BEFORE the type is dropped, which contradicts
// the required order
//
//	consumerDelete < typeDelete < typeAdd < consumerAddAlter
//
// Tables are unconditional because we explicitly refuse type-recreation
// when a table column depends on the type (see buildSchemaDiff), so the
// recreation edge case never arises for tables.
func (c *compositeTypeSQLVertexGenerator) consumerDepsForDelete(ct schema.CompositeType) []dependency {
	deleteVertexId := buildCompositeTypeVertexId(ct.SchemaQualifiedName, diffTypeDelete)
	ctName := ct.GetName()

	newFunctionsByName := make(map[string]schema.Function, len(c.newSchema.Functions))
	for _, f := range c.newSchema.Functions {
		newFunctionsByName[f.GetName()] = f
	}
	newProceduresByName := make(map[string]schema.Procedure, len(c.newSchema.Procedures))
	for _, p := range c.newSchema.Procedures {
		newProceduresByName[p.GetName()] = p
	}

	var deps []dependency
	for _, t := range c.oldSchema.Tables {
		deps = append(deps, mustRun(deleteVertexId).after(buildTableVertexId(t.SchemaQualifiedName, diffTypeDelete)))
		deps = append(deps, mustRun(deleteVertexId).after(buildTableVertexId(t.SchemaQualifiedName, diffTypeAddAlter)))
	}
	for _, f := range c.oldSchema.Functions {
		deps = append(deps, mustRun(deleteVertexId).after(buildFunctionVertexId(f.SchemaQualifiedName, diffTypeDelete)))
		newDeps := newFunctionsByName[f.GetName()].DependsOnCompositeTypes
		if !consumerStillDependsOnType(newDeps, ctName) && !dependsOnAnyRecreatedType(newDeps, c.recreatedCompositeTypes) {
			deps = append(deps, mustRun(deleteVertexId).after(buildFunctionVertexId(f.SchemaQualifiedName, diffTypeAddAlter)))
		}
	}
	for _, p := range c.oldSchema.Procedures {
		deps = append(deps, mustRun(deleteVertexId).after(buildProcedureVertexId(p.SchemaQualifiedName, diffTypeDelete)))
		newDeps := newProceduresByName[p.GetName()].DependsOnCompositeTypes
		if !consumerStillDependsOnType(newDeps, ctName) && !dependsOnAnyRecreatedType(newDeps, c.recreatedCompositeTypes) {
			deps = append(deps, mustRun(deleteVertexId).after(buildProcedureVertexId(p.SchemaQualifiedName, diffTypeAddAlter)))
		}
	}
	return deps
}

func (c *compositeTypeSQLVertexGenerator) compositeTypeDepsForDelete(ct schema.CompositeType) []dependency {
	deleteVertexId := buildCompositeTypeVertexId(ct.SchemaQualifiedName, diffTypeDelete)

	var deps []dependency
	for _, dep := range ct.DependsOnCompositeTypes {
		deps = append(deps, mustRun(deleteVertexId).before(buildCompositeTypeVertexId(dep, diffTypeDelete)))
	}
	return deps
}

func consumerStillDependsOnType(deps []schema.SchemaQualifiedName, ctName string) bool {
	for _, d := range deps {
		if d.GetName() == ctName {
			return true
		}
	}
	return false
}
