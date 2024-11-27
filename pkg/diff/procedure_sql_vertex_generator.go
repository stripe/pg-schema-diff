package diff

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type procedureSQLVertexGenerator struct {
	newSchema schema.Schema
}

func newProcedureSqlVertexGenerator(newSchema schema.Schema) sqlVertexGenerator[schema.Procedure, procedureDiff] {
	return &procedureSQLVertexGenerator{
		newSchema: newSchema,
	}
}

func (p procedureSQLVertexGenerator) Add(s schema.Procedure) (partialSQLGraph, error) {
	// Procedures can't be added until all dependencies have been added. Weirdly, Postgres ONLY enforces these
	// dependencies at creation time and not after...so we will make a best effort to order this statement after
	// all other dependencies that procedures might depend on.

	var deps []dependency

	// Run after all tables have been added/altered, since a procedure might query a table.
	for _, t := range p.newSchema.Tables {
		deps = append(deps, mustRun(buildProcedureVertexId(s.SchemaQualifiedName, diffTypeAddAlter)).after(buildTableVertexId(t.SchemaQualifiedName, diffTypeAddAlter)))
	}

	// Run after all functions, since a procedure might call a function.
	for _, f := range p.newSchema.Functions {
		deps = append(deps, mustRun(buildProcedureVertexId(s.SchemaQualifiedName, diffTypeAddAlter)).after(buildFunctionVertexId(f.SchemaQualifiedName, diffTypeAddAlter)))
	}

	// Run after all sequences, since a procedure might call a sequence.
	for _, seq := range p.newSchema.Sequences {
		deps = append(deps, mustRun(buildProcedureVertexId(s.SchemaQualifiedName, diffTypeAddAlter)).after(buildSequenceVertexId(seq.SchemaQualifiedName, diffTypeAddAlter)))
	}

	return partialSQLGraph{
		vertices: []sqlVertex{{
			id:       buildProcedureVertexId(s.SchemaQualifiedName, diffTypeAddAlter),
			priority: sqlPrioritySooner,
			statements: []Statement{{
				DDL:         s.Def,
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
				Hazards: []MigrationHazard{{
					Type: MigrationHazardTypeHasUntrackableDependencies,
					Message: "Dependencies of procedures are not tracked by Postgres. " +
						"As a result, we cannot guarantee that this procedure's dependencies are ordered properly relative to " +
						"this statement. For adds, this means you need to ensure that all objects this function depends on " +
						"are added before this statement.",
				}},
			}},
		}},
		dependencies: deps,
	}, nil
}

func (p procedureSQLVertexGenerator) Delete(s schema.Procedure) (partialSQLGraph, error) {
	// Stored procedure dependencies can't be tracked...so they can either be deleted earlier or later. We will
	// delete earlier, since a procedure is more likely to depend on objects that being depended on. Thus, we will have
	// a stored procedure drop before other objects that might depend on it.
	var deps []dependency

	// Run before all tables have been added/altered, since a procedure might query a table. This does not work for columns
	// being dropped because column drops are not "trackable" from external SQL generators until
	// https://github.com/stripe/pg-schema-diff/issues/131 is fully implemented.
	for _, t := range p.newSchema.Tables {
		deps = append(deps, mustRun(buildProcedureVertexId(s.SchemaQualifiedName, diffTypeDelete)).after(buildTableVertexId(t.SchemaQualifiedName, diffTypeAddAlter)))
	}

	// Run before all functions, since a procedure might call a function.
	for _, f := range p.newSchema.Functions {
		deps = append(deps, mustRun(buildProcedureVertexId(s.SchemaQualifiedName, diffTypeDelete)).after(buildFunctionVertexId(f.SchemaQualifiedName, diffTypeAddAlter)))
	}

	// Run before all sequences, since a procedure might call a sequence.
	for _, seq := range p.newSchema.Sequences {
		deps = append(deps, mustRun(buildProcedureVertexId(s.SchemaQualifiedName, diffTypeDelete)).after(buildSequenceVertexId(seq.SchemaQualifiedName, diffTypeAddAlter)))
	}

	return partialSQLGraph{
		vertices: []sqlVertex{{
			id:       buildProcedureVertexId(s.SchemaQualifiedName, diffTypeDelete),
			priority: sqlPriorityLater,
			statements: []Statement{{
				DDL:         fmt.Sprintf("DROP PROCEDURE %s", s.GetFQEscapedName()),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
				Hazards: []MigrationHazard{{
					Type: MigrationHazardTypeHasUntrackableDependencies,
					Message: "Dependencies of procedures are not tracked by Postgres. " +
						"As a result, we cannot guarantee that this procedure's dependencies are ordered properly relative to " +
						"this statement. For drops, this means you need to ensure that all objects this function depends on " +
						"are dropped after this statement.",
				}},
			}},
		}},
		dependencies: deps,
	}, nil
}

func (p procedureSQLVertexGenerator) Alter(d procedureDiff) (partialSQLGraph, error) {
	if cmp.Equal(d.old, d.new) {
		return partialSQLGraph{}, nil
	}
	// New adds or replaces the procedure.
	return p.Add(d.new)
}

func buildProcedureVertexId(name schema.SchemaQualifiedName, diffType diffType) sqlVertexId {
	return buildSchemaObjVertexId("procedure", name.GetFQEscapedName(), diffType)
}
