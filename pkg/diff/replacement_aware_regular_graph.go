package diff

import (
	"fmt"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

type replacementAwareVertexKind string

const (
	replacementAwareVertexKindEnumAddAlter replacementAwareVertexKind = "enum_add_alter"
	replacementAwareVertexKindLegacySuffix replacementAwareVertexKind = "legacy_suffix"
)

type replacementAwareVertexID struct {
	kind replacementAwareVertexKind
}

func (id replacementAwareVertexID) String() string {
	return "replacement_aware:" + string(id.kind)
}

// generateReplacementAwareSchemaSQL is dormant until archival activation.
func generateReplacementAwareSchemaSQL(
	generator *schemaSQLGenerator,
	diff schemaDiff,
	dispositions tableDispositions,
	archivalRequest plainTableArchivalRequest,
) ([]Statement, error) {
	groups, err := preparePlainTableArchivalGroups(archivalRequest)
	if err != nil {
		return nil, err
	}
	configured := *generator
	configured.tableDispositions = dispositions
	configured.plainTableArchivalGroups = groups
	return configured.Alter(diff)
}

func validateReplacementAwareStage13Scope(
	diff schemaDiff,
	dispositions tableDispositions,
	groups []preparedPlainTableArchivalGroup,
) error {
	if len(groups) == 0 {
		return nil
	}
	for _, foreignKey := range diff.old.ForeignKeyConstraints {
		if tableIsPreserved(dispositions, foreignKey.OwningTable.GetName()) ||
			tableIsPreserved(dispositions, foreignKey.ForeignTable.GetName()) {
			return fmt.Errorf(
				"foreign key %s touches an archived table; foreign-key isolation is deferred to Stage 14",
				foreignKey.GetName(),
			)
		}
	}
	return nil
}

func integrateReplacementAwareRegularGraph(
	regularGraph *sqlGraph,
	diff schemaDiff,
	groups []preparedPlainTableArchivalGroup,
	dispositions tableDispositions,
	enumStatements sqlGroupedByEffect[schema.Enum, enumDiff],
	legacySuffix []Statement,
) (*sqlGraph, error) {
	archivalGraph, err := buildPlainTableArchivalGraph(groups)
	if err != nil {
		return nil, err
	}
	if err := regularGraph.Union(archivalGraph.Graph, mergeVertices); err != nil {
		return nil, fmt.Errorf("merging archival and regular SQL graphs: %w", err)
	}

	enumID := replacementAwareVertexID{kind: replacementAwareVertexKindEnumAddAlter}
	regularGraph.AddVertex(sqlVertex{
		id: enumID, priority: sqlPrioritySooner,
		statements: append(enumStatements.Adds, enumStatements.Alters...),
	})

	targetIDs := replacementAwareTargetVertexIDs(diff)
	for _, group := range groups {
		moveID := plainTableArchivalMoveVertexID(group)
		if err := addSQLGraphDependency(regularGraph, mustRun(moveID).before(enumID)); err != nil {
			return nil, err
		}
		for _, targetID := range targetIDs {
			if err := addSQLGraphDependency(regularGraph,
				mustRun(moveID).before(targetID)); err != nil {
				return nil, err
			}
		}
	}
	for _, targetID := range targetIDs {
		if err := addSQLGraphDependency(regularGraph, mustRun(enumID).before(targetID)); err != nil {
			return nil, err
		}
	}

	for _, replacementID := range movedReplacementTableVertexIDs(diff, dispositions) {
		for _, routineID := range replacementAwareRoutineVertexIDs(diff) {
			if err := addSQLGraphDependency(regularGraph,
				mustRun(replacementID).before(routineID)); err != nil {
				return nil, err
			}
		}
	}

	suffixID := replacementAwareVertexID{kind: replacementAwareVertexKindLegacySuffix}
	regularGraph.AddVertex(sqlVertex{id: suffixID, priority: sqlPriorityLater, statements: legacySuffix})
	vertices, err := regularGraph.TopologicallySort()
	if err != nil {
		return nil, fmt.Errorf("reading integrated SQL graph vertices: %w", err)
	}
	for _, vertex := range vertices {
		if vertex.GetId() == suffixID.String() || isArchivalAssertionVertex(vertex.id) {
			continue
		}
		if err := addSQLGraphDependency(regularGraph, mustRun(vertex.id).before(suffixID)); err != nil {
			return nil, err
		}
	}
	for _, group := range groups {
		assertionID := plainTableArchivalVertexID{
			kind: plainTableArchivalVertexKindCatalogAssertion, groupID: group.id,
		}
		if err := addSQLGraphDependency(regularGraph, mustRun(suffixID).before(assertionID)); err != nil {
			return nil, err
		}
	}
	return regularGraph, nil
}

func addSQLGraphDependency(graph *sqlGraph, dep dependency) error {
	addVertexIfNotExists(graph, dep.source)
	addVertexIfNotExists(graph, dep.target)
	if err := graph.AddEdge(dep.source.String(), dep.target.String()); err != nil {
		return fmt.Errorf("adding replacement-aware edge from %s to %s: %w", dep.source, dep.target, err)
	}
	return nil
}

func plainTableArchivalMoveVertexID(group preparedPlainTableArchivalGroup) plainTableArchivalVertexID {
	kind := plainTableArchivalVertexKindTableMove
	if group.resume != nil {
		kind = plainTableArchivalVertexKindResumeTableMove
	}
	return plainTableArchivalVertexID{kind: kind, groupID: group.id}
}

func isArchivalAssertionVertex(id sqlVertexId) bool {
	archivalID, ok := id.(plainTableArchivalVertexID)
	return ok && archivalID.kind == plainTableArchivalVertexKindCatalogAssertion
}

func replacementAwareTargetVertexIDs(diff schemaDiff) []sqlVertexId {
	var ids []sqlVertexId
	for _, table := range diff.tableDiffs.adds {
		ids = append(ids, buildTableVertexId(table.SchemaQualifiedName, diffTypeAddAlter))
	}
	for _, tableDiff := range diff.tableDiffs.alters {
		ids = append(ids, buildTableVertexId(tableDiff.new.SchemaQualifiedName, diffTypeAddAlter))
	}
	for _, index := range diff.indexDiffs.adds {
		ids = append(ids, buildIndexVertexId(index.GetSchemaQualifiedName(), diffTypeAddAlter))
	}
	for _, indexDiff := range diff.indexDiffs.alters {
		ids = append(ids, buildIndexVertexId(indexDiff.new.GetSchemaQualifiedName(), diffTypeAddAlter))
	}
	for _, sequence := range diff.sequenceDiffs.adds {
		ids = append(ids, buildSequenceVertexId(sequence.SchemaQualifiedName, diffTypeAddAlter))
	}
	for _, sequenceDiff := range diff.sequenceDiffs.alters {
		ids = append(ids, buildSequenceVertexId(sequenceDiff.new.SchemaQualifiedName, diffTypeAddAlter))
	}
	for _, function := range diff.functionDiffs.adds {
		ids = append(ids, buildFunctionVertexId(function.SchemaQualifiedName, diffTypeAddAlter))
	}
	for _, functionDiff := range diff.functionDiffs.alters {
		ids = append(ids, buildFunctionVertexId(functionDiff.new.SchemaQualifiedName, diffTypeAddAlter))
	}
	for _, procedure := range diff.proceduresDiffs.adds {
		ids = append(ids, buildProcedureVertexId(procedure.SchemaQualifiedName, diffTypeAddAlter))
	}
	for _, procedureDiff := range diff.proceduresDiffs.alters {
		ids = append(ids, buildProcedureVertexId(procedureDiff.new.SchemaQualifiedName, diffTypeAddAlter))
	}
	for _, view := range diff.viewDiff.adds {
		ids = append(ids, buildTableVertexId(view.SchemaQualifiedName, diffTypeAddAlter))
	}
	for _, viewDiff := range diff.viewDiff.alters {
		ids = append(ids, buildTableVertexId(viewDiff.new.SchemaQualifiedName, diffTypeAddAlter))
	}
	for _, view := range diff.materializedViewDiffs.adds {
		ids = append(ids, buildMaterializedViewVertexId(view.SchemaQualifiedName, diffTypeAddAlter))
	}
	for _, viewDiff := range diff.materializedViewDiffs.alters {
		ids = append(ids, buildMaterializedViewVertexId(viewDiff.new.SchemaQualifiedName, diffTypeAddAlter))
	}
	return ids
}

func movedReplacementTableVertexIDs(diff schemaDiff, dispositions tableDispositions) []sqlVertexId {
	addedByName := buildSchemaObjByNameMap(diff.tableDiffs.adds)
	var ids []sqlVertexId
	for tableName, disposition := range dispositions {
		if disposition.Kind != tableDispositionKindArchivalMove {
			continue
		}
		if table, ok := addedByName[tableName]; ok {
			ids = append(ids, buildTableVertexId(table.SchemaQualifiedName, diffTypeAddAlter))
		}
	}
	return ids
}

func replacementAwareRoutineVertexIDs(diff schemaDiff) []sqlVertexId {
	var ids []sqlVertexId
	for _, function := range diff.functionDiffs.adds {
		ids = append(ids, buildFunctionVertexId(function.SchemaQualifiedName, diffTypeAddAlter))
	}
	for _, functionDiff := range diff.functionDiffs.alters {
		ids = append(ids, buildFunctionVertexId(functionDiff.new.SchemaQualifiedName, diffTypeAddAlter))
	}
	for _, procedure := range diff.proceduresDiffs.adds {
		ids = append(ids, buildProcedureVertexId(procedure.SchemaQualifiedName, diffTypeAddAlter))
	}
	for _, procedureDiff := range diff.proceduresDiffs.alters {
		ids = append(ids, buildProcedureVertexId(procedureDiff.new.SchemaQualifiedName, diffTypeAddAlter))
	}
	return ids
}
