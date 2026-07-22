package diff

import (
	"cmp"
	"fmt"
	"slices"

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

// generateReplacementAwareSchemaSQL integrates archival moves with regular SQL.
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
	isolation, err := planArchivalIsolation(archivalRequest.CurrentInventory, diff.new,
		archivalRequest.SourcePreflight, archivalRequest.DependencyClosure, groups)
	if err != nil {
		return nil, fmt.Errorf("planning archival isolation: %w", err)
	}
	configured := *generator
	configured.tableDispositions = dispositions
	configured.archivalGroups = groups
	configured.archivalInventory = archivalRequest.CurrentInventory
	configured.archivalIsolation = isolation
	return configured.Alter(diff)
}

func validateReplacementAwareStage13Scope(
	diff schemaDiff,
	dispositions tableDispositions,
	groups []preparedArchivalGroup,
	isolation archivalIsolationPlan,
) error {
	if len(groups) == 0 {
		return nil
	}
	for _, foreignKey := range diff.old.ForeignKeyConstraints {
		if (tableIsPreserved(dispositions, foreignKey.OwningTable.GetName()) ||
			tableIsPreserved(dispositions, foreignKey.ForeignTable.GetName())) &&
			!isolationOwnsOldForeignKey(isolation, foreignKey) {
			return fmt.Errorf("foreign key %s touches an archived table but is not fully represented by Stage 14",
				foreignKey.GetName())
		}
	}
	for _, foreignKeyDiff := range diff.foreignKeyConstraintDiffs.alters {
		if tableIsPreserved(dispositions, foreignKeyDiff.old.OwningTable.GetName()) ||
			tableIsPreserved(dispositions, foreignKeyDiff.old.ForeignTable.GetName()) {
			return fmt.Errorf("altered foreign key %s touches an archived table but is not fully represented by Stage 14",
				foreignKeyDiff.old.GetName())
		}
	}
	if len(isolation.Groups) != len(groups) {
		return fmt.Errorf("stage 14 isolation plan does not cover every archival group")
	}
	return nil
}

func integrateReplacementAwareRegularGraph(
	regularGraph *sqlGraph,
	diff schemaDiff,
	inventory schema.CatalogInventory,
	groups []preparedArchivalGroup,
	isolation archivalIsolationPlan,
	dispositions tableDispositions,
	enumStatements sqlGroupedByEffect[schema.Enum, enumDiff],
	legacySuffix []Statement,
) (*sqlGraph, error) {
	archivalGraph, err := buildPlainTableArchivalGraph(inventory, groups, isolation)
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
	replacementStart := plainTableArchivalPhaseBarrierID{phase: 6}
	replacementEnd := plainTableArchivalPhaseBarrierID{phase: 7}
	if err := addSQLGraphDependency(regularGraph, mustRun(replacementStart).before(enumID)); err != nil {
		return nil, err
	}
	if err := addSQLGraphDependency(regularGraph, mustRun(enumID).before(replacementEnd)); err != nil {
		return nil, err
	}
	for _, targetID := range targetIDs {
		if err := addSQLGraphDependency(regularGraph,
			mustRun(replacementStart).before(targetID)); err != nil {
			return nil, err
		}
		if err := addSQLGraphDependency(regularGraph,
			mustRun(targetID).before(replacementEnd)); err != nil {
			return nil, err
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
	for _, dependency := range isolation.IncomingDependencyDeletes {
		deleteID, err := incomingDependencyDeleteVertexID(dependency)
		if err != nil {
			return nil, err
		}
		if err := addSQLGraphDependency(regularGraph,
			mustRun(deleteID).before(plainTableArchivalPhaseBarrierID{phase: 4})); err != nil {
			return nil, err
		}
	}

	suffixID := replacementAwareVertexID{kind: replacementAwareVertexKindLegacySuffix}
	regularGraph.AddVertex(sqlVertex{id: suffixID, priority: sqlPriorityLater, statements: legacySuffix})
	vertices, err := regularGraph.TopologicallySort()
	if err != nil {
		return nil, fmt.Errorf("reading integrated SQL graph vertices: %w", err)
	}
	for _, vertex := range vertices {
		if vertex.GetId() == suffixID.String() || isAnyArchivalVertex(vertex.id) {
			continue
		}
		if err := addSQLGraphDependency(regularGraph, mustRun(vertex.id).before(suffixID)); err != nil {
			return nil, err
		}
	}
	if err := addSQLGraphDependency(regularGraph,
		mustRun(plainTableArchivalPhaseBarrierID{phase: 8}).before(suffixID)); err != nil {
		return nil, err
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

func rewriteSchemaDiffForArchivalIsolation(
	diff schemaDiff,
	plan archivalIsolationPlan,
	dispositions tableDispositions,
) schemaDiff {
	diff.foreignKeyConstraintDiffs.deletes = slices.DeleteFunc(
		slices.Clone(diff.foreignKeyConstraintDiffs.deletes),
		func(foreignKey schema.ForeignKeyConstraint) bool {
			return isolationOwnsOldForeignKey(plan, foreignKey)
		},
	)
	diff.foreignKeyConstraintDiffs.adds = slices.DeleteFunc(
		slices.Clone(diff.foreignKeyConstraintDiffs.adds),
		func(foreignKey schema.ForeignKeyConstraint) bool {
			for _, operation := range plan.IncomingForeignKeyReadds {
				if operation.Constraint.GetName() == foreignKey.GetName() {
					return true
				}
			}
			return false
		},
	)

	// SET SCHEMA carries an owned sequence with its archived table. If the
	// target retains that sequence identity, create a replacement after the
	// archival move instead of trying to alter the now-moved source sequence.
	targetSequences := buildSchemaObjByNameMap(diff.new.Sequences)
	addedSequences := buildSchemaObjByNameMap(diff.sequenceDiffs.adds)
	for _, oldSequence := range diff.old.Sequences {
		if oldSequence.Owner == nil ||
			!tableIsPreserved(dispositions, oldSequence.Owner.TableName.GetName()) {
			continue
		}
		targetSequence, retained := targetSequences[oldSequence.GetName()]
		if !retained {
			continue
		}
		diff.sequenceDiffs.alters = slices.DeleteFunc(
			slices.Clone(diff.sequenceDiffs.alters),
			func(value sequenceDiff) bool {
				return value.old.GetName() == oldSequence.GetName()
			},
		)
		if _, alreadyAdded := addedSequences[targetSequence.GetName()]; !alreadyAdded {
			diff.sequenceDiffs.adds = append(diff.sequenceDiffs.adds, targetSequence)
			addedSequences[targetSequence.GetName()] = targetSequence
		}
	}

	for _, assignment := range plan.DependencyMoves {
		sourceName := markerObjectSchemaName(assignment.Source)
		switch assignment.Source.Kind {
		case archivalMarkerObjectKindType:
			diff.enumDiffs.deletes = slices.DeleteFunc(slices.Clone(diff.enumDiffs.deletes),
				func(value schema.Enum) bool { return value.GetName() == sourceName })
			var remaining []enumDiff
			for _, alteration := range diff.enumDiffs.alters {
				if alteration.old.GetName() == sourceName {
					diff.enumDiffs.adds = append(diff.enumDiffs.adds, alteration.new)
				} else {
					remaining = append(remaining, alteration)
				}
			}
			diff.enumDiffs.alters = remaining
		case archivalMarkerObjectKindSequence:
			diff.sequenceDiffs.deletes = slices.DeleteFunc(
				slices.Clone(diff.sequenceDiffs.deletes),
				func(value schema.Sequence) bool { return value.GetName() == sourceName },
			)
			var remaining []sequenceDiff
			for _, alteration := range diff.sequenceDiffs.alters {
				if alteration.old.GetName() == sourceName {
					diff.sequenceDiffs.adds = append(diff.sequenceDiffs.adds, alteration.new)
				} else {
					remaining = append(remaining, alteration)
				}
			}
			diff.sequenceDiffs.alters = remaining
		case archivalMarkerObjectKindFunction:
			diff.functionDiffs.deletes = slices.DeleteFunc(
				slices.Clone(diff.functionDiffs.deletes),
				func(value schema.Function) bool { return value.GetName() == sourceName },
			)
		}
	}
	for _, follower := range plan.RetainedRoutineFollowers {
		sourceName := markerObjectSchemaName(follower)
		diff.functionDiffs.deletes = slices.DeleteFunc(
			slices.Clone(diff.functionDiffs.deletes),
			func(value schema.Function) bool { return value.GetName() == sourceName },
		)
	}
	slices.SortFunc(diff.enumDiffs.adds, func(a, b schema.Enum) int {
		return cmp.Compare(a.GetName(), b.GetName())
	})
	slices.SortFunc(diff.sequenceDiffs.adds, func(a, b schema.Sequence) int {
		return cmp.Compare(a.GetName(), b.GetName())
	})
	return diff
}

func isolationOwnsOldForeignKey(plan archivalIsolationPlan, foreignKey schema.ForeignKeyConstraint) bool {
	for _, operation := range plan.BoundaryForeignKeyDrops {
		if foreignKey.OwningTable.SchemaName == operation.Record.OwningTable.SchemaName &&
			foreignKey.OwningTable.EscapedName ==
				schema.EscapeIdentifier(operation.Record.OwningTable.Name) &&
			foreignKey.EscapedName == schema.EscapeIdentifier(operation.Record.Name) {
			return true
		}
	}
	for _, preserved := range plan.PreservedForeignKeys {
		if foreignKey.OwningTable.SchemaName == preserved.ForeignKey.OwningSchemaName &&
			foreignKey.OwningTable.EscapedName ==
				schema.EscapeIdentifier(preserved.ForeignKey.OwningRelationName) &&
			foreignKey.EscapedName == schema.EscapeIdentifier(preserved.ForeignKey.Name) {
			return true
		}
	}
	return false
}

func markerObjectSchemaName(object archivalMarkerObjectIdentity) string {
	escapedName := schema.EscapeIdentifier(object.Name)
	if object.Kind == archivalMarkerObjectKindFunction {
		escapedName += "(" + markerIdentityArgument(object) + ")"
	}
	return schema.SchemaQualifiedName{SchemaName: object.SchemaName, EscapedName: escapedName}.GetName()
}

func incomingDependencyDeleteVertexID(dependency sourceSafetyIncomingDependency) (sqlVertexId, error) {
	name := schema.SchemaQualifiedName{
		SchemaName:  dependency.Dependent.SchemaName,
		EscapedName: schema.EscapeIdentifier(dependency.Dependent.Name),
	}
	switch dependency.Dependent.Kind {
	case sourceSafetyIncomingDependencyKindView:
		return buildTableVertexId(name, diffTypeDelete), nil
	case sourceSafetyIncomingDependencyKindMaterializedView:
		return buildMaterializedViewVertexId(name, diffTypeDelete), nil
	case sourceSafetyIncomingDependencyKindRoutine:
		routineName := schema.SchemaQualifiedName{
			SchemaName: dependency.Dependent.SchemaName,
			EscapedName: schema.EscapeIdentifier(dependency.Dependent.Name) +
				"(" + archivedDependencyIdentityArguments(dependency.Dependent.Identity) + ")",
		}
		return buildFunctionVertexId(routineName, diffTypeDelete), nil
	case sourceSafetyIncomingDependencyKindRowTypeConsumer:
		return buildTableVertexId(name, diffTypeDelete), nil
	default:
		return nil, fmt.Errorf("incoming %s has no ordinary deletion vertex", dependency.Dependent.Kind)
	}
}

func addSQLGraphDependency(graph *sqlGraph, dep dependency) error {
	addVertexIfNotExists(graph, dep.source)
	addVertexIfNotExists(graph, dep.target)
	if err := graph.AddEdge(dep.source.String(), dep.target.String()); err != nil {
		return fmt.Errorf("adding replacement-aware edge from %s to %s: %w", dep.source, dep.target, err)
	}
	return nil
}

func isAnyArchivalVertex(id sqlVertexId) bool {
	switch id.(type) {
	case plainTableArchivalVertexID, plainTableArchivalPhaseBarrierID, archivalIsolationVertexID:
		return true
	default:
		return false
	}
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
