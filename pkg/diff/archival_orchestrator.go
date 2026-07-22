package diff

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type archivalGenerationResult struct {
	current           schema.SchemaSnapshot
	target            schema.SchemaSnapshot
	statements        []Statement
	cleanup           globalCleanupPlanResult
	preflight         sourceSafetyPreflightResult
	dependencyClosure archivedDependencyClosureResult
	isolation         archivalIsolationPlan
}

type newArchivalGroupSpec struct {
	nameRequest archivalGroupNameAllocationRequest
	detached    *archivalDetachedSubtreeRequest
}

// GetSchemaHash returns the versioned immutable snapshot hash used by Generate.
// Callers normally use pkg/schema.GetSchemaHash or
// pkg/schema.GetSchemaHashWithArchivalPrefix instead.
func GetSchemaHash(
	ctx context.Context,
	connPool *pgxpool.Pool,
	archivalPrefix string,
	opts ...schema.GetSchemaOpt,
) (string, error) {
	if err := validateSchemaPartialArchivalPrefix(archivalPrefix); err != nil {
		return "", err
	}
	current, err := schema.GetSchemaSnapshot(ctx, connPool, opts...)
	if err != nil {
		return "", fmt.Errorf("getting schema snapshot: %w", err)
	}
	resolution, err := resolveArchivedState(archivalPrefix, current, schema.SchemaSnapshot{})
	if err != nil {
		return "", err
	}
	target := current
	target.Schema = schema.ExcludeSchemaNames(target.Schema,
		resolution.ProvisionalUntrustedSchemaNames)
	_, _, cleanup, err := trustExistingArchivalGroups(current, target, resolution)
	if err != nil {
		return "", fmt.Errorf("validating existing archival state: %w", err)
	}
	current.Schema = schema.ExcludeSchemaNames(current.Schema, cleanup.TrustedSchemaNames)
	return buildCandidatePlanSnapshotHash(current, cleanup)
}

func orchestrateArchivalGeneration(
	current schema.SchemaSnapshot,
	target schema.SchemaSnapshot,
	options *planOptions,
) (archivalGenerationResult, error) {
	resolution, err := resolveArchivedState(options.schemaPartialArchivalPrefix, current, target)
	if err != nil {
		return archivalGenerationResult{}, fmt.Errorf(
			"resolving existing archival state: %w", err,
		)
	}
	_, _, trustedExisting, err := trustExistingArchivalGroups(current, target, resolution)
	if err != nil {
		return archivalGenerationResult{}, fmt.Errorf(
			"validating existing archival state: %w", err,
		)
	}
	current.Schema = schema.ExcludeSchemaNames(current.Schema, trustedExisting.TrustedSchemaNames)

	schemaDiff, _, err := buildSchemaDiff(current.Schema, target.Schema)
	if err != nil {
		return archivalGenerationResult{}, fmt.Errorf("building managed schema diff: %w", err)
	}
	existingRelationOIDs := archivedCandidateRelationOIDSet(resolution.CandidateGroups)
	newSpecs, err := buildNewArchivalGroupSpecs(current.Inventory,
		schemaDiff.tableDiffs.deletes, existingRelationOIDs)
	if err != nil {
		return archivalGenerationResult{}, fmt.Errorf(
			"grouping table removals for archival: %w", err,
		)
	}
	nameRequests := make([]archivalGroupNameAllocationRequest, 0, len(newSpecs))
	for _, spec := range newSpecs {
		nameRequests = append(nameRequests, spec.nameRequest)
	}
	allocations, err := allocateArchivalNames(options, current.Inventory, target.Inventory, nameRequests)
	if err != nil {
		return archivalGenerationResult{}, fmt.Errorf("allocating archival names: %w", err)
	}

	proposedClosureGroups := make([]archivedDependencyClosureGroupRequest, 0, len(allocations))
	proposedRelationOIDs := archivedCandidateRelationOIDs(resolution.CandidateGroups)
	for idx := range allocations {
		relationOIDs := archivalNameRequestRelationOIDs(newSpecs[idx].nameRequest)
		proposedRelationOIDs = append(proposedRelationOIDs, relationOIDs...)
		proposedClosureGroups = append(proposedClosureGroups, archivedDependencyClosureGroupRequest{
			GroupID: allocations[idx].GroupID, TableRelationOIDs: relationOIDs,
			DependencySchemaName: allocations[idx].DependencySchemaName,
		})
	}
	slices.Sort(proposedRelationOIDs)
	proposedRelationOIDs = slices.Compact(proposedRelationOIDs)
	preflight, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		ProposedTableRelationOIDs: proposedRelationOIDs,
	})
	if err != nil {
		return archivalGenerationResult{}, fmt.Errorf(
			"running archival source safety preflight: %w", err,
		)
	}
	closure, err := planArchivedDependencyClosure(archivedDependencyClosureRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		ProposedGroups: proposedClosureGroups, CandidateGroups: resolution.CandidateGroups,
		SourcePreflight: preflight,
	})
	if err != nil {
		return archivalGenerationResult{}, fmt.Errorf(
			"planning archived dependency closure: %w", err,
		)
	}

	groupRequests, err := buildArchivalGroupRequests(current.Inventory, allocations,
		newSpecs, resolution.CandidateGroups, preflight, closure)
	if err != nil {
		return archivalGenerationResult{}, fmt.Errorf("constructing archival groups: %w", err)
	}
	regularRequest := plainTableArchivalRequest{
		CurrentInventory: current.Inventory, TargetSchema: target.Schema, Groups: groupRequests,
		SourcePreflight: preflight, DependencyClosure: closure,
	}
	groups, err := preparePlainTableArchivalGroups(regularRequest)
	if err != nil {
		return archivalGenerationResult{}, err
	}
	groups, err = finalizeGlobalCleanupComponentInputs(groups, closure)
	if err != nil {
		return archivalGenerationResult{}, fmt.Errorf(
			"finalizing cleanup component inputs: %w", err,
		)
	}
	isolationDraft, err := buildArchivalIsolationPlan(current.Inventory, target.Schema,
		preflight, closure, groups)
	if err != nil {
		return archivalGenerationResult{}, fmt.Errorf(
			"planning archival isolation metadata: %w", err,
		)
	}
	if err := applyArchivalIsolationMarkerMetadata(groups, isolationDraft); err != nil {
		return archivalGenerationResult{}, err
	}
	isolation, err := planArchivalIsolation(current.Inventory, target.Schema, preflight, closure, groups)
	if err != nil {
		return archivalGenerationResult{}, fmt.Errorf("validating archival isolation: %w", err)
	}
	cleanup, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: current.Inventory, PredictedGroups: groups,
		RootIntents: archivalCleanupRootIntents(groups), DependencyClosure: closure,
		Isolation: isolation,
	})
	if err != nil {
		return archivalGenerationResult{}, fmt.Errorf(
			"planning global archival cleanup: %w", err,
		)
	}

	dispositions, err := buildArchivalTableDispositions(schemaDiff.tableDiffs.deletes,
		cleanup.FinalizedRegularGroups)
	if err != nil {
		return archivalGenerationResult{}, err
	}
	generator := newSchemaSQLGenerator(options.randReader, options)
	generator.tableDispositions = dispositions
	generator.archivalGroups = cleanup.FinalizedRegularGroups
	generator.archivalInventory = current.Inventory
	generator.archivalIsolation = isolation
	statements, err := generator.Alter(schemaDiff)
	if err != nil {
		return archivalGenerationResult{}, fmt.Errorf(
			"generating integrated regular statements: %w", err,
		)
	}
	statements = append(statements, cleanup.MarkerUpdateStatements...)
	if err := validateGeneratedArchivalStatements(statements, dispositions); err != nil {
		return archivalGenerationResult{}, err
	}

	return archivalGenerationResult{
		current: current, target: target, statements: statements, cleanup: cleanup,
		preflight: preflight, dependencyClosure: closure, isolation: isolation,
	}, nil
}

func trustExistingArchivalGroups(
	current schema.SchemaSnapshot,
	target schema.SchemaSnapshot,
	resolution archivedStateResolution,
) (sourceSafetyPreflightResult, archivedDependencyClosureResult, globalCleanupPlanResult, error) {
	relationOIDs := archivedCandidateRelationOIDs(resolution.CandidateGroups)
	preflight, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: target, ProposedTableRelationOIDs: relationOIDs,
	})
	if err != nil {
		return sourceSafetyPreflightResult{}, archivedDependencyClosureResult{}, globalCleanupPlanResult{}, err
	}
	closure, err := planArchivedDependencyClosure(archivedDependencyClosureRequest{
		CurrentSnapshot: current, TargetSnapshot: target, CandidateGroups: resolution.CandidateGroups,
		SourcePreflight: preflight,
	})
	if err != nil {
		return sourceSafetyPreflightResult{}, archivedDependencyClosureResult{}, globalCleanupPlanResult{}, err
	}
	cleanup, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: current.Inventory, DependencyClosure: closure,
	})
	if err != nil {
		return sourceSafetyPreflightResult{}, archivedDependencyClosureResult{}, globalCleanupPlanResult{}, err
	}
	return preflight, closure, cleanup, nil
}

func archivedCandidateRelationOIDs(groups []structurallyValidArchivedCandidateGroup) []uint32 {
	var result []uint32
	for _, group := range groups {
		result = append(result, orchestrationMarkerRelationOIDs(group.Marker)...)
	}
	slices.Sort(result)
	return slices.Compact(result)
}

func archivedCandidateRelationOIDSet(groups []structurallyValidArchivedCandidateGroup) map[uint32]struct{} {
	result := make(map[uint32]struct{})
	for _, oid := range archivedCandidateRelationOIDs(groups) {
		result[oid] = struct{}{}
	}
	return result
}

func buildNewArchivalGroupSpecs(
	inventory schema.CatalogInventory,
	deletedTables []schema.Table,
	existingRelationOIDs map[uint32]struct{},
) ([]newArchivalGroupSpec, error) {
	deletedOIDs := make(map[uint32]struct{}, len(deletedTables))
	for _, table := range deletedTables {
		relation, err := catalogRelationForModeledTable(inventory, table)
		if err != nil {
			return nil, err
		}
		if _, existing := existingRelationOIDs[relation.OID]; existing {
			continue
		}
		kind, tableLike := inventory.ClassifyTable(relation.OID)
		if !tableLike || !isSupportedArchivalTableKind(kind) {
			return nil, fmt.Errorf("removed table %s has unsupported archival classification %q",
				table.GetName(), kind)
		}
		deletedOIDs[relation.OID] = struct{}{}
	}

	parentsByChild := make(map[uint32][]uint32)
	for _, edge := range inventory.InheritanceEdges {
		parentsByChild[edge.ChildRelationOID] = append(
			parentsByChild[edge.ChildRelationOID], edge.ParentRelationOID,
		)
	}
	var roots []uint32
	for oid := range deletedOIDs {
		deletedParent := false
		for _, parentOID := range parentsByChild[oid] {
			if _, deleted := deletedOIDs[parentOID]; deleted {
				deletedParent = true
			}
		}
		if !deletedParent {
			roots = append(roots, oid)
		}
	}
	slices.SortFunc(roots, func(a, b uint32) int {
		return compareCatalogRelations(*catalogRelationWithOID(inventory, a),
			*catalogRelationWithOID(inventory, b))
	})
	covered := make(map[uint32]struct{}, len(deletedOIDs))
	var specs []newArchivalGroupSpec
	for _, rootOID := range roots {
		kind, _ := inventory.ClassifyTable(rootOID)
		request := archivalGroupNameAllocationRequest{
			RootRelationOID: rootOID,
			Members:         []archivalPhysicalMemberNameAllocationRequest{{RelationOID: rootOID}},
		}
		if kind == schema.CatalogTableKindPartitioned ||
			kind == schema.CatalogTableKindDeclarativePartition {
			var err error
			request, err = buildCompletePartitionTreeArchivalNameAllocationRequest(inventory, rootOID)
			if err != nil {
				return nil, err
			}
		}
		for _, member := range request.Members {
			if _, deleted := deletedOIDs[member.RelationOID]; !deleted {
				relation := catalogRelationWithOID(inventory, member.RelationOID)
				return nil, fmt.Errorf("archival partition group rooted at OID %d includes retained or excluded member %s.%s",
					rootOID, relation.SchemaName, relation.Name)
			}
			if _, duplicate := covered[member.RelationOID]; duplicate {
				return nil, fmt.Errorf("removed relation OID %d belongs to multiple archival groups", member.RelationOID)
			}
			covered[member.RelationOID] = struct{}{}
		}
		spec := newArchivalGroupSpec{nameRequest: request}
		parents := parentsByChild[rootOID]
		if len(parents) > 1 {
			return nil, fmt.Errorf("removed partition root OID %d has multiple inheritance parents", rootOID)
		}
		if len(parents) == 1 {
			if _, parentDeleted := deletedOIDs[parents[0]]; !parentDeleted {
				spec.detached = &archivalDetachedSubtreeRequest{
					RootRelationOID: rootOID, ParentRelationOID: parents[0],
				}
			}
		}
		specs = append(specs, spec)
	}
	if len(covered) != len(deletedOIDs) {
		return nil, fmt.Errorf("not every removed table was assigned to an archival group")
	}
	return specs, nil
}

func catalogRelationForModeledTable(
	inventory schema.CatalogInventory,
	table schema.Table,
) (schema.CatalogRelation, error) {
	var matches []schema.CatalogRelation
	for _, relation := range inventory.Relations {
		if relation.SchemaName == table.SchemaName &&
			schema.EscapeIdentifier(relation.Name) == table.EscapedName && isTableRelationKind(relation.Kind) {
			matches = append(matches, relation)
		}
	}
	if len(matches) != 1 {
		return schema.CatalogRelation{}, fmt.Errorf(
			"modeled table %s has %d catalog relation identities",
			table.GetName(), len(matches),
		)
	}
	return matches[0], nil
}

func archivalNameRequestRelationOIDs(request archivalGroupNameAllocationRequest) []uint32 {
	result := make([]uint32, 0, len(request.Members))
	for _, member := range request.Members {
		result = append(result, member.RelationOID)
	}
	slices.Sort(result)
	return result
}

func buildArchivalGroupRequests(
	inventory schema.CatalogInventory,
	allocations []archivalGroupNameAllocation,
	specs []newArchivalGroupSpec,
	candidates []structurallyValidArchivedCandidateGroup,
	preflight sourceSafetyPreflightResult,
	closure archivedDependencyClosureResult,
) ([]plainTableArchivalGroupRequest, error) {
	requests := make([]plainTableArchivalGroupRequest, 0, len(allocations)+len(candidates))
	markers := make([]archivalMarkerV1, 0, len(allocations))
	for idx := range allocations {
		marker, err := buildAllocatedArchivalMarker(inventory, allocations[idx], specs[idx].detached)
		if err != nil {
			return nil, err
		}
		markers = append(markers, marker)
	}
	groupByRelationOID := make(map[uint32]preparedArchivalGroup)
	for _, marker := range markers {
		group := preparedArchivalGroup{id: marker.GroupID, marker: marker}
		if err := populatePreparedArchivalMembers(&group); err != nil {
			return nil, err
		}
		for _, member := range marker.Members {
			groupByRelationOID[member.SourceTable.OID] = group
		}
	}
	for markerIdx := range markers {
		marker := &markers[markerIdx]
		for memberIdx := range marker.Members {
			member := &marker.Members[memberIdx]
			move, err := inventory.ExpectedTableMove(member.SourceTable.OID)
			if err != nil {
				return nil, err
			}
			move = filterPlainTableIsolationObjects(inventory, move, preflight, groupByRelationOID)
			move = filterLostParentClonedTriggers(move, *marker, *member)
			member.AutomaticallyMovedObjects = markerObjectsFromCatalog(move.CleanupSchemaObjects,
				member.CleanupTable.SchemaName)
			member.AttachedObjects = markerObjectsFromCatalog(move.AttachedObjects,
				member.CleanupTable.SchemaName)
			member.ExplicitlyMovedObjects = markerObjectsFromCatalog(move.ExplicitMoveObjects,
				member.CleanupTable.SchemaName)
			member.InternalToastObjects = markerObjectsFromCatalog(move.InternalObjects, "")
		}
		filterIsolatedPartitionEdgeTriggers(marker)
		marker.ExclusiveDependencyObjects = nil
		for _, assignment := range closure.Assignments {
			if assignment.GroupID == marker.GroupID {
				marker.ExclusiveDependencyObjects = append(marker.ExclusiveDependencyObjects,
					assignment.Destination)
			}
		}
		marker.SharedCleanupComponentGroupEdges = incidentArchivedDependencyEdges(
			marker.GroupID, closure.SharedGroupEdges,
		)
		*marker = canonicalizeArchivalMarker(*marker)
		text, err := marshalArchivalMarker(*marker)
		if err != nil {
			return nil, err
		}
		requests = append(requests, plainTableArchivalGroupRequest{
			Allocation: &allocations[markerIdx], FinalizedMarker: text,
			DetachedSubtree: specs[markerIdx].detached,
		})
	}
	for idx := range candidates {
		candidate := &candidates[idx]
		if candidate.State == archivedCandidateGroupStateCompleteCandidate {
			continue
		}
		text, err := marshalArchivalMarker(candidate.Marker)
		if err != nil {
			return nil, err
		}
		var detached *archivalDetachedSubtreeRequest
		if len(candidate.Marker.LostParentAttachments) == 1 {
			root, err := orchestrationMarkerRoot(candidate.Marker)
			if err != nil {
				return nil, err
			}
			detached = &archivalDetachedSubtreeRequest{
				RootRelationOID:   root.SourceTable.OID,
				ParentRelationOID: candidate.Marker.LostParentAttachments[0].ParentTable.OID,
			}
		}
		requests = append(requests, plainTableArchivalGroupRequest{
			FinalizedMarker: text, Resume: &candidate.Resume, DetachedSubtree: detached,
		})
	}
	return requests, nil
}

func filterIsolatedPartitionEdgeTriggers(marker *archivalMarkerV1) {
	attachedTriggerOIDs := make(map[uint32]struct{})
	for _, member := range marker.Members {
		for _, object := range member.AttachedObjects {
			if object.Kind == archivalMarkerObjectKindTrigger {
				attachedTriggerOIDs[object.OID] = struct{}{}
			}
		}
	}
	for idx := range marker.PartitionEdges {
		edge := &marker.PartitionEdges[idx]
		edge.ClonedTriggers = slices.DeleteFunc(edge.ClonedTriggers,
			func(trigger archivalMarkerClonedTriggerV1) bool {
				_, parentAttached := attachedTriggerOIDs[trigger.ParentTrigger.OID]
				_, childAttached := attachedTriggerOIDs[trigger.ChildTrigger.OID]
				return !parentAttached || !childAttached
			})
	}
}

func buildAllocatedArchivalMarker(
	inventory schema.CatalogInventory,
	allocation archivalGroupNameAllocation,
	detached *archivalDetachedSubtreeRequest,
) (archivalMarkerV1, error) {
	marker := archivalMarkerV1{
		Version: archivalMarkerVersion, GroupID: allocation.GroupID,
		ExclusiveDependencySchemas: []archivalMarkerSchemaIdentity{{
			Name: allocation.DependencySchemaName,
		}},
		CleanupDigest: cleanupOperationDigest("sha256:" + strings.Repeat("0", 64)),
	}
	memberIDByOID := make(map[uint32]string, len(allocation.Members))
	for _, allocated := range allocation.Members {
		relation, err := uniqueRelationByOID(inventory, allocated.RelationOID)
		if err != nil {
			return archivalMarkerV1{}, err
		}
		memberID := fmt.Sprintf("member-%d", relation.OID)
		memberIDByOID[relation.OID] = memberID
		marker.Members = append(marker.Members, archivalMarkerMemberV1{
			MemberID: memberID,
			SourceTable: archivalMarkerObjectIdentity{
				Kind: archivalMarkerObjectKindTable, OID: relation.OID,
				SchemaName: relation.SchemaName, Name: relation.Name,
			},
			CleanupTable: archivalMarkerObjectIdentity{
				Kind: archivalMarkerObjectKindTable, OID: relation.OID,
				SchemaName: allocated.CleanupSchemaName, Name: relation.Name,
			},
		})
	}
	for _, edge := range inventory.InheritanceEdges {
		parentID, parentFound := memberIDByOID[edge.ParentRelationOID]
		childID, childFound := memberIDByOID[edge.ChildRelationOID]
		if parentFound && childFound {
			marker.PartitionEdges = append(marker.PartitionEdges, archivalMarkerPartitionEdgeV1{
				ParentMemberID: parentID, ChildMemberID: childID, BoundExpression: "pending",
			})
		}
	}
	if err := populateArchivalMarkerPartitionMetadata(inventory, &marker); err != nil {
		return archivalMarkerV1{}, err
	}
	if detached != nil {
		group := preparedArchivalGroup{id: marker.GroupID, marker: marker}
		if err := populatePreparedArchivalMembers(&group); err != nil {
			return archivalMarkerV1{}, err
		}
		parent, err := uniqueRelationByOID(inventory, detached.ParentRelationOID)
		if err != nil {
			return archivalMarkerV1{}, err
		}
		root := preparedArchivalMemberByID(group.members, group.rootMemberID).marker
		lost, err := archivalLostParentAttachmentMetadata(inventory, group, root,
			archivalMarkerObjectIdentity{
				Kind: archivalMarkerObjectKindTable, OID: parent.OID,
				SchemaName: parent.SchemaName, Name: parent.Name,
			})
		if err != nil {
			return archivalMarkerV1{}, err
		}
		marker.LostParentAttachments = []archivalMarkerLostParentAttachmentV1{lost}
		removeLostParentClonedTriggersFromMarker(&marker)
	}
	return canonicalizeArchivalMarker(marker), nil
}

func orchestrationMarkerRelationOIDs(marker archivalMarkerV1) []uint32 {
	result := make([]uint32, 0, len(marker.Members))
	for _, member := range marker.Members {
		result = append(result, member.SourceTable.OID)
	}
	slices.Sort(result)
	return result
}

func orchestrationMarkerRoot(marker archivalMarkerV1) (archivalMarkerMemberV1, error) {
	children := make(map[string]struct{}, len(marker.PartitionEdges))
	for _, edge := range marker.PartitionEdges {
		children[edge.ChildMemberID] = struct{}{}
	}
	var root archivalMarkerMemberV1
	for _, member := range marker.Members {
		if _, child := children[member.MemberID]; child {
			continue
		}
		if root.MemberID != "" {
			return archivalMarkerMemberV1{}, fmt.Errorf(
				"archival group %q has multiple marker roots", marker.GroupID,
			)
		}
		root = member
	}
	if root.MemberID == "" {
		return archivalMarkerMemberV1{}, fmt.Errorf("archival group %q has no marker root", marker.GroupID)
	}
	return root, nil
}

func applyArchivalIsolationMarkerMetadata(
	groups []preparedArchivalGroup,
	isolation archivalIsolationPlan,
) error {
	for idx := range groups {
		groupPlan, ok := archivalIsolationGroupPlanByID(isolation.Groups, groups[idx].id)
		if !ok {
			return fmt.Errorf("isolation plan is missing archival group %q", groups[idx].id)
		}
		groups[idx].marker.OriginalACLs = groupPlan.OriginalACLs
		groups[idx].marker.OriginalForeignKeys = groupPlan.OriginalForeignKeys
		groups[idx].marker.OriginalPublicationMemberships = groupPlan.OriginalPublications
		groups[idx].marker.ExclusiveDependencyObjects = groupPlan.Dependencies
		groups[idx].marker.SharedCleanupComponentGroupEdges = groupPlan.SharedDependencyEdges
		groups[idx].marker = canonicalizeArchivalMarker(groups[idx].marker)
		text, err := marshalArchivalMarker(groups[idx].marker)
		if err != nil {
			return fmt.Errorf("encoding isolation marker for group %q: %w", groups[idx].id, err)
		}
		groups[idx].markerText = text
	}
	return nil
}

func buildArchivalTableDispositions(
	deletedTables []schema.Table,
	groups []preparedArchivalGroup,
) (tableDispositions, error) {
	dispositions := make(tableDispositions)
	for _, group := range groups {
		for _, member := range group.members {
			kind := tableDispositionKindCleanupOnly
			if member.remainingMove != nil {
				kind = tableDispositionKindArchivalMove
			}
			dispositions[markerTableName(member.marker.SourceTable).GetName()] = tableDisposition{
				Kind: kind, GroupID: group.id,
			}
		}
	}
	for _, table := range deletedTables {
		disposition, ok := dispositions[table.GetName()]
		if !ok {
			return nil, fmt.Errorf("deleted table %s has no archival disposition", table.GetName())
		}
		if disposition.Kind == tableDispositionKindPhysicalDelete ||
			disposition.Kind == tableDispositionKindUnknown {
			return nil, fmt.Errorf("deleted table %s has a destructive or unknown disposition", table.GetName())
		}
	}
	return dispositions, nil
}

func validateGeneratedArchivalStatements(
	statements []Statement,
	dispositions tableDispositions,
) error {
	for tableName, disposition := range dispositions {
		if disposition.Kind == tableDispositionKindPhysicalDelete {
			return fmt.Errorf("table %s retained a physical-delete disposition", tableName)
		}
	}
	for idx, statement := range statements {
		upper := strings.ToUpper(strings.TrimSpace(statement.DDL))
		if strings.HasPrefix(upper, "DROP TABLE ") || strings.HasPrefix(upper, "TRUNCATE ") ||
			strings.HasPrefix(upper, "DELETE ") || strings.Contains(upper, "DROP SCHEMA") && strings.Contains(upper, "CASCADE") {
			return fmt.Errorf("ordinary archival statement %d contains forbidden destructive SQL", idx)
		}
		for _, hazard := range statement.Hazards {
			if hazard.Type == MigrationHazardType("TABLE_REMOVAL") {
				return fmt.Errorf("ordinary archival statement %d carries TABLE_REMOVAL", idx)
			}
		}
	}
	return nil
}
