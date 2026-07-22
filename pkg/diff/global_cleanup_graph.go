package diff

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"unicode"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

// globalCleanupPlanRequest is the dormant Stage 16 boundary. PredictedGroups
// are the post-regular groups prepared by Stages 12-15; complete existing
// groups come from DependencyClosure.
type globalCleanupPlanRequest struct {
	CurrentInventory  schema.CatalogInventory
	PredictedGroups   []preparedArchivalGroup
	RootIntents       []archivalCleanupRootIntent
	DependencyClosure archivedDependencyClosureResult
	Isolation         archivalIsolationPlan
}

type globalCleanupFinalizedMarker struct {
	GroupID archivalGroupID
	Payload archivalMarkerV1
	Text    string
}

type globalCleanupPlanResult struct {
	Operations             []cleanupOperationV1
	CleanupStatements      []Statement
	FinalizedMarkers       []globalCleanupFinalizedMarker
	FinalizedRegularGroups []preparedArchivalGroup
	MarkerUpdateStatements []Statement
	TrustedGroupIDs        []archivalGroupID
	TrustedSchemaNames     []string
}

type globalCleanupGroup struct {
	id            archivalGroupID
	marker        archivalMarkerV1
	existing      *structurallyValidArchivedCandidateGroup
	predicted     *preparedArchivalGroup
	cleanupRoot   archivalMarkerObjectIdentity
	hasIndexes    bool
	oldMarkerText string
}

type globalCleanupDependency struct {
	object        archivalMarkerObjectIdentity
	groupIDs      []archivalGroupID
	ownerGroupID  archivalGroupID
	sourceAddress schema.CatalogDependencyObject
}

type globalCleanupPlannedOperation struct {
	operation cleanupOperationV1
	groupIDs  []archivalGroupID
}

var (
	migrationHazardCleanupTableDeletesData = MigrationHazard{
		Type:    MigrationHazardTypeDeletesData,
		Message: "Dropping the retained table permanently deletes its rows.",
	}
	migrationHazardCleanupTableLock = MigrationHazard{
		Type:    MigrationHazardTypeAcquiresAccessExclusiveLock,
		Message: "Dropping the retained table acquires an ACCESS EXCLUSIVE lock on it.",
	}
	migrationHazardCleanupDependencyDeletesData = MigrationHazard{
		Type:    MigrationHazardTypeDeletesData,
		Message: "Dropping the retained standalone sequence permanently deletes its current counter.",
	}
	migrationHazardCleanupDependencyCorrectness = MigrationHazard{
		Type:    MigrationHazardTypeCorrectness,
		Message: "Dropping the retained dependency removes behavior or type semantics used by archived objects.",
	}
	migrationHazardCleanupDependencyLock = MigrationHazard{
		Type:    MigrationHazardTypeAcquiresAccessExclusiveLock,
		Message: "Dropping the retained dependency acquires an ACCESS EXCLUSIVE lock on that object.",
	}
	migrationHazardCleanupSchemaLock = MigrationHazard{
		Type:    MigrationHazardTypeAcquiresAccessExclusiveLock,
		Message: "Dropping the empty archival schema acquires an ACCESS EXCLUSIVE lock on the schema.",
	}
)

// planGlobalCleanup is dormant until archival activation. Canonical operations
// are authoritative: marker digests and rendered statements are both derived
// from the same ordered operation graph.
func planGlobalCleanup(request globalCleanupPlanRequest) (globalCleanupPlanResult, error) {
	inventory := request.CurrentInventory.Normalize()
	groups, err := prepareGlobalCleanupGroups(inventory, request)
	if err != nil {
		return globalCleanupPlanResult{}, err
	}
	if len(groups) == 0 {
		if len(request.DependencyClosure.Objects) > 0 ||
			len(request.DependencyClosure.Assignments) > 0 ||
			len(request.Isolation.DependencyMoves) > 0 {
			return globalCleanupPlanResult{}, fmt.Errorf(
				"cleanup dependency state exists without an archival group",
			)
		}
		return globalCleanupPlanResult{}, nil
	}

	finalDependencies, err := finalGlobalCleanupDependencies(groups, request.DependencyClosure)
	if err != nil {
		return globalCleanupPlanResult{}, err
	}
	if err := validateGlobalCleanupDependencyMoves(inventory, finalDependencies,
		request.DependencyClosure.Assignments, request.Isolation.DependencyMoves); err != nil {
		return globalCleanupPlanResult{}, err
	}

	existingGroups := existingGlobalCleanupGroups(groups)
	if len(existingGroups) > 0 {
		oldDependencies, err := previousGlobalCleanupDependencies(existingGroups, finalDependencies)
		if err != nil {
			return globalCleanupPlanResult{}, err
		}
		oldEdges, err := globalCleanupEdgesFromDependencies(oldDependencies)
		if err != nil {
			return globalCleanupPlanResult{}, err
		}
		if err := validateGlobalCleanupMarkerEdges(existingGroups, oldEdges); err != nil {
			return globalCleanupPlanResult{}, err
		}
		oldOperations, err := buildGlobalCleanupOperations(inventory, existingGroups, oldDependencies)
		if err != nil {
			return globalCleanupPlanResult{}, err
		}
		if err := validatePreviousGlobalCleanupDigests(existingGroups, oldEdges, oldOperations); err != nil {
			return globalCleanupPlanResult{}, err
		}
	}

	finalEdges, err := globalCleanupEdgesFromDependencies(finalDependencies)
	if err != nil {
		return globalCleanupPlanResult{}, err
	}
	declaredFinalEdges := canonicalArchivedDependencyGroupEdges(
		request.DependencyClosure.SharedGroupEdges,
	)
	if !slices.Equal(finalEdges, declaredFinalEdges) {
		return globalCleanupPlanResult{}, fmt.Errorf(
			"stage 11 shared dependency edges do not match the global cleanup graph",
		)
	}
	if err := validateGlobalCleanupComponentChanges(groups, finalDependencies, finalEdges); err != nil {
		return globalCleanupPlanResult{}, err
	}

	plannedOperations, err := buildGlobalCleanupOperations(inventory, groups, finalDependencies)
	if err != nil {
		return globalCleanupPlanResult{}, err
	}
	orderedOperations, err := canonicalizeCleanupOperations(
		globalCleanupOperations(plannedOperations),
	)
	if err != nil {
		return globalCleanupPlanResult{}, fmt.Errorf(
			"canonicalizing global cleanup graph: %w", err,
		)
	}

	finalMarkers, err := finalizeGlobalCleanupMarkers(
		groups, finalDependencies, finalEdges, plannedOperations, request.Isolation,
	)
	if err != nil {
		return globalCleanupPlanResult{}, err
	}
	statements, err := renderGlobalCleanupStatements(orderedOperations, groups)
	if err != nil {
		return globalCleanupPlanResult{}, err
	}

	result := globalCleanupPlanResult{
		Operations:        cloneCleanupOperations(orderedOperations),
		CleanupStatements: statements,
		FinalizedMarkers:  finalMarkers,
	}
	finalMarkerByGroup := make(map[archivalGroupID]globalCleanupFinalizedMarker, len(finalMarkers))
	for _, finalized := range finalMarkers {
		finalMarkerByGroup[finalized.GroupID] = finalized
		result.TrustedGroupIDs = append(result.TrustedGroupIDs, finalized.GroupID)
		result.TrustedSchemaNames = append(result.TrustedSchemaNames,
			archivedMarkerSchemaNames(finalized.Payload)...)
	}
	for _, group := range groups {
		if group.predicted == nil {
			continue
		}
		prepared := clonePreparedArchivalGroup(*group.predicted)
		finalized := finalMarkerByGroup[group.id]
		prepared.marker = finalized.Payload
		prepared.markerText = finalized.Text
		result.FinalizedRegularGroups = append(result.FinalizedRegularGroups, prepared)
	}
	for _, group := range groups {
		if group.existing == nil || group.predicted != nil {
			continue
		}
		finalized := finalMarkerByGroup[group.id]
		if finalized.Text != group.oldMarkerText {
			result.MarkerUpdateStatements = append(result.MarkerUpdateStatements,
				renderGlobalCleanupMarkerUpdate(group.existing.SchemaNames, finalized.Text))
		}
	}
	slices.Sort(result.TrustedSchemaNames)
	result.TrustedSchemaNames = slices.Compact(result.TrustedSchemaNames)
	return result, nil
}

// finalizeGlobalCleanupComponentInputs resolves the marker/component half of
// the Stage 14/16 cycle before isolation planning. planGlobalCleanup then
// computes the digest from the resulting authoritative operation graph.
func finalizeGlobalCleanupComponentInputs(
	groups []preparedArchivalGroup,
	closure archivedDependencyClosureResult,
) ([]preparedArchivalGroup, error) {
	result := make([]preparedArchivalGroup, 0, len(groups))
	objectsByOwner := make(map[archivalGroupID][]archivalMarkerObjectIdentity)
	seenAssignments := make(map[uint32]struct{})
	for _, assignment := range closure.Assignments {
		if _, duplicate := seenAssignments[assignment.Source.OID]; duplicate {
			return nil, fmt.Errorf("cleanup dependency OID %d has duplicate component assignments", assignment.Source.OID)
		}
		seenAssignments[assignment.Source.OID] = struct{}{}
		objectsByOwner[assignment.GroupID] = append(
			objectsByOwner[assignment.GroupID], cloneMarkerObject(assignment.Destination),
		)
	}
	for _, group := range groups {
		prepared := clonePreparedArchivalGroup(group)
		prepared.marker.ExclusiveDependencyObjects =
			canonicalMarkerObjects(objectsByOwner[group.id])
		prepared.marker.SharedCleanupComponentGroupEdges = incidentArchivedDependencyEdges(
			group.id, closure.SharedGroupEdges,
		)
		prepared.marker = canonicalizeArchivalMarker(prepared.marker)
		markerText, err := marshalArchivalMarker(prepared.marker)
		if err != nil {
			return nil, fmt.Errorf("finalizing cleanup component input for group %q: %w", group.id, err)
		}
		prepared.markerText = markerText
		result = append(result, prepared)
	}
	slices.SortFunc(result, func(a, b preparedArchivalGroup) int { return cmp.Compare(a.id, b.id) })
	return result, nil
}

func prepareGlobalCleanupGroups(
	inventory schema.CatalogInventory,
	request globalCleanupPlanRequest,
) ([]globalCleanupGroup, error) {
	byID := make(map[archivalGroupID]*globalCleanupGroup)
	for idx := range request.PredictedGroups {
		prepared := clonePreparedArchivalGroup(request.PredictedGroups[idx])
		if _, duplicate := byID[prepared.id]; duplicate {
			return nil, fmt.Errorf("predicted cleanup group %q is duplicated", prepared.id)
		}
		byID[prepared.id] = &globalCleanupGroup{
			id: prepared.id, marker: canonicalizeArchivalMarker(prepared.marker),
			predicted: &prepared, cleanupRoot: cloneMarkerObject(prepared.cleanupRoot),
			hasIndexes: archivalMarkerHasIndexes(prepared.marker),
		}
	}
	for _, validated := range request.DependencyClosure.DependencyValidatedCandidateGroups {
		candidate := validated.Candidate
		if err := validateGlobalCleanupCandidateCopies(inventory, candidate); err != nil {
			return nil, err
		}
		markerText, err := marshalArchivalMarker(candidate.Marker)
		if err != nil {
			return nil, fmt.Errorf("canonicalizing candidate marker for group %q: %w", candidate.GroupID, err)
		}
		group := byID[candidate.GroupID]
		if group == nil {
			root, err := globalCleanupRootFromMarker(candidate.Marker)
			if err != nil {
				return nil, err
			}
			group = &globalCleanupGroup{
				id: candidate.GroupID, marker: canonicalizeArchivalMarker(candidate.Marker),
				cleanupRoot: root, hasIndexes: archivalMarkerHasIndexes(candidate.Marker),
			}
			byID[candidate.GroupID] = group
		} else if group.predicted == nil || group.predicted.resume == nil {
			return nil, fmt.Errorf("existing cleanup group %q conflicts with a new predicted group", candidate.GroupID)
		}
		candidateCopy := candidate
		group.existing = &candidateCopy
		group.oldMarkerText = markerText
	}

	groups := make([]globalCleanupGroup, 0, len(byID))
	for _, group := range byID {
		if group.predicted != nil && group.predicted.resume != nil && group.existing == nil {
			return nil, fmt.Errorf("resumed cleanup group %q has no Stage 11 dependency-validated candidate", group.id)
		}
		groups = append(groups, *group)
	}
	slices.SortFunc(groups, func(a, b globalCleanupGroup) int { return cmp.Compare(a.id, b.id) })

	expectedIDs := make([]archivalGroupID, 0, len(groups))
	for _, group := range groups {
		expectedIDs = append(expectedIDs, group.id)
	}
	validatedIDs := slices.Clone(request.DependencyClosure.ValidatedGroupIDs)
	slices.Sort(validatedIDs)
	if !slices.Equal(expectedIDs, validatedIDs) {
		return nil, fmt.Errorf("stage 11 dependency closure did not validate exactly the global cleanup groups")
	}
	if err := validateGlobalCleanupRootIntents(groups, request.RootIntents); err != nil {
		return nil, err
	}
	return groups, nil
}

func validateGlobalCleanupCandidateCopies(
	inventory schema.CatalogInventory,
	candidate structurallyValidArchivedCandidateGroup,
) error {
	if candidate.GroupID != candidate.Marker.GroupID {
		return fmt.Errorf("candidate group %q marker declares group %q", candidate.GroupID, candidate.Marker.GroupID)
	}
	markerText, err := marshalArchivalMarker(candidate.Marker)
	if err != nil {
		return fmt.Errorf("validating existing cleanup marker for group %q: %w", candidate.GroupID, err)
	}
	expectedSchemas := archivedMarkerSchemaNames(candidate.Marker)
	actualSchemas := slices.Clone(candidate.SchemaNames)
	slices.Sort(actualSchemas)
	if !slices.Equal(expectedSchemas, actualSchemas) {
		return fmt.Errorf("candidate cleanup group %q schema names do not match its marker", candidate.GroupID)
	}
	for _, schemaName := range expectedSchemas {
		catalogSchema := catalogSchemaWithName(inventory, schemaName)
		if catalogSchema == nil {
			return fmt.Errorf("candidate cleanup group %q is missing marker copy on schema %q", candidate.GroupID, schemaName)
		}
		if catalogSchema.Comment != markerText {
			return fmt.Errorf("candidate cleanup group %q has inconsistent marker copy on schema %q", candidate.GroupID, schemaName)
		}
	}
	return nil
}

func validateGlobalCleanupRootIntents(
	groups []globalCleanupGroup,
	intents []archivalCleanupRootIntent,
) error {
	expected := make(map[archivalGroupID]archivalMarkerObjectIdentity)
	for _, group := range groups {
		if group.predicted != nil {
			expected[group.id] = group.cleanupRoot
		}
	}
	if len(intents) != len(expected) {
		return fmt.Errorf("stage 15 supplied %d cleanup-root intents for %d predicted groups", len(intents), len(expected))
	}
	for _, intent := range intents {
		root, ok := expected[intent.GroupID]
		if !ok {
			return fmt.Errorf("cleanup-root intent references unpredicted group %q", intent.GroupID)
		}
		if compareMarkerObjects(root, intent.Root) != 0 {
			return fmt.Errorf("cleanup-root intent for group %q does not match its predicted root", intent.GroupID)
		}
		delete(expected, intent.GroupID)
	}
	if len(expected) != 0 {
		return fmt.Errorf("a predicted archival group is missing its cleanup-root intent")
	}
	return nil
}

func finalGlobalCleanupDependencies(
	groups []globalCleanupGroup,
	closure archivedDependencyClosureResult,
) ([]globalCleanupDependency, error) {
	groupIDs := make(map[archivalGroupID]struct{}, len(groups))
	for _, group := range groups {
		groupIDs[group.id] = struct{}{}
	}
	assignmentsByOID := make(map[uint32][]archivedDependencyAssignment)
	for _, assignment := range closure.Assignments {
		assignmentsByOID[assignment.Source.OID] =
			append(assignmentsByOID[assignment.Source.OID], assignment)
	}
	seenObjects := make(map[uint32]struct{})
	var dependencies []globalCleanupDependency
	for _, object := range closure.Objects {
		if object.Classification != archivedDependencyClassificationExclusiveMovable &&
			object.Classification != archivedDependencyClassificationSharedArchivedOnly {
			continue
		}
		if _, duplicate := seenObjects[object.Identity.OID]; duplicate {
			return nil, fmt.Errorf("cleanup dependency OID %d has duplicate ownership", object.Identity.OID)
		}
		seenObjects[object.Identity.OID] = struct{}{}
		assignments := assignmentsByOID[object.Identity.OID]
		if len(assignments) != 1 {
			return nil, fmt.Errorf("cleanup dependency OID %d has %d move assignments; exactly one is required",
				object.Identity.OID, len(assignments))
		}
		assignment := assignments[0]
		if assignment.GroupID != object.OwnerGroupID ||
			compareMarkerObjects(assignment.Source, object.Identity) != 0 {
			return nil, fmt.Errorf("cleanup dependency OID %d has an inconsistent owner assignment", object.Identity.OID)
		}
		if _, ok := groupIDs[assignment.GroupID]; !ok {
			return nil, fmt.Errorf("cleanup dependency OID %d is assigned to unknown group %q",
				object.Identity.OID, assignment.GroupID)
		}
		owners := slices.Clone(object.GroupIDs)
		slices.Sort(owners)
		owners = slices.Compact(owners)
		if len(owners) == 0 {
			return nil, fmt.Errorf("cleanup dependency OID %d has no referencing cleanup roots", object.Identity.OID)
		}
		for _, groupID := range owners {
			if _, ok := groupIDs[groupID]; !ok {
				return nil, fmt.Errorf("cleanup dependency OID %d references unknown group %q", object.Identity.OID, groupID)
			}
		}
		dependencies = append(dependencies, globalCleanupDependency{
			object: cloneMarkerObject(assignment.Destination), groupIDs: owners,
			ownerGroupID: assignment.GroupID, sourceAddress: object.Address,
		})
	}
	for oid, assignments := range assignmentsByOID {
		if _, ok := seenObjects[oid]; !ok || len(assignments) != 1 {
			return nil, fmt.Errorf("cleanup dependency assignment for OID %d has no unique retained-only closure object", oid)
		}
	}
	slices.SortFunc(dependencies, compareGlobalCleanupDependencies)
	return dependencies, nil
}

func previousGlobalCleanupDependencies(
	groups []globalCleanupGroup,
	final []globalCleanupDependency,
) ([]globalCleanupDependency, error) {
	existingIDs := make(map[archivalGroupID]struct{}, len(groups))
	finalByOID := make(map[uint32]globalCleanupDependency, len(final))
	for _, group := range groups {
		existingIDs[group.id] = struct{}{}
	}
	for _, dependency := range final {
		finalByOID[dependency.object.OID] = dependency
	}
	seen := make(map[uint32]archivalGroupID)
	var previous []globalCleanupDependency
	for _, group := range groups {
		for _, object := range group.marker.ExclusiveDependencyObjects {
			if owner, duplicate := seen[object.OID]; duplicate {
				return nil, fmt.Errorf("cleanup dependency OID %d is owned by both groups %q and %q",
					object.OID, owner, group.id)
			}
			seen[object.OID] = group.id
			finalDependency, ok := finalByOID[object.OID]
			if !ok || finalDependency.ownerGroupID != group.id ||
				compareMarkerObjects(finalDependency.object, object) != 0 {
				return nil, fmt.Errorf("existing cleanup group %q has stale unexplained dependency OID %d", group.id, object.OID)
			}
			owners := slices.DeleteFunc(slices.Clone(finalDependency.groupIDs), func(id archivalGroupID) bool {
				_, exists := existingIDs[id]
				return !exists
			})
			if len(owners) == 0 {
				return nil, fmt.Errorf("existing cleanup dependency OID %d has no existing referencing root", object.OID)
			}
			previous = append(previous, globalCleanupDependency{
				object: cloneMarkerObject(object), groupIDs: owners, ownerGroupID: group.id,
				sourceAddress: finalDependency.sourceAddress,
			})
		}
	}
	slices.SortFunc(previous, compareGlobalCleanupDependencies)
	return previous, nil
}

func validateGlobalCleanupDependencyMoves(
	inventory schema.CatalogInventory,
	dependencies []globalCleanupDependency,
	assignments []archivedDependencyAssignment,
	moves []archivalObjectMoveOperation,
) error {
	requiredByOID := make(map[uint32]archivedDependencyAssignment)
	for _, dependency := range dependencies {
		var assignment *archivedDependencyAssignment
		for idx := range assignments {
			if assignments[idx].Source.OID == dependency.object.OID {
				if assignment != nil {
					return fmt.Errorf("cleanup dependency OID %d is assigned more than once", dependency.object.OID)
				}
				assignment = &assignments[idx]
			}
		}
		if assignment == nil {
			return fmt.Errorf("cleanup dependency OID %d has no move assignment", dependency.object.OID)
		}
		actual, err := findCurrentCatalogObject(inventory, assignment.Source)
		if err != nil {
			return err
		}
		switch actual.SchemaName {
		case assignment.Source.SchemaName:
			requiredByOID[assignment.Source.OID] = *assignment
		case assignment.Destination.SchemaName:
		default:
			return fmt.Errorf("cleanup dependency OID %d is in unexpected schema %q",
				assignment.Source.OID, actual.SchemaName)
		}
	}
	seenMoves := make(map[uint32]struct{})
	for _, move := range moves {
		if _, duplicate := seenMoves[move.Source.OID]; duplicate {
			return fmt.Errorf("cleanup dependency OID %d is moved more than once", move.Source.OID)
		}
		seenMoves[move.Source.OID] = struct{}{}
		expected, ok := requiredByOID[move.Source.OID]
		if !ok || move.GroupID != expected.GroupID ||
			compareMarkerObjects(move.Source, expected.Source) != 0 ||
			compareMarkerObjects(move.Destination, expected.Destination) != 0 {
			return fmt.Errorf("stage 14 contains an unexpected cleanup dependency move for OID %d", move.Source.OID)
		}
	}
	if len(seenMoves) != len(requiredByOID) {
		for oid := range requiredByOID {
			if _, ok := seenMoves[oid]; !ok {
				return fmt.Errorf("stage 14 is missing the cleanup dependency move for OID %d", oid)
			}
		}
	}
	return nil
}

func globalCleanupEdgesFromDependencies(
	dependencies []globalCleanupDependency,
) ([]archivalMarkerSharedGroupEdgeV1, error) {
	var edges []archivalMarkerSharedGroupEdgeV1
	for _, dependency := range dependencies {
		if len(dependency.groupIDs) > 1 {
			edges = append(edges, completeArchivedDependencyGroupEdges(dependency.groupIDs)...)
		}
	}
	return canonicalArchivedDependencyGroupEdges(edges), nil
}

func validateGlobalCleanupMarkerEdges(
	groups []globalCleanupGroup,
	edges []archivalMarkerSharedGroupEdgeV1,
) error {
	groupIDs := make(map[archivalGroupID]struct{}, len(groups))
	for _, group := range groups {
		groupIDs[group.id] = struct{}{}
	}
	for _, edge := range edges {
		if _, ok := groupIDs[edge.FirstGroupID]; !ok {
			return fmt.Errorf("cleanup component edge references missing group %q", edge.FirstGroupID)
		}
		if _, ok := groupIDs[edge.SecondGroupID]; !ok {
			return fmt.Errorf("cleanup component edge references missing group %q", edge.SecondGroupID)
		}
	}
	for _, group := range groups {
		expected := incidentArchivedDependencyEdges(group.id, edges)
		actual := canonicalArchivedDependencyGroupEdges(
			group.marker.SharedCleanupComponentGroupEdges,
		)
		if !slices.Equal(expected, actual) {
			return fmt.Errorf("candidate cleanup group %q shared component edges do not match its previous cleanup graph", group.id)
		}
	}
	return nil
}

func validateGlobalCleanupComponentChanges(
	groups []globalCleanupGroup,
	dependencies []globalCleanupDependency,
	finalEdges []archivalMarkerSharedGroupEdgeV1,
) error {
	newGroups := make(map[archivalGroupID]struct{})
	newDependencyOIDs := make(map[uint32]struct{})
	for _, group := range groups {
		if group.existing == nil {
			newGroups[group.id] = struct{}{}
		}
	}
	for _, dependency := range dependencies {
		owner := globalCleanupGroupByID(groups, dependency.ownerGroupID)
		if owner == nil || owner.existing == nil || !containsMarkerObject(
			owner.marker.ExclusiveDependencyObjects, dependency.object,
		) {
			newDependencyOIDs[dependency.object.OID] = struct{}{}
		}
	}
	for _, group := range groups {
		if group.existing == nil {
			continue
		}
		oldEdges := canonicalArchivedDependencyGroupEdges(
			group.marker.SharedCleanupComponentGroupEdges,
		)
		newEdges := incidentArchivedDependencyEdges(group.id, finalEdges)
		if slices.Equal(oldEdges, newEdges) {
			continue
		}
		explained := false
		for _, edge := range newEdges {
			if slices.Contains(oldEdges, edge) {
				continue
			}
			if _, ok := newGroups[edge.FirstGroupID]; ok {
				explained = true
			}
			if _, ok := newGroups[edge.SecondGroupID]; ok {
				explained = true
			}
		}
		if !explained && len(newDependencyOIDs) == 0 {
			return fmt.Errorf("candidate cleanup group %q has an unexplained shared-edge mismatch", group.id)
		}
	}
	return nil
}

func buildGlobalCleanupOperations(
	inventory schema.CatalogInventory,
	groups []globalCleanupGroup,
	dependencies []globalCleanupDependency,
) ([]globalCleanupPlannedOperation, error) {
	var planned []globalCleanupPlannedOperation
	rootIDByGroup := make(map[archivalGroupID]string, len(groups))
	for _, group := range groups {
		if err := validateMarkerObjectKind(group.cleanupRoot, archivalMarkerObjectKindTable); err != nil {
			return nil, fmt.Errorf("cleanup root for group %q: %w", group.id, err)
		}
		id := fmt.Sprintf("cleanup:01:table:%s:%010d", group.id, group.cleanupRoot.OID)
		rootIDByGroup[group.id] = id
		planned = append(planned, globalCleanupPlannedOperation{
			operation: cleanupOperationV1{
				Version: cleanupOperationCodecVersion, ID: id,
				Kind: cleanupOperationKindDropTable, Object: cloneMarkerObject(group.cleanupRoot), Restrict: true,
			},
			groupIDs: []archivalGroupID{group.id},
		})
	}

	dependencyIDByAddress := make(map[archivedDependencyAddressKey]string, len(dependencies))
	dependencyIndexByAddress := make(map[archivedDependencyAddressKey]int, len(dependencies))
	for _, dependency := range dependencies {
		kind, err := cleanupOperationKindForMarkerObject(dependency.object.Kind)
		if err != nil {
			return nil, err
		}
		id := fmt.Sprintf("cleanup:02:%s:%010d", kind, dependency.object.OID)
		dependsOn := make([]string, 0, len(dependency.groupIDs))
		for _, groupID := range dependency.groupIDs {
			rootID, ok := rootIDByGroup[groupID]
			if !ok {
				return nil, fmt.Errorf("cleanup dependency OID %d references missing root group %q",
					dependency.object.OID, groupID)
			}
			dependsOn = append(dependsOn, rootID)
		}
		key := archivedDependencyAddressKey{
			classOID:  dependency.sourceAddress.ClassOID,
			objectOID: dependency.sourceAddress.ObjectOID,
		}
		if _, duplicate := dependencyIDByAddress[key]; duplicate {
			return nil, fmt.Errorf("duplicate cleanup dependency catalog address %d/%d", key.classOID, key.objectOID)
		}
		dependencyIDByAddress[key] = id
		dependencyIndexByAddress[key] = len(planned)
		planned = append(planned, globalCleanupPlannedOperation{
			operation: cleanupOperationV1{
				Version: cleanupOperationCodecVersion, ID: id, Kind: kind,
				Object: cloneMarkerObject(dependency.object), DependsOn: dependsOn, Restrict: true,
			},
			groupIDs: slices.Clone(dependency.groupIDs),
		})
	}
	for _, dependency := range inventory.Dependencies {
		dependentKey := archivedDependencyAddressKey{
			classOID: dependency.Dependent.ClassOID, objectOID: dependency.Dependent.ObjectOID,
		}
		referencedKey := archivedDependencyAddressKey{
			classOID: dependency.Referenced.ClassOID, objectOID: dependency.Referenced.ObjectOID,
		}
		dependentID, dependentRetained := dependencyIDByAddress[dependentKey]
		referencedIdx, referencedRetained := dependencyIndexByAddress[referencedKey]
		if !dependentRetained || !referencedRetained || dependentKey == referencedKey {
			continue
		}
		planned[referencedIdx].operation.DependsOn = append(
			planned[referencedIdx].operation.DependsOn, dependentID,
		)
	}

	edges, err := globalCleanupEdgesFromDependencies(dependencies)
	if err != nil {
		return nil, err
	}
	components, err := globalCleanupGroupComponents(groups, edges)
	if err != nil {
		return nil, err
	}
	componentByGroup := make(map[archivalGroupID]map[archivalGroupID]struct{}, len(groups))
	for _, component := range components {
		members := make(map[archivalGroupID]struct{}, len(component))
		for _, groupID := range component {
			members[groupID] = struct{}{}
		}
		for _, groupID := range component {
			componentByGroup[groupID] = members
		}
	}
	seenSchemas := make(map[string]archivalGroupID)
	for _, group := range groups {
		for _, schemaName := range archivedMarkerSchemaNames(group.marker) {
			if owner, duplicate := seenSchemas[schemaName]; duplicate {
				return nil, fmt.Errorf("cleanup schema %q is assigned to groups %q and %q", schemaName, owner, group.id)
			}
			seenSchemas[schemaName] = group.id
			var dependsOn []string
			for _, operation := range planned {
				if slices.ContainsFunc(operation.groupIDs, func(groupID archivalGroupID) bool {
					_, sameComponent := componentByGroup[group.id][groupID]
					return sameComponent
				}) {
					dependsOn = append(dependsOn, operation.operation.ID)
				}
			}
			planned = append(planned, globalCleanupPlannedOperation{
				operation: cleanupOperationV1{
					Version: cleanupOperationCodecVersion,
					ID:      "cleanup:03:schema:" + schemaName,
					Kind:    cleanupOperationKindDropSchema,
					Object: archivalMarkerObjectIdentity{
						Kind: archivalMarkerObjectKindSchema, Name: schemaName,
					},
					DependsOn: dependsOn, Restrict: true,
				},
				groupIDs: []archivalGroupID{group.id},
			})
		}
	}
	if _, err := canonicalizeCleanupOperations(globalCleanupOperations(planned)); err != nil {
		return nil, fmt.Errorf("building global cleanup graph: %w", err)
	}
	return planned, nil
}

func validatePreviousGlobalCleanupDigests(
	groups []globalCleanupGroup,
	edges []archivalMarkerSharedGroupEdgeV1,
	operations []globalCleanupPlannedOperation,
) error {
	components, err := globalCleanupGroupComponents(groups, edges)
	if err != nil {
		return err
	}
	for _, component := range components {
		digest, err := computeCleanupOperationDigest(
			globalCleanupComponentOperations(operations, component),
		)
		if err != nil {
			return err
		}
		for _, groupID := range component {
			group := globalCleanupGroupByID(groups, groupID)
			if group == nil {
				return fmt.Errorf("cleanup component references missing group %q", groupID)
			}
			stored, err := parseCleanupOperationDigest(group.marker.CleanupDigest.String())
			if err != nil {
				return fmt.Errorf("candidate cleanup group %q has malformed digest: %w", groupID, err)
			}
			if stored != digest {
				return fmt.Errorf("candidate cleanup group %q has a stale cleanup digest", groupID)
			}
		}
	}
	return nil
}

func finalizeGlobalCleanupMarkers(
	groups []globalCleanupGroup,
	dependencies []globalCleanupDependency,
	edges []archivalMarkerSharedGroupEdgeV1,
	operations []globalCleanupPlannedOperation,
	isolation archivalIsolationPlan,
) ([]globalCleanupFinalizedMarker, error) {
	objectsByOwner := make(map[archivalGroupID][]archivalMarkerObjectIdentity)
	for _, dependency := range dependencies {
		objectsByOwner[dependency.ownerGroupID] = append(
			objectsByOwner[dependency.ownerGroupID], cloneMarkerObject(dependency.object),
		)
	}
	components, err := globalCleanupGroupComponents(groups, edges)
	if err != nil {
		return nil, err
	}
	digestByGroup := make(map[archivalGroupID]cleanupOperationDigest, len(groups))
	for _, component := range components {
		digest, err := computeCleanupOperationDigest(
			globalCleanupComponentOperations(operations, component),
		)
		if err != nil {
			return nil, err
		}
		for _, groupID := range component {
			digestByGroup[groupID] = digest
		}
	}
	finalized := make([]globalCleanupFinalizedMarker, 0, len(groups))
	isolationByGroup := make(map[archivalGroupID]archivalIsolationGroupPlan, len(isolation.Groups))
	for _, groupPlan := range isolation.Groups {
		if _, duplicate := isolationByGroup[groupPlan.GroupID]; duplicate {
			return nil, fmt.Errorf("isolation plan duplicates cleanup group %q", groupPlan.GroupID)
		}
		isolationByGroup[groupPlan.GroupID] = groupPlan
	}
	for _, group := range groups {
		marker := canonicalizeArchivalMarker(group.marker)
		marker.ExclusiveDependencyObjects = canonicalMarkerObjects(objectsByOwner[group.id])
		marker.SharedCleanupComponentGroupEdges =
			incidentArchivedDependencyEdges(group.id, edges)
		if groupPlan, ok := isolationByGroup[group.id]; ok {
			marker.OriginalACLs = groupPlan.OriginalACLs
			marker.OriginalForeignKeys = groupPlan.OriginalForeignKeys
			marker.OriginalPublicationMemberships = groupPlan.OriginalPublications
		}
		marker.CleanupDigest = digestByGroup[group.id]
		marker = canonicalizeArchivalMarker(marker)
		text, err := marshalArchivalMarker(marker)
		if err != nil {
			return nil, fmt.Errorf("finalizing cleanup marker for group %q: %w", group.id, err)
		}
		finalized = append(finalized, globalCleanupFinalizedMarker{
			GroupID: group.id, Payload: marker, Text: text,
		})
	}
	return finalized, nil
}

func renderGlobalCleanupStatements(
	operations []cleanupOperationV1,
	groups []globalCleanupGroup,
) ([]Statement, error) {
	indexedTables := make(map[uint32]bool, len(groups))
	for _, group := range groups {
		indexedTables[group.cleanupRoot.OID] = group.hasIndexes
	}
	statements := make([]Statement, 0, len(operations))
	for _, operation := range operations {
		if !operation.Restrict {
			return nil, fmt.Errorf("cleanup operation %q does not use RESTRICT", operation.ID)
		}
		object := operation.Object
		qualifiedName := schema.EscapeIdentifier(object.SchemaName) + "." + schema.EscapeIdentifier(object.Name)
		statement := Statement{}
		switch operation.Kind {
		case cleanupOperationKindDropTable:
			statement.DDL = "DROP TABLE " + qualifiedName + " RESTRICT"
			statement.Hazards = []MigrationHazard{
				migrationHazardCleanupTableDeletesData, migrationHazardCleanupTableLock,
			}
			if indexedTables[object.OID] {
				statement.Hazards = append(statement.Hazards, migrationHazardIndexDroppedQueryPerf)
			}
		case cleanupOperationKindDropSequence:
			statement.DDL = "DROP SEQUENCE " + qualifiedName + " RESTRICT"
			statement.Hazards = []MigrationHazard{
				migrationHazardCleanupDependencyDeletesData, migrationHazardCleanupDependencyLock,
			}
		case cleanupOperationKindDropFunction:
			if len(object.IdentityArguments) > 1 {
				return nil, fmt.Errorf("cleanup function OID %d has an invalid identity signature", object.OID)
			}
			statement.DDL = fmt.Sprintf("DROP FUNCTION %s(%s) RESTRICT", qualifiedName, markerIdentityArgument(object))
			statement.Hazards = cleanupDependencyHazards()
		case cleanupOperationKindDropType:
			statement.DDL = "DROP TYPE " + qualifiedName + " RESTRICT"
			statement.Hazards = cleanupDependencyHazards()
		case cleanupOperationKindDropCollation:
			statement.DDL = "DROP COLLATION " + qualifiedName + " RESTRICT"
			statement.Hazards = cleanupDependencyHazards()
		case cleanupOperationKindDropOperator:
			if len(object.IdentityArguments) != 2 ||
				!isSafeCleanupOperatorName(object.Name) {
				return nil, fmt.Errorf("cleanup operator OID %d has an invalid identity signature", object.OID)
			}
			statement.DDL = fmt.Sprintf("DROP OPERATOR %s.%s (%s, %s) RESTRICT",
				schema.EscapeIdentifier(object.SchemaName), object.Name,
				object.IdentityArguments[0], object.IdentityArguments[1])
			statement.Hazards = cleanupDependencyHazards()
		case cleanupOperationKindDropSchema:
			statement.DDL = "DROP SCHEMA " + schema.EscapeIdentifier(object.Name) + " RESTRICT"
			statement.Hazards = []MigrationHazard{migrationHazardCleanupSchemaLock}
		default:
			return nil, fmt.Errorf("cleanup operation %q has unsupported marker object kind %q",
				operation.ID, object.Kind)
		}
		statements = append(statements, statement)
	}
	return statements, nil
}

func renderGlobalCleanupMarkerUpdate(
	schemaNames []string,
	markerText string,
) Statement {
	names := slices.Clone(schemaNames)
	slices.Sort(names)
	var body strings.Builder
	body.WriteString("BEGIN\n")
	for _, schemaName := range names {
		fmt.Fprintf(&body, "    COMMENT ON SCHEMA %s IS %s;\n",
			schema.EscapeIdentifier(schemaName), escapeArchivalMarkerSQLLiteral(markerText))
	}
	body.WriteString("END")
	return Statement{DDL: doBlock(body.String())}
}

func cleanupDependencyHazards() []MigrationHazard {
	return []MigrationHazard{migrationHazardCleanupDependencyCorrectness, migrationHazardCleanupDependencyLock}
}

func cleanupOperationKindForMarkerObject(kind archivalMarkerObjectKind) (cleanupOperationKind, error) {
	switch kind {
	case archivalMarkerObjectKindSequence:
		return cleanupOperationKindDropSequence, nil
	case archivalMarkerObjectKindFunction:
		return cleanupOperationKindDropFunction, nil
	case archivalMarkerObjectKindType:
		return cleanupOperationKindDropType, nil
	case archivalMarkerObjectKindCollation:
		return cleanupOperationKindDropCollation, nil
	case archivalMarkerObjectKindOperator:
		return cleanupOperationKindDropOperator, nil
	default:
		return "", fmt.Errorf("unsupported cleanup marker object kind %q", kind)
	}
}

func globalCleanupGroupComponents(
	groups []globalCleanupGroup,
	edges []archivalMarkerSharedGroupEdgeV1,
) ([][]archivalGroupID, error) {
	adjacent := make(map[archivalGroupID][]archivalGroupID, len(groups))
	for _, group := range groups {
		adjacent[group.id] = nil
	}
	for _, edge := range edges {
		if _, ok := adjacent[edge.FirstGroupID]; !ok {
			return nil, fmt.Errorf("cleanup component edge references missing group %q", edge.FirstGroupID)
		}
		if _, ok := adjacent[edge.SecondGroupID]; !ok {
			return nil, fmt.Errorf("cleanup component edge references missing group %q", edge.SecondGroupID)
		}
		adjacent[edge.FirstGroupID] = append(adjacent[edge.FirstGroupID], edge.SecondGroupID)
		adjacent[edge.SecondGroupID] = append(adjacent[edge.SecondGroupID], edge.FirstGroupID)
	}
	var components [][]archivalGroupID
	visited := make(map[archivalGroupID]struct{}, len(groups))
	for _, group := range groups {
		if _, ok := visited[group.id]; ok {
			continue
		}
		queue := []archivalGroupID{group.id}
		var component []archivalGroupID
		for len(queue) > 0 {
			groupID := queue[0]
			queue = queue[1:]
			if _, ok := visited[groupID]; ok {
				continue
			}
			visited[groupID] = struct{}{}
			component = append(component, groupID)
			queue = append(queue, adjacent[groupID]...)
		}
		slices.Sort(component)
		components = append(components, component)
	}
	return components, nil
}

func globalCleanupComponentOperations(
	operations []globalCleanupPlannedOperation,
	component []archivalGroupID,
) []cleanupOperationV1 {
	member := make(map[archivalGroupID]struct{}, len(component))
	for _, groupID := range component {
		member[groupID] = struct{}{}
	}
	var result []cleanupOperationV1
	for _, planned := range operations {
		include := false
		for _, groupID := range planned.groupIDs {
			if _, ok := member[groupID]; ok {
				include = true
				break
			}
		}
		if !include {
			continue
		}
		operation := planned.operation
		operation.DependsOn = slices.DeleteFunc(slices.Clone(operation.DependsOn), func(id string) bool {
			return !slices.ContainsFunc(operations, func(
				candidate globalCleanupPlannedOperation,
			) bool {
				if candidate.operation.ID != id {
					return false
				}
				return slices.ContainsFunc(candidate.groupIDs, func(groupID archivalGroupID) bool {
					_, ok := member[groupID]
					return ok
				})
			})
		})
		result = append(result, operation)
	}
	return result
}

func globalCleanupOperations(operations []globalCleanupPlannedOperation) []cleanupOperationV1 {
	result := make([]cleanupOperationV1, 0, len(operations))
	for _, operation := range operations {
		result = append(result, operation.operation)
	}
	return result
}

func existingGlobalCleanupGroups(groups []globalCleanupGroup) []globalCleanupGroup {
	var result []globalCleanupGroup
	for _, group := range groups {
		if group.existing != nil {
			copy := group
			copy.marker = canonicalizeArchivalMarker(group.existing.Marker)
			result = append(result, copy)
		}
	}
	return result
}

func globalCleanupRootFromMarker(marker archivalMarkerV1) (archivalMarkerObjectIdentity, error) {
	children := make(map[string]struct{}, len(marker.PartitionEdges))
	for _, edge := range marker.PartitionEdges {
		children[edge.ChildMemberID] = struct{}{}
	}
	var root *archivalMarkerObjectIdentity
	for _, member := range marker.Members {
		if _, child := children[member.MemberID]; child {
			continue
		}
		if root != nil {
			return archivalMarkerObjectIdentity{},
				fmt.Errorf("cleanup group %q has multiple root members", marker.GroupID)
		}
		copy := cloneMarkerObject(member.CleanupTable)
		root = &copy
	}
	if root == nil {
		return archivalMarkerObjectIdentity{},
			fmt.Errorf("cleanup group %q has no root member", marker.GroupID)
	}
	return *root, nil
}

func archivalMarkerHasIndexes(marker archivalMarkerV1) bool {
	for _, member := range marker.Members {
		for _, object := range member.AutomaticallyMovedObjects {
			if object.Kind == archivalMarkerObjectKindIndex {
				return true
			}
		}
	}
	return false
}

func clonePreparedArchivalGroup(group preparedArchivalGroup) preparedArchivalGroup {
	group.marker = canonicalizeArchivalMarker(group.marker)
	group.markerText = strings.Clone(group.markerText)
	group.members = slices.Clone(group.members)
	for idx := range group.members {
		group.members[idx].marker = canonicalizeArchivalMarker(archivalMarkerV1{
			Version: archivalMarkerVersion, GroupID: group.id,
			Members:               []archivalMarkerMemberV1{group.members[idx].marker},
			LostParentAttachments: []archivalMarkerLostParentAttachmentV1{},
			CleanupDigest:         cleanupOperationDigest("sha256:" + strings.Repeat("0", 64)),
		}).Members[0]
	}
	return group
}

func globalCleanupGroupByID(groups []globalCleanupGroup, id archivalGroupID) *globalCleanupGroup {
	for idx := range groups {
		if groups[idx].id == id {
			return &groups[idx]
		}
	}
	return nil
}

func compareGlobalCleanupDependencies(a, b globalCleanupDependency) int {
	return cmp.Or(compareMarkerObjects(a.object, b.object),
		cmp.Compare(a.ownerGroupID, b.ownerGroupID))
}

func isSafeCleanupOperatorName(name string) bool {
	if name == "" || strings.Contains(name, "--") || strings.Contains(name, "/*") || strings.Contains(name, "*/") {
		return false
	}
	const operatorCharacters = "+-*/<>=~!@#%^&|`?"
	return strings.IndexFunc(name, func(r rune) bool {
		return r > unicode.MaxASCII || !strings.ContainsRune(operatorCharacters, r)
	}) == -1
}
