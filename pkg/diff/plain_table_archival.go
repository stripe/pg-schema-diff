package diff

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

type plainTableArchivalRequest struct {
	CurrentInventory  schema.CatalogInventory
	TargetSchema      schema.Schema
	Groups            []plainTableArchivalGroupRequest
	SourcePreflight   sourceSafetyPreflightResult
	DependencyClosure archivedDependencyClosureResult
}

type plainTableArchivalGroupRequest struct {
	Allocation      *archivalGroupNameAllocation
	FinalizedMarker string
	Resume          *archivedGroupResumeDescriptor
	DetachedSubtree *archivalDetachedSubtreeRequest
}

type archivalDetachedSubtreeRequest struct {
	RootRelationOID   uint32
	ParentRelationOID uint32
}

type plainTableArchivalVertexKind string

const (
	plainTableArchivalVertexKindGroupInitialization plainTableArchivalVertexKind = "group_initialization"
	plainTableArchivalVertexKindTableMove           plainTableArchivalVertexKind = "table_move"
	plainTableArchivalVertexKindResumeTableMove     plainTableArchivalVertexKind = "resume_table_move"
	plainTableArchivalVertexKindPartitionDetach     plainTableArchivalVertexKind = "partition_detach"
	plainTableArchivalVertexKindMarkerRefresh       plainTableArchivalVertexKind = "marker_refresh"
	plainTableArchivalVertexKindCatalogAssertion    plainTableArchivalVertexKind = "catalog_assertion"
)

type plainTableArchivalVertexID struct {
	kind      plainTableArchivalVertexKind
	groupID   archivalGroupID
	memberKey string
}

func (id plainTableArchivalVertexID) String() string {
	base := fmt.Sprintf("archival:%02d:%s:%s", plainTableArchivalVertexPhase(id.kind), id.groupID, id.kind)
	if id.memberKey != "" {
		return base + ":" + id.memberKey
	}
	return base
}

func plainTableArchivalVertexPhase(kind plainTableArchivalVertexKind) int {
	switch kind {
	case plainTableArchivalVertexKindGroupInitialization:
		return 1
	case plainTableArchivalVertexKindMarkerRefresh:
		return 2
	case plainTableArchivalVertexKindTableMove, plainTableArchivalVertexKindResumeTableMove,
		plainTableArchivalVertexKindPartitionDetach:
		return 4
	case plainTableArchivalVertexKindCatalogAssertion:
		return 8
	default:
		return 99
	}
}

type preparedArchivalMember struct {
	marker        archivalMarkerMemberV1
	depth         int
	relationKind  schema.RelKind
	remainingMove *archivedMemberMoveDescriptor
}

type archivalPartitionDetachOperation struct {
	root   archivalMarkerMemberV1
	parent archivalMarkerObjectIdentity
}

type preparedArchivalGroup struct {
	id           archivalGroupID
	marker       archivalMarkerV1
	markerText   string
	members      []preparedArchivalMember
	rootMemberID string
	cleanupRoot  archivalMarkerObjectIdentity
	allocation   *archivalGroupNameAllocation
	resume       *archivedGroupResumeDescriptor
	detach       *archivalPartitionDetachOperation
}

var migrationHazardPlainTableSetSchema = MigrationHazard{
	Type: MigrationHazardTypeAcquiresAccessExclusiveLock,
	Message: "Moving a table to another schema acquires an ACCESS EXCLUSIVE lock on the table. " +
		"The catalog-only move should be brief, but all table access is blocked while the lock is held.",
}

var migrationHazardArchivalPartitionDetach = []MigrationHazard{
	{
		Type:    MigrationHazardTypeAcquiresAccessExclusiveLock,
		Message: "Detaching an archived partition acquires ACCESS EXCLUSIVE locks on the active parent and retained subtree root.",
	},
	{
		Type:    MigrationHazardTypeCorrectness,
		Message: "Detaching the retained subtree removes it from the active parent's partition routing and partitioned indexes.",
	},
}

var migrationHazardArchivalSchemaLockdown = MigrationHazard{
	Type:    MigrationHazardTypeAuthzUpdate,
	Message: "The archival schemas are initialized without access for non-owner roles.",
}

// generatePlainTableArchivalStatements is dormant until archival activation.
func generatePlainTableArchivalStatements(request plainTableArchivalRequest) ([]Statement, error) {
	groups, err := preparePlainTableArchivalGroups(request)
	if err != nil {
		return nil, err
	}
	isolation, err := planArchivalIsolation(request.CurrentInventory, request.TargetSchema,
		request.SourcePreflight, request.DependencyClosure, groups)
	if err != nil {
		return nil, err
	}
	graph, err := buildPlainTableArchivalGraph(request.CurrentInventory, groups, isolation)
	if err != nil {
		return nil, err
	}
	return graph.toOrderedStatements()
}

func preparePlainTableArchivalGroups(
	request plainTableArchivalRequest,
) ([]preparedArchivalGroup, error) {
	if len(request.Groups) == 0 {
		return nil, nil
	}
	inventory := request.CurrentInventory.Normalize()
	groups := make([]preparedArchivalGroup, 0, len(request.Groups))
	seenGroups := make(map[archivalGroupID]struct{}, len(request.Groups))
	seenRelations := make(map[uint32]struct{}, len(request.Groups))
	for idx, groupRequest := range request.Groups {
		group, err := preparePlainTableArchivalGroup(inventory, request.DependencyClosure, groupRequest)
		if err != nil {
			return nil, fmt.Errorf("preparing archival group %d: %w", idx, err)
		}
		if _, duplicate := seenGroups[group.id]; duplicate {
			return nil, fmt.Errorf("archival group %q is duplicated", group.id)
		}
		seenGroups[group.id] = struct{}{}
		for _, member := range group.members {
			if _, duplicate := seenRelations[member.marker.SourceTable.OID]; duplicate {
				return nil, fmt.Errorf("archival relation OID %d belongs to more than one group",
					member.marker.SourceTable.OID)
			}
			seenRelations[member.marker.SourceTable.OID] = struct{}{}
		}
		groups = append(groups, group)
	}
	slices.SortFunc(groups, func(a, b preparedArchivalGroup) int {
		return cmp.Compare(a.id, b.id)
	})
	if err := validatePlainTableArchivalPreflight(inventory, groups, request.SourcePreflight); err != nil {
		return nil, err
	}
	if err := validatePlainTableArchivalClosure(groups, request.DependencyClosure); err != nil {
		return nil, err
	}
	return groups, nil
}

func preparePlainTableArchivalGroup(
	inventory schema.CatalogInventory,
	closure archivedDependencyClosureResult,
	request plainTableArchivalGroupRequest,
) (preparedArchivalGroup, error) {
	marker, err := parseArchivalMarker(request.FinalizedMarker)
	if err != nil {
		return preparedArchivalGroup{},
			fmt.Errorf("parsing finalized marker: %w", err)
	}
	canonicalMarker, err := marshalArchivalMarker(marker)
	if err != nil {
		return preparedArchivalGroup{},
			fmt.Errorf("canonicalizing finalized marker: %w", err)
	}
	if request.FinalizedMarker != canonicalMarker {
		return preparedArchivalGroup{},
			fmt.Errorf("finalized marker for group %q is not canonical", marker.GroupID)
	}
	if (request.Allocation == nil) == (request.Resume == nil) {
		return preparedArchivalGroup{}, fmt.Errorf(
			"group %q must contain exactly one of a Stage 7 allocation or Stage 9 resume descriptor", marker.GroupID,
		)
	}
	if err := validateStage15ArchivalMarker(marker, request.DetachedSubtree); err != nil {
		return preparedArchivalGroup{}, fmt.Errorf(
			"validating finalized marker for group %q: %w",
			marker.GroupID, err,
		)
	}
	group := preparedArchivalGroup{
		id: marker.GroupID, marker: marker, markerText: canonicalMarker,
		allocation: request.Allocation, resume: request.Resume,
	}
	if err := populatePreparedArchivalMembers(&group); err != nil {
		return preparedArchivalGroup{}, err
	}
	for idx := range group.members {
		relation, err := uniqueRelationByOID(inventory,
			group.members[idx].marker.SourceTable.OID)
		if err != nil {
			return preparedArchivalGroup{}, err
		}
		group.members[idx].relationKind = relation.Kind
	}
	root := preparedArchivalMemberByID(group.members, group.rootMemberID)
	group.cleanupRoot = root.marker.CleanupTable
	if request.Allocation != nil {
		if err := validateNewArchivalGroup(inventory, group, request.DetachedSubtree); err != nil {
			return preparedArchivalGroup{}, err
		}
		for idx := range group.members {
			member := &group.members[idx]
			move := archivedMemberMoveDescriptor{
				MemberID: member.marker.MemberID, RelationOID: member.marker.SourceTable.OID,
				SourceTable:      member.marker.SourceTable,
				DestinationTable: member.marker.CleanupTable,
			}
			member.remainingMove = &move
		}
		if request.DetachedSubtree != nil {
			group.detach = &archivalPartitionDetachOperation{
				root: root.marker, parent: marker.LostParentAttachments[0].ParentTable,
			}
		}
		return group, nil
	}
	if err := validateResumedArchivalGroup(inventory, closure, request.DetachedSubtree, &group); err != nil {
		return preparedArchivalGroup{}, err
	}
	return group, nil
}

func validateStage15ArchivalMarker(
	marker archivalMarkerV1,
	detachedSubtree *archivalDetachedSubtreeRequest,
) error {
	if len(marker.ExclusiveDependencySchemas) != 1 {
		return fmt.Errorf("archival group must declare exactly one dependency schema, got %d",
			len(marker.ExclusiveDependencySchemas))
	}
	if detachedSubtree == nil && len(marker.LostParentAttachments) != 0 {
		return fmt.Errorf("complete archival group must not declare a lost parent attachment")
	}
	if detachedSubtree != nil && len(marker.LostParentAttachments) != 1 {
		return fmt.Errorf("detached subtree archival group must declare exactly one lost parent attachment")
	}
	return nil
}

func validateNewArchivalGroup(
	inventory schema.CatalogInventory,
	group preparedArchivalGroup,
	detachedSubtree *archivalDetachedSubtreeRequest,
) error {
	allocation := *group.allocation
	if allocation.GroupID != group.id {
		return fmt.Errorf("stage 7 allocation group %q does not match marker group %q", allocation.GroupID, group.id)
	}
	timestamp, nonce, err := parseArchivalGroupID(group.id)
	if err != nil {
		return err
	}
	if allocation.Timestamp != timestamp || allocation.Nonce != nonce {
		return fmt.Errorf("stage 7 allocation timestamp or nonce does not match group %q", group.id)
	}
	if len(allocation.Members) != len(group.members) {
		return fmt.Errorf("stage 7 allocation for group %q contains %d members; marker declares %d",
			group.id, len(allocation.Members), len(group.members))
	}
	allocatedByOID := make(map[uint32]archivalPhysicalMemberNameAllocation, len(allocation.Members))
	for _, allocated := range allocation.Members {
		if _, duplicate := allocatedByOID[allocated.RelationOID]; duplicate {
			return fmt.Errorf("stage 7 allocation for group %q duplicates relation OID %d",
				group.id, allocated.RelationOID)
		}
		allocatedByOID[allocated.RelationOID] = allocated
	}
	for _, preparedMember := range group.members {
		member := preparedMember.marker
		allocated, ok := allocatedByOID[member.SourceTable.OID]
		if !ok || allocated.SourceSchemaName != member.SourceTable.SchemaName ||
			allocated.SourceTableName != member.SourceTable.Name ||
			allocated.CleanupSchemaName != member.CleanupTable.SchemaName {
			return fmt.Errorf("stage 7 member allocation does not match finalized marker for group %q", group.id)
		}
		if allocated.EscapedCleanupSchemaName !=
			schema.EscapeIdentifier(allocated.CleanupSchemaName) {
			return fmt.Errorf("stage 7 cleanup schema quoting does not match group %q", group.id)
		}
	}
	dependencySchema := group.marker.ExclusiveDependencySchemas[0].Name
	if allocation.DependencySchemaName != dependencySchema ||
		allocation.EscapedDependencySchemaName != schema.EscapeIdentifier(dependencySchema) {
		return fmt.Errorf("stage 7 dependency schema allocation does not match finalized marker for group %q", group.id)
	}
	return validateArchivalGroupAgainstInventory(inventory, group, detachedSubtree, nil)
}

func validateResumedArchivalGroup(
	inventory schema.CatalogInventory,
	closure archivedDependencyClosureResult,
	detachedSubtree *archivalDetachedSubtreeRequest,
	group *preparedArchivalGroup,
) error {
	resume := *group.resume
	if resume.GroupID != group.id {
		return fmt.Errorf("stage 9 resume group %q does not match marker group %q", resume.GroupID, group.id)
	}
	if len(resume.RemainingMemberMoves) > len(group.members) {
		return fmt.Errorf("stage 9 resume group %q contains %d table moves for %d members",
			group.id, len(resume.RemainingMemberMoves), len(group.members))
	}
	candidate, err := dependencyValidatedCandidateForGroup(closure, group.id)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(candidate.Resume, resume) {
		return fmt.Errorf("stage 9 resume descriptor for group %q was not validated by stage 11", group.id)
	}
	expectedFinal := canonicalizeArchivalMarker(candidate.Marker)
	expectedFinal.CleanupDigest = group.marker.CleanupDigest
	if !reflect.DeepEqual(expectedFinal, group.marker) {
		return fmt.Errorf("finalized marker for resumed group %q changes state other than the cleanup digest", group.id)
	}
	expectedSchemas := archivedMarkerSchemaNames(group.marker)
	if !slices.Equal(candidate.SchemaNames, expectedSchemas) {
		return fmt.Errorf("stage 9 schema names for group %q do not match the finalized marker", group.id)
	}
	if candidate.ExpectedDependencySchemaName != group.marker.ExclusiveDependencySchemas[0].Name {
		return fmt.Errorf("stage 9 dependency schema for group %q does not match the finalized marker", group.id)
	}
	oldMarker, err := marshalArchivalMarker(candidate.Marker)
	if err != nil {
		return fmt.Errorf("canonicalizing Stage 9 marker for group %q: %w", group.id, err)
	}
	if err := validateResumeMarkerUpdates(inventory, resume, expectedSchemas, oldMarker); err != nil {
		return err
	}
	remainingByOID := make(map[uint32]archivedMemberMoveDescriptor, len(resume.RemainingMemberMoves))
	for _, move := range resume.RemainingMemberMoves {
		if _, duplicate := remainingByOID[move.RelationOID]; duplicate {
			return fmt.Errorf("stage 9 group %q duplicates table move OID %d", group.id, move.RelationOID)
		}
		remainingByOID[move.RelationOID] = move
	}
	movedByOID := make(map[uint32]bool, len(group.members))
	for idx := range group.members {
		member := &group.members[idx]
		relation, err := uniqueRelationByOID(inventory, member.marker.SourceTable.OID)
		if err != nil {
			return err
		}
		moved := relation.SchemaName == member.marker.CleanupTable.SchemaName
		movedByOID[member.marker.SourceTable.OID] = moved
		move, remaining := remainingByOID[member.marker.SourceTable.OID]
		if remaining {
			if !sameArchivedMemberMove(move, member.marker) {
				return fmt.Errorf("stage 9 table move for group %q does not match finalized member %q",
					group.id, member.marker.MemberID)
			}
			if moved {
				return fmt.Errorf("stage 9 group %q requests a move for member %q already in its cleanup schema",
					group.id, member.marker.MemberID)
			}
			moveCopy := move
			member.remainingMove = &moveCopy
		} else if !moved {
			return fmt.Errorf("stage 9 group %q omits remaining move for member %q",
				group.id, member.marker.MemberID)
		}
	}
	for _, edge := range group.marker.PartitionEdges {
		parent := preparedArchivalMemberByID(group.members, edge.ParentMemberID)
		child := preparedArchivalMemberByID(group.members, edge.ChildMemberID)
		if movedByOID[parent.marker.SourceTable.OID] &&
			!movedByOID[child.marker.SourceTable.OID] {
			return fmt.Errorf("stage 9 group %q moved parent member %q before descendant %q",
				group.id, parent.marker.MemberID, child.marker.MemberID)
		}
	}
	if resume.RemainingPartitionDetach {
		if detachedSubtree == nil {
			return fmt.Errorf("stage 9 group %q requests partition detach without detached-subtree intent", group.id)
		}
		root := preparedArchivalMemberByID(group.members, group.rootMemberID)
		group.detach = &archivalPartitionDetachOperation{
			root: root.marker, parent: group.marker.LostParentAttachments[0].ParentTable,
		}
	}
	return validateArchivalGroupAgainstInventory(inventory, *group, detachedSubtree, movedByOID)
}

func dependencyValidatedCandidateForGroup(
	closure archivedDependencyClosureResult,
	groupID archivalGroupID,
) (structurallyValidArchivedCandidateGroup, error) {
	var matches []structurallyValidArchivedCandidateGroup
	for _, validated := range closure.DependencyValidatedCandidateGroups {
		if validated.Candidate.GroupID == groupID {
			matches = append(matches, validated.Candidate)
		}
	}
	if len(matches) != 1 {
		return structurallyValidArchivedCandidateGroup{}, fmt.Errorf(
			"resumed group %q has %d Stage 11 dependency-validated candidates; exactly one is required",
			groupID, len(matches),
		)
	}
	return matches[0], nil
}

func validateResumeMarkerUpdates(
	inventory schema.CatalogInventory,
	resume archivedGroupResumeDescriptor,
	expectedSchemas []string,
	oldMarker string,
) error {
	updates := slices.Clone(resume.RemainingMarkerUpdates)
	slices.SortFunc(updates, func(a, b archivedMarkerUpdateDescriptor) int {
		return cmp.Compare(a.SchemaName, b.SchemaName)
	})
	if len(updates) != len(expectedSchemas) {
		return fmt.Errorf("stage 9 resume group %q must refresh every declared schema marker", resume.GroupID)
	}
	for idx, update := range updates {
		if update.SchemaName != expectedSchemas[idx] || update.Marker != oldMarker {
			return fmt.Errorf("stage 9 marker refresh for group %q does not match its validated state", resume.GroupID)
		}
		catalogSchema := catalogSchemaWithName(inventory, update.SchemaName)
		if catalogSchema == nil || catalogSchema.Comment != oldMarker {
			return fmt.Errorf("stage 9 schema %q for group %q no longer has its validated marker",
				update.SchemaName, resume.GroupID)
		}
	}
	return nil
}

func sameArchivedMemberMove(move archivedMemberMoveDescriptor, member archivalMarkerMemberV1) bool {
	return move.MemberID == member.MemberID && move.RelationOID == member.SourceTable.OID &&
		compareMarkerObjects(move.SourceTable, member.SourceTable) == 0 &&
		compareMarkerObjects(move.DestinationTable, member.CleanupTable) == 0
}

func validatePlainTableArchivalPreflight(
	inventory schema.CatalogInventory,
	groups []preparedArchivalGroup,
	preflight sourceSafetyPreflightResult,
) error {
	expectedOIDs := plainTableArchivalRelationOIDs(groups)
	validatedOIDs := slices.Clone(preflight.ValidatedTableRelationOIDs)
	slices.Sort(validatedOIDs)
	if !slices.Equal(expectedOIDs, validatedOIDs) {
		return fmt.Errorf("stage 10 preflight did not validate exactly the requested archival relation OIDs")
	}
	if err := validateStage12PlatformInventory(inventory, groups); err != nil {
		return err
	}
	groupByRelationOID := archivalGroupByRelationOID(groups)
	for _, group := range groups {
		declaredSchemas := make(map[string]struct{}, len(group.members))
		for _, member := range group.members {
			declaredSchemas[member.marker.CleanupTable.SchemaName] = struct{}{}
		}
		for _, member := range group.members {
			move, err := inventory.ExpectedTableMove(member.marker.SourceTable.OID)
			if err != nil {
				return err
			}
			move = filterPlainTableIsolationObjects(move, preflight, groupByRelationOID)
			move = filterLostParentClonedTriggers(move, group.marker, member.marker)
			relation, err := uniqueRelationByOID(inventory, member.marker.SourceTable.OID)
			if err != nil {
				return err
			}
			moved := relation.SchemaName == member.marker.CleanupTable.SchemaName
			if err := validateMemberCatalogObjects(inventory, member.marker, move, moved,
				declaredSchemas, make(map[string]struct{})); err != nil {
				return fmt.Errorf("validating Stage 14 table-local isolation for group %q member %q: %w",
					group.id, member.marker.MemberID, err)
			}
		}
	}
	return nil
}

func filterPlainTableIsolationObjects(
	move schema.CatalogExpectedTableMove,
	preflight sourceSafetyPreflightResult,
	groupByRelationOID map[uint32]preparedArchivalGroup,
) schema.CatalogExpectedTableMove {
	constraints := make(map[uint32]struct{})
	triggers := make(map[uint32]struct{})
	for _, foreignKey := range preflight.ForeignKeys {
		owning, owningArchived := groupByRelationOID[foreignKey.ForeignKey.OwningRelationOID]
		referenced, referencedArchived := groupByRelationOID[foreignKey.ForeignKey.ReferencedRelationOID]
		if owningArchived && referencedArchived && owning.id == referenced.id {
			continue
		}
		constraints[foreignKey.ForeignKey.OID] = struct{}{}
		for _, triggerOID := range foreignKey.TriggerOIDs {
			triggers[triggerOID] = struct{}{}
		}
	}
	move.CleanupSchemaObjects = slices.DeleteFunc(slices.Clone(move.CleanupSchemaObjects),
		func(object schema.CatalogObjectIdentity) bool {
			_, drop := constraints[object.OID]
			return object.Kind == schema.CatalogObjectKindConstraint && drop
		})
	move.AttachedObjects = slices.DeleteFunc(slices.Clone(move.AttachedObjects),
		func(object schema.CatalogObjectIdentity) bool {
			_, drop := triggers[object.OID]
			return object.Kind == schema.CatalogObjectKindTrigger && drop
		})
	return move
}

func validateStage12PlatformInventory(
	inventory schema.CatalogInventory,
	groups []preparedArchivalGroup,
) error {
	for _, trigger := range inventory.EventTriggers {
		if trigger.EnabledMode != "D" {
			return fmt.Errorf("enabled event trigger %q is not supported by the archival move engine", trigger.Name)
		}
	}
	for _, publication := range inventory.Publications {
		if publication.PublishesAllTables {
			return fmt.Errorf("FOR ALL TABLES publication %q is not supported by the archival move engine",
				publication.Name)
		}
	}
	for _, member := range inventory.ExtensionMembers {
		if stage12ExtensionMemberIsRetained(member.Object, groups) {
			return fmt.Errorf("extension member %s is not supported by the archival move engine",
				sourceSafetyCatalogObjectDescription(member.Object))
		}
	}
	return nil
}

func stage12ExtensionMemberIsRetained(
	member schema.CatalogDependencyObject,
	groups []preparedArchivalGroup,
) bool {
	for _, group := range groups {
		for _, preparedMember := range group.members {
			for _, object := range slices.Concat(
				preparedMember.marker.AutomaticallyMovedObjects,
				preparedMember.marker.AttachedObjects,
				preparedMember.marker.InternalToastObjects,
			) {
				classOID := uint32(0)
				switch object.Kind {
				case archivalMarkerObjectKindTable, archivalMarkerObjectKindIndex,
					archivalMarkerObjectKindOwnedSequence, archivalMarkerObjectKindToastRelation:
					classOID = pgClassCatalogOID
				case archivalMarkerObjectKindRowType, archivalMarkerObjectKindArrayType:
					classOID = pgTypeCatalogOID
				case archivalMarkerObjectKindConstraint:
					classOID = pgConstraintCatalogOID
				case archivalMarkerObjectKindTrigger:
					classOID = pgTriggerCatalogOID
				case archivalMarkerObjectKindRule:
					classOID = pgRewriteCatalogOID
				case archivalMarkerObjectKindPolicy:
					classOID = pgPolicyCatalogOID
				}
				if member.ClassOID == classOID && member.ObjectOID == object.OID {
					return true
				}
			}
		}
	}
	return false
}

func validatePlainTableArchivalClosure(
	groups []preparedArchivalGroup,
	closure archivedDependencyClosureResult,
) error {
	expected := make([]archivalGroupID, 0,
		len(groups)+len(closure.DependencyValidatedCandidateGroups))
	requested := make(map[archivalGroupID]struct{}, len(groups))
	for _, group := range groups {
		expected = append(expected, group.id)
		requested[group.id] = struct{}{}
	}
	for _, candidate := range closure.DependencyValidatedCandidateGroups {
		if _, requestedGroup := requested[candidate.Candidate.GroupID]; !requestedGroup {
			expected = append(expected, candidate.Candidate.GroupID)
		}
	}
	slices.Sort(expected)
	validated := slices.Clone(closure.ValidatedGroupIDs)
	slices.Sort(validated)
	if !slices.Equal(expected, validated) {
		return fmt.Errorf("stage 11 dependency closure did not validate exactly the requested and existing groups")
	}
	resumeGroupIDs := make([]archivalGroupID, 0, len(groups))
	for _, group := range groups {
		if group.resume != nil {
			resumeGroupIDs = append(resumeGroupIDs, group.id)
		}
	}
	validatedCandidateIDs := make([]archivalGroupID, 0, len(resumeGroupIDs))
	for _, candidate := range closure.DependencyValidatedCandidateGroups {
		if _, requestedGroup := requested[candidate.Candidate.GroupID]; requestedGroup {
			validatedCandidateIDs = append(validatedCandidateIDs, candidate.Candidate.GroupID)
		}
	}
	slices.Sort(validatedCandidateIDs)
	if !slices.Equal(resumeGroupIDs, validatedCandidateIDs) {
		return fmt.Errorf("stage 11 candidate validation does not match the requested resume groups")
	}
	for _, object := range closure.Objects {
		if len(object.GroupIDs) == 0 {
			return fmt.Errorf("dependency %s has no validated archival groups",
				markerObjectDisplayName(object.Identity))
		}
		for _, groupID := range object.GroupIDs {
			if !slices.Contains(expected, groupID) {
				return fmt.Errorf("dependency %s references unrequested archival group %q",
					markerObjectDisplayName(object.Identity), groupID)
			}
		}
		switch object.Classification {
		case archivedDependencyClassificationTargetCompatible,
			archivedDependencyClassificationSharedWithTarget,
			archivedDependencyClassificationExclusiveMovable,
			archivedDependencyClassificationSharedArchivedOnly:
		case archivedDependencyClassificationUnsupported:
			return fmt.Errorf("dependency %s requires later-stage handling",
				markerObjectDisplayName(object.Identity))
		default:
			return fmt.Errorf("dependency %s has unvalidated closure classification %q",
				markerObjectDisplayName(object.Identity), object.Classification)
		}
	}
	return nil
}

func plainTableArchivalRelationOIDs(groups []preparedArchivalGroup) []uint32 {
	var oids []uint32
	for _, group := range groups {
		for _, member := range group.members {
			oids = append(oids, member.marker.SourceTable.OID)
		}
	}
	slices.Sort(oids)
	return oids
}

type plainTableArchivalPhaseBarrierID struct {
	phase int
}

func (id plainTableArchivalPhaseBarrierID) String() string {
	return fmt.Sprintf("archival:%02d:phase_barrier", id.phase)
}

func buildPlainTableArchivalGraph(
	inventory schema.CatalogInventory,
	groups []preparedArchivalGroup,
	isolation archivalIsolationPlan,
) (*sqlGraph, error) {
	graph := newSqlGraph()
	phaseIDs := make(map[int][]sqlVertexId, 8)
	for idx := range groups {
		group := &groups[idx]
		groupIsolation, ok := archivalIsolationGroupPlanByID(isolation.Groups, group.id)
		if !ok {
			return nil, fmt.Errorf("isolation plan is missing group %q", group.id)
		}
		vertices := []sqlVertex{
			plainTableArchivalVertex(group.id, plainTableArchivalVertexKindGroupInitialization,
				renderPlainTableArchivalInitialization(*group)),
			plainTableArchivalVertex(group.id, plainTableArchivalVertexKindMarkerRefresh,
				renderPlainTableArchivalMarkerRefresh(*group)),
			plainTableArchivalVertex(group.id, plainTableArchivalVertexKindCatalogAssertion,
				[]Statement{{DDL: renderPlainTableArchivalAssertion(*group, groupIsolation, isolation)}}),
		}
		for _, vertex := range vertices {
			graph.AddVertex(vertex)
			id := vertex.id.(plainTableArchivalVertexID)
			phase := plainTableArchivalVertexPhase(id.kind)
			phaseIDs[phase] = append(phaseIDs[phase], id)
		}
		moveKind := plainTableArchivalVertexKindTableMove
		if group.resume != nil {
			moveKind = plainTableArchivalVertexKindResumeTableMove
		}
		moveIDs := make(map[string]plainTableArchivalVertexID)
		for _, member := range orderedRemainingArchivalMembers(*group) {
			id := archivalMemberMoveVertexID(*group, member, moveKind)
			graph.AddVertex(sqlVertex{id: id, statements: renderArchivalMemberMove(member)})
			phaseIDs[plainTableArchivalVertexPhase(moveKind)] = append(
				phaseIDs[plainTableArchivalVertexPhase(moveKind)], id,
			)
			moveIDs[member.marker.MemberID] = id
		}
		for _, edge := range group.marker.PartitionEdges {
			childID, childMoves := moveIDs[edge.ChildMemberID]
			parentID, parentMoves := moveIDs[edge.ParentMemberID]
			if childMoves && parentMoves {
				if err := graph.AddEdge(childID.String(), parentID.String()); err != nil {
					return nil, fmt.Errorf("ordering archival descendant before parent: %w", err)
				}
			}
		}
		if group.detach != nil {
			detachID := plainTableArchivalVertexID{
				kind: plainTableArchivalVertexKindPartitionDetach, groupID: group.id,
			}
			graph.AddVertex(sqlVertex{
				id:         detachID,
				statements: []Statement{renderArchivalPartitionDetach(*group.detach)},
			})
			phaseIDs[plainTableArchivalVertexPhase(detachID.kind)] = append(
				phaseIDs[plainTableArchivalVertexPhase(detachID.kind)], detachID,
			)
			for _, moveID := range moveIDs {
				if err := graph.AddEdge(moveID.String(), detachID.String()); err != nil {
					return nil, fmt.Errorf("ordering archival move before partition detach: %w", err)
				}
			}
		}
	}
	isolationVertices, err := archivalIsolationVertices(inventory, isolation)
	if err != nil {
		return nil, err
	}
	for _, vertex := range isolationVertices {
		graph.AddVertex(vertex)
		id := vertex.id.(archivalIsolationVertexID)
		phaseIDs[id.phase] = append(phaseIDs[id.phase], id)
	}
	for phase := 1; phase <= 9; phase++ {
		graph.AddVertex(sqlVertex{id: plainTableArchivalPhaseBarrierID{phase: phase}})
	}
	for phase := 1; phase <= 9; phase++ {
		barrier := plainTableArchivalPhaseBarrierID{phase: phase}
		if phase == 9 {
			continue
		}
		next := plainTableArchivalPhaseBarrierID{phase: phase + 1}
		if len(phaseIDs[phase]) == 0 {
			if err := graph.AddEdge(barrier.String(), next.String()); err != nil {
				return nil, fmt.Errorf("adding empty archival phase dependency: %w", err)
			}
			continue
		}
		for _, operationID := range phaseIDs[phase] {
			if err := graph.AddEdge(barrier.String(), operationID.String()); err != nil {
				return nil, fmt.Errorf("adding archival phase start dependency: %w", err)
			}
			if err := graph.AddEdge(operationID.String(), next.String()); err != nil {
				return nil, fmt.Errorf("adding archival phase end dependency: %w", err)
			}
		}
	}
	return graph, nil
}

func plainTableArchivalVertex(
	groupID archivalGroupID,
	kind plainTableArchivalVertexKind,
	statements []Statement,
) sqlVertex {
	return sqlVertex{id: plainTableArchivalVertexID{kind: kind, groupID: groupID}, statements: statements}
}

func archivalMemberMoveVertexID(
	group preparedArchivalGroup,
	member preparedArchivalMember,
	kind plainTableArchivalVertexKind,
) plainTableArchivalVertexID {
	memberKey := ""
	if len(group.members) > 1 {
		memberKey = fmt.Sprintf("%06d:%s.%s", 999999-member.depth,
			member.marker.SourceTable.SchemaName, member.marker.SourceTable.Name)
	}
	return plainTableArchivalVertexID{kind: kind, groupID: group.id, memberKey: memberKey}
}

func orderedRemainingArchivalMembers(group preparedArchivalGroup) []preparedArchivalMember {
	var members []preparedArchivalMember
	for _, member := range group.members {
		if member.remainingMove != nil {
			members = append(members, member)
		}
	}
	slices.SortFunc(members, func(a, b preparedArchivalMember) int {
		return cmp.Or(cmp.Compare(b.depth, a.depth),
			compareMarkerObjects(a.marker.SourceTable, b.marker.SourceTable))
	})
	return members
}

func renderPlainTableArchivalInitialization(group preparedArchivalGroup) []Statement {
	if group.allocation == nil {
		return nil
	}
	var body strings.Builder
	body.WriteString("DECLARE\n    archival_role_name text;\nBEGIN\n")
	for _, schemaName := range archivedMarkerSchemaNames(group.marker) {
		escapedName := schema.EscapeIdentifier(schemaName)
		fmt.Fprintf(&body, "    CREATE SCHEMA %s;\n", escapedName)
		fmt.Fprintf(&body, "    REVOKE ALL PRIVILEGES ON SCHEMA %s FROM PUBLIC;\n", escapedName)
		body.WriteString("    FOR archival_role_name IN\n")
		body.WriteString("        SELECT pg_catalog.pg_get_userbyid(acl.grantee)\n")
		body.WriteString("        FROM pg_catalog.pg_namespace AS n\n")
		body.WriteString("        CROSS JOIN LATERAL pg_catalog.aclexplode(\n")
		body.WriteString("            COALESCE(n.nspacl, pg_catalog.acldefault('n', n.nspowner))\n")
		body.WriteString("        ) AS acl\n")
		fmt.Fprintf(&body, "        WHERE n.nspname = %s\n", schema.EscapeLiteral(schemaName))
		body.WriteString("          AND acl.grantee <> 0\n")
		body.WriteString("          AND acl.grantee <> n.nspowner\n")
		body.WriteString("        GROUP BY acl.grantee\n")
		body.WriteString("        ORDER BY acl.grantee\n")
		body.WriteString("    LOOP\n")
		fmt.Fprintf(&body, "        EXECUTE pg_catalog.format(%s, %s, archival_role_name);\n",
			schema.EscapeLiteral("REVOKE ALL PRIVILEGES ON SCHEMA %I FROM %I"),
			schema.EscapeLiteral(schemaName))
		body.WriteString("    END LOOP;\n")
		fmt.Fprintf(&body, "    COMMENT ON SCHEMA %s IS %s;\n", escapedName,
			escapeArchivalMarkerSQLLiteral(group.markerText))
	}
	body.WriteString("END")
	return []Statement{{
		DDL: doBlock(body.String()), Hazards: []MigrationHazard{migrationHazardArchivalSchemaLockdown},
	}}
}

func renderArchivalMemberMove(member preparedArchivalMember) []Statement {
	move := member.remainingMove
	if move == nil {
		return nil
	}
	return []Statement{{
		DDL: fmt.Sprintf("ALTER TABLE %s.%s SET SCHEMA %s",
			schema.EscapeIdentifier(move.SourceTable.SchemaName),
			schema.EscapeIdentifier(move.SourceTable.Name),
			schema.EscapeIdentifier(move.DestinationTable.SchemaName)),
		Hazards: []MigrationHazard{migrationHazardPlainTableSetSchema},
	}}
}

func renderArchivalPartitionDetach(operation archivalPartitionDetachOperation) Statement {
	return Statement{
		DDL: fmt.Sprintf("ALTER TABLE %s.%s DETACH PARTITION %s.%s",
			schema.EscapeIdentifier(operation.parent.SchemaName),
			schema.EscapeIdentifier(operation.parent.Name),
			schema.EscapeIdentifier(operation.root.CleanupTable.SchemaName),
			schema.EscapeIdentifier(operation.root.CleanupTable.Name)),
		Hazards: slices.Clone(migrationHazardArchivalPartitionDetach),
	}
}

func renderPlainTableArchivalMarkerRefresh(group preparedArchivalGroup) []Statement {
	if group.resume == nil {
		return nil
	}
	updates := slices.Clone(group.resume.RemainingMarkerUpdates)
	slices.SortFunc(updates, func(a, b archivedMarkerUpdateDescriptor) int {
		return cmp.Compare(a.SchemaName, b.SchemaName)
	})
	var body strings.Builder
	body.WriteString("BEGIN\n")
	for _, update := range updates {
		fmt.Fprintf(&body, "    COMMENT ON SCHEMA %s IS %s;\n",
			schema.EscapeIdentifier(update.SchemaName),
			escapeArchivalMarkerSQLLiteral(group.markerText))
	}
	body.WriteString("END")
	return []Statement{{DDL: doBlock(body.String())}}
}

func renderPlainTableArchivalAssertion(
	group preparedArchivalGroup,
	isolation archivalIsolationGroupPlan,
	completeIsolation archivalIsolationPlan,
) string {
	var body strings.Builder
	body.WriteString("BEGIN\n")
	for _, schemaName := range archivedMarkerSchemaNames(group.marker) {
		appendPlainTableAssertion(&body, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM pg_catalog.pg_namespace AS n WHERE n.nspname = %s AND "+
				"pg_catalog.obj_description(n.oid, 'pg_namespace') = %s)",
			schema.EscapeLiteral(schemaName), schema.EscapeLiteral(group.markerText),
		),
			fmt.Sprintf("archival marker mismatch for group %s schema %s", group.id, schemaName))
	}
	for _, member := range group.members {
		appendPlainTableMemberAssertions(&body, group, member)
	}
	appendArchivalPartitionAssertions(&body, group)
	appendPlainTableDependencySchemaAssertions(&body, group)
	appendArchivalIsolationAssertions(&body, group, isolation)
	appendPlainTablePreservedForeignKeyAssertions(&body, group.id, completeIsolation)
	body.WriteString("END")
	return doBlock(body.String())
}

func appendPlainTableMemberAssertions(
	body *strings.Builder,
	group preparedArchivalGroup,
	preparedMember preparedArchivalMember,
) {
	member := preparedMember.marker
	tableOID := member.CleanupTable.OID
	for _, object := range slices.Concat(member.AutomaticallyMovedObjects, member.AttachedObjects,
		member.InternalToastObjects) {
		predicate, ok := plainTableMarkerObjectAssertion(
			object, tableOID, preparedMember.relationKind, member.MemberID != group.rootMemberID,
		)
		if !ok {
			continue
		}
		appendPlainTableAssertion(body, predicate, fmt.Sprintf(
			"archival %s identity mismatch for group %s OID %d", object.Kind, group.id, object.OID,
		))
	}
	appendExactOIDSetAssertion(body, "cleanup relation", group.id,
		fmt.Sprintf("SELECT c.oid::bigint FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = c.relnamespace WHERE n.nspname = %s",
			schema.EscapeLiteral(member.CleanupTable.SchemaName)),
		markerObjectOIDs(member.AutomaticallyMovedObjects,
			archivalMarkerObjectKindTable, archivalMarkerObjectKindIndex, archivalMarkerObjectKindOwnedSequence))
	appendExactOIDSetAssertion(body, "cleanup type", group.id,
		fmt.Sprintf("SELECT t.oid::bigint FROM pg_catalog.pg_type AS t JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = t.typnamespace WHERE n.nspname = %s",
			schema.EscapeLiteral(member.CleanupTable.SchemaName)),
		markerObjectOIDs(member.AutomaticallyMovedObjects,
			archivalMarkerObjectKindRowType, archivalMarkerObjectKindArrayType))
	appendExactOIDSetAssertion(body, "cleanup constraint", group.id,
		fmt.Sprintf("SELECT c.oid::bigint FROM pg_catalog.pg_constraint AS c JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = c.connamespace WHERE n.nspname = %s",
			schema.EscapeLiteral(member.CleanupTable.SchemaName)),
		markerObjectOIDs(member.AutomaticallyMovedObjects, archivalMarkerObjectKindConstraint))
	appendExactOIDSetAssertion(body, "attached trigger", group.id,
		fmt.Sprintf("SELECT t.oid::bigint FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = %d", tableOID),
		markerObjectOIDs(member.AttachedObjects, archivalMarkerObjectKindTrigger))
	appendExactOIDSetAssertion(body, "attached rule", group.id,
		fmt.Sprintf("SELECT r.oid::bigint FROM pg_catalog.pg_rewrite AS r WHERE r.ev_class = %d", tableOID),
		markerObjectOIDs(member.AttachedObjects, archivalMarkerObjectKindRule))
	appendExactOIDSetAssertion(body, "attached policy", group.id,
		fmt.Sprintf("SELECT p.oid::bigint FROM pg_catalog.pg_policy AS p WHERE p.polrelid = %d", tableOID),
		markerObjectOIDs(member.AttachedObjects, archivalMarkerObjectKindPolicy))
	appendExactOIDSetAssertion(body, "TOAST relation", group.id,
		fmt.Sprintf("SELECT c.reltoastrelid::bigint FROM pg_catalog.pg_class AS c "+
			"WHERE c.oid = %d AND c.reltoastrelid <> 0", tableOID),
		markerObjectOIDs(member.InternalToastObjects, archivalMarkerObjectKindToastRelation))
	appendExactOIDSetAssertion(body, "extended statistic", group.id,
		fmt.Sprintf("SELECT s.oid::bigint FROM pg_catalog.pg_statistic_ext AS s JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = s.stxnamespace WHERE n.nspname = %s",
			schema.EscapeLiteral(member.CleanupTable.SchemaName)),
		markerObjectOIDs(member.ExplicitlyMovedObjects, archivalMarkerObjectKindExtendedStatistic))
	for _, object := range member.ExplicitlyMovedObjects {
		predicate, ok := plainTableMarkerObjectAssertion(object, tableOID, "", false)
		if ok {
			appendPlainTableAssertion(body, predicate, fmt.Sprintf(
				"archival %s identity mismatch for group %s OID %d", object.Kind, group.id, object.OID,
			))
		}
	}
	for _, catalog := range []string{"pg_proc", "pg_collation", "pg_operator"} {
		appendPlainTableAssertion(body, fmt.Sprintf(
			"NOT EXISTS (SELECT 1 FROM pg_catalog.%s AS o JOIN pg_catalog.pg_namespace AS n "+
				"ON n.oid = o.%s WHERE n.nspname = %s)",
			catalog, plainTableCatalogNamespaceColumn(catalog),
			schema.EscapeLiteral(member.CleanupTable.SchemaName),
		),
			fmt.Sprintf("unexpected %s object in group %s member schema", catalog, group.id))
	}
}

func appendPlainTableDependencySchemaAssertions(body *strings.Builder, group preparedArchivalGroup) {
	schemaName := group.marker.ExclusiveDependencySchemas[0].Name
	for _, object := range group.marker.ExclusiveDependencyObjects {
		appendPlainTableAssertion(body, archivalDependencyObjectAssertion(object), fmt.Sprintf(
			"archival dependency identity mismatch for group %s OID %d in schema %s",
			group.id, object.OID, schemaName,
		))
	}
}

func plainTableMarkerObjectAssertion(
	object archivalMarkerObjectIdentity,
	tableOID uint32,
	relationKind schema.RelKind,
	isPartition bool,
) (string, bool) {
	schemaLiteral := schema.EscapeLiteral(object.SchemaName)
	nameLiteral := schema.EscapeLiteral(object.Name)
	switch object.Kind {
	case archivalMarkerObjectKindTable:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = c.relnamespace WHERE c.oid = %d AND n.nspname = %s AND c.relname = %s "+
			"AND c.relkind = %s AND c.relispartition = %t)", object.OID, schemaLiteral, nameLiteral,
			schema.EscapeLiteral(string(relationKind)), isPartition), true
	case archivalMarkerObjectKindRowType:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_type AS t JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = t.typnamespace WHERE t.oid = %d AND n.nspname = %s AND t.typname = %s "+
			"AND t.typrelid = %d)", object.OID, schemaLiteral, nameLiteral, tableOID), true
	case archivalMarkerObjectKindArrayType:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_type AS t JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = t.typnamespace WHERE t.oid = %d AND n.nspname = %s AND t.typname = %s)",
			object.OID, schemaLiteral, nameLiteral), true
	case archivalMarkerObjectKindIndex:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = c.relnamespace JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.oid "+
			"WHERE c.oid = %d AND n.nspname = %s AND c.relname = %s AND i.indrelid = %d)",
			object.OID, schemaLiteral, nameLiteral, tableOID), true
	case archivalMarkerObjectKindConstraint:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_constraint AS c JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = c.connamespace WHERE c.oid = %d AND n.nspname = %s AND c.conname = %s "+
			"AND c.conrelid = %d)", object.OID, schemaLiteral, nameLiteral, tableOID), true
	case archivalMarkerObjectKindOwnedSequence:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = c.relnamespace JOIN pg_catalog.pg_depend AS d ON d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass "+
			"AND d.objid = c.oid AND d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass "+
			"WHERE c.oid = %d AND n.nspname = %s AND c.relname = %s AND c.relkind = 'S' "+
			"AND d.refobjid = %d AND d.refobjsubid > 0 AND d.deptype IN ('a', 'i'))",
			object.OID, schemaLiteral, nameLiteral, tableOID), true
	case archivalMarkerObjectKindTrigger:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_trigger AS t WHERE t.oid = %d "+
			"AND t.tgrelid = %d AND t.tgname = %s)", object.OID, tableOID, nameLiteral), true
	case archivalMarkerObjectKindRule:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_rewrite AS r WHERE r.oid = %d "+
			"AND r.ev_class = %d AND r.rulename = %s)", object.OID, tableOID, nameLiteral), true
	case archivalMarkerObjectKindPolicy:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_policy AS p WHERE p.oid = %d "+
			"AND p.polrelid = %d AND p.polname = %s)", object.OID, tableOID, nameLiteral), true
	case archivalMarkerObjectKindToastRelation:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = c.relnamespace JOIN pg_catalog.pg_class AS owner ON owner.reltoastrelid = c.oid "+
			"WHERE c.oid = %d AND n.nspname = %s AND c.relname = %s AND owner.oid = %d)",
			object.OID, schemaLiteral, nameLiteral, tableOID), true
	case archivalMarkerObjectKindExtendedStatistic:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_statistic_ext AS s "+
			"JOIN pg_catalog.pg_namespace AS n ON n.oid = s.stxnamespace "+
			"WHERE s.oid = %d AND n.nspname = %s AND s.stxname = %s AND s.stxrelid = %d)",
			object.OID, schemaLiteral, nameLiteral, tableOID), true
	default:
		return "", false
	}
}

func archivalDependencyObjectAssertion(object archivalMarkerObjectIdentity) string {
	schemaLiteral := schema.EscapeLiteral(object.SchemaName)
	nameLiteral := schema.EscapeLiteral(object.Name)
	switch object.Kind {
	case archivalMarkerObjectKindSequence:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = c.relnamespace WHERE c.oid = %d AND n.nspname = %s AND c.relname = %s AND c.relkind = 'S')",
			object.OID, schemaLiteral, nameLiteral)
	case archivalMarkerObjectKindFunction:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_proc AS p JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = p.pronamespace WHERE p.oid = %d AND n.nspname = %s AND p.proname = %s)",
			object.OID, schemaLiteral, nameLiteral)
	case archivalMarkerObjectKindType:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_type AS t JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = t.typnamespace WHERE t.oid = %d AND n.nspname = %s AND t.typname = %s)",
			object.OID, schemaLiteral, nameLiteral)
	case archivalMarkerObjectKindCollation:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_collation AS c JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = c.collnamespace WHERE c.oid = %d AND n.nspname = %s AND c.collname = %s)",
			object.OID, schemaLiteral, nameLiteral)
	case archivalMarkerObjectKindOperator:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_operator AS o JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = o.oprnamespace WHERE o.oid = %d AND n.nspname = %s AND o.oprname = %s)",
			object.OID, schemaLiteral, nameLiteral)
	default:
		return "FALSE"
	}
}

func appendExactOIDSetAssertion(
	body *strings.Builder,
	label string,
	groupID archivalGroupID,
	query string,
	expected []uint32,
) {
	appendPlainTableAssertion(body, fmt.Sprintf(
		"(SELECT COALESCE(pg_catalog.array_agg(actual.oid ORDER BY actual.oid), ARRAY[]::bigint[]) "+
			"FROM (%s) AS actual(oid)) = %s", query, postgresBigintArray(expected),
	),
		fmt.Sprintf("archival %s set mismatch for group %s", label, groupID))
}

func appendPlainTableAssertion(body *strings.Builder, predicate, message string) {
	fmt.Fprintf(body, "    IF NOT (%s) THEN\n", predicate)
	fmt.Fprintf(body, "        RAISE EXCEPTION USING MESSAGE = %s;\n", schema.EscapeLiteral(message))
	body.WriteString("    END IF;\n")
}

func markerObjectOIDs(
	objects []archivalMarkerObjectIdentity,
	kinds ...archivalMarkerObjectKind,
) []uint32 {
	var result []uint32
	for _, object := range objects {
		if slices.Contains(kinds, object.Kind) {
			result = append(result, object.OID)
		}
	}
	slices.Sort(result)
	return result
}

func postgresBigintArray(values []uint32) string {
	if len(values) == 0 {
		return "ARRAY[]::bigint[]"
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%d::bigint", value))
	}
	return "ARRAY[" + strings.Join(parts, ", ") + "]"
}

func plainTableCatalogNamespaceColumn(catalog string) string {
	switch catalog {
	case "pg_proc":
		return "pronamespace"
	case "pg_collation":
		return "collnamespace"
	case "pg_operator":
		return "oprnamespace"
	case "pg_statistic_ext":
		return "stxnamespace"
	default:
		return ""
	}
}

func catalogSchemaWithName(inventory schema.CatalogInventory, name string) *schema.CatalogSchema {
	for idx := range inventory.Schemas {
		if inventory.Schemas[idx].Name == name {
			return &inventory.Schemas[idx]
		}
	}
	return nil
}

func doBlock(body string) string {
	tag := "$pgschemadiff_stage12$"
	for suffix := 1; strings.Contains(body, tag); suffix++ {
		tag = fmt.Sprintf("$pgschemadiff_stage12_%d$", suffix)
	}
	return "DO " + tag + "\n" + body + "\n" + tag
}
