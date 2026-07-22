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
}

type plainTableArchivalVertexKind string

const (
	plainTableArchivalVertexKindGroupInitialization plainTableArchivalVertexKind = "group_initialization"
	plainTableArchivalVertexKindTableMove           plainTableArchivalVertexKind = "table_move"
	plainTableArchivalVertexKindResumeTableMove     plainTableArchivalVertexKind = "resume_table_move"
	plainTableArchivalVertexKindMarkerRefresh       plainTableArchivalVertexKind = "marker_refresh"
	plainTableArchivalVertexKindCatalogAssertion    plainTableArchivalVertexKind = "catalog_assertion"
)

type plainTableArchivalVertexID struct {
	kind    plainTableArchivalVertexKind
	groupID archivalGroupID
}

func (id plainTableArchivalVertexID) String() string {
	return fmt.Sprintf("archival:%02d:%s:%s", plainTableArchivalVertexPhase(id.kind), id.groupID, id.kind)
}

func plainTableArchivalVertexPhase(kind plainTableArchivalVertexKind) int {
	switch kind {
	case plainTableArchivalVertexKindGroupInitialization:
		return 1
	case plainTableArchivalVertexKindMarkerRefresh:
		return 2
	case plainTableArchivalVertexKindTableMove, plainTableArchivalVertexKindResumeTableMove:
		return 4
	case plainTableArchivalVertexKindCatalogAssertion:
		return 8
	default:
		return 99
	}
}

type preparedPlainTableArchivalGroup struct {
	id            archivalGroupID
	marker        archivalMarkerV1
	markerText    string
	member        archivalMarkerMemberV1
	allocation    *archivalGroupNameAllocation
	resume        *archivedGroupResumeDescriptor
	remainingMove *archivedMemberMoveDescriptor
}

var migrationHazardPlainTableSetSchema = MigrationHazard{
	Type: MigrationHazardTypeAcquiresAccessExclusiveLock,
	Message: "Moving a table to another schema acquires an ACCESS EXCLUSIVE lock on the table. " +
		"The catalog-only move should be brief, but all table access is blocked while the lock is held.",
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
) ([]preparedPlainTableArchivalGroup, error) {
	if len(request.Groups) == 0 {
		return nil, nil
	}
	inventory := request.CurrentInventory.Normalize()
	groups := make([]preparedPlainTableArchivalGroup, 0, len(request.Groups))
	seenGroups := make(map[archivalGroupID]struct{}, len(request.Groups))
	seenRelations := make(map[uint32]struct{}, len(request.Groups))
	for idx, groupRequest := range request.Groups {
		group, err := preparePlainTableArchivalGroup(inventory, request.DependencyClosure, groupRequest)
		if err != nil {
			return nil, fmt.Errorf("preparing plain-table archival group %d: %w", idx, err)
		}
		if _, duplicate := seenGroups[group.id]; duplicate {
			return nil, fmt.Errorf("plain-table archival group %q is duplicated", group.id)
		}
		seenGroups[group.id] = struct{}{}
		if _, duplicate := seenRelations[group.member.SourceTable.OID]; duplicate {
			return nil, fmt.Errorf("plain-table relation OID %d belongs to more than one group",
				group.member.SourceTable.OID)
		}
		seenRelations[group.member.SourceTable.OID] = struct{}{}
		groups = append(groups, group)
	}
	slices.SortFunc(groups, func(a, b preparedPlainTableArchivalGroup) int {
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
) (preparedPlainTableArchivalGroup, error) {
	marker, err := parseArchivalMarker(request.FinalizedMarker)
	if err != nil {
		return preparedPlainTableArchivalGroup{},
			fmt.Errorf("parsing finalized marker: %w", err)
	}
	canonicalMarker, err := marshalArchivalMarker(marker)
	if err != nil {
		return preparedPlainTableArchivalGroup{},
			fmt.Errorf("canonicalizing finalized marker: %w", err)
	}
	if request.FinalizedMarker != canonicalMarker {
		return preparedPlainTableArchivalGroup{},
			fmt.Errorf("finalized marker for group %q is not canonical", marker.GroupID)
	}
	if (request.Allocation == nil) == (request.Resume == nil) {
		return preparedPlainTableArchivalGroup{}, fmt.Errorf(
			"group %q must contain exactly one of a Stage 7 allocation or Stage 9 resume descriptor", marker.GroupID,
		)
	}
	if err := validateStage12PlainMarker(marker); err != nil {
		return preparedPlainTableArchivalGroup{}, fmt.Errorf(
			"validating finalized marker for group %q: %w",
			marker.GroupID, err,
		)
	}
	group := preparedPlainTableArchivalGroup{
		id: marker.GroupID, marker: marker, markerText: canonicalMarker, member: marker.Members[0],
		allocation: request.Allocation, resume: request.Resume,
	}
	if request.Allocation != nil {
		if err := validateNewPlainTableArchivalGroup(inventory, group); err != nil {
			return preparedPlainTableArchivalGroup{}, err
		}
		move := archivedMemberMoveDescriptor{
			MemberID: group.member.MemberID, RelationOID: group.member.SourceTable.OID,
			SourceTable: group.member.SourceTable, DestinationTable: group.member.CleanupTable,
		}
		group.remainingMove = &move
		return group, nil
	}
	if err := validateResumedPlainTableArchivalGroup(inventory, closure, &group); err != nil {
		return preparedPlainTableArchivalGroup{}, err
	}
	return group, nil
}

func validateStage12PlainMarker(marker archivalMarkerV1) error {
	if len(marker.Members) != 1 {
		return fmt.Errorf("plain-table group must declare exactly one member, got %d", len(marker.Members))
	}
	if len(marker.PartitionEdges) != 0 {
		return fmt.Errorf("plain-table group must not declare partition edges")
	}
	if len(marker.ExclusiveDependencySchemas) != 1 {
		return fmt.Errorf("plain-table group must declare exactly one dependency schema, got %d",
			len(marker.ExclusiveDependencySchemas))
	}
	return nil
}

func validateNewPlainTableArchivalGroup(
	inventory schema.CatalogInventory,
	group preparedPlainTableArchivalGroup,
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
	if len(allocation.Members) != 1 {
		return fmt.Errorf("stage 7 allocation for group %q must contain one member, got %d",
			group.id, len(allocation.Members))
	}
	allocated := allocation.Members[0]
	if allocated.RelationOID != group.member.SourceTable.OID ||
		allocated.SourceSchemaName != group.member.SourceTable.SchemaName ||
		allocated.SourceTableName != group.member.SourceTable.Name ||
		allocated.CleanupSchemaName != group.member.CleanupTable.SchemaName {
		return fmt.Errorf("stage 7 member allocation does not match finalized marker for group %q", group.id)
	}
	if allocated.EscapedCleanupSchemaName !=
		schema.EscapeIdentifier(allocated.CleanupSchemaName) {
		return fmt.Errorf("stage 7 cleanup schema quoting does not match group %q", group.id)
	}
	dependencySchema := group.marker.ExclusiveDependencySchemas[0].Name
	if allocation.DependencySchemaName != dependencySchema ||
		allocation.EscapedDependencySchemaName != schema.EscapeIdentifier(dependencySchema) {
		return fmt.Errorf("stage 7 dependency schema allocation does not match finalized marker for group %q", group.id)
	}
	return validatePlainTableMemberAgainstInventory(inventory, group.member, false)
}

func validateResumedPlainTableArchivalGroup(
	inventory schema.CatalogInventory,
	closure archivedDependencyClosureResult,
	group *preparedPlainTableArchivalGroup,
) error {
	resume := *group.resume
	if resume.GroupID != group.id {
		return fmt.Errorf("stage 9 resume group %q does not match marker group %q", resume.GroupID, group.id)
	}
	if len(resume.RemainingMemberMoves) > 1 {
		return fmt.Errorf("stage 9 resume group %q contains %d table moves; one plain table is required",
			group.id, len(resume.RemainingMemberMoves))
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
	relation, err := uniqueRelationByOID(inventory, group.member.SourceTable.OID)
	if err != nil {
		return err
	}
	moved := relation.SchemaName == group.member.CleanupTable.SchemaName
	if len(resume.RemainingMemberMoves) == 1 {
		move := resume.RemainingMemberMoves[0]
		if !sameArchivedMemberMove(move, group.member) {
			return fmt.Errorf("stage 9 table move for group %q does not match the finalized marker", group.id)
		}
		if moved {
			return fmt.Errorf("stage 9 group %q requests a move for a table already in its cleanup schema", group.id)
		}
		group.remainingMove = &move
	} else if !moved {
		return fmt.Errorf("stage 9 group %q omits the remaining source-table move", group.id)
	}
	return validatePlainTableMemberAgainstInventory(inventory, group.member, moved)
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

func validatePlainTableMemberAgainstInventory(
	inventory schema.CatalogInventory,
	member archivalMarkerMemberV1,
	moved bool,
) error {
	relation, err := uniqueRelationByOID(inventory, member.SourceTable.OID)
	if err != nil {
		return err
	}
	kind, tableLike := inventory.ClassifyTable(relation.OID)
	if !tableLike || kind != schema.CatalogTableKindOrdinary {
		return fmt.Errorf("relation OID %d (%s.%s) has unsupported plain-table classification %q",
			relation.OID, relation.SchemaName, relation.Name, kind)
	}
	expectedSchema := member.SourceTable.SchemaName
	if moved {
		expectedSchema = member.CleanupTable.SchemaName
	}
	if relation.SchemaName != expectedSchema || relation.Name != member.SourceTable.Name {
		return fmt.Errorf("relation OID %d is %s.%s instead of expected %s.%s",
			relation.OID, relation.SchemaName, relation.Name, expectedSchema, member.SourceTable.Name)
	}
	return nil
}

func validatePlainTableArchivalPreflight(
	inventory schema.CatalogInventory,
	groups []preparedPlainTableArchivalGroup,
	preflight sourceSafetyPreflightResult,
) error {
	expectedOIDs := plainTableArchivalRelationOIDs(groups)
	validatedOIDs := slices.Clone(preflight.ValidatedTableRelationOIDs)
	slices.Sort(validatedOIDs)
	if !slices.Equal(expectedOIDs, validatedOIDs) {
		return fmt.Errorf("stage 10 preflight did not validate exactly the requested plain-table OIDs")
	}
	if err := validateStage12PlatformInventory(inventory, groups); err != nil {
		return err
	}
	for _, group := range groups {
		move, err := inventory.ExpectedTableMove(group.member.SourceTable.OID)
		if err != nil {
			return err
		}
		move = filterPlainTableIsolationObjects(move, preflight)
		relation, err := uniqueRelationByOID(inventory, group.member.SourceTable.OID)
		if err != nil {
			return err
		}
		moved := relation.SchemaName == group.member.CleanupTable.SchemaName
		declaredSchemas := map[string]struct{}{group.member.CleanupTable.SchemaName: {}}
		if err := validateMemberCatalogObjects(inventory, group.member, move, moved,
			declaredSchemas, make(map[string]struct{})); err != nil {
			return fmt.Errorf("validating Stage 14 table-local isolation for group %q: %w", group.id, err)
		}
	}
	return nil
}

func filterPlainTableIsolationObjects(
	move schema.CatalogExpectedTableMove,
	preflight sourceSafetyPreflightResult,
) schema.CatalogExpectedTableMove {
	constraints := make(map[uint32]struct{})
	triggers := make(map[uint32]struct{})
	for _, foreignKey := range preflight.ForeignKeys {
		if foreignKey.Direction == sourceSafetyForeignKeyDirectionSelf {
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
	groups []preparedPlainTableArchivalGroup,
) error {
	for _, trigger := range inventory.EventTriggers {
		if trigger.EnabledMode != "D" {
			return fmt.Errorf("enabled event trigger %q is not supported by the plain-table move engine", trigger.Name)
		}
	}
	for _, publication := range inventory.Publications {
		if publication.PublishesAllTables {
			return fmt.Errorf("FOR ALL TABLES publication %q is not supported by the plain-table move engine",
				publication.Name)
		}
	}
	for _, member := range inventory.ExtensionMembers {
		if stage12ExtensionMemberIsRetained(member.Object, groups) {
			return fmt.Errorf("extension member %s is not supported by the plain-table move engine",
				sourceSafetyCatalogObjectDescription(member.Object))
		}
	}
	return nil
}

func stage12ExtensionMemberIsRetained(
	member schema.CatalogDependencyObject,
	groups []preparedPlainTableArchivalGroup,
) bool {
	for _, group := range groups {
		for _, object := range slices.Concat(
			group.member.AutomaticallyMovedObjects,
			group.member.AttachedObjects,
			group.member.InternalToastObjects,
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
	return false
}

func validatePlainTableArchivalClosure(
	groups []preparedPlainTableArchivalGroup,
	closure archivedDependencyClosureResult,
) error {
	expected := make([]archivalGroupID, 0, len(groups))
	for _, group := range groups {
		expected = append(expected, group.id)
	}
	validated := slices.Clone(closure.ValidatedGroupIDs)
	slices.Sort(validated)
	if !slices.Equal(expected, validated) {
		return fmt.Errorf("stage 11 dependency closure did not validate exactly the requested groups")
	}
	resumeGroupIDs := make([]archivalGroupID, 0, len(groups))
	for _, group := range groups {
		if group.resume != nil {
			resumeGroupIDs = append(resumeGroupIDs, group.id)
		}
	}
	validatedCandidateIDs := make([]archivalGroupID, 0,
		len(closure.DependencyValidatedCandidateGroups))
	for _, candidate := range closure.DependencyValidatedCandidateGroups {
		validatedCandidateIDs = append(validatedCandidateIDs, candidate.Candidate.GroupID)
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

func plainTableArchivalRelationOIDs(groups []preparedPlainTableArchivalGroup) []uint32 {
	oids := make([]uint32, 0, len(groups))
	for _, group := range groups {
		oids = append(oids, group.member.SourceTable.OID)
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
	groups []preparedPlainTableArchivalGroup,
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
		moveKind := plainTableArchivalVertexKindTableMove
		if group.resume != nil {
			moveKind = plainTableArchivalVertexKindResumeTableMove
		}
		vertices := []sqlVertex{
			plainTableArchivalVertex(group.id, plainTableArchivalVertexKindGroupInitialization,
				renderPlainTableArchivalInitialization(*group)),
			plainTableArchivalVertex(group.id, moveKind, renderPlainTableArchivalMove(*group)),
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

func renderPlainTableArchivalInitialization(group preparedPlainTableArchivalGroup) []Statement {
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

func renderPlainTableArchivalMove(group preparedPlainTableArchivalGroup) []Statement {
	if group.remainingMove == nil {
		return nil
	}
	move := group.remainingMove
	return []Statement{{
		DDL: fmt.Sprintf("ALTER TABLE %s.%s SET SCHEMA %s",
			schema.EscapeIdentifier(move.SourceTable.SchemaName),
			schema.EscapeIdentifier(move.SourceTable.Name),
			schema.EscapeIdentifier(move.DestinationTable.SchemaName)),
		Hazards: []MigrationHazard{migrationHazardPlainTableSetSchema},
	}}
}

func renderPlainTableArchivalMarkerRefresh(group preparedPlainTableArchivalGroup) []Statement {
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
	group preparedPlainTableArchivalGroup,
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
	appendPlainTableMemberAssertions(&body, group)
	appendPlainTableDependencySchemaAssertions(&body, group)
	appendArchivalIsolationAssertions(&body, group, isolation)
	appendPlainTablePreservedForeignKeyAssertions(&body, group.id, completeIsolation)
	body.WriteString("END")
	return doBlock(body.String())
}

func appendPlainTableMemberAssertions(body *strings.Builder, group preparedPlainTableArchivalGroup) {
	member := group.member
	tableOID := member.CleanupTable.OID
	for _, object := range slices.Concat(member.AutomaticallyMovedObjects, member.AttachedObjects,
		member.InternalToastObjects) {
		predicate, ok := plainTableMarkerObjectAssertion(object, tableOID)
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
		predicate, ok := plainTableMarkerObjectAssertion(object, tableOID)
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

func appendPlainTableDependencySchemaAssertions(body *strings.Builder, group preparedPlainTableArchivalGroup) {
	schemaName := group.marker.ExclusiveDependencySchemas[0].Name
	for _, object := range group.marker.ExclusiveDependencyObjects {
		appendPlainTableAssertion(body, archivalDependencyObjectAssertion(object), fmt.Sprintf(
			"archival dependency identity mismatch for group %s OID %d in schema %s",
			group.id, object.OID, schemaName,
		))
	}
}

func plainTableMarkerObjectAssertion(object archivalMarkerObjectIdentity, tableOID uint32) (string, bool) {
	schemaLiteral := schema.EscapeLiteral(object.SchemaName)
	nameLiteral := schema.EscapeLiteral(object.Name)
	switch object.Kind {
	case archivalMarkerObjectKindTable:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n "+
			"ON n.oid = c.relnamespace WHERE c.oid = %d AND n.nspname = %s AND c.relname = %s "+
			"AND c.relkind = 'r' AND NOT c.relispartition)", object.OID, schemaLiteral, nameLiteral), true
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
