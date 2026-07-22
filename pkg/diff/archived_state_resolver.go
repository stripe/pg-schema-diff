package diff

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

type archivedCandidateGroupState string

const (
	archivedCandidateGroupStateEmptyInitialized  archivedCandidateGroupState = "empty_initialized"
	archivedCandidateGroupStatePartialResumable  archivedCandidateGroupState = "partial_resumable"
	archivedCandidateGroupStateCompleteCandidate archivedCandidateGroupState = "complete_candidate"
)

// archivedStateResolution is intentionally provisional. Later stages must
// validate safety, dependency closure, shared components, and cleanup digests
// before using these names to filter the managed schema.
type archivedStateResolution struct {
	CandidateGroups                 []structurallyValidArchivedCandidateGroup
	ProvisionalUntrustedSchemaNames []string
}

type structurallyValidArchivedCandidateGroup struct {
	GroupID                      archivalGroupID
	State                        archivedCandidateGroupState
	Marker                       archivalMarkerV1
	SchemaNames                  []string
	ExpectedDependencySchemaName string
	Resume                       archivedGroupResumeDescriptor
}

type archivedGroupResumeDescriptor struct {
	GroupID                        archivalGroupID
	RemainingMemberMoves           []archivedMemberMoveDescriptor
	RemainingPartitionDetach       bool
	RemainingExplicitObjectMoves   []archivedObjectMoveDescriptor
	RemainingDependencyObjectMoves []archivedObjectMoveDescriptor
	RemainingACLRevokes            []archivedACLRevokeDescriptor
	RemainingMarkerUpdates         []archivedMarkerUpdateDescriptor
}

type archivedACLRevokeDescriptor struct {
	Kind   schema.CatalogACLRevokeKind
	Record archivalMarkerACLRecordV1
}

type archivedMemberMoveDescriptor struct {
	MemberID         string
	RelationOID      uint32
	SourceTable      archivalMarkerObjectIdentity
	DestinationTable archivalMarkerObjectIdentity
}

type archivedObjectMoveDescriptor struct {
	MemberID    string
	Source      archivalMarkerObjectIdentity
	Destination archivalMarkerObjectIdentity
}

type archivedMarkerUpdateDescriptor struct {
	SchemaName string
	Marker     string
}

type discoveredArchivedSchema struct {
	schema          schema.CatalogSchema
	payload         archivalMarkerV1
	canonicalMarker string
}

// resolveArchivedState reads only the unfiltered catalog inventories. It is
// Existing groups are filtered only after all later trust gates pass.
func resolveArchivedState(
	prefix string,
	currentSnapshot schema.SchemaSnapshot,
	targetSnapshot schema.SchemaSnapshot,
) (archivedStateResolution, error) {
	if err := validateSchemaPartialArchivalPrefix(prefix); err != nil {
		return archivedStateResolution{}, err
	}
	if err := validateNoReservedArchivalTargetSchemas(prefix, targetSnapshot.Inventory); err != nil {
		return archivedStateResolution{}, err
	}

	currentSchemas, err := uniqueCatalogSchemas(currentSnapshot.Inventory)
	if err != nil {
		return archivedStateResolution{}, fmt.Errorf(
			"discovering archived-state candidates: %w", err,
		)
	}
	candidateNames := make([]string, 0)
	for name := range currentSchemas {
		if isReservedArchivalSchemaName(name, prefix) {
			candidateNames = append(candidateNames, name)
		}
	}
	slices.Sort(candidateNames)

	discoveredByName := make(map[string]discoveredArchivedSchema, len(candidateNames))
	groupsByID := make(map[archivalGroupID][]discoveredArchivedSchema)
	for _, name := range candidateNames {
		catalogSchema := currentSchemas[name]
		if catalogSchema.Comment == "" {
			return archivedStateResolution{}, fmt.Errorf(
				"archival schema %q matches the reserved naming grammar but has no archival marker", name,
			)
		}
		payload, err := parseArchivalMarker(catalogSchema.Comment)
		if err != nil {
			return archivedStateResolution{}, fmt.Errorf(
				"parsing archival marker on schema %q: %w", name, err,
			)
		}
		canonicalMarker, err := marshalArchivalMarker(payload)
		if err != nil {
			return archivedStateResolution{}, fmt.Errorf(
				"canonicalizing archival marker on schema %q: %w", name, err,
			)
		}
		if err := validateArchivedMarkerNames(prefix, name, payload); err != nil {
			return archivedStateResolution{}, fmt.Errorf(
				"validating archival marker on schema %q: %w", name, err,
			)
		}
		discovered := discoveredArchivedSchema{
			schema: catalogSchema, payload: payload, canonicalMarker: canonicalMarker,
		}
		discoveredByName[name] = discovered
		groupsByID[payload.GroupID] = append(groupsByID[payload.GroupID], discovered)
	}

	groupIDs := make([]archivalGroupID, 0, len(groupsByID))
	for groupID := range groupsByID {
		groupIDs = append(groupIDs, groupID)
	}
	slices.Sort(groupIDs)

	resolution := archivedStateResolution{}
	for _, groupID := range groupIDs {
		discovered := groupsByID[groupID]
		slices.SortFunc(discovered, func(a, b discoveredArchivedSchema) int {
			return cmp.Compare(a.schema.Name, b.schema.Name)
		})
		payload := discovered[0].payload
		canonicalMarker := discovered[0].canonicalMarker
		declaredNames := archivedMarkerSchemaNames(payload)
		if len(discovered) != len(declaredNames) {
			return archivedStateResolution{}, fmt.Errorf(
				"archival group %q has %d discovered marker schemas but declares %d schemas",
				groupID, len(discovered), len(declaredNames),
			)
		}
		for _, schemaName := range declaredNames {
			if _, ok := currentSchemas[schemaName]; !ok {
				return archivedStateResolution{}, fmt.Errorf(
					"archival group %q declares missing schema %q", groupID, schemaName,
				)
			}
			copy, ok := discoveredByName[schemaName]
			if !ok {
				return archivedStateResolution{}, fmt.Errorf(
					"archival group %q declares schema %q outside the configured reserved naming grammar",
					groupID, schemaName,
				)
			}
			if copy.payload.GroupID != groupID {
				return archivedStateResolution{}, fmt.Errorf(
					"archival schema %q marker declares group %q instead of %q",
					schemaName, copy.payload.GroupID, groupID,
				)
			}
			if copy.canonicalMarker != canonicalMarker {
				return archivedStateResolution{}, fmt.Errorf(
					"archival group %q has inconsistent marker payload on schema %q", groupID, schemaName,
				)
			}
		}

		candidate, err := resolveArchivedCandidateGroup(
			currentSnapshot.Inventory, payload, declaredNames, canonicalMarker,
		)
		if err != nil {
			return archivedStateResolution{}, fmt.Errorf(
				"resolving archival group %q: %w", groupID, err,
			)
		}
		timestamp, nonce, err := parseArchivalGroupID(groupID)
		if err != nil {
			return archivedStateResolution{}, fmt.Errorf(
				"resolving archival group %q dependency schema: %w", groupID, err,
			)
		}
		candidate.ExpectedDependencySchemaName, err = buildArchivalDependencySchemaName(
			prefix, timestamp, nonce,
		)
		if err != nil {
			return archivedStateResolution{}, fmt.Errorf(
				"resolving archival group %q dependency schema: %w", groupID, err,
			)
		}
		resolution.CandidateGroups = append(resolution.CandidateGroups, candidate)
		resolution.ProvisionalUntrustedSchemaNames = append(
			resolution.ProvisionalUntrustedSchemaNames, declaredNames...,
		)
	}
	slices.Sort(resolution.ProvisionalUntrustedSchemaNames)
	return resolution, nil
}

func validateNoReservedArchivalTargetSchemas(prefix string, inventory schema.CatalogInventory) error {
	if err := validateSchemaPartialArchivalPrefix(prefix); err != nil {
		return err
	}
	for _, targetSchema := range inventory.Schemas {
		if isReservedArchivalSchemaName(targetSchema.Name, prefix) {
			return fmt.Errorf("target schema %q uses the reserved archival naming grammar for prefix %q",
				targetSchema.Name, prefix)
		}
	}
	return nil
}

func uniqueCatalogSchemas(inventory schema.CatalogInventory) (map[string]schema.CatalogSchema, error) {
	result := make(map[string]schema.CatalogSchema, len(inventory.Schemas))
	for _, catalogSchema := range inventory.Schemas {
		if existing, duplicate := result[catalogSchema.Name]; duplicate {
			return nil, fmt.Errorf("duplicate catalog schema %q with OIDs %d and %d",
				catalogSchema.Name, existing.OID, catalogSchema.OID)
		}
		result[catalogSchema.Name] = catalogSchema
	}
	return result, nil
}

func validateArchivedMarkerNames(prefix, containingSchema string, payload archivalMarkerV1) error {
	timestamp, nonce, err := parseArchivalGroupID(payload.GroupID)
	if err != nil {
		return err
	}
	declaresContainingSchema := false
	seenOIDs := make(map[uint32]string)
	for _, member := range payload.Members {
		expectedName, err := buildArchivalSchemaName(
			prefix, member.SourceTable.SchemaName, member.SourceTable.Name, timestamp, nonce,
		)
		if err != nil {
			return fmt.Errorf("building expected schema name for member %q: %w", member.MemberID, err)
		}
		if member.CleanupTable.SchemaName != expectedName {
			return fmt.Errorf("member %q cleanup schema %q disagrees with generated name %q",
				member.MemberID, member.CleanupTable.SchemaName, expectedName)
		}
		if member.SourceTable.SchemaName == member.CleanupTable.SchemaName {
			return fmt.Errorf("member %q source and cleanup schemas must differ", member.MemberID)
		}
		if member.CleanupTable.SchemaName == containingSchema {
			declaresContainingSchema = true
		}
		if previous, duplicate := seenOIDs[member.SourceTable.OID]; duplicate {
			return fmt.Errorf("catalog OID %d is declared by both %s and member %q",
				member.SourceTable.OID, previous, member.MemberID)
		}
		seenOIDs[member.SourceTable.OID] = fmt.Sprintf("member %q table", member.MemberID)
		for _, object := range slices.Concat(
			member.AutomaticallyMovedObjects, member.AttachedObjects,
			member.ExplicitlyMovedObjects, member.InternalToastObjects,
		) {
			if object.OID == member.SourceTable.OID &&
				object.Kind == archivalMarkerObjectKindTable {
				continue
			}
			if previous, duplicate := seenOIDs[object.OID]; duplicate {
				return fmt.Errorf("catalog OID %d is declared by both %s and member %q object %s.%s",
					object.OID, previous, member.MemberID, object.SchemaName, object.Name)
			}
			seenOIDs[object.OID] = fmt.Sprintf("member %q object %s.%s",
				member.MemberID, object.SchemaName, object.Name)
		}
	}
	for _, dependencySchema := range payload.ExclusiveDependencySchemas {
		if !isReservedArchivalSchemaName(dependencySchema.Name, prefix) ||
			!strings.HasSuffix(dependencySchema.Name, "_"+string(payload.GroupID)) {
			return fmt.Errorf("exclusive dependency schema %q disagrees with group %q and prefix %q",
				dependencySchema.Name, payload.GroupID, prefix)
		}
		if dependencySchema.Name == containingSchema {
			declaresContainingSchema = true
		}
	}
	for _, object := range payload.ExclusiveDependencyObjects {
		if previous, duplicate := seenOIDs[object.OID]; duplicate {
			return fmt.Errorf("catalog OID %d is declared by both %s and exclusive dependency %s.%s",
				object.OID, previous, object.SchemaName, object.Name)
		}
		seenOIDs[object.OID] = fmt.Sprintf("exclusive dependency %s.%s", object.SchemaName, object.Name)
	}
	if !declaresContainingSchema {
		return fmt.Errorf("marker does not declare containing schema %q", containingSchema)
	}
	return nil
}

func parseArchivalGroupID(groupID archivalGroupID) (string, string, error) {
	value := string(groupID)
	if len(value) != archivalTimestampLen+1+archivalNonceLength || value[archivalTimestampLen] != '_' {
		return "", "", fmt.Errorf("archival group ID %q does not use <timestamp>_<nonce>", groupID)
	}
	timestamp, nonce := value[:archivalTimestampLen], value[archivalTimestampLen+1:]
	if !isArchivalTimestamp(timestamp) || !isArchivalNonce(nonce) {
		return "", "", fmt.Errorf("archival group ID %q has an invalid timestamp or nonce", groupID)
	}
	return timestamp, nonce, nil
}

func archivedMarkerSchemaNames(payload archivalMarkerV1) []string {
	names := make([]string, 0, len(payload.Members)+len(payload.ExclusiveDependencySchemas))
	for _, member := range payload.Members {
		names = append(names, member.CleanupTable.SchemaName)
	}
	for _, dependencySchema := range payload.ExclusiveDependencySchemas {
		names = append(names, dependencySchema.Name)
	}
	slices.Sort(names)
	return names
}

func resolveArchivedCandidateGroup(
	inventory schema.CatalogInventory,
	payload archivalMarkerV1,
	declaredSchemaNames []string,
	canonicalMarker string,
) (structurallyValidArchivedCandidateGroup, error) {
	declaredSchemas := make(map[string]struct{}, len(declaredSchemaNames))
	for _, name := range declaredSchemaNames {
		declaredSchemas[name] = struct{}{}
	}
	allowedSchemaObjects := make(map[string]struct{})
	allowedInternalRelations := make(map[uint32]struct{})
	resume := archivedGroupResumeDescriptor{GroupID: payload.GroupID}
	movedCount := 0
	totalMoveCount := 0

	memberRelationOIDs := make(map[string]uint32, len(payload.Members))
	for _, member := range payload.Members {
		relation, err := uniqueRelationByOID(inventory, member.SourceTable.OID)
		if err != nil {
			return structurallyValidArchivedCandidateGroup{},
				fmt.Errorf("member %q table: %w", member.MemberID, err)
		}
		if relation.Name != member.SourceTable.Name {
			return structurallyValidArchivedCandidateGroup{}, fmt.Errorf(
				"member %q table OID %d is named %s.%s instead of %s.%s",
				member.MemberID, relation.OID, relation.SchemaName, relation.Name,
				member.SourceTable.SchemaName, member.SourceTable.Name,
			)
		}
		memberMoved := false
		switch relation.SchemaName {
		case member.SourceTable.SchemaName:
		case member.CleanupTable.SchemaName:
			memberMoved = true
		default:
			return structurallyValidArchivedCandidateGroup{}, fmt.Errorf(
				"member %q table OID %d is in unexpected schema %q", member.MemberID, relation.OID, relation.SchemaName,
			)
		}
		kind, tableLike := inventory.ClassifyTable(relation.OID)
		if !tableLike || !isSupportedArchivalTableKind(kind) {
			return structurallyValidArchivedCandidateGroup{}, fmt.Errorf(
				"member %q table OID %d has unsupported classification %q", member.MemberID, relation.OID, kind,
			)
		}
		move, err := inventory.ExpectedTableMove(relation.OID)
		if err != nil {
			return structurallyValidArchivedCandidateGroup{},
				fmt.Errorf("member %q expected move: %w", member.MemberID, err)
		}
		move = filterLostParentClonedTriggers(move, payload, member)
		if err := validateMemberCatalogObjects(inventory, member, move, memberMoved,
			declaredSchemas, allowedSchemaObjects); err != nil {
			return structurallyValidArchivedCandidateGroup{},
				fmt.Errorf("member %q: %w", member.MemberID, err)
		}

		totalMoveCount++
		if memberMoved {
			movedCount++
		} else {
			resume.RemainingMemberMoves = append(resume.RemainingMemberMoves, archivedMemberMoveDescriptor{
				MemberID: member.MemberID, RelationOID: relation.OID,
				SourceTable: member.SourceTable, DestinationTable: member.CleanupTable,
			})
		}
		for _, object := range member.ExplicitlyMovedObjects {
			totalMoveCount++
			actual, err := findCurrentCatalogObject(inventory, object)
			if err != nil {
				return structurallyValidArchivedCandidateGroup{},
					fmt.Errorf("member %q explicit object: %w", member.MemberID, err)
			}
			switch actual.SchemaName {
			case object.SchemaName:
				movedCount++
				allowedSchemaObjects[schemaObjectKey(actual.Kind, actual.OID)] = struct{}{}
			default:
				if _, wrongArchiveSchema := declaredSchemas[actual.SchemaName]; wrongArchiveSchema {
					return structurallyValidArchivedCandidateGroup{}, fmt.Errorf(
						"member %q explicit object OID %d is in wrong archival schema %q",
						member.MemberID, object.OID, actual.SchemaName,
					)
				}
				resume.RemainingExplicitObjectMoves = append(
					resume.RemainingExplicitObjectMoves,
					archivedObjectMoveDescriptor{MemberID: member.MemberID, Source: actual, Destination: object},
				)
			}
		}
		memberRelationOIDs[member.MemberID] = relation.OID
	}

	for _, object := range payload.ExclusiveDependencyObjects {
		totalMoveCount++
		actual, err := findCurrentCatalogObject(inventory, object)
		if err != nil {
			return structurallyValidArchivedCandidateGroup{},
				fmt.Errorf("exclusive dependency object: %w", err)
		}
		switch actual.SchemaName {
		case object.SchemaName:
			movedCount++
			allowedSchemaObjects[schemaObjectKey(actual.Kind, actual.OID)] = struct{}{}
			allowDependencyInternalObjects(inventory, actual, allowedSchemaObjects, allowedInternalRelations)
		default:
			if _, wrongArchiveSchema := declaredSchemas[actual.SchemaName]; wrongArchiveSchema {
				return structurallyValidArchivedCandidateGroup{}, fmt.Errorf(
					"exclusive dependency object OID %d is in wrong archival schema %q", object.OID, actual.SchemaName,
				)
			}
			resume.RemainingDependencyObjectMoves = append(
				resume.RemainingDependencyObjectMoves,
				archivedObjectMoveDescriptor{Source: actual, Destination: object},
			)
		}
	}

	parentAttachmentPresent, err := validateArchivedPartitionTopology(inventory, payload, memberRelationOIDs)
	if err != nil {
		return structurallyValidArchivedCandidateGroup{}, err
	}
	if len(payload.LostParentAttachments) == 1 {
		if parentAttachmentPresent {
			for _, trigger := range payload.LostParentAttachments[0].ClonedTriggers {
				actual, err := findCurrentCatalogObject(inventory, trigger.ChildTrigger)
				if err != nil {
					return structurallyValidArchivedCandidateGroup{}, err
				}
				if _, declared := declaredSchemas[actual.SchemaName]; declared {
					allowedSchemaObjects[schemaObjectKey(actual.Kind, actual.OID)] = struct{}{}
				}
			}
		}
		totalMoveCount++
		if parentAttachmentPresent {
			resume.RemainingPartitionDetach = true
		} else {
			movedCount++
		}
	}
	if err := validateDeclaredArchivedSchemaContents(
		inventory, declaredSchemas, allowedSchemaObjects, allowedInternalRelations,
	); err != nil {
		return structurallyValidArchivedCandidateGroup{}, err
	}
	remainingACLRevokes, err := resolveRemainingArchivedACLRevokes(inventory, payload)
	if err != nil {
		return structurallyValidArchivedCandidateGroup{}, err
	}
	resume.RemainingACLRevokes = remainingACLRevokes

	state := archivedCandidateGroupStatePartialResumable
	switch movedCount {
	case 0:
		state = archivedCandidateGroupStateEmptyInitialized
	case totalMoveCount:
		if len(resume.RemainingACLRevokes) == 0 {
			state = archivedCandidateGroupStateCompleteCandidate
		}
	}
	if state != archivedCandidateGroupStateCompleteCandidate {
		for _, schemaName := range declaredSchemaNames {
			resume.RemainingMarkerUpdates = append(resume.RemainingMarkerUpdates, archivedMarkerUpdateDescriptor{
				SchemaName: schemaName, Marker: canonicalMarker,
			})
		}
	}
	slices.SortFunc(resume.RemainingMemberMoves, func(a, b archivedMemberMoveDescriptor) int {
		return cmp.Compare(a.MemberID, b.MemberID)
	})
	slices.SortFunc(resume.RemainingExplicitObjectMoves, compareArchivedObjectMoves)
	slices.SortFunc(resume.RemainingDependencyObjectMoves, compareArchivedObjectMoves)

	return structurallyValidArchivedCandidateGroup{
		GroupID: payload.GroupID, State: state, Marker: canonicalizeArchivalMarker(payload),
		SchemaNames: slices.Clone(declaredSchemaNames), Resume: resume,
	}, nil
}

func resolveRemainingArchivedACLRevokes(
	inventory schema.CatalogInventory,
	payload archivalMarkerV1,
) ([]archivedACLRevokeDescriptor, error) {
	addresses := archivedMarkerACLAddresses(payload)
	plan, err := inventory.PlanACLRevokes(addresses)
	if err != nil {
		return nil, fmt.Errorf("planning remaining archival ACL isolation: %w", err)
	}
	result := make([]archivedACLRevokeDescriptor, 0, len(plan.Revokes))
	for _, revoke := range plan.Revokes {
		record, err := matchingOriginalArchivalACL(inventory, payload.OriginalACLs, revoke.Grant)
		if err != nil {
			return nil, err
		}
		result = append(result, archivedACLRevokeDescriptor{Kind: revoke.Kind, Record: record})
	}
	return result, nil
}

func archivedMarkerACLAddresses(payload archivalMarkerV1) []schema.CatalogDependencyObject {
	byOID := make(map[uint32]schema.CatalogDependencyObject)
	add := func(object archivalMarkerObjectIdentity) {
		byOID[object.OID] = schema.CatalogDependencyObject{
			ClassOID: pgClassCatalogOID, ObjectOID: object.OID,
		}
	}
	for _, member := range payload.Members {
		add(member.SourceTable)
		for _, object := range member.AutomaticallyMovedObjects {
			if object.Kind == archivalMarkerObjectKindOwnedSequence {
				add(object)
			}
		}
	}
	for _, object := range payload.ExclusiveDependencyObjects {
		if object.Kind == archivalMarkerObjectKindSequence {
			add(object)
		}
	}
	result := make([]schema.CatalogDependencyObject, 0, len(byOID))
	for _, address := range byOID {
		result = append(result, address)
	}
	slices.SortFunc(result, func(a, b schema.CatalogDependencyObject) int {
		return cmp.Compare(a.ObjectOID, b.ObjectOID)
	})
	return result
}

func matchingOriginalArchivalACL(
	inventory schema.CatalogInventory,
	originals []archivalMarkerACLRecordV1,
	grant schema.CatalogACLGrant,
) (archivalMarkerACLRecordV1, error) {
	current, err := archivalMarkerACLRecord(inventory, grant)
	if err != nil {
		return archivalMarkerACLRecordV1{}, err
	}
	var matches []archivalMarkerACLRecordV1
	for _, original := range originals {
		if original.ObjectClass != current.ObjectClass ||
			original.Object.Kind != current.Object.Kind ||
			original.Object.OID != current.Object.OID ||
			original.Object.Name != current.Object.Name ||
			original.ColumnName != current.ColumnName ||
			original.OwnerName != current.OwnerName ||
			original.GrantorName != current.GrantorName ||
			original.GranteeName != current.GranteeName ||
			original.GranteeIsPublic != current.GranteeIsPublic ||
			original.Privilege != current.Privilege ||
			current.IsGrantable && !original.IsGrantable {
			continue
		}
		matches = append(matches, original)
	}
	if len(matches) != 1 {
		return archivalMarkerACLRecordV1{}, fmt.Errorf(
			"current ACL grant on object OID %d has %d matching original marker records",
			grant.Object.ObjectOID, len(matches),
		)
	}
	return matches[0], nil
}

func validateMemberCatalogObjects(
	inventory schema.CatalogInventory,
	member archivalMarkerMemberV1,
	move schema.CatalogExpectedTableMove,
	memberMoved bool,
	declaredSchemas map[string]struct{},
	allowedSchemaObjects map[string]struct{},
) error {
	expectedAutomatic := markerObjectsFromCatalog(move.CleanupSchemaObjects, member.CleanupTable.SchemaName)
	expectedAttached := markerObjectsFromCatalog(move.AttachedObjects, member.CleanupTable.SchemaName)
	expectedExplicit := markerObjectsFromCatalog(move.ExplicitMoveObjects, member.CleanupTable.SchemaName)
	expectedInternal := markerObjectsFromCatalog(move.InternalObjects, "")
	for _, category := range []struct {
		label   string
		marker  []archivalMarkerObjectIdentity
		catalog []archivalMarkerObjectIdentity
	}{
		{label: "automatically moved", marker: member.AutomaticallyMovedObjects, catalog: expectedAutomatic},
		{label: "attached", marker: member.AttachedObjects, catalog: expectedAttached},
		{label: "explicitly moved", marker: member.ExplicitlyMovedObjects, catalog: expectedExplicit},
		{label: "internal TOAST", marker: member.InternalToastObjects, catalog: expectedInternal},
	} {
		if err := validateExactMarkerObjectSet(category.label, category.marker, category.catalog); err != nil {
			return err
		}
	}

	expectedCurrentSchema := member.SourceTable.SchemaName
	if memberMoved {
		expectedCurrentSchema = member.CleanupTable.SchemaName
	}
	for _, object := range move.CleanupSchemaObjects {
		if object.SchemaName != expectedCurrentSchema {
			return fmt.Errorf("%s object OID %d is in schema %q instead of %q",
				object.Kind, object.OID, object.SchemaName, expectedCurrentSchema)
		}
		if _, inArchive := declaredSchemas[object.SchemaName]; inArchive {
			markerKind, err := markerKindFromCatalogKind(object.Kind)
			if err != nil {
				return err
			}
			allowedSchemaObjects[schemaObjectKey(markerKind, object.OID)] = struct{}{}
		}
	}
	for _, object := range move.AttachedObjects {
		if object.SchemaName != expectedCurrentSchema {
			return fmt.Errorf("attached %s object OID %d is associated with schema %q instead of %q",
				object.Kind, object.OID, object.SchemaName, expectedCurrentSchema)
		}
		if _, inArchive := declaredSchemas[object.SchemaName]; inArchive {
			markerKind, err := markerKindFromCatalogKind(object.Kind)
			if err != nil {
				return err
			}
			allowedSchemaObjects[schemaObjectKey(markerKind, object.OID)] = struct{}{}
		}
	}
	for _, object := range member.InternalToastObjects {
		actual, err := findCurrentCatalogObject(inventory, object)
		if err != nil {
			return err
		}
		if compareMarkerObjects(actual, object) != 0 {
			return fmt.Errorf("internal TOAST object OID %d does not match marker identity", object.OID)
		}
	}
	return nil
}

func markerObjectsFromCatalog(
	objects []schema.CatalogObjectIdentity,
	destinationSchema string,
) []archivalMarkerObjectIdentity {
	result := make([]archivalMarkerObjectIdentity, 0, len(objects))
	for _, object := range objects {
		markerKind, err := markerKindFromCatalogKind(object.Kind)
		if err != nil {
			continue
		}
		schemaName := destinationSchema
		if schemaName == "" {
			schemaName = object.SchemaName
		}
		result = append(result, archivalMarkerObjectIdentity{
			Kind: markerKind, OID: object.OID, SchemaName: schemaName, Name: object.Name,
		})
	}
	return canonicalMarkerObjects(result)
}

func markerKindFromCatalogKind(kind schema.CatalogObjectKind) (archivalMarkerObjectKind, error) {
	switch kind {
	case schema.CatalogObjectKindRelation:
		return archivalMarkerObjectKindTable, nil
	case schema.CatalogObjectKindRowType:
		return archivalMarkerObjectKindRowType, nil
	case schema.CatalogObjectKindArrayType:
		return archivalMarkerObjectKindArrayType, nil
	case schema.CatalogObjectKindIndex:
		return archivalMarkerObjectKindIndex, nil
	case schema.CatalogObjectKindConstraint:
		return archivalMarkerObjectKindConstraint, nil
	case schema.CatalogObjectKindTrigger:
		return archivalMarkerObjectKindTrigger, nil
	case schema.CatalogObjectKindRule:
		return archivalMarkerObjectKindRule, nil
	case schema.CatalogObjectKindPolicy:
		return archivalMarkerObjectKindPolicy, nil
	case schema.CatalogObjectKindOwnedSequence:
		return archivalMarkerObjectKindOwnedSequence, nil
	case schema.CatalogObjectKindExtendedStatistic:
		return archivalMarkerObjectKindExtendedStatistic, nil
	case schema.CatalogObjectKindToastRelation:
		return archivalMarkerObjectKindToastRelation, nil
	default:
		return "", fmt.Errorf("unsupported expected catalog object kind %q", kind)
	}
}

func validateExactMarkerObjectSet(
	label string,
	markerObjects []archivalMarkerObjectIdentity,
	catalogObjects []archivalMarkerObjectIdentity,
) error {
	markerByKey := make(map[string]archivalMarkerObjectIdentity, len(markerObjects))
	for _, object := range markerObjects {
		key := markerObjectCatalogKey(object)
		if _, duplicate := markerByKey[key]; duplicate {
			return fmt.Errorf("duplicate expected %s %s object %s OID %d in marker",
				label, object.Kind, markerObjectDisplayName(object), object.OID)
		}
		markerByKey[key] = object
	}
	catalogByKey := make(map[string]archivalMarkerObjectIdentity, len(catalogObjects))
	for _, object := range catalogObjects {
		key := markerObjectCatalogKey(object)
		if _, duplicate := catalogByKey[key]; duplicate {
			return fmt.Errorf("duplicate current %s %s object %s OID %d in catalog",
				label, object.Kind, markerObjectDisplayName(object), object.OID)
		}
		catalogByKey[key] = object
	}
	for _, key := range sortedStringMapKeys(markerByKey) {
		object := markerByKey[key]
		if _, ok := catalogByKey[key]; !ok {
			return fmt.Errorf("missing expected %s %s object %s OID %d from current catalog",
				label, object.Kind, markerObjectDisplayName(object), object.OID)
		}
	}
	for _, key := range sortedStringMapKeys(catalogByKey) {
		object := catalogByKey[key]
		if _, ok := markerByKey[key]; !ok {
			return fmt.Errorf("unexpected %s %s object %s OID %d in current catalog",
				label, object.Kind, markerObjectDisplayName(object), object.OID)
		}
	}
	return nil
}

func uniqueRelationByOID(inventory schema.CatalogInventory, oid uint32) (schema.CatalogRelation, error) {
	var found *schema.CatalogRelation
	for idx := range inventory.Relations {
		if inventory.Relations[idx].OID != oid {
			continue
		}
		if found != nil {
			return schema.CatalogRelation{}, fmt.Errorf(
				"duplicate relation catalog identity for OID %d", oid,
			)
		}
		copy := inventory.Relations[idx]
		found = &copy
	}
	if found == nil {
		return schema.CatalogRelation{}, fmt.Errorf(
			"relation OID %d is missing from the current catalog", oid,
		)
	}
	return *found, nil
}

func findCurrentCatalogObject(
	inventory schema.CatalogInventory,
	expected archivalMarkerObjectIdentity,
) (archivalMarkerObjectIdentity, error) {
	actual, count := currentCatalogObjectsWithOID(inventory, expected.Kind, expected.OID)
	if count == 0 {
		return archivalMarkerObjectIdentity{}, fmt.Errorf(
			"%s object OID %d (%s) is missing from the current catalog",
			expected.Kind, expected.OID, markerObjectDisplayName(expected),
		)
	}
	if count > 1 {
		return archivalMarkerObjectIdentity{}, fmt.Errorf(
			"duplicate %s catalog identity for OID %d",
			expected.Kind, expected.OID,
		)
	}
	if actual.Name != expected.Name || !slices.Equal(actual.IdentityArguments, expected.IdentityArguments) {
		return archivalMarkerObjectIdentity{}, fmt.Errorf(
			"%s object OID %d is %s instead of marker identity %s",
			expected.Kind, expected.OID, markerObjectDisplayName(actual), markerObjectDisplayName(expected),
		)
	}
	return actual, nil
}

func currentCatalogObjectsWithOID(
	inventory schema.CatalogInventory,
	kind archivalMarkerObjectKind,
	oid uint32,
) (archivalMarkerObjectIdentity, int) {
	var result archivalMarkerObjectIdentity
	count := 0
	add := func(schemaName, name string, arguments []string) {
		result = archivalMarkerObjectIdentity{
			Kind: kind, OID: oid, SchemaName: schemaName, Name: name,
			IdentityArguments: slices.Clone(arguments),
		}
		count++
	}
	switch kind {
	case archivalMarkerObjectKindTable:
		for _, relation := range inventory.Relations {
			if relation.OID == oid && (relation.Kind == schema.RelKindOrdinaryTable ||
				relation.Kind == schema.RelKindPartitionedTable ||
				relation.Kind == schema.RelKindForeignTable) {
				add(relation.SchemaName, relation.Name, nil)
			}
		}
	case archivalMarkerObjectKindRowType, archivalMarkerObjectKindArrayType, archivalMarkerObjectKindType:
		for _, catalogType := range inventory.Types {
			if catalogType.OID != oid {
				continue
			}
			kindMatches := kind == archivalMarkerObjectKindType ||
				(kind == archivalMarkerObjectKindRowType &&
					catalogType.Kind == schema.CatalogTypeKindRow) ||
				(kind == archivalMarkerObjectKindArrayType &&
					catalogType.Kind == schema.CatalogTypeKindArray)
			if kindMatches {
				add(catalogType.SchemaName, catalogType.Name, nil)
			}
		}
	case archivalMarkerObjectKindIndex:
		for _, index := range inventory.Indexes {
			if index.OID == oid {
				add(index.SchemaName, index.Name, nil)
			}
		}
	case archivalMarkerObjectKindConstraint:
		for _, constraint := range inventory.Constraints {
			if constraint.OID == oid {
				add(constraint.SchemaName, constraint.Name, nil)
			}
		}
	case archivalMarkerObjectKindTrigger:
		for _, trigger := range inventory.Triggers {
			if trigger.OID == oid {
				if relation, err := uniqueRelationByOID(inventory, trigger.RelationOID); err == nil {
					add(relation.SchemaName, trigger.Name, nil)
				}
			}
		}
	case archivalMarkerObjectKindRule:
		for _, rule := range inventory.Rules {
			if rule.OID == oid {
				if relation, err := uniqueRelationByOID(inventory, rule.RelationOID); err == nil {
					add(relation.SchemaName, rule.Name, nil)
				}
			}
		}
	case archivalMarkerObjectKindPolicy:
		for _, policy := range inventory.Policies {
			if policy.OID == oid {
				if relation, err := uniqueRelationByOID(inventory, policy.RelationOID); err == nil {
					add(relation.SchemaName, policy.Name, nil)
				}
			}
		}
	case archivalMarkerObjectKindOwnedSequence, archivalMarkerObjectKindSequence:
		for _, sequence := range inventory.Sequences {
			if sequence.OID != oid {
				continue
			}
			owned := false
			for _, ownership := range inventory.OwnedSequences {
				if ownership.SequenceOID == oid {
					owned = true
				}
			}
			if owned == (kind == archivalMarkerObjectKindOwnedSequence) {
				add(sequence.SchemaName, sequence.Name, nil)
			}
		}
	case archivalMarkerObjectKindExtendedStatistic:
		for _, statistic := range inventory.ExtendedStatistics {
			if statistic.OID == oid {
				add(statistic.SchemaName, statistic.Name, nil)
			}
		}
	case archivalMarkerObjectKindToastRelation:
		for _, relation := range inventory.Relations {
			if relation.ToastRelation != nil && relation.ToastRelation.OID == oid {
				add(relation.ToastRelation.SchemaName, relation.ToastRelation.Name, nil)
			}
		}
	case archivalMarkerObjectKindFunction:
		for _, routine := range inventory.Routines {
			if routine.OID == oid {
				arguments := []string{}
				if routine.IdentityArguments != "" {
					arguments = []string{routine.IdentityArguments}
				}
				add(routine.SchemaName, routine.Name, arguments)
			}
		}
	case archivalMarkerObjectKindCollation:
		for _, collation := range inventory.Collations {
			if collation.OID == oid {
				add(collation.SchemaName, collation.Name, nil)
			}
		}
	case archivalMarkerObjectKindOperator:
		for _, operator := range inventory.Operators {
			if operator.OID == oid {
				add(operator.SchemaName, operator.Name, []string{
					operator.LeftType, operator.RightType,
				})
			}
		}
	case archivalMarkerObjectKindView, archivalMarkerObjectKindMaterializedView:
		for _, view := range inventory.Views {
			if view.RelationOID != oid {
				continue
			}
			if (kind == archivalMarkerObjectKindView && view.Kind == schema.RelKindView) ||
				(kind == archivalMarkerObjectKindMaterializedView &&
					view.Kind == schema.RelKindMaterializedView) {
				add(view.SchemaName, view.Name, nil)
			}
		}
	}
	return result, count
}

func allowDependencyInternalObjects(
	inventory schema.CatalogInventory,
	dependency archivalMarkerObjectIdentity,
	allowedSchemaObjects map[string]struct{},
	allowedInternalRelations map[uint32]struct{},
) {
	if dependency.Kind != archivalMarkerObjectKindType {
		return
	}
	for _, catalogType := range inventory.Types {
		if catalogType.OID != dependency.OID {
			continue
		}
		if catalogType.RelationOID != 0 {
			allowedInternalRelations[catalogType.RelationOID] = struct{}{}
		}
		if catalogType.ArrayTypeOID != 0 {
			allowedSchemaObjects[schemaObjectKey(archivalMarkerObjectKindType, catalogType.ArrayTypeOID)] = struct{}{}
		}
	}
}

func validateArchivedPartitionTopology(
	inventory schema.CatalogInventory,
	payload archivalMarkerV1,
	memberRelationOIDs map[string]uint32,
) (bool, error) {
	expected := make(map[string]struct{}, len(payload.PartitionEdges))
	for _, edge := range payload.PartitionEdges {
		expected[oidEdgeKey(memberRelationOIDs[edge.ParentMemberID],
			memberRelationOIDs[edge.ChildMemberID])] = struct{}{}
	}
	memberOIDs := make(map[uint32]struct{}, len(memberRelationOIDs))
	for _, oid := range memberRelationOIDs {
		memberOIDs[oid] = struct{}{}
	}
	actualInheritance := make(map[string]struct{})
	for _, edge := range inventory.InheritanceEdges {
		_, childMember := memberOIDs[edge.ChildRelationOID]
		_, parentMember := memberOIDs[edge.ParentRelationOID]
		if !childMember && !parentMember {
			continue
		}
		if !childMember || !parentMember {
			if !isExpectedLostParentEdge(payload, edge.ParentRelationOID, edge.ChildRelationOID,
				memberRelationOIDs) {
				return false, fmt.Errorf("partition topology has an inheritance edge outside the archival group (%d -> %d)",
					edge.ParentRelationOID, edge.ChildRelationOID)
			}
			continue
		}
		key := oidEdgeKey(edge.ParentRelationOID, edge.ChildRelationOID)
		if _, duplicate := actualInheritance[key]; duplicate {
			return false, fmt.Errorf("partition topology has duplicate inheritance edge (%d -> %d)",
				edge.ParentRelationOID, edge.ChildRelationOID)
		}
		actualInheritance[key] = struct{}{}
	}
	actualAttachments := make(map[string]struct{})
	for _, attachment := range inventory.PartitionAttachments {
		_, childMember := memberOIDs[attachment.RelationOID]
		_, parentMember := memberOIDs[attachment.ParentRelationOID]
		if !childMember && !parentMember {
			continue
		}
		if !childMember || !parentMember {
			if !isExpectedLostParentEdge(payload, attachment.ParentRelationOID, attachment.RelationOID,
				memberRelationOIDs) {
				return false, fmt.Errorf("partition topology has an attachment outside the archival group (%d -> %d)",
					attachment.ParentRelationOID, attachment.RelationOID)
			}
			continue
		}
		key := oidEdgeKey(attachment.ParentRelationOID, attachment.RelationOID)
		if _, duplicate := actualAttachments[key]; duplicate {
			return false, fmt.Errorf("partition topology has duplicate attachment (%d -> %d)",
				attachment.ParentRelationOID, attachment.RelationOID)
		}
		actualAttachments[key] = struct{}{}
	}
	if err := validateExactOIDEdges("inheritance", expected, actualInheritance); err != nil {
		return false, err
	}
	if err := validateExactOIDEdges("partition attachment", expected, actualAttachments); err != nil {
		return false, err
	}
	for _, edge := range payload.PartitionEdges {
		parentOID := memberRelationOIDs[edge.ParentMemberID]
		childOID := memberRelationOIDs[edge.ChildMemberID]
		if err := validateCurrentPartitionAttachmentMetadata(
			inventory, parentOID, childOID, edge.BoundExpression,
			edge.PartitionedIndexAttachments, edge.ClonedTriggers, false,
		); err != nil {
			return false, err
		}
	}
	if len(payload.LostParentAttachments) == 0 {
		if err := validateExactArchivedIndexTopology(inventory, payload, false); err != nil {
			return false, err
		}
		if err := validateExactArchivedTriggerTopology(inventory, payload, false); err != nil {
			return false, err
		}
		if err := validateArchivedPartitionMembershipFlags(inventory, payload,
			memberRelationOIDs, false); err != nil {
			return false, err
		}
		return false, nil
	}
	lost := payload.LostParentAttachments[0]
	rootOID := memberRelationOIDs[lost.RootMemberID]
	parent, err := uniqueRelationByOID(inventory, lost.ParentTable.OID)
	if err != nil {
		return false, fmt.Errorf("lost parent table: %w", err)
	}
	if parent.SchemaName != lost.ParentTable.SchemaName || parent.Name != lost.ParentTable.Name ||
		parent.Kind != schema.RelKindPartitionedTable {
		return false, fmt.Errorf("lost parent table OID %d does not match its active partitioned-table identity",
			lost.ParentTable.OID)
	}
	attached := isCatalogInheritanceEdge(inventory, lost.ParentTable.OID, rootOID)
	if err := validateCurrentPartitionAttachmentMetadata(
		inventory, lost.ParentTable.OID, rootOID, lost.BoundExpression,
		lost.PartitionedIndexAttachments, lost.ClonedTriggers, !attached,
	); err != nil {
		return false, err
	}
	if err := validateExactArchivedIndexTopology(inventory, payload, attached); err != nil {
		return false, err
	}
	if err := validateExactArchivedTriggerTopology(inventory, payload, attached); err != nil {
		return false, err
	}
	if err := validateArchivedPartitionMembershipFlags(inventory, payload, memberRelationOIDs, attached); err != nil {
		return false, err
	}
	return attached, nil
}

func validateArchivedPartitionMembershipFlags(
	inventory schema.CatalogInventory,
	payload archivalMarkerV1,
	memberRelationOIDs map[string]uint32,
	lostParentAttached bool,
) error {
	partitionMembers := make(map[string]struct{}, len(payload.PartitionEdges))
	for _, edge := range payload.PartitionEdges {
		partitionMembers[edge.ChildMemberID] = struct{}{}
	}
	if lostParentAttached {
		partitionMembers[payload.LostParentAttachments[0].RootMemberID] = struct{}{}
	}
	for memberID, relationOID := range memberRelationOIDs {
		relation, err := uniqueRelationByOID(inventory, relationOID)
		if err != nil {
			return err
		}
		_, expectedPartition := partitionMembers[memberID]
		if relation.IsPartition != expectedPartition {
			return fmt.Errorf("partition member %q OID %d has relispartition=%t; expected %t",
				memberID, relationOID, relation.IsPartition, expectedPartition)
		}
	}
	return nil
}

func validateExactArchivedIndexTopology(
	inventory schema.CatalogInventory,
	payload archivalMarkerV1,
	lostParentAttached bool,
) error {
	groupIndexOIDs := make(map[uint32]struct{})
	for _, member := range payload.Members {
		for _, object := range member.AutomaticallyMovedObjects {
			if object.Kind == archivalMarkerObjectKindIndex {
				groupIndexOIDs[object.OID] = struct{}{}
			}
		}
	}
	expected := make(map[string]struct{})
	for _, edge := range payload.PartitionEdges {
		for _, attachment := range edge.PartitionedIndexAttachments {
			expected[oidEdgeKey(attachment.ParentIndex.OID, attachment.ChildIndex.OID)] = struct{}{}
		}
	}
	if lostParentAttached && len(payload.LostParentAttachments) == 1 {
		for _, attachment := range payload.LostParentAttachments[0].PartitionedIndexAttachments {
			expected[oidEdgeKey(attachment.ParentIndex.OID, attachment.ChildIndex.OID)] = struct{}{}
		}
	}
	actual := make(map[string]struct{})
	for _, edge := range inventory.InheritanceEdges {
		_, childMemberIndex := groupIndexOIDs[edge.ChildRelationOID]
		_, parentMemberIndex := groupIndexOIDs[edge.ParentRelationOID]
		if childMemberIndex || parentMemberIndex {
			actual[oidEdgeKey(edge.ParentRelationOID, edge.ChildRelationOID)] = struct{}{}
		}
	}
	return validateExactOIDEdges("partitioned index attachment", expected, actual)
}

func validateExactArchivedTriggerTopology(
	inventory schema.CatalogInventory,
	payload archivalMarkerV1,
	lostParentAttached bool,
) error {
	relevantTriggerOIDs := make(map[uint32]struct{})
	for _, member := range payload.Members {
		for _, object := range member.AttachedObjects {
			if object.Kind == archivalMarkerObjectKindTrigger {
				relevantTriggerOIDs[object.OID] = struct{}{}
			}
		}
	}
	expected := make(map[string]struct{})
	for _, edge := range payload.PartitionEdges {
		for _, trigger := range edge.ClonedTriggers {
			expected[oidEdgeKey(trigger.ParentTrigger.OID, trigger.ChildTrigger.OID)] = struct{}{}
		}
	}
	if lostParentAttached && len(payload.LostParentAttachments) == 1 {
		for _, trigger := range payload.LostParentAttachments[0].ClonedTriggers {
			relevantTriggerOIDs[trigger.ChildTrigger.OID] = struct{}{}
			expected[oidEdgeKey(trigger.ParentTrigger.OID, trigger.ChildTrigger.OID)] = struct{}{}
		}
	}
	actual := make(map[string]struct{})
	for _, trigger := range inventory.Triggers {
		if _, relevant := relevantTriggerOIDs[trigger.OID]; !relevant || trigger.ParentTriggerOID == 0 {
			continue
		}
		actual[oidEdgeKey(trigger.ParentTriggerOID, trigger.OID)] = struct{}{}
	}
	return validateExactOIDEdges("cloned trigger", expected, actual)
}

func isExpectedLostParentEdge(
	payload archivalMarkerV1,
	parentOID uint32,
	childOID uint32,
	members map[string]uint32,
) bool {
	return len(payload.LostParentAttachments) == 1 &&
		payload.LostParentAttachments[0].ParentTable.OID == parentOID &&
		members[payload.LostParentAttachments[0].RootMemberID] == childOID
}

func isCatalogInheritanceEdge(inventory schema.CatalogInventory, parentOID, childOID uint32) bool {
	return slices.ContainsFunc(inventory.InheritanceEdges, func(
		edge schema.CatalogInheritanceEdge,
	) bool {
		return edge.ParentRelationOID == parentOID && edge.ChildRelationOID == childOID
	})
}

func validateCurrentPartitionAttachmentMetadata(
	inventory schema.CatalogInventory,
	parentOID uint32,
	childOID uint32,
	bound string,
	indexes []archivalMarkerPartitionedIndexAttachmentV1,
	triggers []archivalMarkerClonedTriggerV1,
	detached bool,
) error {
	attachments := 0
	for _, attachment := range inventory.PartitionAttachments {
		if attachment.ParentRelationOID == parentOID && attachment.RelationOID == childOID {
			attachments++
			if detached || attachment.BoundExpression != bound {
				return fmt.Errorf("partition attachment %d -> %d has unexpected bound %q",
					parentOID, childOID, attachment.BoundExpression)
			}
		}
	}
	if (!detached && attachments != 1) || (detached && attachments != 0) {
		return fmt.Errorf("partition attachment %d -> %d has unexpected catalog count %d",
			parentOID, childOID, attachments)
	}
	for _, index := range indexes {
		parentIndex, parentCount := catalogIndexByOID(inventory, index.ParentIndex.OID)
		childIndex, childCount := catalogIndexByOID(inventory, index.ChildIndex.OID)
		if parentCount != 1 || childCount != 1 || parentIndex.Name != index.ParentIndex.Name ||
			childIndex.Name != index.ChildIndex.Name || parentIndex.RelationOID != parentOID ||
			childIndex.RelationOID != childOID {
			return fmt.Errorf("partitioned index attachment %d -> %d has mismatched endpoint identities",
				index.ParentIndex.OID, index.ChildIndex.OID)
		}
		attached := isCatalogInheritanceEdge(inventory, index.ParentIndex.OID, index.ChildIndex.OID)
		if attached == detached {
			return fmt.Errorf("partitioned index attachment %d -> %d has unexpected detached state",
				index.ParentIndex.OID, index.ChildIndex.OID)
		}
	}
	for _, expected := range triggers {
		found := false
		for _, trigger := range inventory.Triggers {
			if trigger.OID != expected.ChildTrigger.OID {
				continue
			}
			found = true
			if detached {
				return fmt.Errorf("detached cloned trigger OID %d still exists", trigger.OID)
			}
			parentTrigger, parentCount := catalogTriggerByOIDAndCount(inventory, expected.ParentTrigger.OID)
			expectedParent := expected.ParentTrigger.OID
			if parentCount != 1 || parentTrigger.Name != expected.ParentTrigger.Name ||
				trigger.Name != expected.ChildTrigger.Name ||
				trigger.ParentTriggerOID != expectedParent ||
				trigger.FunctionOID != expected.FunctionOID ||
				trigger.Type != expected.Type || trigger.EnabledMode != expected.EnabledMode ||
				trigger.IsInternal != expected.IsInternal ||
				trigger.ConstraintOID != expected.ConstraintOID {
				return fmt.Errorf("cloned trigger OID %d metadata changed", trigger.OID)
			}
		}
		if !found && !detached {
			return fmt.Errorf("cloned trigger OID %d is missing", expected.ChildTrigger.OID)
		}
	}
	return nil
}

func catalogIndexByOID(inventory schema.CatalogInventory, oid uint32) (schema.CatalogIndex, int) {
	var result schema.CatalogIndex
	count := 0
	for _, index := range inventory.Indexes {
		if index.OID == oid {
			result = index
			count++
		}
	}
	return result, count
}

func catalogTriggerByOIDAndCount(inventory schema.CatalogInventory, oid uint32) (schema.CatalogTrigger, int) {
	var result schema.CatalogTrigger
	count := 0
	for _, trigger := range inventory.Triggers {
		if trigger.OID == oid {
			result = trigger
			count++
		}
	}
	return result, count
}

func validateExactOIDEdges(label string, expected, actual map[string]struct{}) error {
	for _, edge := range sortedStringMapKeys(expected) {
		if _, ok := actual[edge]; !ok {
			return fmt.Errorf("missing expected %s edge %s", label,
				strings.ReplaceAll(edge, "\x00", " -> "))
		}
	}
	for _, edge := range sortedStringMapKeys(actual) {
		if _, ok := expected[edge]; !ok {
			return fmt.Errorf("unexpected %s edge %s", label,
				strings.ReplaceAll(edge, "\x00", " -> "))
		}
	}
	return nil
}

func validateDeclaredArchivedSchemaContents(
	inventory schema.CatalogInventory,
	declaredSchemas map[string]struct{},
	allowed map[string]struct{},
	allowedInternalRelations map[uint32]struct{},
) error {
	observed := make(map[string]string)
	observe := func(schemaName string, kind archivalMarkerObjectKind, oid uint32, name string) {
		if _, declared := declaredSchemas[schemaName]; !declared {
			return
		}
		key := schemaObjectKey(kind, oid)
		if _, duplicateRepresentation := observed[key]; !duplicateRepresentation {
			observed[key] = fmt.Sprintf("%s %s.%s OID %d", kind, schemaName, name, oid)
		}
	}
	for _, relation := range inventory.Relations {
		kind := archivalMarkerObjectKind("")
		switch relation.Kind {
		case schema.RelKindOrdinaryTable, schema.RelKindPartitionedTable, schema.RelKindForeignTable:
			kind = archivalMarkerObjectKindTable
		case schema.RelKindIndex, schema.RelKindPartitionedIndex:
			kind = archivalMarkerObjectKindIndex
		case schema.RelKindSequence:
			kind = archivalMarkerObjectKindSequence
			for _, ownership := range inventory.OwnedSequences {
				if ownership.SequenceOID == relation.OID {
					kind = archivalMarkerObjectKindOwnedSequence
				}
			}
		case schema.RelKindView:
			kind = archivalMarkerObjectKindView
		case schema.RelKindMaterializedView:
			kind = archivalMarkerObjectKindMaterializedView
		case schema.RelKindCompositeType:
			if _, ok := allowedInternalRelations[relation.OID]; ok {
				continue
			}
		}
		if kind == "" {
			if _, declared := declaredSchemas[relation.SchemaName]; declared {
				return fmt.Errorf("unexpected relation %s.%s OID %d in declared archival schema",
					relation.SchemaName, relation.Name, relation.OID)
			}
			continue
		}
		observe(relation.SchemaName, kind, relation.OID, relation.Name)
	}
	for _, catalogType := range inventory.Types {
		kind := archivalMarkerObjectKindType
		for _, relation := range inventory.Relations {
			if catalogType.OID == relation.RowTypeOID {
				kind = archivalMarkerObjectKindRowType
				break
			}
			if catalogType.OID == relation.ArrayTypeOID {
				kind = archivalMarkerObjectKindArrayType
				break
			}
		}
		observe(catalogType.SchemaName, kind, catalogType.OID, catalogType.Name)
	}
	for _, index := range inventory.Indexes {
		observe(index.SchemaName, archivalMarkerObjectKindIndex, index.OID, index.Name)
	}
	for _, constraint := range inventory.Constraints {
		observe(constraint.SchemaName, archivalMarkerObjectKindConstraint, constraint.OID, constraint.Name)
	}
	for _, sequence := range inventory.Sequences {
		kind := archivalMarkerObjectKindSequence
		for _, ownership := range inventory.OwnedSequences {
			if ownership.SequenceOID == sequence.OID {
				kind = archivalMarkerObjectKindOwnedSequence
			}
		}
		observe(sequence.SchemaName, kind, sequence.OID, sequence.Name)
	}
	for _, statistic := range inventory.ExtendedStatistics {
		observe(statistic.SchemaName, archivalMarkerObjectKindExtendedStatistic, statistic.OID, statistic.Name)
	}
	for _, routine := range inventory.Routines {
		observe(routine.SchemaName, archivalMarkerObjectKindFunction, routine.OID, routine.Name)
	}
	for _, collation := range inventory.Collations {
		observe(collation.SchemaName, archivalMarkerObjectKindCollation, collation.OID, collation.Name)
	}
	for _, operator := range inventory.Operators {
		observe(operator.SchemaName, archivalMarkerObjectKindOperator, operator.OID, operator.Name)
	}
	for _, view := range inventory.Views {
		kind := archivalMarkerObjectKindView
		if view.Kind == schema.RelKindMaterializedView {
			kind = archivalMarkerObjectKindMaterializedView
		}
		observe(view.SchemaName, kind, view.RelationOID, view.Name)
	}
	for _, trigger := range inventory.Triggers {
		if relation := catalogRelationWithOID(inventory, trigger.RelationOID); relation != nil {
			observe(relation.SchemaName, archivalMarkerObjectKindTrigger, trigger.OID, trigger.Name)
		}
	}
	for _, rule := range inventory.Rules {
		if relation := catalogRelationWithOID(inventory, rule.RelationOID); relation != nil {
			observe(relation.SchemaName, archivalMarkerObjectKindRule, rule.OID, rule.Name)
		}
	}
	for _, policy := range inventory.Policies {
		if relation := catalogRelationWithOID(inventory, policy.RelationOID); relation != nil {
			observe(relation.SchemaName, archivalMarkerObjectKindPolicy, policy.OID, policy.Name)
		}
	}
	for _, key := range sortedStringMapKeys(observed) {
		description := observed[key]
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("unexpected catalog object in declared archival schema: %s", description)
		}
	}
	return nil
}

func compareArchivedObjectMoves(a, b archivedObjectMoveDescriptor) int {
	return cmp.Or(
		cmp.Compare(a.MemberID, b.MemberID),
		compareMarkerObjects(a.Destination, b.Destination),
	)
}

func markerObjectCatalogKey(object archivalMarkerObjectIdentity) string {
	return fmt.Sprintf("%s\x00%d\x00%s\x00%s\x00%s", object.Kind, object.OID,
		object.SchemaName, object.Name, strings.Join(object.IdentityArguments, "\x01"))
}

func schemaObjectKey(kind archivalMarkerObjectKind, oid uint32) string {
	return fmt.Sprintf("%s\x00%d", kind, oid)
}

func markerObjectDisplayName(object archivalMarkerObjectIdentity) string {
	if object.SchemaName == "" {
		return object.Name
	}
	return object.SchemaName + "." + object.Name
}

func oidEdgeKey(parentOID, childOID uint32) string {
	return fmt.Sprintf("%d\x00%d", parentOID, childOID)
}

func sortedStringMapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
