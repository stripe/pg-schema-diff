package diff

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

type archivalCleanupRootIntent struct {
	GroupID archivalGroupID
	Root    archivalMarkerObjectIdentity
}

func archivalCleanupRootIntents(groups []preparedArchivalGroup) []archivalCleanupRootIntent {
	intents := make([]archivalCleanupRootIntent, 0, len(groups))
	for _, group := range groups {
		intents = append(intents, archivalCleanupRootIntent{
			GroupID: group.id, Root: cloneMarkerObject(group.cleanupRoot),
		})
	}
	slices.SortFunc(intents, func(a, b archivalCleanupRootIntent) int {
		return cmp.Or(cmp.Compare(a.GroupID, b.GroupID), compareMarkerObjects(a.Root, b.Root))
	})
	return intents
}

func populatePreparedArchivalMembers(group *preparedArchivalGroup) error {
	parents := make(map[string]string, len(group.marker.PartitionEdges))
	for _, edge := range group.marker.PartitionEdges {
		parents[edge.ChildMemberID] = edge.ParentMemberID
	}
	for _, member := range group.marker.Members {
		if _, hasParent := parents[member.MemberID]; !hasParent {
			if group.rootMemberID != "" {
				return fmt.Errorf("archival group %q has multiple roots", group.id)
			}
			group.rootMemberID = member.MemberID
		}
	}
	if group.rootMemberID == "" {
		return fmt.Errorf("archival group %q has no root", group.id)
	}
	for _, member := range group.marker.Members {
		depth := 0
		for current := member.MemberID; current != group.rootMemberID; {
			parent, ok := parents[current]
			if !ok {
				return fmt.Errorf("archival member %q is disconnected from root %q", current, group.rootMemberID)
			}
			depth++
			current = parent
		}
		group.members = append(group.members, preparedArchivalMember{marker: member, depth: depth})
	}
	slices.SortFunc(group.members, func(a, b preparedArchivalMember) int {
		return compareMarkerObjects(a.marker.SourceTable, b.marker.SourceTable)
	})
	return nil
}

func preparedArchivalMemberByID(
	members []preparedArchivalMember,
	memberID string,
) preparedArchivalMember {
	for _, member := range members {
		if member.marker.MemberID == memberID {
			return member
		}
	}
	return preparedArchivalMember{}
}

func archivalGroupByRelationOID(
	groups []preparedArchivalGroup,
) map[uint32]preparedArchivalGroup {
	result := make(map[uint32]preparedArchivalGroup)
	for _, group := range groups {
		for _, member := range group.members {
			result[member.marker.SourceTable.OID] = group
		}
	}
	return result
}

func populateArchivalMarkerPartitionMetadata(
	inventory schema.CatalogInventory,
	marker *archivalMarkerV1,
) error {
	group := preparedArchivalGroup{id: marker.GroupID, marker: *marker}
	if err := populatePreparedArchivalMembers(&group); err != nil {
		return err
	}
	for idx := range group.members {
		relation, err := uniqueRelationByOID(inventory,
			group.members[idx].marker.SourceTable.OID)
		if err != nil {
			return err
		}
		group.members[idx].relationKind = relation.Kind
	}
	for idx, edge := range marker.PartitionEdges {
		parent := preparedArchivalMemberByID(group.members, edge.ParentMemberID)
		child := preparedArchivalMemberByID(group.members, edge.ChildMemberID)
		populated, err := archivalPartitionEdgeMetadata(inventory, group,
			parent.marker.SourceTable.OID, child.marker.SourceTable.OID)
		if err != nil {
			return err
		}
		marker.PartitionEdges[idx] = populated
	}
	return nil
}

func validateArchivalGroupAgainstInventory(
	inventory schema.CatalogInventory,
	group preparedArchivalGroup,
	detachedSubtree *archivalDetachedSubtreeRequest,
	movedByOID map[uint32]bool,
) error {
	membersByOID := make(map[uint32]preparedArchivalMember, len(group.members))
	membersByID := make(map[string]preparedArchivalMember, len(group.members))
	for _, member := range group.members {
		membersByOID[member.marker.SourceTable.OID] = member
		membersByID[member.marker.MemberID] = member
		relation, err := uniqueRelationByOID(inventory, member.marker.SourceTable.OID)
		if err != nil {
			return err
		}
		kind, tableLike := inventory.ClassifyTable(relation.OID)
		if !tableLike {
			return fmt.Errorf("archival member relation OID %d is not table-like", relation.OID)
		}
		switch kind {
		case schema.CatalogTableKindTraditionalInheritance,
			schema.CatalogTableKindMultipleInheritance,
			schema.CatalogTableKindForeign,
			schema.CatalogTableKindForeignPartition:
			return fmt.Errorf("relation OID %d (%s.%s) has unsupported archival classification %q",
				relation.OID, relation.SchemaName, relation.Name, kind)
		}
		expectedSchema := member.marker.SourceTable.SchemaName
		if movedByOID != nil && movedByOID[relation.OID] {
			expectedSchema = member.marker.CleanupTable.SchemaName
		}
		if relation.SchemaName != expectedSchema ||
			relation.Name != member.marker.SourceTable.Name {
			return fmt.Errorf("relation OID %d is %s.%s instead of expected %s.%s",
				relation.OID, relation.SchemaName, relation.Name, expectedSchema, member.marker.SourceTable.Name)
		}
	}

	expectedEdges := make(map[string]archivalMarkerPartitionEdgeV1, len(group.marker.PartitionEdges))
	for _, edge := range group.marker.PartitionEdges {
		parent := membersByID[edge.ParentMemberID]
		child := membersByID[edge.ChildMemberID]
		key := oidEdgeKey(parent.marker.SourceTable.OID, child.marker.SourceTable.OID)
		expectedEdges[key] = edge
		if group.allocation != nil {
			expected, err := archivalPartitionEdgeMetadata(inventory, group,
				parent.marker.SourceTable.OID, child.marker.SourceTable.OID)
			if err != nil {
				return err
			}
			expected = filterLostClonedTriggersFromPartitionEdge(expected, group.marker)
			expected = filterDetachedPartitionEdgeTriggers(expected, group.marker)
			if !reflect.DeepEqual(canonicalizeArchivalPartitionEdge(edge), expected) {
				return fmt.Errorf("partition metadata for group %q edge %s does not match the source catalog",
					group.id, strings.ReplaceAll(key, "\x00", " -> "))
			}
		}
	}

	actualInternal := make(map[string]struct{}, len(expectedEdges))
	var boundaryEdges []schema.CatalogInheritanceEdge
	parentCounts := make(map[uint32]int)
	for _, edge := range inventory.InheritanceEdges {
		_, childMember := membersByOID[edge.ChildRelationOID]
		_, parentMember := membersByOID[edge.ParentRelationOID]
		if !childMember && !parentMember {
			continue
		}
		childRelation := catalogRelationWithOID(inventory, edge.ChildRelationOID)
		parentRelation := catalogRelationWithOID(inventory, edge.ParentRelationOID)
		if childRelation == nil || parentRelation == nil ||
			!isTableRelationKind(childRelation.Kind) ||
			!isTableRelationKind(parentRelation.Kind) {
			continue
		}
		if childMember {
			parentCounts[edge.ChildRelationOID]++
		}
		if childMember && parentMember {
			actualInternal[oidEdgeKey(edge.ParentRelationOID, edge.ChildRelationOID)] = struct{}{}
			continue
		}
		boundaryEdges = append(boundaryEdges, edge)
	}
	for expected := range expectedEdges {
		if _, ok := actualInternal[expected]; !ok {
			return fmt.Errorf("archival group %q is missing partition edge %s", group.id,
				strings.ReplaceAll(expected, "\x00", " -> "))
		}
	}
	for actual := range actualInternal {
		if _, ok := expectedEdges[actual]; !ok {
			return fmt.Errorf("archival group %q has unexpected partition edge %s", group.id,
				strings.ReplaceAll(actual, "\x00", " -> "))
		}
	}

	root := membersByID[group.rootMemberID]
	if detachedSubtree == nil {
		if len(boundaryEdges) != 0 {
			return fmt.Errorf("complete archival tree %q has %d inheritance edges outside the group",
				group.id, len(boundaryEdges))
		}
		if len(group.members) == 1 {
			kind, _ := inventory.ClassifyTable(root.marker.SourceTable.OID)
			if kind != schema.CatalogTableKindOrdinary &&
				kind != schema.CatalogTableKindPartitioned {
				return fmt.Errorf("single-member archival group %q has unsupported root classification %q", group.id, kind)
			}
		} else if root.relationKind != schema.RelKindPartitionedTable {
			return fmt.Errorf("complete partition tree %q root must be partitioned", group.id)
		}
	} else if err := validateDetachedSubtreeBoundary(
		inventory, group, *detachedSubtree, root, boundaryEdges,
	); err != nil {
		return err
	}

	for _, member := range group.members {
		expectedParents := 1
		if member.marker.MemberID == group.rootMemberID {
			expectedParents = 0
			if detachedSubtree != nil && (group.allocation != nil || group.detach != nil) {
				expectedParents = 1
			}
		}
		if parentCounts[member.marker.SourceTable.OID] != expectedParents {
			return fmt.Errorf("archival member %q has %d parents; expected %d",
				member.marker.MemberID, parentCounts[member.marker.SourceTable.OID], expectedParents)
		}
	}
	return nil
}

func filterDetachedPartitionEdgeTriggers(
	edge archivalMarkerPartitionEdgeV1,
	marker archivalMarkerV1,
) archivalMarkerPartitionEdgeV1 {
	attached := make(map[uint32]struct{})
	for _, member := range marker.Members {
		for _, object := range member.AttachedObjects {
			if object.Kind == archivalMarkerObjectKindTrigger {
				attached[object.OID] = struct{}{}
			}
		}
	}
	edge.ClonedTriggers = slices.DeleteFunc(slices.Clone(edge.ClonedTriggers),
		func(trigger archivalMarkerClonedTriggerV1) bool {
			_, parentAttached := attached[trigger.ParentTrigger.OID]
			_, childAttached := attached[trigger.ChildTrigger.OID]
			return !parentAttached || !childAttached
		})
	return canonicalizeArchivalPartitionEdge(edge)
}

func validateDetachedSubtreeBoundary(
	inventory schema.CatalogInventory,
	group preparedArchivalGroup,
	request archivalDetachedSubtreeRequest,
	root preparedArchivalMember,
	boundaryEdges []schema.CatalogInheritanceEdge,
) error {
	lost := group.marker.LostParentAttachments[0]
	if request.RootRelationOID != root.marker.SourceTable.OID ||
		lost.RootMemberID != root.marker.MemberID {
		return fmt.Errorf("detached subtree root intent does not match group %q root", group.id)
	}
	if request.ParentRelationOID != lost.ParentTable.OID {
		return fmt.Errorf("detached subtree parent intent does not match group %q marker", group.id)
	}
	parent, err := uniqueRelationByOID(inventory, request.ParentRelationOID)
	if err != nil {
		return fmt.Errorf("detached subtree parent: %w", err)
	}
	if parent.SchemaName != lost.ParentTable.SchemaName || parent.Name != lost.ParentTable.Name ||
		parent.Kind != schema.RelKindPartitionedTable {
		return fmt.Errorf("detached subtree parent identity for group %q does not match an active partitioned table", group.id)
	}
	expectAttached := group.allocation != nil || group.detach != nil
	if expectAttached {
		if len(boundaryEdges) != 1 || boundaryEdges[0].ChildRelationOID != request.RootRelationOID ||
			boundaryEdges[0].ParentRelationOID != request.ParentRelationOID {
			return fmt.Errorf("detached subtree group %q has inconsistent active parent boundary", group.id)
		}
		if group.allocation != nil {
			expected, err := archivalLostParentAttachmentMetadata(inventory, group,
				root.marker, lost.ParentTable)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(canonicalizeLostParentAttachment(lost), expected) {
				return fmt.Errorf("lost parent metadata for group %q does not match the source catalog", group.id)
			}
		}
	} else {
		if len(boundaryEdges) != 0 {
			return fmt.Errorf("detached subtree group %q still has an active parent boundary", group.id)
		}
		relation, err := uniqueRelationByOID(inventory, root.marker.SourceTable.OID)
		if err != nil {
			return err
		}
		if relation.IsPartition {
			return fmt.Errorf("detached subtree group %q root is still marked as a partition", group.id)
		}
	}
	return nil
}

func archivalPartitionEdgeMetadata(
	inventory schema.CatalogInventory,
	group preparedArchivalGroup,
	parentOID uint32,
	childOID uint32,
) (archivalMarkerPartitionEdgeV1, error) {
	parent := archivalMemberForRelationOID(group, parentOID)
	child := archivalMemberForRelationOID(group, childOID)
	attachment, err := uniquePartitionAttachment(inventory, parentOID, childOID)
	if err != nil {
		return archivalMarkerPartitionEdgeV1{}, err
	}
	indexes, triggers, err := archivalPartitionLocalMetadata(inventory, group, parentOID, childOID)
	if err != nil {
		return archivalMarkerPartitionEdgeV1{}, err
	}
	return canonicalizeArchivalPartitionEdge(archivalMarkerPartitionEdgeV1{
		ParentMemberID: parent.marker.MemberID, ChildMemberID: child.marker.MemberID,
		BoundExpression:             attachment.BoundExpression,
		PartitionedIndexAttachments: indexes, ClonedTriggers: triggers,
	}), nil
}

func archivalLostParentAttachmentMetadata(
	inventory schema.CatalogInventory,
	group preparedArchivalGroup,
	root archivalMarkerMemberV1,
	parent archivalMarkerObjectIdentity,
) (archivalMarkerLostParentAttachmentV1, error) {
	attachment, err := uniquePartitionAttachment(inventory, parent.OID, root.SourceTable.OID)
	if err != nil {
		return archivalMarkerLostParentAttachmentV1{}, err
	}
	indexes, triggers, err := archivalPartitionLocalMetadata(inventory, group, parent.OID, root.SourceTable.OID)
	if err != nil {
		return archivalMarkerLostParentAttachmentV1{}, err
	}
	lostTriggerOIDs := make(map[uint32]struct{}, len(triggers))
	for _, trigger := range triggers {
		lostTriggerOIDs[trigger.ChildTrigger.OID] = struct{}{}
	}
	// DETACH removes every descendant clone derived from a trigger on the active parent.
	for changed := true; changed; {
		changed = false
		for _, trigger := range inventory.Triggers {
			if _, alreadyLost := lostTriggerOIDs[trigger.OID]; alreadyLost {
				continue
			}
			if _, parentLost := lostTriggerOIDs[trigger.ParentTriggerOID]; !parentLost {
				continue
			}
			if _, member := archivalMemberByRelationOID(group, trigger.RelationOID); !member {
				continue
			}
			record, err := archivalClonedTriggerMetadata(inventory, group, trigger)
			if err != nil {
				return archivalMarkerLostParentAttachmentV1{}, err
			}
			triggers = append(triggers, record)
			lostTriggerOIDs[trigger.OID] = struct{}{}
			changed = true
		}
	}
	return canonicalizeLostParentAttachment(archivalMarkerLostParentAttachmentV1{
		RootMemberID: root.MemberID, ParentTable: parent, BoundExpression: attachment.BoundExpression,
		PartitionedIndexAttachments: indexes, ClonedTriggers: triggers,
	}), nil
}

func archivalPartitionLocalMetadata(
	inventory schema.CatalogInventory,
	group preparedArchivalGroup,
	parentOID uint32,
	childOID uint32,
) ([]archivalMarkerPartitionedIndexAttachmentV1, []archivalMarkerClonedTriggerV1, error) {
	indexesByOID := make(map[uint32]schema.CatalogIndex, len(inventory.Indexes))
	for _, index := range inventory.Indexes {
		indexesByOID[index.OID] = index
	}
	var indexAttachments []archivalMarkerPartitionedIndexAttachmentV1
	for _, edge := range inventory.InheritanceEdges {
		parentIndex, parentOK := indexesByOID[edge.ParentRelationOID]
		childIndex, childOK := indexesByOID[edge.ChildRelationOID]
		if !parentOK || !childOK || parentIndex.RelationOID != parentOID || childIndex.RelationOID != childOID {
			continue
		}
		indexAttachments = append(indexAttachments, archivalMarkerPartitionedIndexAttachmentV1{
			ParentIndex: partitionMarkerObject(parentIndex.OID, archivalMarkerObjectKindIndex,
				archivalRelationDestinationSchema(group, parentOID, parentIndex.SchemaName), parentIndex.Name),
			ChildIndex: partitionMarkerObject(childIndex.OID, archivalMarkerObjectKindIndex,
				archivalRelationDestinationSchema(group, childOID, childIndex.SchemaName), childIndex.Name),
		})
	}

	triggersByOID := make(map[uint32]schema.CatalogTrigger, len(inventory.Triggers))
	for _, trigger := range inventory.Triggers {
		triggersByOID[trigger.OID] = trigger
	}
	var clonedTriggers []archivalMarkerClonedTriggerV1
	for _, childTrigger := range inventory.Triggers {
		if childTrigger.RelationOID != childOID || childTrigger.ParentTriggerOID == 0 {
			continue
		}
		if _, ok := triggersByOID[childTrigger.ParentTriggerOID]; !ok {
			return nil, nil, fmt.Errorf("cloned trigger OID %d references missing parent trigger OID %d",
				childTrigger.OID, childTrigger.ParentTriggerOID)
		}
		record, err := archivalClonedTriggerMetadata(inventory, group, childTrigger)
		if err != nil {
			return nil, nil, err
		}
		clonedTriggers = append(clonedTriggers, record)
	}
	return canonicalizeArchivalPartitionedIndexAttachments(indexAttachments),
		canonicalizeArchivalClonedTriggers(clonedTriggers), nil
}

func archivalClonedTriggerMetadata(
	inventory schema.CatalogInventory,
	group preparedArchivalGroup,
	childTrigger schema.CatalogTrigger,
) (archivalMarkerClonedTriggerV1, error) {
	var parentTrigger *schema.CatalogTrigger
	for idx := range inventory.Triggers {
		if inventory.Triggers[idx].OID == childTrigger.ParentTriggerOID {
			parentTrigger = &inventory.Triggers[idx]
			break
		}
	}
	if parentTrigger == nil {
		return archivalMarkerClonedTriggerV1{}, fmt.Errorf(
			"cloned trigger OID %d references missing parent trigger OID %d",
			childTrigger.OID, childTrigger.ParentTriggerOID,
		)
	}
	parentRelation := catalogRelationWithOID(inventory, parentTrigger.RelationOID)
	childRelation := catalogRelationWithOID(inventory, childTrigger.RelationOID)
	if parentRelation == nil || childRelation == nil {
		return archivalMarkerClonedTriggerV1{},
			fmt.Errorf("cloned trigger relation identity is missing")
	}
	return archivalMarkerClonedTriggerV1{
		ParentTrigger: partitionMarkerObject(parentTrigger.OID, archivalMarkerObjectKindTrigger,
			archivalRelationDestinationSchema(group, parentTrigger.RelationOID, parentRelation.SchemaName),
			parentTrigger.Name),
		ChildTrigger: partitionMarkerObject(childTrigger.OID, archivalMarkerObjectKindTrigger,
			archivalRelationDestinationSchema(group, childTrigger.RelationOID, childRelation.SchemaName),
			childTrigger.Name),
		FunctionOID: childTrigger.FunctionOID, Type: childTrigger.Type,
		EnabledMode: childTrigger.EnabledMode, IsInternal: childTrigger.IsInternal,
		ConstraintOID: childTrigger.ConstraintOID, Definition: childTrigger.Definition,
		Comment: childTrigger.Comment,
	}, nil
}

func partitionMarkerObject(
	oid uint32,
	kind archivalMarkerObjectKind,
	schemaName string,
	name string,
) archivalMarkerObjectIdentity {
	return archivalMarkerObjectIdentity{
		Kind: kind, OID: oid, SchemaName: schemaName, Name: name,
	}
}

func archivalRelationDestinationSchema(
	group preparedArchivalGroup,
	relationOID uint32,
	fallback string,
) string {
	for _, member := range group.members {
		if member.marker.SourceTable.OID == relationOID {
			return member.marker.CleanupTable.SchemaName
		}
	}
	return fallback
}

func archivalMemberForRelationOID(group preparedArchivalGroup, relationOID uint32) preparedArchivalMember {
	for _, member := range group.members {
		if member.marker.SourceTable.OID == relationOID {
			return member
		}
	}
	return preparedArchivalMember{}
}

func archivalMemberByRelationOID(
	group preparedArchivalGroup,
	relationOID uint32,
) (archivalMarkerMemberV1, bool) {
	member := archivalMemberForRelationOID(group, relationOID)
	return member.marker, member.marker.MemberID != ""
}

func uniquePartitionAttachment(
	inventory schema.CatalogInventory,
	parentOID uint32,
	childOID uint32,
) (schema.CatalogPartitionAttachment, error) {
	var matches []schema.CatalogPartitionAttachment
	for _, attachment := range inventory.PartitionAttachments {
		if attachment.ParentRelationOID == parentOID && attachment.RelationOID == childOID {
			matches = append(matches, attachment)
		}
	}
	if len(matches) != 1 {
		return schema.CatalogPartitionAttachment{}, fmt.Errorf(
			"partition attachment %d -> %d has %d catalog records; exactly one is required",
			parentOID, childOID, len(matches),
		)
	}
	return matches[0], nil
}

func canonicalizeLostParentAttachment(
	attachment archivalMarkerLostParentAttachmentV1,
) archivalMarkerLostParentAttachmentV1 {
	attachment.ParentTable = cloneMarkerObject(attachment.ParentTable)
	attachment.PartitionedIndexAttachments = canonicalizeArchivalPartitionedIndexAttachments(
		attachment.PartitionedIndexAttachments,
	)
	attachment.ClonedTriggers = canonicalizeArchivalClonedTriggers(attachment.ClonedTriggers)
	return attachment
}

func isTableRelationKind(kind schema.RelKind) bool {
	return kind == schema.RelKindOrdinaryTable || kind == schema.RelKindPartitionedTable ||
		kind == schema.RelKindForeignTable
}

func filterLostParentClonedTriggers(
	move schema.CatalogExpectedTableMove,
	marker archivalMarkerV1,
	_ archivalMarkerMemberV1,
) schema.CatalogExpectedTableMove {
	if len(marker.LostParentAttachments) != 1 {
		return move
	}
	lostTriggerOIDs := make(map[uint32]struct{}, len(marker.LostParentAttachments[0].ClonedTriggers))
	for _, trigger := range marker.LostParentAttachments[0].ClonedTriggers {
		lostTriggerOIDs[trigger.ChildTrigger.OID] = struct{}{}
	}
	move.AttachedObjects = slices.DeleteFunc(slices.Clone(move.AttachedObjects),
		func(object schema.CatalogObjectIdentity) bool {
			_, lost := lostTriggerOIDs[object.OID]
			return object.Kind == schema.CatalogObjectKindTrigger && lost
		})
	return move
}

func removeLostParentClonedTriggersFromMarker(marker *archivalMarkerV1) {
	if len(marker.LostParentAttachments) != 1 {
		return
	}
	lost := marker.LostParentAttachments[0]
	lostTriggerOIDs := make(map[uint32]struct{}, len(lost.ClonedTriggers))
	for _, trigger := range lost.ClonedTriggers {
		lostTriggerOIDs[trigger.ChildTrigger.OID] = struct{}{}
	}
	// Member and internal-edge sets describe the final detached state; the lost
	// attachment record is the sole source of the removed trigger lineage.
	for idx := range marker.Members {
		marker.Members[idx].AttachedObjects = slices.DeleteFunc(
			slices.Clone(marker.Members[idx].AttachedObjects),
			func(object archivalMarkerObjectIdentity) bool {
				_, lost := lostTriggerOIDs[object.OID]
				return object.Kind == archivalMarkerObjectKindTrigger && lost
			},
		)
	}
	for idx := range marker.PartitionEdges {
		marker.PartitionEdges[idx] = filterLostClonedTriggersFromPartitionEdge(
			marker.PartitionEdges[idx], *marker,
		)
	}
}

func filterLostClonedTriggersFromPartitionEdge(
	edge archivalMarkerPartitionEdgeV1,
	marker archivalMarkerV1,
) archivalMarkerPartitionEdgeV1 {
	if len(marker.LostParentAttachments) != 1 {
		return edge
	}
	lostTriggerOIDs := make(map[uint32]struct{}, len(marker.LostParentAttachments[0].ClonedTriggers))
	for _, trigger := range marker.LostParentAttachments[0].ClonedTriggers {
		lostTriggerOIDs[trigger.ChildTrigger.OID] = struct{}{}
	}
	edge.ClonedTriggers = slices.DeleteFunc(slices.Clone(edge.ClonedTriggers),
		func(trigger archivalMarkerClonedTriggerV1) bool {
			_, lost := lostTriggerOIDs[trigger.ChildTrigger.OID]
			return lost
		})
	return canonicalizeArchivalPartitionEdge(edge)
}

func appendArchivalPartitionAssertions(body *strings.Builder, group preparedArchivalGroup) {
	memberOIDs := make([]uint32, 0, len(group.members))
	for _, member := range group.members {
		memberOIDs = append(memberOIDs, member.marker.SourceTable.OID)
	}
	slices.Sort(memberOIDs)
	memberOIDSQL := uint32SQLList(memberOIDs)
	appendPlainTableAssertion(body, fmt.Sprintf(
		"(SELECT count(*) FROM pg_catalog.pg_inherits AS i WHERE i.inhrelid IN (%s) OR i.inhparent IN (%s)) = %d",
		memberOIDSQL, memberOIDSQL, len(group.marker.PartitionEdges),
	),
		fmt.Sprintf("archival partition topology edge count mismatch for group %s", group.id))
	appendPlainTableAssertion(body, fmt.Sprintf(
		"(SELECT count(*) FROM pg_catalog.pg_class AS c WHERE c.oid IN (%s) AND c.relispartition) = %d",
		memberOIDSQL, len(group.marker.PartitionEdges),
	),
		fmt.Sprintf("archival partition membership mismatch for group %s", group.id))
	var indexOIDs []uint32
	var triggerOIDs []uint32
	expectedIndexEdges := 0
	expectedTriggerEdges := 0
	for _, member := range group.marker.Members {
		indexOIDs = append(indexOIDs,
			markerObjectOIDs(member.AutomaticallyMovedObjects, archivalMarkerObjectKindIndex)...)
		triggerOIDs = append(triggerOIDs,
			markerObjectOIDs(member.AttachedObjects, archivalMarkerObjectKindTrigger)...)
	}
	for _, edge := range group.marker.PartitionEdges {
		expectedIndexEdges += len(edge.PartitionedIndexAttachments)
		expectedTriggerEdges += len(edge.ClonedTriggers)
	}
	if len(indexOIDs) > 0 {
		slices.Sort(indexOIDs)
		indexOIDSQL := uint32SQLList(indexOIDs)
		appendPlainTableAssertion(body, fmt.Sprintf(
			"(SELECT count(*) FROM pg_catalog.pg_inherits AS i WHERE i.inhrelid IN (%s) OR i.inhparent IN (%s)) = %d",
			indexOIDSQL, indexOIDSQL, expectedIndexEdges,
		),
			fmt.Sprintf("archival partitioned-index topology edge count mismatch for group %s", group.id))
	}
	if len(triggerOIDs) > 0 {
		slices.Sort(triggerOIDs)
		appendPlainTableAssertion(body, fmt.Sprintf(
			"(SELECT count(*) FROM pg_catalog.pg_trigger WHERE oid IN (%s) AND tgparentid <> 0) = %d",
			uint32SQLList(triggerOIDs), expectedTriggerEdges,
		),
			fmt.Sprintf("archival cloned-trigger topology edge count mismatch for group %s", group.id))
	}

	for _, edge := range group.marker.PartitionEdges {
		parent := preparedArchivalMemberByID(group.members, edge.ParentMemberID)
		child := preparedArchivalMemberByID(group.members, edge.ChildMemberID)
		appendPlainTableAssertion(body, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM pg_catalog.pg_inherits AS i JOIN pg_catalog.pg_class AS c ON c.oid = i.inhrelid "+
				"WHERE i.inhparent = %d AND i.inhrelid = %d AND pg_catalog.pg_get_expr(c.relpartbound, c.oid) = %s)",
			parent.marker.SourceTable.OID, child.marker.SourceTable.OID,
			schema.EscapeLiteral(edge.BoundExpression),
		),
			fmt.Sprintf("archival partition attachment mismatch for group %s member %s",
				group.id, edge.ChildMemberID))
		appendPartitionLocalMetadataAssertions(body, group.id,
			edge.PartitionedIndexAttachments, edge.ClonedTriggers, false)
	}
	for _, lost := range group.marker.LostParentAttachments {
		root := preparedArchivalMemberByID(group.members, lost.RootMemberID)
		appendPlainTableAssertion(body, fmt.Sprintf(
			"NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhparent = %d AND inhrelid = %d)",
			lost.ParentTable.OID, root.marker.SourceTable.OID,
		),
			fmt.Sprintf("archival detached subtree boundary still exists for group %s", group.id))
		appendPlainTableAssertion(body, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE oid = %d AND NOT relispartition AND relpartbound IS NULL)",
			root.marker.SourceTable.OID,
		),
			fmt.Sprintf("archival detached subtree root state mismatch for group %s", group.id))
		appendPartitionLocalMetadataAssertions(body, group.id,
			lost.PartitionedIndexAttachments, lost.ClonedTriggers, true)
	}
}

func appendPartitionLocalMetadataAssertions(
	body *strings.Builder,
	groupID archivalGroupID,
	indexes []archivalMarkerPartitionedIndexAttachmentV1,
	triggers []archivalMarkerClonedTriggerV1,
	detached bool,
) {
	for _, attachment := range indexes {
		predicate := fmt.Sprintf(
			"EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhparent = %d AND inhrelid = %d)",
			attachment.ParentIndex.OID, attachment.ChildIndex.OID,
		)
		if detached {
			predicate = "NOT " + predicate
		}
		appendPlainTableAssertion(body, predicate,
			fmt.Sprintf("archival partitioned-index attachment mismatch for group %s child index OID %d",
				groupID, attachment.ChildIndex.OID))
	}
	for _, trigger := range triggers {
		predicate := fmt.Sprintf(
			"EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE oid = %d AND tgparentid = %d "+
				"AND tgfoid = %d AND tgtype = %d AND tgenabled = %s AND tgisinternal = %t AND tgconstraint = %d)",
			trigger.ChildTrigger.OID, trigger.ParentTrigger.OID, trigger.FunctionOID, trigger.Type,
			schema.EscapeLiteral(trigger.EnabledMode), trigger.IsInternal, trigger.ConstraintOID,
		)
		if detached {
			predicate = fmt.Sprintf("NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE oid = %d)",
				trigger.ChildTrigger.OID)
		}
		appendPlainTableAssertion(body, predicate,
			fmt.Sprintf("archival cloned-trigger metadata mismatch for group %s child trigger OID %d",
				groupID, trigger.ChildTrigger.OID))
	}
}

func uint32SQLList(values []uint32) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprint(value))
	}
	return strings.Join(parts, ", ")
}
