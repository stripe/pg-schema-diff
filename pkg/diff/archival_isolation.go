package diff

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

type archivalIsolationPlan struct {
	Groups                    []archivalIsolationGroupPlan
	ACLRevokes                []archivalACLRevokeOperation
	BoundaryForeignKeyDrops   []archivalBoundaryForeignKeyOperation
	PublicationRemovals       []archivalPublicationRemovalOperation
	StatisticMoves            []archivalObjectMoveOperation
	DependencyMoves           []archivalObjectMoveOperation
	IncomingForeignKeyReadds  []archivalIncomingForeignKeyOperation
	IncomingDependencyDeletes []sourceSafetyIncomingDependency
	PreservedForeignKeys      []archivalPreservedForeignKey
	RetainedRoutineFollowers  []archivalMarkerObjectIdentity
}

type archivalIsolationGroupPlan struct {
	GroupID               archivalGroupID
	OriginalACLs          []archivalMarkerACLRecordV1
	OriginalForeignKeys   []archivalMarkerForeignKeyV1
	OriginalPublications  []archivalMarkerPublicationMembershipV1
	SchemaPublications    []archivalSchemaPublication
	Statistics            []archivalMarkerObjectIdentity
	Dependencies          []archivalMarkerObjectIdentity
	SharedDependencyEdges []archivalMarkerSharedGroupEdgeV1
}

type archivalACLRevokeOperation struct {
	GroupID     archivalGroupID
	Revoke      schema.CatalogACLRevoke
	Record      archivalMarkerACLRecordV1
	Destination archivalMarkerObjectIdentity
}

type archivalBoundaryForeignKeyOperation struct {
	ForeignKey schema.CatalogForeignKey
	Record     archivalMarkerForeignKeyV1
	GroupIDs   []archivalGroupID
}

type archivalPublicationRemovalOperation struct {
	Membership schema.CatalogPublicationRelation
	Record     archivalMarkerPublicationMembershipV1
	GroupID    archivalGroupID
}

type archivalObjectMoveOperation struct {
	GroupID     archivalGroupID
	Source      archivalMarkerObjectIdentity
	Destination archivalMarkerObjectIdentity
}

type archivalIncomingForeignKeyOperation struct {
	Record     archivalMarkerForeignKeyV1
	Constraint schema.ForeignKeyConstraint
}

type archivalPreservedForeignKey struct {
	GroupID    archivalGroupID
	ForeignKey schema.CatalogForeignKey
}

type archivalSchemaPublication struct {
	PublicationName string
	TableOID        uint32
}

type archivalSelectedACLObject struct {
	address     schema.CatalogDependencyObject
	groupID     archivalGroupID
	destination archivalMarkerObjectIdentity
}

type archivalIsolationVertexKind string

const (
	archivalIsolationVertexKindBoundaryForeignKeyDrop archivalIsolationVertexKind = "boundary_fk_drop"
	archivalIsolationVertexKindPublicationRemoval     archivalIsolationVertexKind = "publication_removal"
	archivalIsolationVertexKindACLRevoke              archivalIsolationVertexKind = "acl_revoke"
	archivalIsolationVertexKindStatisticMove          archivalIsolationVertexKind = "statistic_move"
	archivalIsolationVertexKindDependencyMove         archivalIsolationVertexKind = "dependency_move"
	archivalIsolationVertexKindIncomingForeignKeyAdd  archivalIsolationVertexKind = "incoming_fk_add"
)

type archivalIsolationVertexID struct {
	phase int
	kind  archivalIsolationVertexKind
	key   string
}

func (id archivalIsolationVertexID) String() string {
	return fmt.Sprintf("archival:%02d:isolation:%s:%s", id.phase, id.kind, id.key)
}

var (
	migrationHazardBoundaryForeignKeyDrop = []MigrationHazard{
		{
			Type:    MigrationHazardTypeCorrectness,
			Message: "Dropping a foreign key across an archival boundary removes referential enforcement for retained or active rows.",
		},
		{
			Type:    MigrationHazardTypeAcquiresAccessExclusiveLock,
			Message: "Dropping the foreign key acquires an ACCESS EXCLUSIVE lock on its owning table.",
		},
	}
	migrationHazardArchivalPublicationUpdate = MigrationHazard{
		Type:    MigrationHazardTypeCorrectness,
		Message: "Removing explicit publication membership stops replication of changes to the archived table.",
	}
	migrationHazardArchivalNamespaceUpdate = MigrationHazard{
		Type:    MigrationHazardTypeCorrectness,
		Message: "Moving a retained dependency changes its schema-qualified name while preserving its catalog identity.",
	}
	migrationHazardArchivalDependencyLock = MigrationHazard{
		Type:    MigrationHazardTypeAcquiresAccessExclusiveLock,
		Message: "Moving the retained dependency acquires an ACCESS EXCLUSIVE lock on that object.",
	}
	migrationHazardIncomingForeignKeyAdd = MigrationHazard{
		Type:    MigrationHazardTypeAcquiresShareRowExclusiveLock,
		Message: "Adding the foreign key takes a SHARE ROW EXCLUSIVE lock while referential enforcement is installed.",
	}
)

// planArchivalIsolation is dormant until archival activation. It consumes only
// immutable typed catalog products and validates marker metadata before any SQL
// graph is constructed.
func planArchivalIsolation(
	inventory schema.CatalogInventory,
	target schema.Schema,
	preflight sourceSafetyPreflightResult,
	closure archivedDependencyClosureResult,
	groups []preparedPlainTableArchivalGroup,
) (archivalIsolationPlan, error) {
	if len(groups) == 0 {
		return archivalIsolationPlan{}, nil
	}
	inventory = inventory.Normalize()
	target = target.Normalize()
	groupByRelationOID := make(map[uint32]preparedPlainTableArchivalGroup, len(groups))
	for _, group := range groups {
		groupByRelationOID[group.member.SourceTable.OID] = group
	}

	plan := archivalIsolationPlan{}
	groupPlans := make(map[archivalGroupID]*archivalIsolationGroupPlan, len(groups))
	for _, group := range groups {
		plan.Groups = append(plan.Groups, archivalIsolationGroupPlan{
			GroupID:               group.id,
			Statistics:            canonicalMarkerObjects(group.member.ExplicitlyMovedObjects),
			Dependencies:          canonicalMarkerObjects(nil),
			SharedDependencyEdges: canonicalArchivedDependencyGroupEdges(nil),
		})
	}
	for idx := range plan.Groups {
		groupPlans[plan.Groups[idx].GroupID] = &plan.Groups[idx]
	}

	if err := planArchivalDependencyIsolation(inventory, closure, groups, groupPlans, &plan); err != nil {
		return archivalIsolationPlan{}, err
	}
	if err := planArchivalACLIsolation(inventory, groups, closure, groupPlans, &plan); err != nil {
		return archivalIsolationPlan{}, err
	}
	if err := planArchivalForeignKeyIsolation(preflight, target, groupByRelationOID, groupPlans, &plan); err != nil {
		return archivalIsolationPlan{}, err
	}
	if err := planArchivalPublicationIsolation(preflight, groupByRelationOID, groupPlans, &plan); err != nil {
		return archivalIsolationPlan{}, err
	}
	if err := planArchivalIncomingDependencyDeletes(preflight, &plan); err != nil {
		return archivalIsolationPlan{}, err
	}
	if err := validateArchivalIsolationMarkers(groups, plan.Groups); err != nil {
		return archivalIsolationPlan{}, err
	}
	if err := completeResumedArchivalIncomingForeignKeys(inventory, target, groups, &plan); err != nil {
		return archivalIsolationPlan{}, err
	}

	slices.SortFunc(plan.Groups, func(a, b archivalIsolationGroupPlan) int {
		return cmp.Compare(a.GroupID, b.GroupID)
	})
	slices.SortFunc(plan.BoundaryForeignKeyDrops, func(a, b archivalBoundaryForeignKeyOperation) int {
		return cmp.Compare(archivalMarkerForeignKeyKey(a.Record), archivalMarkerForeignKeyKey(b.Record))
	})
	slices.SortFunc(plan.PublicationRemovals, func(a, b archivalPublicationRemovalOperation) int {
		return cmp.Or(cmp.Compare(a.Record.PublicationName, b.Record.PublicationName),
			compareMarkerObjects(a.Record.Table, b.Record.Table))
	})
	slices.SortFunc(plan.IncomingForeignKeyReadds, func(
		a, b archivalIncomingForeignKeyOperation,
	) int {
		return cmp.Compare(archivalMarkerForeignKeyKey(a.Record), archivalMarkerForeignKeyKey(b.Record))
	})
	return plan, nil
}

func completeResumedArchivalIncomingForeignKeys(
	inventory schema.CatalogInventory,
	target schema.Schema,
	groups []preparedPlainTableArchivalGroup,
	plan *archivalIsolationPlan,
) error {
	archivedOIDs := make(map[uint32]struct{}, len(groups))
	for _, group := range groups {
		archivedOIDs[group.member.SourceTable.OID] = struct{}{}
	}
	for _, group := range groups {
		if group.resume == nil {
			continue
		}
		for _, record := range group.marker.OriginalForeignKeys {
			_, owningArchived := archivedOIDs[record.OwningTable.OID]
			_, referencedArchived := archivedOIDs[record.ReferencedTable.OID]
			if owningArchived || !referencedArchived {
				continue
			}
			constraint, found, err := uniqueTargetForeignKey(target, record)
			if err != nil {
				return err
			}
			if !found || slices.ContainsFunc(plan.IncomingForeignKeyReadds,
				func(operation archivalIncomingForeignKeyOperation) bool {
					return archivalMarkerForeignKeyKey(operation.Record) == archivalMarkerForeignKeyKey(record)
				}) {
				continue
			}
			alreadyRecreated := false
			for _, current := range inventory.ForeignKeys {
				if current.OwningRelationOID == record.OwningTable.OID && current.Name == record.Name &&
					current.ReferencedRelationOID != record.ReferencedTable.OID {
					alreadyRecreated = true
				}
			}
			if !alreadyRecreated {
				plan.IncomingForeignKeyReadds = append(plan.IncomingForeignKeyReadds,
					archivalIncomingForeignKeyOperation{Record: record, Constraint: constraint})
			}
		}
	}
	return nil
}

func planArchivalDependencyIsolation(
	inventory schema.CatalogInventory,
	closure archivedDependencyClosureResult,
	groups []preparedPlainTableArchivalGroup,
	groupPlans map[archivalGroupID]*archivalIsolationGroupPlan,
	plan *archivalIsolationPlan,
) error {
	assignmentsByGroup := make(map[archivalGroupID][]archivalMarkerObjectIdentity)
	for _, assignment := range closure.Assignments {
		_, ok := groupPlans[assignment.GroupID]
		if !ok {
			return fmt.Errorf("dependency assignment references unrequested archival group %q", assignment.GroupID)
		}
		if assignment.Destination.SchemaName == "" ||
			assignment.Destination.SchemaName != groupDependencySchema(groups, assignment.GroupID) {
			return fmt.Errorf("dependency assignment for group %q has an invalid destination schema", assignment.GroupID)
		}
		if err := validateArchivalDependencyMoveIdentity(inventory, assignment); err != nil {
			return err
		}
		assignmentsByGroup[assignment.GroupID] =
			append(assignmentsByGroup[assignment.GroupID], assignment.Destination)
	}
	assignedTypes := make(map[uint32]struct{})
	for _, assignment := range closure.Assignments {
		if assignment.Source.Kind == archivalMarkerObjectKindType {
			assignedTypes[assignment.Source.OID] = struct{}{}
		}
	}
	for _, dependency := range inventory.Dependencies {
		if dependency.Dependent.ClassOID != pgProcCatalogOID ||
			dependency.Referenced.ClassOID != pgTypeCatalogOID ||
			(dependency.Type != "i" && dependency.Type != "a") {
			continue
		}
		if _, retained := assignedTypes[dependency.Referenced.ObjectOID]; !retained {
			continue
		}
		routine, count := catalogRoutineByOIDForClosure(inventory, dependency.Dependent.ObjectOID)
		if count != 1 {
			return fmt.Errorf("retained type follower routine OID %d has %d catalog identities",
				dependency.Dependent.ObjectOID, count)
		}
		identity := archivedDependencyMarkerObject(routine.OID, archivalMarkerObjectKindFunction,
			routine.SchemaName, routine.Name, strings.TrimSpace(routine.IdentityArguments))
		if !slices.ContainsFunc(plan.RetainedRoutineFollowers, func(
			existing archivalMarkerObjectIdentity,
		) bool {
			return compareMarkerObjects(existing, identity) == 0
		}) {
			plan.RetainedRoutineFollowers = append(plan.RetainedRoutineFollowers, identity)
		}
	}
	slices.SortFunc(plan.RetainedRoutineFollowers, compareMarkerObjects)
	for groupID, objects := range assignmentsByGroup {
		groupPlans[groupID].Dependencies = canonicalMarkerObjects(objects)
	}
	for _, group := range groups {
		groupPlans[group.id].SharedDependencyEdges = incidentArchivedDependencyEdges(
			group.id, closure.SharedGroupEdges,
		)
	}
	for _, group := range groups {
		if group.resume == nil {
			for _, assignment := range closure.Assignments {
				if assignment.GroupID == group.id {
					plan.DependencyMoves = append(plan.DependencyMoves, archivalObjectMoveOperation{
						GroupID: group.id, Source: assignment.Source, Destination: assignment.Destination,
					})
				}
			}
			for _, object := range group.member.ExplicitlyMovedObjects {
				source := cloneMarkerObject(object)
				source.SchemaName = group.member.SourceTable.SchemaName
				plan.StatisticMoves = append(plan.StatisticMoves, archivalObjectMoveOperation{
					GroupID: group.id, Source: source, Destination: object,
				})
			}
			continue
		}
		for _, move := range group.resume.RemainingExplicitObjectMoves {
			plan.StatisticMoves = append(plan.StatisticMoves, archivalObjectMoveOperation{
				GroupID: group.id, Source: move.Source, Destination: move.Destination,
			})
		}
		for _, move := range group.resume.RemainingDependencyObjectMoves {
			plan.DependencyMoves = append(plan.DependencyMoves, archivalObjectMoveOperation{
				GroupID: group.id, Source: move.Source, Destination: move.Destination,
			})
		}
	}
	slices.SortFunc(plan.StatisticMoves, compareArchivalObjectMoves)
	slices.SortFunc(plan.DependencyMoves, func(a, b archivalObjectMoveOperation) int {
		return cmp.Or(
			cmp.Compare(archivalDependencyMoveRank(a.Source.Kind),
				archivalDependencyMoveRank(b.Source.Kind)),
			compareArchivalObjectMoves(a, b),
		)
	})
	return nil
}

func planArchivalACLIsolation(
	inventory schema.CatalogInventory,
	groups []preparedPlainTableArchivalGroup,
	closure archivedDependencyClosureResult,
	groupPlans map[archivalGroupID]*archivalIsolationGroupPlan,
	plan *archivalIsolationPlan,
) error {
	var selected []archivalSelectedACLObject
	for _, group := range groups {
		selected = append(selected, archivalSelectedACLObject{
			address: schema.CatalogDependencyObject{
				ClassOID:  pgClassCatalogOID,
				ObjectOID: group.member.SourceTable.OID,
			},
			groupID: group.id, destination: group.member.CleanupTable,
		})
		for _, object := range group.member.AutomaticallyMovedObjects {
			if object.Kind != archivalMarkerObjectKindOwnedSequence {
				continue
			}
			destination := cloneMarkerObject(object)
			destination.Kind = archivalMarkerObjectKindSequence
			selected = append(selected, archivalSelectedACLObject{
				address: schema.CatalogDependencyObject{ClassOID: pgClassCatalogOID, ObjectOID: object.OID},
				groupID: group.id, destination: destination,
			})
		}
	}
	for _, assignment := range closure.Assignments {
		if assignment.Source.Kind != archivalMarkerObjectKindSequence {
			continue
		}
		selected = append(selected, archivalSelectedACLObject{
			address: schema.CatalogDependencyObject{ClassOID: pgClassCatalogOID, ObjectOID: assignment.Source.OID},
			groupID: assignment.GroupID, destination: assignment.Destination,
		})
	}
	addresses := make([]schema.CatalogDependencyObject, 0, len(selected))
	for _, object := range selected {
		addresses = append(addresses, object.address)
	}
	revokePlan, err := inventory.PlanACLRevokes(addresses)
	if err != nil {
		return fmt.Errorf("planning archival ACL isolation: %w", err)
	}
	for _, revoke := range revokePlan.Revokes {
		object, found := selectedACLObject(selected, revoke.Grant.Object.ObjectOID)
		if !found {
			return fmt.Errorf("stage 6 ACL revoke references unselected object OID %d", revoke.Grant.Object.ObjectOID)
		}
		record, err := archivalMarkerACLRecord(inventory, revoke.Grant)
		if err != nil {
			return err
		}
		plan.ACLRevokes = append(plan.ACLRevokes, archivalACLRevokeOperation{
			GroupID: object.groupID, Revoke: revoke, Record: record, Destination: object.destination,
		})
		if !containsMarkerACL(groupPlans[object.groupID].OriginalACLs, record) {
			groupPlans[object.groupID].OriginalACLs =
				append(groupPlans[object.groupID].OriginalACLs, record)
		}
	}
	for _, group := range groupPlans {
		group.OriginalACLs = canonicalizeArchivalMarker(archivalMarkerV1{
			OriginalACLs: group.OriginalACLs,
		}).OriginalACLs
	}
	return nil
}

func planArchivalForeignKeyIsolation(
	preflight sourceSafetyPreflightResult,
	target schema.Schema,
	groupByRelationOID map[uint32]preparedPlainTableArchivalGroup,
	groupPlans map[archivalGroupID]*archivalIsolationGroupPlan,
	plan *archivalIsolationPlan,
) error {
	for _, classified := range preflight.ForeignKeys {
		foreignKey := classified.ForeignKey
		owningGroup, owningArchived := groupByRelationOID[foreignKey.OwningRelationOID]
		referencedGroup, referencedArchived := groupByRelationOID[foreignKey.ReferencedRelationOID]
		if !owningArchived && !referencedArchived {
			return fmt.Errorf("stage 10 foreign key %s.%s does not touch a requested archival group",
				foreignKey.OwningSchemaName, foreignKey.Name)
		}
		if owningArchived && referencedArchived && owningGroup.id == referencedGroup.id {
			plan.PreservedForeignKeys = append(plan.PreservedForeignKeys, archivalPreservedForeignKey{
				GroupID: owningGroup.id, ForeignKey: foreignKey,
			})
			continue
		}
		record := archivalMarkerForeignKey(foreignKey)
		groupIDs := boundaryForeignKeyGroupIDs(owningGroup, owningArchived, referencedGroup, referencedArchived)
		plan.BoundaryForeignKeyDrops = append(plan.BoundaryForeignKeyDrops,
			archivalBoundaryForeignKeyOperation{ForeignKey: foreignKey, Record: record, GroupIDs: groupIDs})
		for _, groupID := range groupIDs {
			groupPlans[groupID].OriginalForeignKeys =
				append(groupPlans[groupID].OriginalForeignKeys, record)
		}
		if !owningArchived && referencedArchived {
			if classified.ManagedScope == sourceSafetyManagedScopeExcluded {
				return fmt.Errorf("incoming foreign key %s.%s is outside managed scope and cannot be recreated",
					foreignKey.OwningSchemaName, foreignKey.Name)
			}
			constraint, found, err := uniqueTargetForeignKey(target, record)
			if err != nil {
				return err
			}
			if classified.TargetIntent == sourceSafetyTargetIntentPersistent && !found {
				return fmt.Errorf("managed incoming foreign key %s.%s is required by the target but has no target definition",
					foreignKey.OwningSchemaName, foreignKey.Name)
			}
			if found {
				plan.IncomingForeignKeyReadds = append(plan.IncomingForeignKeyReadds,
					archivalIncomingForeignKeyOperation{Record: record, Constraint: constraint})
			}
		}
	}
	for _, group := range groupPlans {
		group.OriginalForeignKeys = canonicalizeArchivalMarker(archivalMarkerV1{
			OriginalForeignKeys: group.OriginalForeignKeys,
		}).OriginalForeignKeys
	}
	slices.SortFunc(plan.PreservedForeignKeys, func(a, b archivalPreservedForeignKey) int {
		return cmp.Or(cmp.Compare(a.GroupID, b.GroupID),
			cmp.Compare(a.ForeignKey.OID, b.ForeignKey.OID))
	})
	return nil
}

func planArchivalPublicationIsolation(
	preflight sourceSafetyPreflightResult,
	groupByRelationOID map[uint32]preparedPlainTableArchivalGroup,
	groupPlans map[archivalGroupID]*archivalIsolationGroupPlan,
	plan *archivalIsolationPlan,
) error {
	for _, membership := range preflight.PublicationRelations {
		if membership.PublicationName == "" || membership.RelationSchemaName == "" || membership.RelationName == "" {
			return fmt.Errorf("explicit publication metadata for relation OID %d is incomplete", membership.RelationOID)
		}
		group, ok := groupByRelationOID[membership.RelationOID]
		if !ok {
			return fmt.Errorf("explicit publication %q references an unrequested relation OID %d",
				membership.PublicationName, membership.RelationOID)
		}
		record := archivalMarkerPublicationMembership(membership)
		plan.PublicationRemovals = append(plan.PublicationRemovals, archivalPublicationRemovalOperation{
			Membership: membership, Record: record, GroupID: group.id,
		})
		groupPlans[group.id].OriginalPublications =
			append(groupPlans[group.id].OriginalPublications, record)
	}
	for _, membership := range preflight.PublicationSchemas {
		if membership.PublicationName == "" || membership.SchemaName == "" {
			return fmt.Errorf("schema publication metadata is incomplete")
		}
		matched := false
		for _, group := range groupByRelationOID {
			if group.member.SourceTable.SchemaName == membership.SchemaName {
				matched = true
				entry := archivalSchemaPublication{
					PublicationName: membership.PublicationName, TableOID: group.member.SourceTable.OID,
				}
				if !slices.Contains(groupPlans[group.id].SchemaPublications, entry) {
					groupPlans[group.id].SchemaPublications =
						append(groupPlans[group.id].SchemaPublications, entry)
				}
			}
		}
		if !matched {
			return fmt.Errorf("schema publication %q does not cover a requested archival table",
				membership.PublicationName)
		}
	}
	for _, group := range groupPlans {
		group.OriginalPublications = canonicalizeArchivalMarker(archivalMarkerV1{
			OriginalPublicationMemberships: group.OriginalPublications,
		}).OriginalPublicationMemberships
		slices.SortFunc(group.SchemaPublications, func(a, b archivalSchemaPublication) int {
			return cmp.Or(cmp.Compare(a.PublicationName, b.PublicationName), cmp.Compare(a.TableOID, b.TableOID))
		})
	}
	return nil
}

func planArchivalIncomingDependencyDeletes(
	preflight sourceSafetyPreflightResult,
	plan *archivalIsolationPlan,
) error {
	for _, dependency := range preflight.IncomingDependencies {
		if dependency.ManagedScope != sourceSafetyManagedScopeManaged ||
			dependency.TargetIntent != sourceSafetyTargetIntentExplicitlyAbsent ||
			dependency.Disposition != sourceSafetyDependencyDispositionNormalDeletion {
			return fmt.Errorf("incoming %s %s is not represented by a managed ordinary deletion",
				dependency.Dependent.Kind, sourceSafetyDependentDescription(dependency.Dependent))
		}
		switch dependency.Dependent.Kind {
		case sourceSafetyIncomingDependencyKindView,
			sourceSafetyIncomingDependencyKindMaterializedView,
			sourceSafetyIncomingDependencyKindRoutine,
			sourceSafetyIncomingDependencyKindRowTypeConsumer:
			plan.IncomingDependencyDeletes = append(plan.IncomingDependencyDeletes, dependency)
		default:
			return fmt.Errorf("incoming %s %s has no supported ordinary deletion vertex",
				dependency.Dependent.Kind, sourceSafetyDependentDescription(dependency.Dependent))
		}
	}
	return nil
}

func validateArchivalIsolationMarkers(
	groups []preparedPlainTableArchivalGroup,
	plans []archivalIsolationGroupPlan,
) error {
	for _, group := range groups {
		groupPlan, ok := archivalIsolationGroupPlanByID(plans, group.id)
		if !ok {
			return fmt.Errorf("isolation plan is missing archival group %q", group.id)
		}
		marker := canonicalizeArchivalMarker(group.marker)
		if group.allocation != nil {
			if !reflect.DeepEqual(marker.OriginalACLs, groupPlan.OriginalACLs) {
				return fmt.Errorf("finalized marker for group %q has ACL records that do not match Stage 6 revoke metadata", group.id)
			}
			if !reflect.DeepEqual(marker.OriginalForeignKeys, groupPlan.OriginalForeignKeys) {
				return fmt.Errorf("finalized marker for group %q has foreign-key records that do not match Stage 10 boundary metadata", group.id)
			}
			if !reflect.DeepEqual(marker.OriginalPublicationMemberships, groupPlan.OriginalPublications) {
				return fmt.Errorf("finalized marker for group %q has publication records that do not match Stage 10 metadata", group.id)
			}
		} else {
			if !markerACLSubset(groupPlan.OriginalACLs, marker.OriginalACLs) ||
				!markerForeignKeySubset(groupPlan.OriginalForeignKeys, marker.OriginalForeignKeys) ||
				!markerPublicationSubset(groupPlan.OriginalPublications,
					marker.OriginalPublicationMemberships) {
				return fmt.Errorf("resumed marker for group %q does not contain all still-present isolation metadata", group.id)
			}
			groupPlan.OriginalACLs = marker.OriginalACLs
			groupPlan.OriginalForeignKeys = marker.OriginalForeignKeys
			groupPlan.OriginalPublications = marker.OriginalPublicationMemberships
		}
		if !reflect.DeepEqual(marker.ExclusiveDependencyObjects, groupPlan.Dependencies) {
			return fmt.Errorf("finalized marker for group %q has dependency objects that do not match Stage 11 assignments", group.id)
		}
		expectedEdges := groupPlan.SharedDependencyEdges
		if !slices.Equal(marker.SharedCleanupComponentGroupEdges, expectedEdges) {
			return fmt.Errorf("finalized marker for group %q has shared dependency edges that do not match Stage 11 assignments", group.id)
		}
		if !reflect.DeepEqual(marker.Members[0].ExplicitlyMovedObjects, groupPlan.Statistics) {
			return fmt.Errorf("finalized marker for group %q has extended-statistics metadata that does not match the table inventory", group.id)
		}
		if group.resume != nil {
			for planIdx := range plans {
				if plans[planIdx].GroupID == group.id {
					plans[planIdx].OriginalACLs = marker.OriginalACLs
					plans[planIdx].OriginalForeignKeys = marker.OriginalForeignKeys
					plans[planIdx].OriginalPublications = marker.OriginalPublicationMemberships
				}
			}
		}
	}
	return nil
}

func renderArchivalACLRevoke(operation archivalACLRevokeOperation) Statement {
	grant := operation.Revoke.Grant
	var privilege strings.Builder
	if operation.Revoke.Kind == schema.CatalogACLRevokeKindGrantOption {
		privilege.WriteString("GRANT OPTION FOR ")
	}
	privilege.WriteString(grant.Privilege)
	if operation.Record.ColumnName != "" {
		fmt.Fprintf(&privilege, " (%s)", schema.EscapeIdentifier(operation.Record.ColumnName))
	}
	objectClass := "TABLE"
	if grant.ObjectClass == schema.CatalogACLObjectClassSequence {
		objectClass = "SEQUENCE"
	}
	grantee := schema.EscapeIdentifier(grant.GranteeName)
	if grant.GranteeIsPublic {
		grantee = schema.CatalogPublicRoleName
	}
	revoke := fmt.Sprintf("REVOKE %s ON %s %s.%s FROM %s RESTRICT",
		privilege.String(), objectClass,
		schema.EscapeIdentifier(operation.Destination.SchemaName),
		schema.EscapeIdentifier(operation.Destination.Name),
		grantee)
	escapedGrantor := schema.EscapeIdentifier(grant.GrantorName)
	escapedSchema := schema.EscapeIdentifier(operation.Destination.SchemaName)
	ddl := fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s;\nSET LOCAL ROLE %s;\n%s;\nRESET ROLE;\n"+
		"REVOKE USAGE ON SCHEMA %s FROM %s RESTRICT",
		escapedSchema, escapedGrantor, escapedGrantor, revoke, escapedSchema, escapedGrantor)
	return Statement{
		DDL: ddl, Hazards: []MigrationHazard{migrationHazardPrivilegeRevoked}, SkipValidation: true,
	}
}

func renderArchivalBoundaryForeignKeyDrop(operation archivalBoundaryForeignKeyOperation) Statement {
	return Statement{
		DDL: fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s",
			schema.EscapeIdentifier(operation.ForeignKey.OwningSchemaName),
			schema.EscapeIdentifier(operation.ForeignKey.OwningRelationName),
			schema.EscapeIdentifier(operation.ForeignKey.Name)),
		Hazards: slices.Clone(migrationHazardBoundaryForeignKeyDrop),
	}
}

func renderArchivalPublicationRemoval(operation archivalPublicationRemovalOperation) Statement {
	return Statement{
		DDL: fmt.Sprintf("ALTER PUBLICATION %s DROP TABLE %s.%s",
			schema.EscapeIdentifier(operation.Membership.PublicationName),
			schema.EscapeIdentifier(operation.Membership.RelationSchemaName),
			schema.EscapeIdentifier(operation.Membership.RelationName)),
		Hazards: []MigrationHazard{migrationHazardArchivalPublicationUpdate},
	}
}

func renderArchivalStatisticMove(operation archivalObjectMoveOperation) Statement {
	return Statement{
		DDL: fmt.Sprintf("ALTER STATISTICS %s.%s SET SCHEMA %s",
			schema.EscapeIdentifier(operation.Source.SchemaName),
			schema.EscapeIdentifier(operation.Source.Name),
			schema.EscapeIdentifier(operation.Destination.SchemaName)),
		Hazards: []MigrationHazard{migrationHazardArchivalNamespaceUpdate},
	}
}

func renderArchivalDependencyMove(
	inventory schema.CatalogInventory,
	operation archivalObjectMoveOperation,
) (Statement, error) {
	prefix := ""
	switch operation.Source.Kind {
	case archivalMarkerObjectKindSequence:
		prefix = fmt.Sprintf("ALTER SEQUENCE %s.%s",
			schema.EscapeIdentifier(operation.Source.SchemaName),
			schema.EscapeIdentifier(operation.Source.Name))
	case archivalMarkerObjectKindFunction:
		routine, count := catalogRoutineByOIDForClosure(inventory, operation.Source.OID)
		if count != 1 || routine.Kind != "f" {
			return Statement{}, fmt.Errorf("dependency function OID %d is not one supported function", operation.Source.OID)
		}
		arguments := markerIdentityArgument(operation.Source)
		prefix = fmt.Sprintf("ALTER FUNCTION %s.%s(%s)",
			schema.EscapeIdentifier(operation.Source.SchemaName),
			schema.EscapeIdentifier(operation.Source.Name), arguments)
	case archivalMarkerObjectKindType:
		prefix = fmt.Sprintf("ALTER TYPE %s.%s",
			schema.EscapeIdentifier(operation.Source.SchemaName),
			schema.EscapeIdentifier(operation.Source.Name))
	case archivalMarkerObjectKindCollation:
		prefix = fmt.Sprintf("ALTER COLLATION %s.%s",
			schema.EscapeIdentifier(operation.Source.SchemaName),
			schema.EscapeIdentifier(operation.Source.Name))
	case archivalMarkerObjectKindOperator:
		operator, count := catalogOperatorByOIDForClosure(inventory, operation.Source.OID)
		if count != 1 {
			return Statement{}, fmt.Errorf("dependency operator OID %d is not unique", operation.Source.OID)
		}
		leftType := operator.LeftType
		if operator.LeftTypeOID == 0 {
			leftType = "NONE"
		}
		rightType := operator.RightType
		if operator.RightTypeOID == 0 {
			rightType = "NONE"
		}
		prefix = fmt.Sprintf("ALTER OPERATOR %s.%s (%s, %s)",
			schema.EscapeIdentifier(operation.Source.SchemaName), operation.Source.Name,
			leftType, rightType)
	default:
		return Statement{}, fmt.Errorf("unsupported dependency move kind %q", operation.Source.Kind)
	}
	return Statement{
		DDL:     prefix + " SET SCHEMA " + schema.EscapeIdentifier(operation.Destination.SchemaName),
		Hazards: []MigrationHazard{migrationHazardArchivalNamespaceUpdate, migrationHazardArchivalDependencyLock},
	}, nil
}

func renderArchivalIncomingForeignKeyAdd(operation archivalIncomingForeignKeyOperation) []Statement {
	constraint := operation.Constraint
	add := Statement{
		DDL: fmt.Sprintf("%s %s", addConstraintPrefix(constraint.OwningTable, constraint.EscapedName),
			constraint.ConstraintDef),
		Hazards: []MigrationHazard{migrationHazardIncomingForeignKeyAdd},
	}
	if !constraint.IsValid {
		return []Statement{add}
	}
	add.DDL += " NOT VALID"
	return []Statement{add, validateConstraintStatement(constraint.OwningTable, constraint.EscapedName)}
}

func archivalIsolationVertices(
	inventory schema.CatalogInventory,
	plan archivalIsolationPlan,
) ([]sqlVertex, error) {
	var vertices []sqlVertex
	for _, operation := range plan.BoundaryForeignKeyDrops {
		vertices = append(vertices, sqlVertex{
			id: archivalIsolationVertexID{
				phase: 3, kind: archivalIsolationVertexKindBoundaryForeignKeyDrop,
				key: archivalMarkerForeignKeyKey(operation.Record),
			},
			statements: []Statement{renderArchivalBoundaryForeignKeyDrop(operation)},
		})
	}
	for _, operation := range plan.PublicationRemovals {
		vertices = append(vertices, sqlVertex{
			id: archivalIsolationVertexID{
				phase: 3, kind: archivalIsolationVertexKindPublicationRemoval,
				key: operation.Record.PublicationName + ":" +
					markerObjectIdentityKey(operation.Record.Table),
			},
			statements: []Statement{renderArchivalPublicationRemoval(operation)},
		})
	}
	for idx, operation := range plan.ACLRevokes {
		vertices = append(vertices, sqlVertex{
			id: archivalIsolationVertexID{
				phase: 5, kind: archivalIsolationVertexKindACLRevoke,
				key: fmt.Sprintf("%06d:%s", idx, markerACLKey(operation.Record)),
			},
			statements: []Statement{renderArchivalACLRevoke(operation)},
		})
	}
	for _, operation := range plan.StatisticMoves {
		vertices = append(vertices, sqlVertex{
			id: archivalIsolationVertexID{
				phase: 5, kind: archivalIsolationVertexKindStatisticMove,
				key: markerObjectIdentityKey(operation.Source),
			},
			statements: []Statement{renderArchivalStatisticMove(operation)},
		})
	}
	for _, operation := range plan.DependencyMoves {
		statement, err := renderArchivalDependencyMove(inventory, operation)
		if err != nil {
			return nil, err
		}
		vertices = append(vertices, sqlVertex{
			id: archivalIsolationVertexID{
				phase: 5, kind: archivalIsolationVertexKindDependencyMove,
				key: markerObjectIdentityKey(operation.Source),
			},
			statements: []Statement{statement},
		})
	}
	for _, operation := range plan.IncomingForeignKeyReadds {
		vertices = append(vertices, sqlVertex{
			id: archivalIsolationVertexID{
				phase: 7, kind: archivalIsolationVertexKindIncomingForeignKeyAdd,
				key: archivalMarkerForeignKeyKey(operation.Record),
			},
			statements: renderArchivalIncomingForeignKeyAdd(operation),
		})
	}
	return vertices, nil
}

func appendArchivalIsolationAssertions(
	body *strings.Builder,
	group preparedPlainTableArchivalGroup,
	plan archivalIsolationGroupPlan,
) {
	for _, schemaName := range archivedMarkerSchemaNames(group.marker) {
		appendPlainTableAssertion(body, fmt.Sprintf(
			"NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace AS n CROSS JOIN LATERAL "+
				"pg_catalog.aclexplode(NULLIF(COALESCE(n.nspacl, pg_catalog.acldefault('n', n.nspowner)), "+
				"'{}'::aclitem[])) AS acl "+
				"WHERE n.nspname = %s AND acl.grantee <> n.nspowner)", schema.EscapeLiteral(schemaName),
		),
			fmt.Sprintf("archival schema ACL isolation mismatch for group %s schema %s", group.id, schemaName))
	}
	for _, record := range plan.OriginalACLs {
		appendPlainTableAssertion(body, "NOT ("+archivalACLRecordExistsPredicate(record)+")",
			fmt.Sprintf("archival ACL isolation mismatch for group %s object %s", group.id,
				markerObjectDisplayName(record.Object)))
	}
	for _, foreignKey := range plan.OriginalForeignKeys {
		appendPlainTableAssertion(body, fmt.Sprintf(
			"NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = %d "+
				"AND c.confrelid = %d AND c.conname = %s)",
			foreignKey.OwningTable.OID, foreignKey.ReferencedTable.OID,
			schema.EscapeLiteral(foreignKey.Name),
		),
			fmt.Sprintf("archival boundary foreign key still exists for group %s: %s", group.id, foreignKey.Name))
	}
	for _, publication := range slices.Concat(plan.OriginalPublications,
		publicationRecordsForSchemaAssertions(plan)) {
		appendPlainTableAssertion(body, fmt.Sprintf(
			"NOT EXISTS (SELECT 1 FROM pg_catalog.pg_publication AS p WHERE p.pubname = %s AND ("+
				"EXISTS (SELECT 1 FROM pg_catalog.pg_publication_rel AS pr WHERE pr.prpubid = p.oid "+
				"AND pr.prrelid = %d) OR EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c "+
				"JOIN pg_catalog.pg_publication_namespace AS publication_namespace "+
				"ON publication_namespace.pnnspid = c.relnamespace "+
				"WHERE publication_namespace.pnpubid = p.oid AND c.oid = %d)))",
			schema.EscapeLiteral(publication.PublicationName), publication.Table.OID, publication.Table.OID,
		),
			fmt.Sprintf("archival publication isolation mismatch for group %s publication %s",
				group.id, publication.PublicationName))
	}
}

func archivalACLRecordExistsPredicate(record archivalMarkerACLRecordV1) string {
	grantor := schema.EscapeLiteral(record.GrantorName)
	granteeOID := "acl.grantee = 0"
	if !record.GranteeIsPublic {
		granteeOID = "pg_catalog.pg_get_userbyid(acl.grantee) = " +
			schema.EscapeLiteral(record.GranteeName)
	}
	common := fmt.Sprintf("acl.privilege_type = %s AND acl.is_grantable = %t AND "+
		"pg_catalog.pg_get_userbyid(acl.grantor) = %s AND %s",
		schema.EscapeLiteral(record.Privilege), record.IsGrantable, grantor, granteeOID)
	if record.ColumnName != "" {
		return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_attribute AS a "+
			"CROSS JOIN LATERAL pg_catalog.aclexplode(NULLIF(COALESCE(a.attacl, '{}'::aclitem[]), "+
			"'{}'::aclitem[])) AS acl "+
			"WHERE a.attrelid = %d AND a.attname = %s AND %s)", record.Object.OID,
			schema.EscapeLiteral(record.ColumnName), common)
	}
	defaultKind := "r"
	if record.ObjectClass == string(schema.CatalogACLObjectClassSequence) {
		defaultKind = "S"
	}
	return fmt.Sprintf("EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c "+
		"CROSS JOIN LATERAL pg_catalog.aclexplode(NULLIF(COALESCE(c.relacl, "+
		"pg_catalog.acldefault(%s, c.relowner)), '{}'::aclitem[])) AS acl "+
		"WHERE c.oid = %d AND %s)", schema.EscapeLiteral(defaultKind), record.Object.OID, common)
}

func publicationRecordsForSchemaAssertions(plan archivalIsolationGroupPlan) []archivalMarkerPublicationMembershipV1 {
	var result []archivalMarkerPublicationMembershipV1
	for _, publication := range plan.SchemaPublications {
		result = append(result, archivalMarkerPublicationMembershipV1{
			PublicationName: publication.PublicationName,
			Table: archivalMarkerObjectIdentity{
				Kind: archivalMarkerObjectKindTable, OID: publication.TableOID,
				SchemaName: "schema-publication", Name: "schema-publication",
			},
		})
	}
	return result
}

func appendPlainTablePreservedForeignKeyAssertions(
	body *strings.Builder,
	groupID archivalGroupID,
	plan archivalIsolationPlan,
) {
	for _, preserved := range plan.PreservedForeignKeys {
		if preserved.GroupID != groupID {
			continue
		}
		foreignKey := preserved.ForeignKey
		appendPlainTableAssertion(body, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM pg_catalog.pg_constraint AS c WHERE c.oid = %d AND c.conrelid = %d "+
				"AND c.confrelid = %d AND c.conname = %s)", foreignKey.OID, foreignKey.OwningRelationOID,
			foreignKey.ReferencedRelationOID, schema.EscapeLiteral(foreignKey.Name),
		),
			fmt.Sprintf("preserved archival foreign key mismatch for group %s: %s", groupID, foreignKey.Name))
	}
}

func validateArchivalDependencyMoveIdentity(
	inventory schema.CatalogInventory,
	assignment archivedDependencyAssignment,
) error {
	actual, count := currentCatalogObjectsWithOID(inventory, assignment.Source.Kind, assignment.Source.OID)
	if count != 1 {
		return fmt.Errorf("dependency assignment source %s OID %d has %d catalog identities",
			assignment.Source.Kind, assignment.Source.OID, count)
	}
	if compareMarkerObjects(actual, assignment.Source) != 0 &&
		compareMarkerObjects(actual, assignment.Destination) != 0 {
		return fmt.Errorf("dependency assignment source OID %d does not match its current catalog identity",
			assignment.Source.OID)
	}
	switch assignment.Source.Kind {
	case archivalMarkerObjectKindSequence, archivalMarkerObjectKindFunction,
		archivalMarkerObjectKindType, archivalMarkerObjectKindCollation,
		archivalMarkerObjectKindOperator:
		return nil
	default:
		return fmt.Errorf("unsupported Stage 14 dependency assignment kind %q", assignment.Source.Kind)
	}
}

func archivalMarkerACLRecord(
	inventory schema.CatalogInventory,
	grant schema.CatalogACLGrant,
) (archivalMarkerACLRecordV1, error) {
	if err := validateCatalogACLRoleNames(inventory, grant); err != nil {
		return archivalMarkerACLRecordV1{}, err
	}
	kind := archivalMarkerObjectKindTable
	if grant.ObjectClass == schema.CatalogACLObjectClassSequence {
		kind = archivalMarkerObjectKindSequence
	} else if grant.ObjectClass != schema.CatalogACLObjectClassTable {
		return archivalMarkerACLRecordV1{}, fmt.Errorf(
			"unsupported archival ACL object class %q", grant.ObjectClass,
		)
	}
	object, count := currentCatalogObjectsWithOID(inventory, kind, grant.Object.ObjectOID)
	if kind == archivalMarkerObjectKindSequence && count == 0 {
		for _, sequence := range inventory.Sequences {
			if sequence.OID == grant.Object.ObjectOID {
				object = archivalMarkerObjectIdentity{
					Kind: kind, OID: sequence.OID,
					SchemaName: sequence.SchemaName, Name: sequence.Name,
				}
				count++
			}
		}
	}
	if count != 1 {
		return archivalMarkerACLRecordV1{}, fmt.Errorf(
			"ACL object OID %d has %d catalog identities",
			grant.Object.ObjectOID, count,
		)
	}
	columnName := ""
	if grant.Object.SubObjectID > 0 {
		for _, column := range inventory.Columns {
			if column.RelationOID == grant.Object.ObjectOID &&
				int32(column.Number) == grant.Object.SubObjectID &&
				!column.IsDropped {
				if columnName != "" {
					return archivalMarkerACLRecordV1{}, fmt.Errorf(
						"ACL column %d on relation OID %d is ambiguous",
						grant.Object.SubObjectID, grant.Object.ObjectOID,
					)
				}
				columnName = column.Name
			}
		}
		if columnName == "" {
			return archivalMarkerACLRecordV1{}, fmt.Errorf(
				"ACL column %d on relation OID %d is missing",
				grant.Object.SubObjectID, grant.Object.ObjectOID,
			)
		}
	}
	return archivalMarkerACLRecordV1{
		ObjectClass: string(grant.ObjectClass), Object: object, ColumnName: columnName,
		OwnerName: grant.OwnerName, GrantorName: grant.GrantorName, GranteeName: grant.GranteeName,
		GranteeIsPublic: grant.GranteeIsPublic, Privilege: grant.Privilege, IsGrantable: grant.IsGrantable,
	}, nil
}

func validateCatalogACLRoleNames(inventory schema.CatalogInventory, grant schema.CatalogACLGrant) error {
	roles := make(map[uint32]string, len(inventory.Roles))
	for _, role := range inventory.Roles {
		roles[role.OID] = role.Name
	}
	for _, identity := range []struct {
		oid  uint32
		name string
		kind string
	}{
		{grant.OwnerOID, grant.OwnerName, "owner"},
		{grant.GrantorOID, grant.GrantorName, "grantor"},
	} {
		if roles[identity.oid] != identity.name {
			return fmt.Errorf("ACL %s role OID %d name %q does not match role inventory",
				identity.kind, identity.oid, identity.name)
		}
	}
	if !grant.GranteeIsPublic && roles[grant.GranteeOID] != grant.GranteeName {
		return fmt.Errorf("ACL grantee role OID %d name %q does not match role inventory",
			grant.GranteeOID, grant.GranteeName)
	}
	return nil
}

func archivalMarkerForeignKey(foreignKey schema.CatalogForeignKey) archivalMarkerForeignKeyV1 {
	columns := make([]archivalMarkerForeignKeyColumnV1, 0, len(foreignKey.Columns))
	for _, column := range foreignKey.Columns {
		columns = append(columns, archivalMarkerForeignKeyColumnV1{
			OwningColumnName: column.OwningName, ReferencedColumnName: column.ReferencedName,
		})
	}
	return archivalMarkerForeignKeyV1{
		Name: foreignKey.Name,
		OwningTable: archivalMarkerObjectIdentity{
			Kind: archivalMarkerObjectKindTable,
			OID:  foreignKey.OwningRelationOID, SchemaName: foreignKey.OwningSchemaName,
			Name: foreignKey.OwningRelationName,
		},
		ReferencedTable: archivalMarkerObjectIdentity{
			Kind: archivalMarkerObjectKindTable,
			OID:  foreignKey.ReferencedRelationOID, SchemaName: foreignKey.ReferencedSchemaName,
			Name: foreignKey.ReferencedRelationName,
		},
		Columns: columns, MatchType: foreignKey.MatchType, UpdateAction: foreignKey.UpdateAction,
		DeleteAction: foreignKey.DeleteAction, IsDeferrable: foreignKey.IsDeferrable,
		IsInitiallyDeferred: foreignKey.IsDeferred, IsValidated: foreignKey.IsValidated,
		Definition: foreignKey.Definition,
	}
}

func archivalMarkerPublicationMembership(
	membership schema.CatalogPublicationRelation,
) archivalMarkerPublicationMembershipV1 {
	return archivalMarkerPublicationMembershipV1{
		PublicationName: membership.PublicationName,
		Table: archivalMarkerObjectIdentity{
			Kind: archivalMarkerObjectKindTable,
			OID:  membership.RelationOID, SchemaName: membership.RelationSchemaName, Name: membership.RelationName,
		},
		ColumnNames: slices.Clone(membership.ColumnNames), RowFilter: membership.RowFilter,
	}
}

func uniqueTargetForeignKey(
	target schema.Schema,
	record archivalMarkerForeignKeyV1,
) (schema.ForeignKeyConstraint, bool, error) {
	var matches []schema.ForeignKeyConstraint
	for _, candidate := range target.ForeignKeyConstraints {
		if candidate.OwningTable.SchemaName == record.OwningTable.SchemaName &&
			candidate.OwningTable.EscapedName == schema.EscapeIdentifier(record.OwningTable.Name) &&
			candidate.EscapedName == schema.EscapeIdentifier(record.Name) {
			matches = append(matches, candidate)
		}
	}
	if len(matches) > 1 {
		return schema.ForeignKeyConstraint{}, false, fmt.Errorf(
			"target foreign key %s.%s is ambiguous",
			record.OwningTable.SchemaName, record.Name,
		)
	}
	if len(matches) == 0 {
		return schema.ForeignKeyConstraint{}, false, nil
	}
	return matches[0], true, nil
}

func selectedACLObject(
	objects []archivalSelectedACLObject,
	oid uint32,
) (archivalSelectedACLObject, bool) {
	for _, object := range objects {
		if object.address.ObjectOID == oid {
			return object, true
		}
	}
	return archivalSelectedACLObject{}, false
}

func boundaryForeignKeyGroupIDs(
	owning preparedPlainTableArchivalGroup,
	owningArchived bool,
	referenced preparedPlainTableArchivalGroup,
	referencedArchived bool,
) []archivalGroupID {
	var result []archivalGroupID
	if owningArchived {
		result = append(result, owning.id)
	}
	if referencedArchived && (!owningArchived || owning.id != referenced.id) {
		result = append(result, referenced.id)
	}
	slices.Sort(result)
	return result
}

func groupDependencySchema(groups []preparedPlainTableArchivalGroup, groupID archivalGroupID) string {
	for _, group := range groups {
		if group.id == groupID {
			return group.marker.ExclusiveDependencySchemas[0].Name
		}
	}
	return ""
}

func archivalIsolationGroupPlanByID(
	plans []archivalIsolationGroupPlan,
	groupID archivalGroupID,
) (archivalIsolationGroupPlan, bool) {
	for _, plan := range plans {
		if plan.GroupID == groupID {
			return plan, true
		}
	}
	return archivalIsolationGroupPlan{}, false
}

func compareArchivalObjectMoves(a, b archivalObjectMoveOperation) int {
	return cmp.Or(cmp.Compare(a.GroupID, b.GroupID), compareMarkerObjects(a.Source, b.Source))
}

func archivalDependencyMoveRank(kind archivalMarkerObjectKind) int {
	switch kind {
	case archivalMarkerObjectKindOperator:
		return 1
	case archivalMarkerObjectKindFunction:
		return 2
	case archivalMarkerObjectKindSequence:
		return 3
	case archivalMarkerObjectKindCollation:
		return 4
	case archivalMarkerObjectKindType:
		return 5
	default:
		return 99
	}
}

func containsMarkerACL(records []archivalMarkerACLRecordV1, expected archivalMarkerACLRecordV1) bool {
	return slices.ContainsFunc(records, func(record archivalMarkerACLRecordV1) bool {
		return reflect.DeepEqual(record, expected)
	})
}

func markerACLSubset(subset, set []archivalMarkerACLRecordV1) bool {
	for _, record := range subset {
		if !containsMarkerACL(set, record) {
			return false
		}
	}
	return true
}

func markerForeignKeySubset(subset, set []archivalMarkerForeignKeyV1) bool {
	for _, record := range subset {
		if !slices.ContainsFunc(set, func(candidate archivalMarkerForeignKeyV1) bool {
			return reflect.DeepEqual(candidate, record)
		}) {
			return false
		}
	}
	return true
}

func markerPublicationSubset(subset, set []archivalMarkerPublicationMembershipV1) bool {
	for _, record := range subset {
		if !slices.ContainsFunc(set, func(candidate archivalMarkerPublicationMembershipV1) bool {
			return reflect.DeepEqual(candidate, record)
		}) {
			return false
		}
	}
	return true
}

func archivalMarkerForeignKeyKey(record archivalMarkerForeignKeyV1) string {
	return markerObjectIdentityKey(record.OwningTable) + ":" + record.Name
}

func markerACLKey(record archivalMarkerACLRecordV1) string {
	return strings.Join([]string{
		record.ObjectClass, markerObjectIdentityKey(record.Object), record.ColumnName,
		record.GrantorName, record.GranteeName, record.Privilege, fmt.Sprint(record.IsGrantable),
	}, ":")
}
