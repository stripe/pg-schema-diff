package diff

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

const (
	pgAuthIDCatalogOID    uint32 = 1260
	pgLanguageCatalogOID  uint32 = 2612
	pgNamespaceCatalogOID uint32 = 2615
	pgCastCatalogOID      uint32 = 2605
	pgOperatorCatalogOID  uint32 = 2617
	pgExtensionCatalogOID uint32 = 3079
	pgCollationCatalogOID uint32 = 3456
)

type archivedDependencyClassification string

const (
	archivedDependencyClassificationTargetCompatible   archivedDependencyClassification = "target_compatible"
	archivedDependencyClassificationExclusiveMovable   archivedDependencyClassification = "exclusive_movable"
	archivedDependencyClassificationSharedWithTarget   archivedDependencyClassification = "shared_with_managed_target"
	archivedDependencyClassificationSharedArchivedOnly archivedDependencyClassification = "shared_only_by_archived_groups"
	archivedDependencyClassificationUnsupported        archivedDependencyClassification = "unsupported_non_movable"
)

type archivedDependencyClosureGroupRequest struct {
	GroupID              archivalGroupID
	TableRelationOIDs    []uint32
	DependencySchemaName string
}

type archivedDependencyClosureRequest struct {
	CurrentSnapshot schema.SchemaSnapshot
	TargetSnapshot  schema.SchemaSnapshot

	ProposedGroups  []archivedDependencyClosureGroupRequest
	CandidateGroups []structurallyValidArchivedCandidateGroup
	SourcePreflight sourceSafetyPreflightResult
}

type archivedDependencyClosureResult struct {
	ValidatedGroupIDs                  []archivalGroupID
	Objects                            []archivedDependencyClosureObject
	Assignments                        []archivedDependencyAssignment
	SharedGroupEdges                   []archivalMarkerSharedGroupEdgeV1
	DependencyValidatedCandidateGroups []dependencyValidatedArchivedCandidateGroup
}

type archivedDependencyClosureObject struct {
	Kind               archivalMarkerObjectKind
	Address            schema.CatalogDependencyObject
	Identity           archivalMarkerObjectIdentity
	SemanticDefinition string
	Classification     archivedDependencyClassification
	GroupIDs           []archivalGroupID
	OwnerGroupID       archivalGroupID
	DestinationSchema  string
	Movable            bool
	ExtensionName      string
}

type archivedDependencyAssignment struct {
	GroupID     archivalGroupID
	Source      archivalMarkerObjectIdentity
	Destination archivalMarkerObjectIdentity
}

// dependencyValidatedArchivedCandidateGroup has passed only the local Stage 11
// dependency checks. Stage 16 must still validate its global component and digest.
type dependencyValidatedArchivedCandidateGroup struct {
	Candidate structurallyValidArchivedCandidateGroup
}

type preparedArchivedDependencyGroup struct {
	id                      archivalGroupID
	tableRelationOIDs       []uint32
	dependencySchemaName    string
	candidate               *structurallyValidArchivedCandidateGroup
	reachableDependencyKeys map[archivedDependencyAddressKey]struct{}
}

type archivedDependencyAddressKey struct {
	classOID  uint32
	objectOID uint32
}

type resolvedArchivedDependency struct {
	kind               archivalMarkerObjectKind
	address            schema.CatalogDependencyObject
	identity           archivalMarkerObjectIdentity
	identityKey        string
	semanticDefinition string
	movable            bool
	extensionName      string
}

type archivedDependencyTraversal struct {
	outgoing           map[archivedDependencyAddressKey][]schema.CatalogDependency
	internalDependents map[archivedDependencyAddressKey][]schema.CatalogDependencyObject
	supportFunctions   map[archivedDependencyAddressKey][]schema.CatalogDependencyObject
}

// planArchivedDependencyClosure is dormant until archival activation. Raw
// pg_depend direction is dependent -> referenced: traversal follows outgoing
// references and follows reverse edges only for owned/internal helper objects.
func planArchivedDependencyClosure(
	request archivedDependencyClosureRequest,
) (archivedDependencyClosureResult, error) {
	current := request.CurrentSnapshot
	target := request.TargetSnapshot
	current.Inventory = current.Inventory.Normalize()
	target.Inventory = target.Inventory.Normalize()

	groups, err := prepareArchivedDependencyGroups(request, current.Inventory)
	if err != nil {
		return archivedDependencyClosureResult{}, err
	}
	if len(groups) == 0 {
		return archivedDependencyClosureResult{}, nil
	}

	currentTraversal := buildArchivedDependencyTraversal(current.Inventory)
	removeArchivedBoundaryForeignKeyTraversal(&currentTraversal, request.SourcePreflight)
	targetTraversal := buildArchivedDependencyTraversal(target.Inventory)
	managedTargetAddresses := managedTargetDependencyAddresses(target, targetTraversal)
	resolvedByAddress := make(map[archivedDependencyAddressKey]resolvedArchivedDependency)
	ownersByAddress := make(map[archivedDependencyAddressKey][]archivalGroupID)

	for groupIdx := range groups {
		group := &groups[groupIdx]
		seeds, err := archivedDependencyGroupSeeds(*group, request.SourcePreflight)
		if err != nil {
			return archivedDependencyClosureResult{}, err
		}
		reachable, helpers := walkArchivedDependencyGraph(currentTraversal, seeds)
		group.reachableDependencyKeys = reachable
		keys := sortedArchivedDependencyAddressKeys(reachable)
		for _, key := range keys {
			address := currentTraversal.addressForKey(key)
			if isArchivedDependencyLocalAddress(key, seeds) ||
				(helpers[key] && isAutomaticallyMovedArchivedDependencyHelper(current.Inventory, key)) ||
				isIgnoredArchivedDependencyClass(key.classOID) {
				continue
			}
			resolved, supported, err := resolveArchivedDependency(current.Inventory, address)
			if err != nil {
				return archivedDependencyClosureResult{}, fmt.Errorf(
					"resolving dependency for archival group %q: %w", group.id, err,
				)
			}
			if !supported {
				if isPlatformArchivedDependency(address) {
					if err := validateCompatiblePlatformDependency(address, target.Inventory); err != nil {
						return archivedDependencyClosureResult{}, fmt.Errorf(
							"archival group %q has unsupported/non-movable dependency: %w", group.id, err,
						)
					}
					continue
				}
				return archivedDependencyClosureResult{}, fmt.Errorf(
					"archival group %q has unsupported/non-movable dependency %s",
					group.id, sourceSafetyCatalogObjectDescription(address),
				)
			}
			if existing, duplicate := resolvedByAddress[key]; duplicate {
				if existing.identityKey != resolved.identityKey ||
					existing.semanticDefinition != resolved.semanticDefinition {
					return archivedDependencyClosureResult{}, fmt.Errorf(
						"catalog address %d/%d resolves inconsistently", key.classOID, key.objectOID,
					)
				}
			} else {
				resolvedByAddress[key] = resolved
			}
			ownersByAddress[key] = append(ownersByAddress[key], group.id)
		}
	}

	allReachable := make(map[archivedDependencyAddressKey]struct{})
	for _, group := range groups {
		for key := range group.reachableDependencyKeys {
			allReachable[key] = struct{}{}
		}
	}
	groupByID := make(map[archivalGroupID]preparedArchivedDependencyGroup, len(groups))
	for _, group := range groups {
		groupByID[group.id] = group
	}

	result := archivedDependencyClosureResult{}
	for _, key := range sortedArchivedDependencyAddressKeysFromResolved(resolvedByAddress) {
		resolved := resolvedByAddress[key]
		owners := slices.Clone(ownersByAddress[key])
		slices.Sort(owners)
		owners = slices.Compact(owners)

		targetMatch, targetCompatible, targetManaged, err := matchArchivedDependencyInTarget(
			resolved, target, managedTargetAddresses,
		)
		if err != nil {
			return archivedDependencyClosureResult{}, err
		}
		object := archivedDependencyClosureObject{
			Kind: resolved.kind, Address: resolved.address, Identity: resolved.identity,
			SemanticDefinition: resolved.semanticDefinition, GroupIDs: owners,
			Movable: resolved.movable, ExtensionName: resolved.extensionName,
		}
		switch {
		case targetCompatible && targetManaged:
			object.Classification = archivedDependencyClassificationSharedWithTarget
		case targetCompatible:
			object.Classification = archivedDependencyClassificationTargetCompatible
		default:
			if !resolved.movable {
				change := "removed from the target"
				if targetMatch {
					change = "replaced incompatibly in the target"
				}
				return archivedDependencyClosureResult{}, fmt.Errorf(
					"unsupported/non-movable dependency %s is %s",
					archivedDependencyDescription(resolved), change,
				)
			}
			if external := firstExternalArchivedDependencyConsumer(
				current.Inventory, resolved.address, allReachable,
			); external != nil {
				return archivedDependencyClosureResult{}, fmt.Errorf(
					"dependency %s cannot move because it is also used by %s",
					archivedDependencyDescription(resolved),
					sourceSafetyCatalogObjectDescription(*external),
				)
			}
			owner := owners[0]
			destination := groupByID[owner].dependencySchemaName
			if destination == "" {
				return archivedDependencyClosureResult{}, fmt.Errorf(
					"archival group %q needs a dependency schema for %s", owner,
					archivedDependencyDescription(resolved),
				)
			}
			object.OwnerGroupID = owner
			object.DestinationSchema = destination
			if len(owners) == 1 {
				object.Classification = archivedDependencyClassificationExclusiveMovable
			} else {
				object.Classification = archivedDependencyClassificationSharedArchivedOnly
				result.SharedGroupEdges = append(
					result.SharedGroupEdges, completeArchivedDependencyGroupEdges(owners)...,
				)
			}
			destinationIdentity := cloneMarkerObject(resolved.identity)
			destinationIdentity.SchemaName = destination
			result.Assignments = append(result.Assignments, archivedDependencyAssignment{
				GroupID: owner, Source: cloneMarkerObject(resolved.identity), Destination: destinationIdentity,
			})
		}
		result.Objects = append(result.Objects, object)
	}

	result.SharedGroupEdges = canonicalArchivedDependencyGroupEdges(result.SharedGroupEdges)
	slices.SortFunc(result.Assignments, compareArchivedDependencyAssignments)
	if err := validateArchivedDependencyDestinationCollisions(result.Assignments); err != nil {
		return archivedDependencyClosureResult{}, err
	}
	validated, err := validateCandidateArchivedDependencies(groups, result)
	if err != nil {
		return archivedDependencyClosureResult{}, err
	}
	result.DependencyValidatedCandidateGroups = validated
	for _, group := range groups {
		result.ValidatedGroupIDs = append(result.ValidatedGroupIDs, group.id)
	}
	return result, nil
}

func removeArchivedBoundaryForeignKeyTraversal(
	traversal *archivedDependencyTraversal,
	preflight sourceSafetyPreflightResult,
) {
	excluded := make(map[archivedDependencyAddressKey]struct{})
	for _, foreignKey := range preflight.ForeignKeys {
		if foreignKey.Direction == sourceSafetyForeignKeyDirectionSelf {
			continue
		}
		excluded[archivedDependencyAddressKey{
			classOID: pgConstraintCatalogOID, objectOID: foreignKey.ForeignKey.OID,
		}] = struct{}{}
		for _, triggerOID := range foreignKey.TriggerOIDs {
			excluded[archivedDependencyAddressKey{classOID: pgTriggerCatalogOID, objectOID: triggerOID}] = struct{}{}
		}
	}
	for key := range excluded {
		delete(traversal.outgoing, key)
		delete(traversal.internalDependents, key)
	}
	for key, dependents := range traversal.internalDependents {
		traversal.internalDependents[key] = slices.DeleteFunc(dependents,
			func(dependent schema.CatalogDependencyObject) bool {
				_, skip := excluded[archivedDependencyKey(dependent)]
				return skip
			})
	}
}

func isAutomaticallyMovedArchivedDependencyHelper(
	inventory schema.CatalogInventory,
	key archivedDependencyAddressKey,
) bool {
	switch key.classOID {
	case pgProcCatalogOID:
		// Range constructors and other generated routines do not follow ALTER TYPE.
		return false
	case pgAttrDefCatalogOID, pgConstraintCatalogOID, pgCastCatalogOID:
		return true
	case pgClassCatalogOID:
		for _, relation := range inventory.Relations {
			if relation.OID == key.objectOID && relation.Kind == schema.RelKindCompositeType {
				return true
			}
		}
		return false
	case pgTypeCatalogOID:
		catalogType, count := catalogTypeByOIDForClosure(inventory, key.objectOID)
		return count == 1 && (catalogType.Kind == schema.CatalogTypeKindArray ||
			catalogType.Kind == schema.CatalogTypeKindRow)
	default:
		return false
	}
}

func prepareArchivedDependencyGroups(
	request archivedDependencyClosureRequest,
	inventory schema.CatalogInventory,
) ([]preparedArchivedDependencyGroup, error) {
	groups := make([]preparedArchivedDependencyGroup, 0,
		len(request.ProposedGroups)+len(request.CandidateGroups))
	seenGroups := make(map[archivalGroupID]struct{})
	seenTables := make(map[uint32]archivalGroupID)
	add := func(group preparedArchivedDependencyGroup) error {
		if group.id == "" {
			return fmt.Errorf("archival dependency group ID must not be empty")
		}
		if _, duplicate := seenGroups[group.id]; duplicate {
			return fmt.Errorf("archival dependency group %q is duplicated", group.id)
		}
		seenGroups[group.id] = struct{}{}
		group.tableRelationOIDs = slices.Clone(group.tableRelationOIDs)
		slices.Sort(group.tableRelationOIDs)
		group.tableRelationOIDs = slices.Compact(group.tableRelationOIDs)
		if len(group.tableRelationOIDs) == 0 {
			return fmt.Errorf("archival dependency group %q has no table relation OIDs", group.id)
		}
		for _, oid := range group.tableRelationOIDs {
			relation := catalogRelationWithOID(inventory, oid)
			if relation == nil {
				return fmt.Errorf("archival dependency group %q references missing table relation OID %d",
					group.id, oid)
			}
			if previous, duplicate := seenTables[oid]; duplicate {
				return fmt.Errorf("table relation OID %d belongs to archival dependency groups %q and %q",
					oid, previous, group.id)
			}
			seenTables[oid] = group.id
		}
		groups = append(groups, group)
		return nil
	}

	for _, proposed := range request.ProposedGroups {
		if proposed.DependencySchemaName == "" {
			return nil, fmt.Errorf("proposed archival dependency group %q has no dependency schema",
				proposed.GroupID)
		}
		if err := add(preparedArchivedDependencyGroup{
			id: proposed.GroupID, tableRelationOIDs: proposed.TableRelationOIDs,
			dependencySchemaName: proposed.DependencySchemaName,
		}); err != nil {
			return nil, err
		}
	}
	for idx := range request.CandidateGroups {
		candidate := request.CandidateGroups[idx]
		if candidate.ExpectedDependencySchemaName == "" {
			return nil, fmt.Errorf(
				"candidate archival group %q has no independently allocated dependency schema",
				candidate.GroupID,
			)
		}
		if len(candidate.Marker.ExclusiveDependencySchemas) != 1 {
			return nil, fmt.Errorf(
				"candidate archival group %q declares %d dependency schemas; exactly one is required",
				candidate.GroupID, len(candidate.Marker.ExclusiveDependencySchemas),
			)
		}
		if candidate.Marker.ExclusiveDependencySchemas[0].Name !=
			candidate.ExpectedDependencySchemaName {
			return nil, fmt.Errorf(
				"candidate archival group %q dependency schema claim %q does not match allocated schema %q",
				candidate.GroupID, candidate.Marker.ExclusiveDependencySchemas[0].Name,
				candidate.ExpectedDependencySchemaName,
			)
		}
		tableOIDs := make([]uint32, 0, len(candidate.Marker.Members))
		for _, member := range candidate.Marker.Members {
			tableOIDs = append(tableOIDs, member.SourceTable.OID)
		}
		if err := add(preparedArchivedDependencyGroup{
			id: candidate.GroupID, tableRelationOIDs: tableOIDs,
			dependencySchemaName: candidate.ExpectedDependencySchemaName,
			candidate:            &candidate,
		}); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(groups, func(a, b preparedArchivedDependencyGroup) int {
		return cmp.Compare(a.id, b.id)
	})
	return groups, nil
}

func archivedDependencyGroupSeeds(
	group preparedArchivedDependencyGroup,
	preflight sourceSafetyPreflightResult,
) ([]schema.CatalogDependencyObject, error) {
	excluded := make(map[archivedDependencyAddressKey]struct{})
	for _, foreignKey := range preflight.ForeignKeys {
		if foreignKey.Direction == sourceSafetyForeignKeyDirectionSelf {
			continue
		}
		excluded[archivedDependencyAddressKey{
			classOID: pgConstraintCatalogOID, objectOID: foreignKey.ForeignKey.OID,
		}] = struct{}{}
		for _, triggerOID := range foreignKey.TriggerOIDs {
			excluded[archivedDependencyAddressKey{
				classOID: pgTriggerCatalogOID, objectOID: triggerOID,
			}] = struct{}{}
		}
	}
	byTable := make(map[uint32][]schema.CatalogDependencyObject)
	for _, retained := range preflight.ExpectedRetainedObjects {
		if _, skip := excluded[archivedDependencyKey(retained.Address)]; skip {
			continue
		}
		byTable[retained.TableRelationOID] = append(byTable[retained.TableRelationOID], retained.Address)
	}
	var seeds []schema.CatalogDependencyObject
	for _, tableOID := range group.tableRelationOIDs {
		retained := byTable[tableOID]
		foundTable := false
		for _, address := range retained {
			if address.ClassOID == pgClassCatalogOID && address.ObjectOID == tableOID {
				foundTable = true
			}
			seeds = append(seeds, address)
		}
		if !foundTable {
			return nil, fmt.Errorf(
				"archival dependency group %q table relation OID %d was not validated by Stage 10 preflight",
				group.id, tableOID,
			)
		}
	}
	slices.SortFunc(seeds, compareArchivedDependencyAddresses)
	return slices.CompactFunc(seeds, sameArchivedDependencyAddress), nil
}

func buildArchivedDependencyTraversal(inventory schema.CatalogInventory) archivedDependencyTraversal {
	result := archivedDependencyTraversal{
		outgoing:           make(map[archivedDependencyAddressKey][]schema.CatalogDependency),
		internalDependents: make(map[archivedDependencyAddressKey][]schema.CatalogDependencyObject),
		supportFunctions:   make(map[archivedDependencyAddressKey][]schema.CatalogDependencyObject),
	}
	for _, dependency := range inventory.Dependencies {
		dependentKey := archivedDependencyKey(dependency.Dependent)
		result.outgoing[dependentKey] = append(result.outgoing[dependentKey], dependency)
		if (dependency.Type == "i" || dependency.Type == "a") &&
			isArchivedDependencyInternalHelper(inventory, dependency.Dependent) {
			referencedKey := archivedDependencyKey(dependency.Referenced)
			result.internalDependents[referencedKey] = append(
				result.internalDependents[referencedKey], dependency.Dependent,
			)
		}
	}
	for key := range result.outgoing {
		slices.SortFunc(result.outgoing[key], compareSourceSafetyCatalogDependencies)
	}
	for key := range result.internalDependents {
		slices.SortFunc(result.internalDependents[key], compareArchivedDependencyAddresses)
	}
	for _, support := range inventory.TypeSupportFunctions {
		key := archivedDependencyAddressKey{classOID: pgTypeCatalogOID, objectOID: support.TypeOID}
		result.supportFunctions[key] = append(result.supportFunctions[key], schema.CatalogDependencyObject{
			ClassOID: pgProcCatalogOID, ObjectOID: support.FunctionOID, ObjectType: "function",
			SchemaName: support.FunctionSchemaName, Name: support.FunctionName,
			Identity: archivedRoutineIdentity(support.FunctionSchemaName, support.FunctionName,
				support.FunctionIdentityArguments),
		})
	}
	for key := range result.supportFunctions {
		slices.SortFunc(result.supportFunctions[key], compareArchivedDependencyAddresses)
	}
	return result
}

func (t archivedDependencyTraversal) addressForKey(
	key archivedDependencyAddressKey,
) schema.CatalogDependencyObject {
	for _, dependency := range t.outgoing[key] {
		if archivedDependencyKey(dependency.Dependent) == key {
			return dependency.Dependent
		}
	}
	for _, dependencies := range t.outgoing {
		for _, dependency := range dependencies {
			if archivedDependencyKey(dependency.Referenced) == key {
				return dependency.Referenced
			}
		}
	}
	for _, functions := range t.supportFunctions {
		for _, function := range functions {
			if archivedDependencyKey(function) == key {
				return function
			}
		}
	}
	return schema.CatalogDependencyObject{ClassOID: key.classOID, ObjectOID: key.objectOID}
}

func walkArchivedDependencyGraph(
	traversal archivedDependencyTraversal,
	seeds []schema.CatalogDependencyObject,
) (map[archivedDependencyAddressKey]struct{}, map[archivedDependencyAddressKey]bool) {
	queue := slices.Clone(seeds)
	reachable := make(map[archivedDependencyAddressKey]struct{})
	helpers := make(map[archivedDependencyAddressKey]bool)
	for len(queue) > 0 {
		address := queue[0]
		queue = queue[1:]
		key := archivedDependencyKey(address)
		if _, visited := reachable[key]; visited || key.classOID == 0 || key.objectOID == 0 {
			continue
		}
		reachable[key] = struct{}{}
		for _, dependency := range traversal.outgoing[key] {
			queue = append(queue, dependency.Referenced)
		}
		for _, helper := range traversal.internalDependents[key] {
			helpers[archivedDependencyKey(helper)] = true
			queue = append(queue, helper)
		}
		queue = append(queue, traversal.supportFunctions[key]...)
	}
	return reachable, helpers
}

func managedTargetDependencyAddresses(
	target schema.SchemaSnapshot,
	traversal archivedDependencyTraversal,
) map[archivedDependencyAddressKey]struct{} {
	modeledTables := make(map[string]struct{}, len(target.Schema.Tables))
	for _, table := range target.Schema.Tables {
		modeledTables[table.GetName()] = struct{}{}
	}
	var seeds []schema.CatalogDependencyObject
	for _, relation := range target.Inventory.Relations {
		name := schema.SchemaQualifiedName{
			SchemaName: relation.SchemaName, EscapedName: schema.EscapeIdentifier(relation.Name),
		}.GetName()
		if _, managed := modeledTables[name]; !managed {
			continue
		}
		seeds = append(seeds, schema.CatalogDependencyObject{
			ClassOID: pgClassCatalogOID, ObjectOID: relation.OID,
			SchemaName: relation.SchemaName, Name: relation.Name,
		})
	}
	reachable, _ := walkArchivedDependencyGraph(traversal, seeds)
	return reachable
}

func resolveArchivedDependency(
	inventory schema.CatalogInventory,
	address schema.CatalogDependencyObject,
) (resolvedArchivedDependency, bool, error) {
	switch address.ClassOID {
	case pgTypeCatalogOID:
		catalogType, count := catalogTypeByOIDForClosure(inventory, address.ObjectOID)
		if count == 0 {
			return resolvePlatformArchivedDependency(address, archivalMarkerObjectKindType)
		}
		if count > 1 {
			return resolvedArchivedDependency{}, false,
				fmt.Errorf("ambiguous type OID %d", address.ObjectOID)
		}
		if catalogType.Kind == schema.CatalogTypeKindArray ||
			catalogType.Kind == schema.CatalogTypeKindRow {
			return resolvedArchivedDependency{}, false, nil
		}
		definition, err := archivedTypeDefinition(inventory, catalogType)
		if err != nil {
			return resolvedArchivedDependency{}, false, err
		}
		identity := archivedDependencyMarkerObject(catalogType.OID, archivalMarkerObjectKindType,
			catalogType.SchemaName, catalogType.Name)
		extensionName := archivedDependencyExtensionName(inventory, address)
		return newResolvedArchivedDependency(
			address, identity, definition, isMovableArchivedDependencySchema(catalogType.SchemaName) &&
				extensionName == "", extensionName,
		), true, nil
	case pgProcCatalogOID:
		routine, count := catalogRoutineByOIDForClosure(inventory, address.ObjectOID)
		if count == 0 {
			return resolvePlatformArchivedDependency(address, archivalMarkerObjectKindFunction)
		}
		if count > 1 {
			return resolvedArchivedDependency{}, false,
				fmt.Errorf("ambiguous routine OID %d", address.ObjectOID)
		}
		definition := canonicalArchivedDependencyDefinition(struct {
			Kind          string
			Language      string
			Arguments     string
			Result        string
			ArgumentModes []string
			ArgumentNames []string
			ReturnsSet    bool
			Source        string
			Binary        string
			HasSQLBody    bool
			SQLBody       string
			Configuration []string
			BodyForm      schema.CatalogRoutineBodyForm
			Trackability  schema.CatalogRoutineReferenceTrackability
		}{
			Kind: routine.Kind, Language: routine.LanguageName,
			Arguments:     strings.TrimSpace(routine.Arguments),
			Result:        strings.TrimSpace(routine.Result),
			ArgumentModes: slices.Clone(routine.ArgumentModes),
			ArgumentNames: slices.Clone(routine.ArgumentNames), ReturnsSet: routine.ReturnsSet,
			Source: routine.Source, Binary: routine.Binary, HasSQLBody: routine.HasSQLBody,
			SQLBody: routine.SQLBody, Configuration: sortedStringsForClosure(routine.Configuration),
			BodyForm: routine.BodyForm, Trackability: routine.ReferenceTrackability,
		})
		identity := archivedDependencyMarkerObject(routine.OID, archivalMarkerObjectKindFunction,
			routine.SchemaName, routine.Name,
			strings.TrimSpace(routine.IdentityArguments))
		extensionName := archivedDependencyExtensionName(inventory, address)
		return newResolvedArchivedDependency(
			address, identity, definition, isMovableArchivedDependencySchema(routine.SchemaName) &&
				extensionName == "", extensionName,
		), true, nil
	case pgCollationCatalogOID:
		collation, count := catalogCollationByOIDForClosure(inventory, address.ObjectOID)
		if count == 0 {
			return resolvePlatformArchivedDependency(address, archivalMarkerObjectKindCollation)
		}
		if count > 1 {
			return resolvedArchivedDependency{}, false,
				fmt.Errorf("ambiguous collation OID %d", address.ObjectOID)
		}
		definition := canonicalArchivedDependencyDefinition(struct {
			Provider        string
			IsDeterministic bool
			Encoding        int32
			Collate         string
			CType           string
			Locale          string
			ICURules        string
			Version         string
		}{
			collation.Provider, collation.IsDeterministic, collation.Encoding, collation.Collate,
			collation.CType, collation.Locale, collation.ICURules, collation.Version,
		})
		identity := archivedDependencyMarkerObject(collation.OID, archivalMarkerObjectKindCollation,
			collation.SchemaName, collation.Name)
		extensionName := archivedDependencyExtensionName(inventory, address)
		return newResolvedArchivedDependency(
			address, identity, definition, isMovableArchivedDependencySchema(collation.SchemaName) &&
				extensionName == "", extensionName,
		), true, nil
	case pgOperatorCatalogOID:
		operator, count := catalogOperatorByOIDForClosure(inventory, address.ObjectOID)
		if count == 0 {
			return resolvePlatformArchivedDependency(address, archivalMarkerObjectKindOperator)
		}
		if count > 1 {
			return resolvedArchivedDependency{}, false,
				fmt.Errorf("ambiguous operator OID %d", address.ObjectOID)
		}
		definition, err := archivedOperatorDefinition(inventory, operator)
		if err != nil {
			return resolvedArchivedDependency{}, false, err
		}
		identity := archivedDependencyMarkerObject(operator.OID, archivalMarkerObjectKindOperator,
			operator.SchemaName, operator.Name, operator.LeftType, operator.RightType)
		extensionName := archivedDependencyExtensionName(inventory, address)
		return newResolvedArchivedDependency(
			address, identity, definition, isMovableArchivedDependencySchema(operator.SchemaName) &&
				extensionName == "", extensionName,
		), true, nil
	case pgClassCatalogOID:
		sequence, count := standaloneSequenceByOIDForClosure(inventory, address.ObjectOID)
		if count == 0 {
			return resolvedArchivedDependency{}, false, nil
		}
		if count > 1 {
			return resolvedArchivedDependency{}, false,
				fmt.Errorf("ambiguous standalone sequence OID %d", address.ObjectOID)
		}
		dataType, err := archivedTypeReference(inventory, sequence.DataTypeOID)
		if err != nil {
			return resolvedArchivedDependency{}, false,
				fmt.Errorf("standalone sequence %s.%s data type: %w", sequence.SchemaName, sequence.Name, err)
		}
		definition := canonicalArchivedDependencyDefinition(struct {
			Persistence    schema.RelationPersistence
			DataType       string
			StartValue     int64
			IncrementValue int64
			MaxValue       int64
			MinValue       int64
			CacheSize      int64
			IsCycle        bool
		}{
			sequence.Persistence, dataType, sequence.StartValue, sequence.IncrementValue,
			sequence.MaxValue, sequence.MinValue, sequence.CacheSize, sequence.IsCycle,
		})
		identity := archivedDependencyMarkerObject(sequence.OID, archivalMarkerObjectKindSequence,
			sequence.SchemaName, sequence.Name)
		extensionName := archivedDependencyExtensionName(inventory, address)
		return newResolvedArchivedDependency(
			address, identity, definition, isMovableArchivedDependencySchema(sequence.SchemaName) &&
				extensionName == "", extensionName,
		), true, nil
	case pgExtensionCatalogOID:
		extension, count := catalogExtensionByOIDForClosure(inventory, address.ObjectOID)
		if count == 0 {
			return resolvedArchivedDependency{}, false,
				fmt.Errorf("unresolved extension OID %d", address.ObjectOID)
		}
		if count > 1 {
			return resolvedArchivedDependency{}, false,
				fmt.Errorf("ambiguous extension OID %d", address.ObjectOID)
		}
		definition := canonicalArchivedDependencyDefinition(struct {
			SchemaName    string
			Version       string
			IsRelocatable bool
		}{extension.SchemaName, extension.Version, extension.IsRelocatable})
		identity := archivedDependencyMarkerObject(
			extension.OID, archivalMarkerObjectKindSchema, "", extension.Name,
		)
		resolved := newResolvedArchivedDependency(address, identity, definition, false, extension.Name)
		resolved.identityKey = "extension\x00" + extension.Name
		return resolved, true, nil
	default:
		return resolvedArchivedDependency{}, false, nil
	}
}

func newResolvedArchivedDependency(
	address schema.CatalogDependencyObject,
	identity archivalMarkerObjectIdentity,
	definition string,
	movable bool,
	extensionName string,
) resolvedArchivedDependency {
	return resolvedArchivedDependency{
		kind: identity.Kind, address: address, identity: identity,
		identityKey: markerObjectIdentityKey(identity), semanticDefinition: definition,
		movable: movable, extensionName: extensionName,
	}
}

func resolvePlatformArchivedDependency(
	address schema.CatalogDependencyObject,
	kind archivalMarkerObjectKind,
) (resolvedArchivedDependency, bool, error) {
	if !isPlatformArchivedDependency(address) {
		return resolvedArchivedDependency{}, false, nil
	}
	identityArguments := []string(nil)
	if kind == archivalMarkerObjectKindFunction {
		identityArguments = []string{archivedDependencyIdentityArguments(address.Identity)}
	}
	identity := archivedDependencyMarkerObject(address.ObjectOID, kind, address.SchemaName, address.Name,
		identityArguments...)
	definition := canonicalArchivedDependencyDefinition(struct {
		CatalogClass uint32
		Identity     string
	}{address.ClassOID, normalizedArchivedDependencyIdentity(address)})
	return newResolvedArchivedDependency(address, identity, definition, false, ""), true, nil
}

func archivedTypeDefinition(
	inventory schema.CatalogInventory,
	catalogType schema.CatalogType,
) (string, error) {
	baseType, err := archivedOptionalTypeReference(inventory, catalogType.BaseTypeOID)
	if err != nil {
		return "", fmt.Errorf("type %s.%s base type: %w", catalogType.SchemaName, catalogType.Name, err)
	}
	collation, err := archivedOptionalCatalogReference(inventory, pgCollationCatalogOID,
		catalogType.CollationOID)
	if err != nil {
		return "", fmt.Errorf("type %s.%s collation: %w", catalogType.SchemaName, catalogType.Name, err)
	}

	type compositeAttribute struct {
		Number       int16
		Name         string
		Type         string
		TypeModifier int32
		Collation    string
	}
	var attributes []compositeAttribute
	for _, attribute := range inventory.CompositeAttributes {
		if attribute.TypeOID != catalogType.OID {
			continue
		}
		attributeType, err := archivedTypeReference(inventory, attribute.AttributeTypeOID)
		if err != nil {
			return "", fmt.Errorf("composite type %s.%s attribute %q: %w",
				catalogType.SchemaName, catalogType.Name, attribute.Name, err)
		}
		attributeCollation, err := archivedOptionalCatalogReference(
			inventory, pgCollationCatalogOID, attribute.CollationOID,
		)
		if err != nil {
			return "", fmt.Errorf("composite type %s.%s attribute %q collation: %w",
				catalogType.SchemaName, catalogType.Name, attribute.Name, err)
		}
		attributes = append(attributes, compositeAttribute{
			Number: attribute.Number, Name: attribute.Name, Type: attributeType,
			TypeModifier: attribute.TypeModifier, Collation: attributeCollation,
		})
	}
	slices.SortFunc(attributes, func(a, b compositeAttribute) int {
		return cmp.Compare(a.Number, b.Number)
	})

	type domainConstraint struct {
		Name         string
		IsDeferrable bool
		IsDeferred   bool
		IsValidated  bool
		Definition   string
	}
	var constraints []domainConstraint
	for _, constraint := range inventory.DomainConstraints {
		if constraint.TypeOID == catalogType.OID {
			constraints = append(constraints, domainConstraint{
				Name: constraint.Name, IsDeferrable: constraint.IsDeferrable,
				IsDeferred: constraint.IsDeferred, IsValidated: constraint.IsValidated,
				Definition: strings.TrimSpace(constraint.Definition),
			})
		}
	}
	slices.SortFunc(constraints, func(a, b domainConstraint) int { return cmp.Compare(a.Name, b.Name) })

	type sortableEnumLabel struct {
		SortOrder float64
		Label     string
	}
	var sortableLabels []sortableEnumLabel
	for _, label := range inventory.EnumLabels {
		if label.TypeOID == catalogType.OID {
			sortableLabels = append(sortableLabels, sortableEnumLabel{
				SortOrder: label.SortOrder, Label: label.Label,
			})
		}
	}
	slices.SortFunc(sortableLabels, func(a, b sortableEnumLabel) int {
		return cmp.Compare(a.SortOrder, b.SortOrder)
	})
	labels := make([]string, 0, len(sortableLabels))
	for _, label := range sortableLabels {
		labels = append(labels, label.Label)
	}

	type rangeDefinition struct {
		RangeType      string
		Subtype        string
		MultirangeType string
		Collation      string
		OperatorClass  string
		Canonical      string
		SubtypeDiff    string
	}
	var rangeData rangeDefinition
	for _, catalogRange := range inventory.Ranges {
		if catalogRange.TypeOID == catalogType.OID ||
			catalogRange.MultirangeTypeOID == catalogType.OID {
			rangeData.RangeType, err = archivedTypeReference(inventory, catalogRange.TypeOID)
			if err != nil {
				return "", err
			}
			rangeData.Subtype, err = archivedTypeReference(inventory, catalogRange.SubtypeOID)
			if err != nil {
				return "", err
			}
			rangeData.MultirangeType, err = archivedOptionalTypeReference(
				inventory, catalogRange.MultirangeTypeOID,
			)
			if err != nil {
				return "", err
			}
			rangeData.Collation, err = archivedOptionalCatalogReference(
				inventory, pgCollationCatalogOID, catalogRange.CollationOID,
			)
			if err != nil {
				return "", err
			}
			if catalogRange.OperatorClassOID != 0 {
				if catalogRange.OperatorClassSchemaName == "" || catalogRange.OperatorClassName == "" {
					return "", fmt.Errorf("range type %s has unresolved operator class OID %d",
						rangeData.RangeType, catalogRange.OperatorClassOID)
				}
				rangeData.OperatorClass = catalogRange.OperatorClassSchemaName + "." +
					catalogRange.OperatorClassName
			}
			rangeData.Canonical, err = archivedOptionalCatalogReference(
				inventory, pgProcCatalogOID, catalogRange.CanonicalFunctionOID,
			)
			if err != nil {
				return "", err
			}
			rangeData.SubtypeDiff, err = archivedOptionalCatalogReference(
				inventory, pgProcCatalogOID, catalogRange.SubtypeDiffFunctionOID,
			)
			if err != nil {
				return "", err
			}
			break
		}
	}

	type supportFunction struct {
		Role     string
		Function string
	}
	var supportFunctions []supportFunction
	for _, support := range inventory.TypeSupportFunctions {
		if support.TypeOID == catalogType.OID {
			supportFunctions = append(supportFunctions, supportFunction{
				Role: support.Role,
				Function: archivedRoutineIdentity(support.FunctionSchemaName, support.FunctionName,
					support.FunctionIdentityArguments),
			})
		}
	}
	slices.SortFunc(supportFunctions, func(a, b supportFunction) int {
		return cmp.Or(cmp.Compare(a.Role, b.Role), cmp.Compare(a.Function, b.Function))
	})

	return canonicalArchivedDependencyDefinition(struct {
		Kind             schema.CatalogTypeKind
		RawKind          string
		Category         string
		IsPreferred      bool
		IsDefined        bool
		InternalLength   int16
		PassedByValue    bool
		Delimiter        string
		Alignment        string
		Storage          string
		BaseType         string
		TypeModifier     int32
		Dimensions       int32
		Collation        string
		IsNotNull        bool
		DefaultValue     string
		Attributes       []compositeAttribute
		Constraints      []domainConstraint
		EnumLabels       []string
		Range            rangeDefinition
		SupportFunctions []supportFunction
		Extension        string
	}{
		Kind: catalogType.Kind, RawKind: catalogType.RawKind, Category: catalogType.Category,
		IsPreferred: catalogType.IsPreferred, IsDefined: catalogType.IsDefined,
		InternalLength: catalogType.InternalLength, PassedByValue: catalogType.IsPassedByValue,
		Delimiter: catalogType.Delimiter, Alignment: catalogType.Alignment, Storage: catalogType.Storage,
		BaseType: baseType, TypeModifier: catalogType.TypeModifier, Dimensions: catalogType.Dimensions,
		Collation: collation, IsNotNull: catalogType.IsNotNull,
		DefaultValue: strings.TrimSpace(catalogType.DefaultValue), Attributes: attributes,
		Constraints: constraints, EnumLabels: labels, Range: rangeData,
		SupportFunctions: supportFunctions, Extension: archivedCatalogExtensionName(catalogType.Extension),
	}), nil
}

func archivedOperatorDefinition(
	inventory schema.CatalogInventory,
	operator schema.CatalogOperator,
) (string, error) {
	references := make([]string, 0, 6)
	for _, reference := range []struct {
		classOID uint32
		oid      uint32
	}{
		{pgTypeCatalogOID, operator.LeftTypeOID},
		{pgTypeCatalogOID, operator.RightTypeOID},
		{pgTypeCatalogOID, operator.ResultTypeOID},
		{pgProcCatalogOID, operator.FunctionOID},
		{pgProcCatalogOID, operator.RestrictionFunctionOID},
		{
			pgProcCatalogOID, operator.JoinFunctionOID,
		},
		{pgOperatorCatalogOID, operator.CommutatorOperatorOID},
		{pgOperatorCatalogOID, operator.NegatorOperatorOID},
	} {
		identity, err := archivedOptionalCatalogReference(inventory, reference.classOID, reference.oid)
		if err != nil {
			return "", fmt.Errorf("operator %s.%s reference: %w", operator.SchemaName, operator.Name, err)
		}
		references = append(references, identity)
	}
	return canonicalArchivedDependencyDefinition(struct {
		Kind       string
		CanMerge   bool
		CanHash    bool
		References []string
	}{operator.Kind, operator.CanMerge, operator.CanHash, references}), nil
}

func matchArchivedDependencyInTarget(
	source resolvedArchivedDependency,
	target schema.SchemaSnapshot,
	managedAddresses map[archivedDependencyAddressKey]struct{},
) (bool, bool, bool, error) {
	candidates, err := targetArchivedDependencyCandidates(source, target.Inventory)
	if err != nil {
		return false, false, false, err
	}
	if len(candidates) > 1 {
		return false, false, false, fmt.Errorf(
			"ambiguous target identity for dependency %s", archivedDependencyDescription(source),
		)
	}
	if len(candidates) == 0 {
		return false, false, false, nil
	}
	candidate := candidates[0]
	compatible := candidate.semanticDefinition == source.semanticDefinition
	_, managed := managedAddresses[archivedDependencyKey(candidate.address)]
	return true, compatible, compatible && managed, nil
}

func targetArchivedDependencyCandidates(
	source resolvedArchivedDependency,
	inventory schema.CatalogInventory,
) ([]resolvedArchivedDependency, error) {
	var addresses []schema.CatalogDependencyObject
	switch source.kind {
	case archivalMarkerObjectKindType:
		for _, object := range inventory.Types {
			if object.SchemaName == source.identity.SchemaName && object.Name == source.identity.Name {
				addresses = append(addresses, schema.CatalogDependencyObject{
					ClassOID: pgTypeCatalogOID, ObjectOID: object.OID,
					SchemaName: object.SchemaName, Name: object.Name,
				})
			}
		}
	case archivalMarkerObjectKindFunction:
		arguments := markerIdentityArgument(source.identity)
		for _, object := range inventory.Routines {
			if object.SchemaName == source.identity.SchemaName && object.Name == source.identity.Name &&
				strings.TrimSpace(object.IdentityArguments) == arguments {
				addresses = append(addresses, schema.CatalogDependencyObject{
					ClassOID: pgProcCatalogOID, ObjectOID: object.OID,
					SchemaName: object.SchemaName, Name: object.Name,
				})
			}
		}
		for _, support := range inventory.TypeSupportFunctions {
			if support.FunctionSchemaName == source.identity.SchemaName &&
				support.FunctionName == source.identity.Name &&
				strings.TrimSpace(support.FunctionIdentityArguments) == arguments {
				addresses = append(addresses, schema.CatalogDependencyObject{
					ClassOID: pgProcCatalogOID, ObjectOID: support.FunctionOID, ObjectType: "function",
					SchemaName: support.FunctionSchemaName, Name: support.FunctionName,
					Identity: archivedRoutineIdentity(support.FunctionSchemaName, support.FunctionName,
						support.FunctionIdentityArguments),
				})
			}
		}
	case archivalMarkerObjectKindCollation:
		for _, object := range inventory.Collations {
			if object.SchemaName == source.identity.SchemaName && object.Name == source.identity.Name {
				addresses = append(addresses, schema.CatalogDependencyObject{
					ClassOID: pgCollationCatalogOID, ObjectOID: object.OID,
					SchemaName: object.SchemaName, Name: object.Name,
				})
			}
		}
	case archivalMarkerObjectKindOperator:
		for _, object := range inventory.Operators {
			if object.SchemaName == source.identity.SchemaName && object.Name == source.identity.Name &&
				slices.Equal([]string{object.LeftType, object.RightType}, source.identity.IdentityArguments) {
				addresses = append(addresses, schema.CatalogDependencyObject{
					ClassOID: pgOperatorCatalogOID, ObjectOID: object.OID,
					SchemaName: object.SchemaName, Name: object.Name,
				})
			}
		}
	case archivalMarkerObjectKindSequence:
		for _, object := range inventory.Sequences {
			if object.SchemaName == source.identity.SchemaName && object.Name == source.identity.Name {
				addresses = append(addresses, schema.CatalogDependencyObject{
					ClassOID: pgClassCatalogOID, ObjectOID: object.OID,
					SchemaName: object.SchemaName, Name: object.Name,
				})
			}
		}
	case archivalMarkerObjectKindSchema:
		for _, object := range inventory.Extensions {
			if object.Name == source.identity.Name {
				addresses = append(addresses, schema.CatalogDependencyObject{
					ClassOID: pgExtensionCatalogOID, ObjectOID: object.OID,
					Name: object.Name, Identity: object.Name,
				})
			}
		}
	}
	addresses = append(addresses, platformArchivedDependencyCandidates(source, inventory)...)
	slices.SortFunc(addresses, compareArchivedDependencyAddresses)
	addresses = slices.CompactFunc(addresses, sameArchivedDependencyAddress)

	var result []resolvedArchivedDependency
	for _, address := range addresses {
		resolved, supported, err := resolveArchivedDependency(inventory, address)
		if err != nil {
			return nil, err
		}
		if supported && resolved.identityKey == source.identityKey {
			result = append(result, resolved)
		}
	}
	return result, nil
}

func platformArchivedDependencyCandidates(
	source resolvedArchivedDependency,
	inventory schema.CatalogInventory,
) []schema.CatalogDependencyObject {
	if !isPlatformArchivedDependency(source.address) {
		return nil
	}
	var result []schema.CatalogDependencyObject
	for _, dependency := range inventory.Dependencies {
		for _, address := range []schema.CatalogDependencyObject{dependency.Dependent, dependency.Referenced} {
			if address.ClassOID == source.address.ClassOID &&
				normalizedArchivedDependencyIdentity(address) ==
					normalizedArchivedDependencyIdentity(source.address) {
				result = append(result, address)
			}
		}
	}
	return result
}

func validateCompatiblePlatformDependency(
	source schema.CatalogDependencyObject,
	target schema.CatalogInventory,
) error {
	identity := normalizedArchivedDependencyIdentity(source)
	for _, dependency := range target.Dependencies {
		for _, candidate := range []schema.CatalogDependencyObject{dependency.Dependent, dependency.Referenced} {
			if candidate.ClassOID == source.ClassOID &&
				normalizedArchivedDependencyIdentity(candidate) == identity {
				return nil
			}
		}
	}
	return fmt.Errorf("platform object %s has no compatible target identity",
		sourceSafetyCatalogObjectDescription(source))
}

func validateCandidateArchivedDependencies(
	groups []preparedArchivedDependencyGroup,
	result archivedDependencyClosureResult,
) ([]dependencyValidatedArchivedCandidateGroup, error) {
	assignmentsByGroup := make(map[archivalGroupID][]archivalMarkerObjectIdentity)
	for _, assignment := range result.Assignments {
		assignmentsByGroup[assignment.GroupID] =
			append(assignmentsByGroup[assignment.GroupID], assignment.Destination)
	}
	var validated []dependencyValidatedArchivedCandidateGroup
	for _, group := range groups {
		if group.candidate == nil {
			continue
		}
		candidate := *group.candidate
		expectedObjects := canonicalMarkerObjects(assignmentsByGroup[group.id])
		actualObjects := canonicalMarkerObjects(candidate.Marker.ExclusiveDependencyObjects)
		if err := validateExactCandidateDependencyObjects(group.id, expectedObjects, actualObjects); err != nil {
			return nil, err
		}
		expectedEdges := incidentArchivedDependencyEdges(group.id, result.SharedGroupEdges)
		actualEdges := canonicalArchivedDependencyGroupEdges(
			candidate.Marker.SharedCleanupComponentGroupEdges,
		)
		if !slices.Equal(expectedEdges, actualEdges) {
			return nil, fmt.Errorf(
				"candidate archival group %q shared dependency edges do not match computed closure",
				group.id,
			)
		}
		validated = append(validated, dependencyValidatedArchivedCandidateGroup{Candidate: candidate})
	}
	slices.SortFunc(validated, func(a, b dependencyValidatedArchivedCandidateGroup) int {
		return cmp.Compare(a.Candidate.GroupID, b.Candidate.GroupID)
	})
	return validated, nil
}

func validateExactCandidateDependencyObjects(
	groupID archivalGroupID,
	expected []archivalMarkerObjectIdentity,
	actual []archivalMarkerObjectIdentity,
) error {
	for idx := 0; idx < min(len(expected), len(actual)); idx++ {
		if compareMarkerObjects(expected[idx], actual[idx]) != 0 {
			return fmt.Errorf(
				"candidate archival group %q exclusive dependency mismatch: expected %s OID %d, got %s OID %d",
				groupID, markerObjectIdentityKey(expected[idx]), expected[idx].OID,
				markerObjectIdentityKey(actual[idx]), actual[idx].OID,
			)
		}
	}
	if len(actual) < len(expected) {
		missing := expected[len(actual)]
		return fmt.Errorf("candidate archival group %q is missing exclusive dependency %s OID %d",
			groupID, markerObjectIdentityKey(missing), missing.OID)
	}
	if len(actual) > len(expected) {
		extra := actual[len(expected)]
		return fmt.Errorf("candidate archival group %q has extra exclusive dependency %s OID %d",
			groupID, markerObjectIdentityKey(extra), extra.OID)
	}
	return nil
}

func validateArchivedDependencyDestinationCollisions(assignments []archivedDependencyAssignment) error {
	seen := make(map[string]archivedDependencyAssignment)
	for _, assignment := range assignments {
		for _, namespaceKey := range archivedDependencyDestinationNamespaceKeys(assignment.Destination) {
			key := assignment.Destination.SchemaName + "\x00" + namespaceKey
			if previous, collision := seen[key]; collision &&
				previous.Source.OID != assignment.Source.OID {
				return fmt.Errorf(
					"dependency schema %q has conflicting %s objects %s (OID %d) and %s (OID %d)",
					assignment.Destination.SchemaName, namespaceKey,
					markerObjectIdentityKey(previous.Source), previous.Source.OID,
					markerObjectIdentityKey(assignment.Source), assignment.Source.OID,
				)
			}
			seen[key] = assignment
		}
	}
	return nil
}

func archivedDependencyDestinationNamespaceKeys(object archivalMarkerObjectIdentity) []string {
	switch object.Kind {
	case archivalMarkerObjectKindType:
		return []string{"type:" + object.Name}
	case archivalMarkerObjectKindSequence:
		return []string{"relation:" + object.Name, "type:" + object.Name}
	case archivalMarkerObjectKindFunction:
		return []string{"function:" + object.Name + "\x00" +
			strings.Join(object.IdentityArguments, "\x00")}
	case archivalMarkerObjectKindOperator:
		return []string{"operator:" + object.Name + "\x00" +
			strings.Join(object.IdentityArguments, "\x00")}
	case archivalMarkerObjectKindCollation:
		return []string{"collation:" + object.Name}
	default:
		return []string{string(object.Kind) + ":" + object.Name}
	}
}

func firstExternalArchivedDependencyConsumer(
	inventory schema.CatalogInventory,
	referenced schema.CatalogDependencyObject,
	reachable map[archivedDependencyAddressKey]struct{},
) *schema.CatalogDependencyObject {
	dependencies := slices.Clone(inventory.Dependencies)
	slices.SortFunc(dependencies, compareSourceSafetyCatalogDependencies)
	for _, dependency := range dependencies {
		if archivedDependencyKey(dependency.Referenced) != archivedDependencyKey(referenced) ||
			dependency.Type == "e" ||
			((dependency.Type == "i" || dependency.Type == "a") &&
				isArchivedDependencyInternalHelper(inventory, dependency.Dependent)) {
			continue
		}
		if _, retained := reachable[archivedDependencyKey(dependency.Dependent)]; retained {
			continue
		}
		dependent := dependency.Dependent
		return &dependent
	}
	return nil
}

func completeArchivedDependencyGroupEdges(
	groups []archivalGroupID,
) []archivalMarkerSharedGroupEdgeV1 {
	var result []archivalMarkerSharedGroupEdgeV1
	for firstIdx := 0; firstIdx < len(groups); firstIdx++ {
		for secondIdx := firstIdx + 1; secondIdx < len(groups); secondIdx++ {
			result = append(result, archivalMarkerSharedGroupEdgeV1{
				FirstGroupID: groups[firstIdx], SecondGroupID: groups[secondIdx],
			})
		}
	}
	return result
}

func canonicalArchivedDependencyGroupEdges(
	edges []archivalMarkerSharedGroupEdgeV1,
) []archivalMarkerSharedGroupEdgeV1 {
	result := make([]archivalMarkerSharedGroupEdgeV1, 0, len(edges))
	for _, edge := range edges {
		first, second := canonicalGroupEdge(edge)
		result = append(result, archivalMarkerSharedGroupEdgeV1{
			FirstGroupID: first, SecondGroupID: second,
		})
	}
	slices.SortFunc(result, func(a, b archivalMarkerSharedGroupEdgeV1) int {
		return cmp.Or(cmp.Compare(a.FirstGroupID, b.FirstGroupID),
			cmp.Compare(a.SecondGroupID, b.SecondGroupID))
	})
	return slices.Compact(result)
}

func incidentArchivedDependencyEdges(
	groupID archivalGroupID,
	edges []archivalMarkerSharedGroupEdgeV1,
) []archivalMarkerSharedGroupEdgeV1 {
	var result []archivalMarkerSharedGroupEdgeV1
	for _, edge := range edges {
		if edge.FirstGroupID == groupID || edge.SecondGroupID == groupID {
			result = append(result, edge)
		}
	}
	return canonicalArchivedDependencyGroupEdges(result)
}

func archivedDependencyExtensionName(
	inventory schema.CatalogInventory,
	address schema.CatalogDependencyObject,
) string {
	for _, member := range inventory.ExtensionMembers {
		if archivedDependencyKey(member.Object) == archivedDependencyKey(address) {
			return member.ExtensionName
		}
	}
	return ""
}

func archivedCatalogExtensionName(extension *schema.CatalogExtension) string {
	if extension == nil {
		return ""
	}
	return extension.Name
}

func archivedTypeReference(inventory schema.CatalogInventory, oid uint32) (string, error) {
	if oid == 0 {
		return "", fmt.Errorf("type OID must be non-zero")
	}
	if catalogType, count := catalogTypeByOIDForClosure(inventory, oid); count == 1 {
		return catalogType.SchemaName + "." + catalogType.Name, nil
	} else if count > 1 {
		return "", fmt.Errorf("ambiguous type OID %d", oid)
	}
	return archivedCatalogReference(inventory, pgTypeCatalogOID, oid)
}

func archivedOptionalTypeReference(inventory schema.CatalogInventory, oid uint32) (string, error) {
	if oid == 0 {
		return "", nil
	}
	return archivedTypeReference(inventory, oid)
}

func archivedOptionalCatalogReference(
	inventory schema.CatalogInventory,
	classOID uint32,
	oid uint32,
) (string, error) {
	if oid == 0 {
		return "", nil
	}
	return archivedCatalogReference(inventory, classOID, oid)
}

func archivedCatalogReference(
	inventory schema.CatalogInventory,
	classOID uint32,
	oid uint32,
) (string, error) {
	switch classOID {
	case pgTypeCatalogOID:
		if catalogType, count := catalogTypeByOIDForClosure(inventory, oid); count == 1 {
			return catalogType.SchemaName + "." + catalogType.Name, nil
		} else if count > 1 {
			return "", fmt.Errorf("ambiguous type OID %d", oid)
		}
	case pgProcCatalogOID:
		if routine, count := catalogRoutineByOIDForClosure(inventory, oid); count == 1 {
			return archivedRoutineIdentity(routine.SchemaName, routine.Name, routine.IdentityArguments), nil
		} else if count > 1 {
			return "", fmt.Errorf("ambiguous routine OID %d", oid)
		}
	case pgCollationCatalogOID:
		if collation, count := catalogCollationByOIDForClosure(inventory, oid); count == 1 {
			return collation.SchemaName + "." + collation.Name, nil
		} else if count > 1 {
			return "", fmt.Errorf("ambiguous collation OID %d", oid)
		}
	case pgOperatorCatalogOID:
		if operator, count := catalogOperatorByOIDForClosure(inventory, oid); count == 1 {
			return operator.SchemaName + "." + operator.Name + "(" +
				operator.LeftType + "," + operator.RightType + ")", nil
		} else if count > 1 {
			return "", fmt.Errorf("ambiguous operator OID %d", oid)
		}
	}
	identities := make(map[string]struct{})
	for _, dependency := range inventory.Dependencies {
		for _, address := range []schema.CatalogDependencyObject{dependency.Dependent, dependency.Referenced} {
			if address.ClassOID == classOID && address.ObjectOID == oid {
				identities[normalizedArchivedDependencyIdentity(address)] = struct{}{}
			}
		}
	}
	for _, support := range inventory.TypeSupportFunctions {
		if classOID == pgProcCatalogOID && support.FunctionOID == oid {
			identities[archivedRoutineIdentity(support.FunctionSchemaName, support.FunctionName,
				support.FunctionIdentityArguments)] = struct{}{}
		}
	}
	values := sortedStringMapKeys(identities)
	if len(values) == 0 {
		return "", fmt.Errorf("unresolved catalog address %d/%d", classOID, oid)
	}
	if len(values) > 1 {
		return "", fmt.Errorf("ambiguous catalog address %d/%d", classOID, oid)
	}
	return values[0], nil
}

func normalizedArchivedDependencyIdentity(address schema.CatalogDependencyObject) string {
	if address.Identity != "" {
		return strings.TrimSpace(address.Identity)
	}
	if address.SchemaName != "" {
		return address.SchemaName + "." + address.Name
	}
	return address.Name
}

func archivedRoutineIdentity(schemaName, name, arguments string) string {
	return schemaName + "." + name + "(" + strings.TrimSpace(arguments) + ")"
}

func archivedDependencyIdentityArguments(identity string) string {
	open := strings.IndexByte(identity, '(')
	if open < 0 || !strings.HasSuffix(identity, ")") {
		return ""
	}
	return strings.TrimSpace(identity[open+1 : len(identity)-1])
}

func markerIdentityArgument(identity archivalMarkerObjectIdentity) string {
	if len(identity.IdentityArguments) == 0 {
		return ""
	}
	return strings.TrimSpace(identity.IdentityArguments[0])
}

func archivedDependencyMarkerObject(
	oid uint32,
	kind archivalMarkerObjectKind,
	schemaName string,
	name string,
	identityArguments ...string,
) archivalMarkerObjectIdentity {
	if len(identityArguments) == 1 && strings.TrimSpace(identityArguments[0]) == "" {
		identityArguments = nil
	}
	return archivalMarkerObjectIdentity{
		Kind: kind, OID: oid, SchemaName: schemaName, Name: name,
		IdentityArguments: slices.Clone(identityArguments),
	}
}

func canonicalArchivedDependencyDefinition(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("marshaling internal archived dependency definition: %v", err))
	}
	return string(encoded)
}

func sortedStringsForClosure(values []string) []string {
	result := slices.Clone(values)
	slices.Sort(result)
	return result
}

func catalogTypeByOIDForClosure(inventory schema.CatalogInventory, oid uint32) (schema.CatalogType, int) {
	var result schema.CatalogType
	count := 0
	for _, object := range inventory.Types {
		if object.OID == oid {
			result = object
			count++
		}
	}
	return result, count
}

func catalogRoutineByOIDForClosure(inventory schema.CatalogInventory, oid uint32) (schema.CatalogRoutine, int) {
	var result schema.CatalogRoutine
	count := 0
	for _, object := range inventory.Routines {
		if object.OID == oid {
			result = object
			count++
		}
	}
	return result, count
}

func catalogCollationByOIDForClosure(inventory schema.CatalogInventory, oid uint32) (schema.CatalogCollation, int) {
	var result schema.CatalogCollation
	count := 0
	for _, object := range inventory.Collations {
		if object.OID == oid {
			result = object
			count++
		}
	}
	return result, count
}

func catalogOperatorByOIDForClosure(inventory schema.CatalogInventory, oid uint32) (schema.CatalogOperator, int) {
	var result schema.CatalogOperator
	count := 0
	for _, object := range inventory.Operators {
		if object.OID == oid {
			result = object
			count++
		}
	}
	return result, count
}

func standaloneSequenceByOIDForClosure(
	inventory schema.CatalogInventory,
	oid uint32,
) (schema.CatalogSequence, int) {
	for _, owned := range inventory.OwnedSequences {
		if owned.SequenceOID == oid {
			return schema.CatalogSequence{}, 0
		}
	}
	var result schema.CatalogSequence
	count := 0
	for _, object := range inventory.Sequences {
		if object.OID == oid {
			result = object
			count++
		}
	}
	return result, count
}

func catalogExtensionByOIDForClosure(
	inventory schema.CatalogInventory,
	oid uint32,
) (schema.CatalogExtensionIdentity, int) {
	var result schema.CatalogExtensionIdentity
	count := 0
	for _, object := range inventory.Extensions {
		if object.OID == oid {
			result = object
			count++
		}
	}
	return result, count
}

func isArchivedDependencyInternalHelper(
	inventory schema.CatalogInventory,
	address schema.CatalogDependencyObject,
) bool {
	switch address.ClassOID {
	case pgAttrDefCatalogOID, pgConstraintCatalogOID, pgCastCatalogOID, pgProcCatalogOID:
		return true
	case pgClassCatalogOID:
		for _, relation := range inventory.Relations {
			if relation.OID == address.ObjectOID &&
				relation.Kind == schema.RelKindCompositeType {
				return true
			}
		}
	case pgTypeCatalogOID:
		catalogType, count := catalogTypeByOIDForClosure(inventory, address.ObjectOID)
		return count == 1 && (catalogType.Kind == schema.CatalogTypeKindArray ||
			catalogType.Kind == schema.CatalogTypeKindRow)
	}
	return false
}

func isIgnoredArchivedDependencyClass(classOID uint32) bool {
	return classOID == pgNamespaceCatalogOID || classOID == pgAuthIDCatalogOID ||
		classOID == pgLanguageCatalogOID
}

func isPlatformArchivedDependency(address schema.CatalogDependencyObject) bool {
	return address.SchemaName == "pg_catalog" || address.SchemaName == "information_schema" ||
		strings.HasPrefix(address.SchemaName, "pg_toast")
}

func isMovableArchivedDependencySchema(schemaName string) bool {
	return schemaName != "pg_catalog" && schemaName != "information_schema" &&
		!strings.HasPrefix(schemaName, "pg_toast") && !strings.HasPrefix(schemaName, "pg_temp")
}

func isArchivedDependencyLocalAddress(
	key archivedDependencyAddressKey,
	seeds []schema.CatalogDependencyObject,
) bool {
	for _, seed := range seeds {
		if archivedDependencyKey(seed) == key {
			return true
		}
	}
	return false
}

func archivedDependencyKey(address schema.CatalogDependencyObject) archivedDependencyAddressKey {
	return archivedDependencyAddressKey{classOID: address.ClassOID, objectOID: address.ObjectOID}
}

func sortedArchivedDependencyAddressKeys(
	values map[archivedDependencyAddressKey]struct{},
) []archivedDependencyAddressKey {
	result := make([]archivedDependencyAddressKey, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	slices.SortFunc(result, compareArchivedDependencyAddressKeys)
	return result
}

func sortedArchivedDependencyAddressKeysFromResolved(
	values map[archivedDependencyAddressKey]resolvedArchivedDependency,
) []archivedDependencyAddressKey {
	result := make([]archivedDependencyAddressKey, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	slices.SortFunc(result, func(a, b archivedDependencyAddressKey) int {
		return cmp.Or(
			cmp.Compare(values[a].identityKey, values[b].identityKey),
			compareArchivedDependencyAddressKeys(a, b),
		)
	})
	return result
}

func compareArchivedDependencyAddressKeys(a, b archivedDependencyAddressKey) int {
	return cmp.Or(cmp.Compare(a.classOID, b.classOID), cmp.Compare(a.objectOID, b.objectOID))
}

func compareArchivedDependencyAddresses(a, b schema.CatalogDependencyObject) int {
	return cmp.Or(
		cmp.Compare(a.ClassOID, b.ClassOID), cmp.Compare(a.ObjectOID, b.ObjectOID),
		cmp.Compare(a.SubObjectID, b.SubObjectID), cmp.Compare(a.Identity, b.Identity),
	)
}

func sameArchivedDependencyAddress(a, b schema.CatalogDependencyObject) bool {
	return a.ClassOID == b.ClassOID && a.ObjectOID == b.ObjectOID && a.SubObjectID == b.SubObjectID
}

func compareArchivedDependencyAssignments(a, b archivedDependencyAssignment) int {
	return cmp.Or(
		cmp.Compare(a.GroupID, b.GroupID), compareMarkerObjects(a.Destination, b.Destination),
	)
}

func archivedDependencyDescription(object resolvedArchivedDependency) string {
	if object.kind == archivalMarkerObjectKindSchema {
		return fmt.Sprintf("extension %q", object.identity.Name)
	}
	return fmt.Sprintf("%s %s.%s (OID %d)", object.kind,
		object.identity.SchemaName, object.identity.Name, object.identity.OID)
}
