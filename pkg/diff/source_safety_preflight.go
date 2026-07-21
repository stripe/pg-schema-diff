package diff

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

const (
	pgClassCatalogOID        uint32 = 1259
	pgTypeCatalogOID         uint32 = 1247
	pgProcCatalogOID         uint32 = 1255
	pgAttrDefCatalogOID      uint32 = 2604
	pgConstraintCatalogOID   uint32 = 2606
	pgTriggerCatalogOID      uint32 = 2620
	pgRewriteCatalogOID      uint32 = 2618
	pgPolicyCatalogOID       uint32 = 3256
	pgStatisticExtCatalogOID uint32 = 3381
)

type sourceSafetyPreflightRequest struct {
	CurrentSnapshot schema.SchemaSnapshot
	TargetSnapshot  schema.SchemaSnapshot

	ProposedTableRelationOIDs []uint32
	ExpectedRetainedObjects   []sourceSafetyExpectedRetainedObject
}

type sourceSafetyExpectedRetainedObject struct {
	TableRelationOID uint32
	Address          schema.CatalogDependencyObject
}

type sourceSafetyPreflightResult struct {
	ExpectedRetainedObjects []sourceSafetyExpectedRetainedObject
	IncomingDependencies    []sourceSafetyIncomingDependency
	ForeignKeys             []sourceSafetyForeignKey
	PublicationRelations    []schema.CatalogPublicationRelation
	PublicationSchemas      []schema.CatalogPublicationSchema
}

type sourceSafetyManagedScope string

const (
	sourceSafetyManagedScopeManaged  sourceSafetyManagedScope = "managed"
	sourceSafetyManagedScopeExcluded sourceSafetyManagedScope = "excluded"
)

type sourceSafetyTargetIntent string

const (
	sourceSafetyTargetIntentExplicitlyAbsent sourceSafetyTargetIntent = "explicitly_absent"
	sourceSafetyTargetIntentPersistent       sourceSafetyTargetIntent = "persistent"
)

type sourceSafetyDependencyDisposition string

const (
	sourceSafetyDependencyDispositionNormalDeletion sourceSafetyDependencyDisposition = "normal_deletion"
	sourceSafetyDependencyDispositionForeignKey     sourceSafetyDependencyDisposition = "foreign_key_inventory"
)

type sourceSafetyIncomingDependencyKind string

const (
	sourceSafetyIncomingDependencyKindView             sourceSafetyIncomingDependencyKind = "view"
	sourceSafetyIncomingDependencyKindMaterializedView sourceSafetyIncomingDependencyKind = "materialized_view"
	sourceSafetyIncomingDependencyKindRule             sourceSafetyIncomingDependencyKind = "rule"
	sourceSafetyIncomingDependencyKindRoutine          sourceSafetyIncomingDependencyKind = "routine"
	sourceSafetyIncomingDependencyKindRowTypeConsumer  sourceSafetyIncomingDependencyKind = "row_type_consumer"
	sourceSafetyIncomingDependencyKindUnsupported      sourceSafetyIncomingDependencyKind = "unsupported"
)

type sourceSafetyDependentIdentity struct {
	Kind                         sourceSafetyIncomingDependencyKind
	SchemaName                   string
	Name                         string
	Identity                     string
	Definition                   string
	CatalogClass                 uint32
	RoutineBodyForm              schema.CatalogRoutineBodyForm
	RoutineReferenceTrackability schema.CatalogRoutineReferenceTrackability
}

type sourceSafetyIncomingDependency struct {
	ProposedTableRelationOID uint32
	Dependent                sourceSafetyDependentIdentity
	ManagedScope             sourceSafetyManagedScope
	TargetIntent             sourceSafetyTargetIntent
	Disposition              sourceSafetyDependencyDisposition
}

type sourceSafetyForeignKeyDirection string

const (
	sourceSafetyForeignKeyDirectionIncoming sourceSafetyForeignKeyDirection = "incoming"
	sourceSafetyForeignKeyDirectionOutgoing sourceSafetyForeignKeyDirection = "outgoing"
	sourceSafetyForeignKeyDirectionSelf     sourceSafetyForeignKeyDirection = "self"
	sourceSafetyForeignKeyDirectionInternal sourceSafetyForeignKeyDirection = "internal"
)

type sourceSafetyForeignKey struct {
	ForeignKey   schema.CatalogForeignKey
	Direction    sourceSafetyForeignKeyDirection
	ManagedScope sourceSafetyManagedScope
	TargetIntent sourceSafetyTargetIntent
	Disposition  sourceSafetyDependencyDisposition
}

type sourceSafetyProtectedAddress struct {
	address          schema.CatalogDependencyObject
	tableRelationOID uint32
}

type resolvedSourceSafetyDependent struct {
	identity     sourceSafetyDependentIdentity
	managedScope sourceSafetyManagedScope
	targetIntent sourceSafetyTargetIntent
}

func runSourceSafetyPreflight(request sourceSafetyPreflightRequest) (sourceSafetyPreflightResult, error) {
	if len(request.ProposedTableRelationOIDs) == 0 {
		return sourceSafetyPreflightResult{}, nil
	}

	current := request.CurrentSnapshot
	target := request.TargetSnapshot
	current.Inventory = current.Inventory.Normalize()
	target.Inventory = target.Inventory.Normalize()

	proposedRelationOIDs := slices.Clone(request.ProposedTableRelationOIDs)
	slices.Sort(proposedRelationOIDs)
	if slices.Contains(proposedRelationOIDs, uint32(0)) {
		return sourceSafetyPreflightResult{}, fmt.Errorf(
			"proposed archival relation OID must be non-zero",
		)
	}
	for idx := 1; idx < len(proposedRelationOIDs); idx++ {
		if proposedRelationOIDs[idx] == proposedRelationOIDs[idx-1] {
			return sourceSafetyPreflightResult{}, fmt.Errorf(
				"proposed archival relation OID %d is duplicated", proposedRelationOIDs[idx],
			)
		}
	}

	proposedRelations := make(map[uint32]schema.CatalogRelation, len(proposedRelationOIDs))
	for _, relationOID := range proposedRelationOIDs {
		relation, err := uniqueSourceSafetyRelation(current.Inventory, relationOID)
		if err != nil {
			return sourceSafetyPreflightResult{}, fmt.Errorf(
				"validating proposed archival table: %w", err,
			)
		}
		if _, tableLike := current.Inventory.ClassifyTable(relationOID); !tableLike {
			return sourceSafetyPreflightResult{}, fmt.Errorf(
				"proposed archival relation %s is not table-like", catalogRelationDescription(relation),
			)
		}
		proposedRelations[relationOID] = relation
	}

	firstRelation := proposedRelations[proposedRelationOIDs[0]]
	if err := validateNoExtensionChangesDuringArchival(current.Inventory, target.Inventory, firstRelation); err != nil {
		return sourceSafetyPreflightResult{}, err
	}
	if err := validateNoEnabledEventTriggers(current.Inventory, firstRelation); err != nil {
		return sourceSafetyPreflightResult{}, err
	}
	if err := validateNoAllTablesPublications(current.Inventory, firstRelation); err != nil {
		return sourceSafetyPreflightResult{}, err
	}

	protectedAddresses, retainedAddresses, err := buildSourceSafetyProtectedAddresses(
		current.Inventory, proposedRelations, request.ExpectedRetainedObjects,
	)
	if err != nil {
		return sourceSafetyPreflightResult{}, err
	}
	if err := validateNoExtensionMemberRetainedObjects(
		current.Inventory, proposedRelations, retainedAddresses,
	); err != nil {
		return sourceSafetyPreflightResult{}, err
	}

	result := sourceSafetyPreflightResult{
		ExpectedRetainedObjects: sourceSafetyExpectedRetainedObjects(retainedAddresses),
		PublicationRelations:    sourceSafetyPublicationRelations(current.Inventory, proposedRelations),
		PublicationSchemas:      sourceSafetyPublicationSchemas(current.Inventory, proposedRelations),
	}

	foreignKeyOIDs := make(map[uint32]struct{})
	for _, foreignKey := range current.Inventory.ForeignKeys {
		direction, touchesProposed := classifySourceSafetyForeignKeyDirection(foreignKey, proposedRelations)
		if !touchesProposed {
			continue
		}
		foreignKeyOIDs[foreignKey.OID] = struct{}{}
		managedScope, targetIntent, err := classifySourceSafetyForeignKeyIntent(
			foreignKey, current.Schema, target.Schema,
		)
		if err != nil {
			return sourceSafetyPreflightResult{}, fmt.Errorf(
				"classifying foreign key %s.%s: %w", foreignKey.OwningSchemaName, foreignKey.Name, err,
			)
		}
		result.ForeignKeys = append(result.ForeignKeys, sourceSafetyForeignKey{
			ForeignKey: foreignKey, Direction: direction, ManagedScope: managedScope,
			TargetIntent: targetIntent, Disposition: sourceSafetyDependencyDispositionForeignKey,
		})
	}
	foreignKeyTriggerOIDs := make(map[uint32]struct{})
	for _, trigger := range current.Inventory.Triggers {
		if _, belongsToForeignKey := foreignKeyOIDs[trigger.ConstraintOID]; belongsToForeignKey {
			foreignKeyTriggerOIDs[trigger.OID] = struct{}{}
		}
	}
	publicationRelationOIDs := make(map[uint32]struct{}, len(result.PublicationRelations))
	for _, membership := range result.PublicationRelations {
		publicationRelationOIDs[membership.OID] = struct{}{}
	}

	incomingByKey := make(map[string]sourceSafetyIncomingDependency)
	dependencies := slices.Clone(current.Inventory.Dependencies)
	slices.SortFunc(dependencies, compareSourceSafetyCatalogDependencies)
	for _, dependency := range dependencies {
		tableRelationOID, touchesProtectedObject := sourceSafetyReferencedTable(
			dependency.Referenced, protectedAddresses,
		)
		if !touchesProtectedObject {
			continue
		}
		if sourceSafetyAddressIsRetained(dependency.Dependent, retainedAddresses) ||
			isSourceSafetyAttachedTableObject(dependency, tableRelationOID) {
			continue
		}
		if _, isForeignKey := foreignKeyOIDs[dependency.Dependent.ObjectOID]; isForeignKey &&
			dependency.Dependent.ClassOID == pgConstraintCatalogOID {
			continue
		}
		if _, isForeignKeyTrigger := foreignKeyTriggerOIDs[dependency.Dependent.ObjectOID]; isForeignKeyTrigger && dependency.Dependent.ClassOID == pgTriggerCatalogOID {
			continue
		}
		if _, isPublicationMembership := publicationRelationOIDs[dependency.Dependent.ObjectOID]; isPublicationMembership && dependency.Dependent.ObjectType == "publication relation" {
			continue
		}

		resolved, err := resolveSourceSafetyDependent(dependency.Dependent, current, target)
		if err != nil {
			return sourceSafetyPreflightResult{}, fmt.Errorf(
				"classifying dependency on proposed table %s: %w",
				catalogRelationDescription(proposedRelations[tableRelationOID]), err,
			)
		}
		classification := sourceSafetyIncomingDependency{
			ProposedTableRelationOID: tableRelationOID,
			Dependent:                resolved.identity,
			ManagedScope:             resolved.managedScope,
			TargetIntent:             resolved.targetIntent,
			Disposition:              sourceSafetyDependencyDispositionNormalDeletion,
		}
		key := sourceSafetyIncomingDependencyKey(classification)
		incomingByKey[key] = classification
	}

	// PostgreSQL does not record body references for these routines. A persistent
	// routine therefore has to be treated as a potential incoming dependency.
	for _, routine := range current.Inventory.Routines {
		if routine.SchemaName == "pg_catalog" || routine.SchemaName == "information_schema" ||
			strings.HasPrefix(routine.SchemaName, "pg_toast") ||
			strings.HasPrefix(routine.SchemaName, "pg_temp") {
			continue
		}
		if routine.ReferenceTrackability != schema.CatalogRoutineReferenceTrackabilityUntrackable {
			continue
		}
		resolved, err := resolveSourceSafetyRoutine(routine, current.Schema, target.Schema)
		if err != nil {
			return sourceSafetyPreflightResult{}, fmt.Errorf(
				"classifying untrackable routine %s: %w",
				catalogRoutineDescription(routine), err,
			)
		}
		classification := sourceSafetyIncomingDependency{
			ProposedTableRelationOID: proposedRelationOIDs[0],
			Dependent:                resolved.identity,
			ManagedScope:             resolved.managedScope,
			TargetIntent:             resolved.targetIntent,
			Disposition:              sourceSafetyDependencyDispositionNormalDeletion,
		}
		incomingByKey[sourceSafetyIncomingDependencyKey(classification)] = classification
	}

	for _, classification := range incomingByKey {
		result.IncomingDependencies = append(result.IncomingDependencies, classification)
	}
	slices.SortFunc(result.IncomingDependencies, compareSourceSafetyIncomingDependencies)
	slices.SortFunc(result.ForeignKeys, compareSourceSafetyForeignKeys)

	for _, dependency := range result.IncomingDependencies {
		if dependency.ManagedScope == sourceSafetyManagedScopeManaged &&
			dependency.TargetIntent == sourceSafetyTargetIntentExplicitlyAbsent {
			continue
		}
		relation := proposedRelations[dependency.ProposedTableRelationOID]
		if dependency.Dependent.Kind == sourceSafetyIncomingDependencyKindRoutine &&
			dependency.Dependent.RoutineReferenceTrackability ==
				schema.CatalogRoutineReferenceTrackabilityUntrackable {
			return sourceSafetyPreflightResult{}, fmt.Errorf(
				"cannot archive table %s: persistent routine %s has untrackable %s references",
				catalogRelationDescription(relation),
				sourceSafetyDependentDescription(dependency.Dependent),
				dependency.Dependent.RoutineBodyForm,
			)
		}
		return sourceSafetyPreflightResult{}, fmt.Errorf(
			"cannot archive table %s: persistent %s %s cannot be safely round-tripped",
			catalogRelationDescription(relation), dependency.Dependent.Kind,
			sourceSafetyDependentDescription(dependency.Dependent),
		)
	}

	return result, nil
}

func sourceSafetyExpectedRetainedObjects(
	retained []sourceSafetyProtectedAddress,
) []sourceSafetyExpectedRetainedObject {
	result := make([]sourceSafetyExpectedRetainedObject, 0, len(retained))
	for _, object := range retained {
		result = append(result, sourceSafetyExpectedRetainedObject{
			TableRelationOID: object.tableRelationOID,
			Address:          object.address,
		})
	}
	return result
}

func buildSourceSafetyProtectedAddresses(
	inventory schema.CatalogInventory,
	proposedRelations map[uint32]schema.CatalogRelation,
	expected []sourceSafetyExpectedRetainedObject,
) ([]sourceSafetyProtectedAddress, []sourceSafetyProtectedAddress, error) {
	var protected []sourceSafetyProtectedAddress
	var retained []sourceSafetyProtectedAddress
	for _, relation := range sortedSourceSafetyRelations(proposedRelations) {
		relationOID := relation.OID
		addresses := []schema.CatalogDependencyObject{{
			ClassOID: pgClassCatalogOID, ObjectOID: relationOID, ObjectType: string(relation.Kind),
			SchemaName: relation.SchemaName, Name: relation.Name,
		}}
		move, err := inventory.ExpectedTableMove(relationOID)
		if err != nil {
			return nil, nil, fmt.Errorf("building expected retained objects for %s: %w",
				catalogRelationDescription(relation), err)
		}
		for _, object := range slices.Concat(
			move.CleanupSchemaObjects, move.AttachedObjects, move.ExplicitMoveObjects, move.InternalObjects,
		) {
			address, ok := sourceSafetyCatalogAddress(object)
			if !ok {
				return nil, nil, fmt.Errorf(
					"building expected retained objects for %s: unsupported expected object kind %q",
					catalogRelationDescription(relation), object.Kind,
				)
			}
			addresses = append(addresses, address)
		}
		for _, address := range addresses {
			entry := sourceSafetyProtectedAddress{address: address, tableRelationOID: relationOID}
			protected = append(protected, entry)
			retained = append(retained, entry)
		}
	}

	for _, object := range expected {
		if _, exists := proposedRelations[object.TableRelationOID]; !exists {
			return nil, nil, fmt.Errorf(
				"expected retained object %s refers to non-proposed table relation OID %d",
				sourceSafetyCatalogObjectDescription(object.Address), object.TableRelationOID,
			)
		}
		if object.Address.ClassOID == 0 || object.Address.ObjectOID == 0 {
			return nil, nil, fmt.Errorf(
				"expected retained object for table relation OID %d has an incomplete catalog address",
				object.TableRelationOID,
			)
		}
		entry := sourceSafetyProtectedAddress{
			address:          object.Address,
			tableRelationOID: object.TableRelationOID,
		}
		protected = append(protected, entry)
		retained = append(retained, entry)
	}

	protected = normalizeSourceSafetyProtectedAddresses(protected)
	retained = normalizeSourceSafetyProtectedAddresses(retained)
	return protected, retained, nil
}

func sourceSafetyCatalogAddress(object schema.CatalogObjectIdentity) (schema.CatalogDependencyObject, bool) {
	classOID := uint32(0)
	switch object.Kind {
	case schema.CatalogObjectKindRelation, schema.CatalogObjectKindIndex,
		schema.CatalogObjectKindOwnedSequence, schema.CatalogObjectKindToastRelation:
		classOID = pgClassCatalogOID
	case schema.CatalogObjectKindRowType, schema.CatalogObjectKindArrayType:
		classOID = pgTypeCatalogOID
	case schema.CatalogObjectKindConstraint:
		classOID = pgConstraintCatalogOID
	case schema.CatalogObjectKindTrigger:
		classOID = pgTriggerCatalogOID
	case schema.CatalogObjectKindRule:
		classOID = pgRewriteCatalogOID
	case schema.CatalogObjectKindPolicy:
		classOID = pgPolicyCatalogOID
	case schema.CatalogObjectKindExtendedStatistic:
		classOID = pgStatisticExtCatalogOID
	default:
		return schema.CatalogDependencyObject{}, false
	}
	return schema.CatalogDependencyObject{
		ClassOID: classOID, ObjectOID: object.OID, SchemaName: object.SchemaName, Name: object.Name,
	}, true
}

func normalizeSourceSafetyProtectedAddresses(addresses []sourceSafetyProtectedAddress) []sourceSafetyProtectedAddress {
	addresses = slices.Clone(addresses)
	slices.SortFunc(addresses, func(a, b sourceSafetyProtectedAddress) int {
		return cmp.Or(
			cmp.Compare(a.address.ClassOID, b.address.ClassOID),
			cmp.Compare(a.address.ObjectOID, b.address.ObjectOID),
			cmp.Compare(a.address.SubObjectID, b.address.SubObjectID),
			cmp.Compare(a.tableRelationOID, b.tableRelationOID),
		)
	})
	return slices.CompactFunc(addresses, func(a, b sourceSafetyProtectedAddress) bool {
		return a.address.ClassOID == b.address.ClassOID &&
			a.address.ObjectOID == b.address.ObjectOID &&
			a.address.SubObjectID == b.address.SubObjectID &&
			a.tableRelationOID == b.tableRelationOID
	})
}

func sourceSafetyReferencedTable(
	referenced schema.CatalogDependencyObject,
	protected []sourceSafetyProtectedAddress,
) (uint32, bool) {
	for _, object := range protected {
		if sourceSafetyAddressContains(object.address, referenced) {
			return object.tableRelationOID, true
		}
	}
	return 0, false
}

func sourceSafetyAddressIsRetained(
	dependent schema.CatalogDependencyObject,
	retained []sourceSafetyProtectedAddress,
) bool {
	for _, object := range retained {
		if sourceSafetyAddressContains(object.address, dependent) {
			return true
		}
	}
	return false
}

func sourceSafetyAddressContains(expected, actual schema.CatalogDependencyObject) bool {
	return expected.ClassOID == actual.ClassOID && expected.ObjectOID == actual.ObjectOID &&
		(expected.SubObjectID == 0 || expected.SubObjectID == actual.SubObjectID)
}

func isSourceSafetyAttachedTableObject(dependency schema.CatalogDependency, relationOID uint32) bool {
	dependent := dependency.Dependent
	if dependent.ClassOID == pgClassCatalogOID && dependent.ObjectOID == relationOID {
		return true
	}
	// An automatic pg_attrdef-to-column edge identifies a default attached to the
	// proposed table. Other default dependencies remain unsupported and fail closed.
	return dependent.ClassOID == pgAttrDefCatalogOID && dependent.ObjectType == "default value" &&
		dependency.Type == "a" && dependency.Referenced.ClassOID == pgClassCatalogOID &&
		dependency.Referenced.ObjectOID == relationOID
}

func resolveSourceSafetyDependent(
	dependent schema.CatalogDependencyObject,
	current schema.SchemaSnapshot,
	target schema.SchemaSnapshot,
) (resolvedSourceSafetyDependent, error) {
	if routine, found := uniqueSourceSafetyRoutineByOID(current.Inventory, dependent.ObjectOID); found {
		return resolveSourceSafetyRoutine(routine, current.Schema, target.Schema)
	}
	if rule, found := uniqueSourceSafetyRuleByOID(current.Inventory, dependent.ObjectOID); found {
		return resolveSourceSafetyRule(rule, current, target)
	}
	if view, found := uniqueSourceSafetyViewByRelationOID(current.Inventory, dependent.ObjectOID); found {
		return resolveSourceSafetyView(view, current.Schema, target.Schema)
	}
	if relation, found := sourceSafetyRelationByOID(current.Inventory, dependent.ObjectOID); found {
		return resolveSourceSafetyRelationConsumer(relation, current.Schema, target.Schema)
	}
	if catalogType, found := uniqueSourceSafetyTypeByOID(current.Inventory, dependent.ObjectOID); found {
		return resolvedSourceSafetyDependent{
			identity: sourceSafetyDependentIdentity{
				Kind: sourceSafetyIncomingDependencyKindRowTypeConsumer, SchemaName: catalogType.SchemaName,
				Name: catalogType.Name, Identity: catalogType.SchemaName + "." + catalogType.Name,
				Definition: string(catalogType.Kind), CatalogClass: pgTypeCatalogOID,
			},
			managedScope: sourceSafetyManagedScopeExcluded,
			targetIntent: sourceSafetyTargetIntentPersistent,
		}, nil
	}

	return resolvedSourceSafetyDependent{}, fmt.Errorf(
		"unsupported catalog dependency from %s", sourceSafetyCatalogObjectDescription(dependent),
	)
}

func resolveSourceSafetyView(
	view schema.CatalogView,
	current schema.Schema,
	target schema.Schema,
) (resolvedSourceSafetyDependent, error) {
	kind := sourceSafetyIncomingDependencyKindView
	currentCount := countModeledViews(current, view)
	targetCount := countModeledViews(target, view)
	if view.Kind == schema.RelKindMaterializedView {
		kind = sourceSafetyIncomingDependencyKindMaterializedView
	}
	if currentCount > 1 || targetCount > 1 {
		return resolvedSourceSafetyDependent{}, fmt.Errorf(
			"ambiguous modeled match for %s %s.%s", kind, view.SchemaName, view.Name,
		)
	}
	managedScope, targetIntent := sourceSafetyScopeAndIntent(currentCount == 1, targetCount == 1)
	return resolvedSourceSafetyDependent{
		identity: sourceSafetyDependentIdentity{
			Kind: kind, SchemaName: view.SchemaName, Name: view.Name,
			Identity: view.SchemaName + "." + view.Name, Definition: view.Definition,
			CatalogClass: pgClassCatalogOID,
		},
		managedScope: managedScope, targetIntent: targetIntent,
	}, nil
}

func resolveSourceSafetyRule(
	rule schema.CatalogRule,
	current schema.SchemaSnapshot,
	target schema.SchemaSnapshot,
) (resolvedSourceSafetyDependent, error) {
	relation, err := uniqueSourceSafetyRelation(current.Inventory, rule.RelationOID)
	if err != nil {
		return resolvedSourceSafetyDependent{},
			fmt.Errorf("resolving rule %q owner: %w", rule.Name, err)
	}
	if view, found := uniqueSourceSafetyViewByRelationOID(current.Inventory, rule.RelationOID); found {
		return resolveSourceSafetyView(view, current.Schema, target.Schema)
	}

	currentCount := countModeledTables(current.Schema, relation.SchemaName, relation.Name)
	targetCount := countModeledTables(target.Schema, relation.SchemaName, relation.Name)
	if currentCount > 1 || targetCount > 1 {
		return resolvedSourceSafetyDependent{}, fmt.Errorf(
			"ambiguous modeled match for rule owner %s.%s", relation.SchemaName, relation.Name,
		)
	}
	managed := currentCount == 1 && targetCount == 0
	managedScope, targetIntent := sourceSafetyScopeAndIntent(managed, false)
	return resolvedSourceSafetyDependent{
		identity: sourceSafetyDependentIdentity{
			Kind: sourceSafetyIncomingDependencyKindRule, SchemaName: relation.SchemaName, Name: rule.Name,
			Identity:   fmt.Sprintf("%s on %s.%s", rule.Name, relation.SchemaName, relation.Name),
			Definition: rule.Definition, CatalogClass: pgRewriteCatalogOID,
		},
		managedScope: managedScope, targetIntent: targetIntent,
	}, nil
}

func resolveSourceSafetyRelationConsumer(
	relation schema.CatalogRelation,
	current schema.Schema,
	target schema.Schema,
) (resolvedSourceSafetyDependent, error) {
	currentCount := countModeledTables(current, relation.SchemaName, relation.Name)
	targetCount := countModeledTables(target, relation.SchemaName, relation.Name)
	if currentCount > 1 || targetCount > 1 {
		return resolvedSourceSafetyDependent{}, fmt.Errorf(
			"ambiguous modeled match for row-type consumer %s.%s", relation.SchemaName, relation.Name,
		)
	}
	managedScope, targetIntent := sourceSafetyScopeAndIntent(currentCount == 1, targetCount == 1)
	return resolvedSourceSafetyDependent{
		identity: sourceSafetyDependentIdentity{
			Kind: sourceSafetyIncomingDependencyKindRowTypeConsumer, SchemaName: relation.SchemaName,
			Name: relation.Name, Identity: relation.SchemaName + "." + relation.Name,
			Definition: string(relation.Kind), CatalogClass: pgClassCatalogOID,
		},
		managedScope: managedScope, targetIntent: targetIntent,
	}, nil
}

func resolveSourceSafetyRoutine(
	routine schema.CatalogRoutine,
	current schema.Schema,
	target schema.Schema,
) (resolvedSourceSafetyDependent, error) {
	currentCount := countModeledRoutines(current, routine)
	targetCount := countModeledRoutines(target, routine)
	if currentCount > 1 || targetCount > 1 {
		return resolvedSourceSafetyDependent{}, fmt.Errorf(
			"ambiguous modeled match for routine %s", catalogRoutineDescription(routine),
		)
	}
	managedScope, targetIntent := sourceSafetyScopeAndIntent(currentCount == 1, targetCount == 1)
	return resolvedSourceSafetyDependent{
		identity: sourceSafetyDependentIdentity{
			Kind: sourceSafetyIncomingDependencyKindRoutine, SchemaName: routine.SchemaName,
			Name: routine.Name, Identity: catalogRoutineDescription(routine), Definition: routine.Definition,
			CatalogClass: pgProcCatalogOID, RoutineBodyForm: routine.BodyForm,
			RoutineReferenceTrackability: routine.ReferenceTrackability,
		},
		managedScope: managedScope, targetIntent: targetIntent,
	}, nil
}

func sourceSafetyScopeAndIntent(managed, targetPresent bool) (sourceSafetyManagedScope, sourceSafetyTargetIntent) {
	if managed {
		if !targetPresent {
			return sourceSafetyManagedScopeManaged,
				sourceSafetyTargetIntentExplicitlyAbsent
		}
		return sourceSafetyManagedScopeManaged, sourceSafetyTargetIntentPersistent
	}
	return sourceSafetyManagedScopeExcluded, sourceSafetyTargetIntentPersistent
}

func classifySourceSafetyForeignKeyIntent(
	foreignKey schema.CatalogForeignKey,
	current schema.Schema,
	target schema.Schema,
) (sourceSafetyManagedScope, sourceSafetyTargetIntent, error) {
	currentCount := countModeledForeignKeys(current, foreignKey)
	targetCount := countModeledForeignKeys(target, foreignKey)
	if currentCount > 1 || targetCount > 1 {
		return "", "", fmt.Errorf("ambiguous modeled foreign-key identity")
	}
	managedScope, targetIntent := sourceSafetyScopeAndIntent(currentCount == 1, targetCount == 1)
	return managedScope, targetIntent, nil
}

func classifySourceSafetyForeignKeyDirection(
	foreignKey schema.CatalogForeignKey,
	proposed map[uint32]schema.CatalogRelation,
) (sourceSafetyForeignKeyDirection, bool) {
	_, owningProposed := proposed[foreignKey.OwningRelationOID]
	_, referencedProposed := proposed[foreignKey.ReferencedRelationOID]
	switch {
	case foreignKey.OwningRelationOID == foreignKey.ReferencedRelationOID && owningProposed:
		return sourceSafetyForeignKeyDirectionSelf, true
	case owningProposed && referencedProposed:
		return sourceSafetyForeignKeyDirectionInternal, true
	case owningProposed:
		return sourceSafetyForeignKeyDirectionOutgoing, true
	case referencedProposed:
		return sourceSafetyForeignKeyDirectionIncoming, true
	default:
		return "", false
	}
}

func validateNoExtensionChangesDuringArchival(
	current schema.CatalogInventory,
	target schema.CatalogInventory,
	relation schema.CatalogRelation,
) error {
	changes, err := sourceSafetyExtensionChanges(current.Extensions, target.Extensions)
	if err != nil {
		return fmt.Errorf("checking extension changes before archiving %s: %w",
			catalogRelationDescription(relation), err)
	}
	if len(changes) > 0 {
		return fmt.Errorf(
			"cannot archive table %s while extension %q is %s",
			catalogRelationDescription(relation), changes[0].name, changes[0].change,
		)
	}
	return nil
}

type sourceSafetyExtensionChange struct {
	name   string
	change string
}

func sourceSafetyExtensionChanges(
	current []schema.CatalogExtensionIdentity,
	target []schema.CatalogExtensionIdentity,
) ([]sourceSafetyExtensionChange, error) {
	currentByName, err := uniqueSourceSafetyExtensions(current)
	if err != nil {
		return nil, err
	}
	targetByName, err := uniqueSourceSafetyExtensions(target)
	if err != nil {
		return nil, err
	}

	nameSet := make(map[string]struct{}, len(currentByName)+len(targetByName))
	for name := range currentByName {
		nameSet[name] = struct{}{}
	}
	for name := range targetByName {
		nameSet[name] = struct{}{}
	}
	names := make([]string, 0, len(nameSet))
	for name := range nameSet {
		names = append(names, name)
	}
	slices.Sort(names)

	var changes []sourceSafetyExtensionChange
	for _, name := range names {
		currentExtension, inCurrent := currentByName[name]
		targetExtension, inTarget := targetByName[name]
		switch {
		case !inCurrent:
			changes = append(changes, sourceSafetyExtensionChange{name: name, change: "created"})
		case !inTarget:
			changes = append(changes, sourceSafetyExtensionChange{name: name, change: "dropped"})
		case currentExtension.Version != targetExtension.Version ||
			currentExtension.SchemaName != targetExtension.SchemaName:
			changes = append(changes, sourceSafetyExtensionChange{name: name, change: "updated"})
		}
	}
	return changes, nil
}

func validateNoEnabledEventTriggers(inventory schema.CatalogInventory, relation schema.CatalogRelation) error {
	for _, eventTrigger := range inventory.EventTriggers {
		if eventTrigger.EnabledMode == "D" {
			continue
		}
		return fmt.Errorf(
			"cannot archive table %s while event trigger %q is enabled in mode %q",
			catalogRelationDescription(relation), eventTrigger.Name, eventTrigger.EnabledMode,
		)
	}
	return nil
}

func validateNoAllTablesPublications(inventory schema.CatalogInventory, relation schema.CatalogRelation) error {
	for _, publication := range inventory.Publications {
		if publication.PublishesAllTables {
			return fmt.Errorf(
				"cannot archive table %s while FOR ALL TABLES publication %q exists",
				catalogRelationDescription(relation), publication.Name,
			)
		}
	}
	return nil
}

func validateNoExtensionMemberRetainedObjects(
	inventory schema.CatalogInventory,
	proposedRelations map[uint32]schema.CatalogRelation,
	retained []sourceSafetyProtectedAddress,
) error {
	for _, relation := range sortedSourceSafetyRelations(proposedRelations) {
		if relation.Extension != nil {
			return fmt.Errorf(
				"cannot archive extension member table %s owned by extension %q",
				catalogRelationDescription(relation), relation.Extension.Name,
			)
		}
	}
	for _, member := range inventory.ExtensionMembers {
		for _, retainedObject := range retained {
			if sourceSafetyAddressContains(retainedObject.address, member.Object) {
				relation := proposedRelations[retainedObject.tableRelationOID]
				return fmt.Errorf(
					"cannot archive table %s because retained object %s is a member of extension %q",
					catalogRelationDescription(relation),
					sourceSafetyCatalogObjectDescription(member.Object),
					member.ExtensionName,
				)
			}
		}
	}
	return nil
}

func sourceSafetyPublicationRelations(
	inventory schema.CatalogInventory,
	proposed map[uint32]schema.CatalogRelation,
) []schema.CatalogPublicationRelation {
	var result []schema.CatalogPublicationRelation
	for _, membership := range inventory.PublicationRelations {
		if _, included := proposed[membership.RelationOID]; included {
			result = append(result, membership)
		}
	}
	return result
}

func sourceSafetyPublicationSchemas(
	inventory schema.CatalogInventory,
	proposed map[uint32]schema.CatalogRelation,
) []schema.CatalogPublicationSchema {
	proposedSchemas := make(map[string]struct{})
	for _, relation := range proposed {
		proposedSchemas[relation.SchemaName] = struct{}{}
	}
	var result []schema.CatalogPublicationSchema
	for _, membership := range inventory.PublicationSchemas {
		if _, included := proposedSchemas[membership.SchemaName]; included {
			result = append(result, membership)
		}
	}
	return result
}

func validateNoChangedExtensionOwnsTable(
	current schema.SchemaSnapshot,
	target schema.SchemaSnapshot,
) error {
	changes, err := sourceSafetyExtensionChanges(
		current.Inventory.Extensions, target.Inventory.Extensions,
	)
	if err != nil {
		return fmt.Errorf("checking extension-owned table safety: %w", err)
	}
	changedExistingExtensions := make(map[string]string)
	currentNames := make(map[string]struct{}, len(current.Inventory.Extensions))
	for _, extension := range current.Inventory.Extensions {
		currentNames[extension.Name] = struct{}{}
	}
	for _, change := range changes {
		if _, existed := currentNames[change.name]; existed && change.change != "created" {
			changedExistingExtensions[change.name] = change.change
		}
	}
	if len(changedExistingExtensions) == 0 {
		return nil
	}

	relations := current.Inventory.Normalize().Relations
	for _, relation := range relations {
		if relation.Extension == nil || !sourceSafetyExtensionTableKind(relation.Kind) {
			continue
		}
		change, changed := changedExistingExtensions[relation.Extension.Name]
		if !changed {
			continue
		}
		return fmt.Errorf(
			"cannot generate extension %s for %q: extension owns table-like relation %s",
			change, relation.Extension.Name, catalogRelationDescription(relation),
		)
	}
	return nil
}

func sourceSafetyExtensionTableKind(kind schema.RelKind) bool {
	return kind == schema.RelKindOrdinaryTable || kind == schema.RelKindPartitionedTable ||
		kind == schema.RelKindForeignTable
}

func uniqueSourceSafetyExtensions(
	extensions []schema.CatalogExtensionIdentity,
) (map[string]schema.CatalogExtensionIdentity, error) {
	result := make(map[string]schema.CatalogExtensionIdentity, len(extensions))
	for _, extension := range extensions {
		if _, duplicate := result[extension.Name]; duplicate {
			return nil, fmt.Errorf("extension identity %q is duplicated", extension.Name)
		}
		result[extension.Name] = extension
	}
	return result, nil
}

func uniqueSourceSafetyRelation(
	inventory schema.CatalogInventory,
	oid uint32,
) (schema.CatalogRelation, error) {
	var matches []schema.CatalogRelation
	for _, relation := range inventory.Relations {
		if relation.OID == oid {
			matches = append(matches, relation)
		}
	}
	if len(matches) == 0 {
		return schema.CatalogRelation{}, fmt.Errorf(
			"relation OID %d is not present in source inventory", oid,
		)
	}
	if len(matches) > 1 {
		return schema.CatalogRelation{}, fmt.Errorf(
			"relation OID %d is ambiguous in source inventory", oid,
		)
	}
	return matches[0], nil
}

func sourceSafetyRelationByOID(
	inventory schema.CatalogInventory,
	oid uint32,
) (schema.CatalogRelation, bool) {
	var found schema.CatalogRelation
	count := 0
	for _, relation := range inventory.Relations {
		if relation.OID == oid {
			found = relation
			count++
		}
	}
	return found, count == 1
}

func uniqueSourceSafetyRoutineByOID(
	inventory schema.CatalogInventory,
	oid uint32,
) (schema.CatalogRoutine, bool) {
	var found schema.CatalogRoutine
	count := 0
	for _, routine := range inventory.Routines {
		if routine.OID == oid {
			found = routine
			count++
		}
	}
	return found, count == 1
}

func uniqueSourceSafetyRuleByOID(
	inventory schema.CatalogInventory,
	oid uint32,
) (schema.CatalogRule, bool) {
	var found schema.CatalogRule
	count := 0
	for _, rule := range inventory.Rules {
		if rule.OID == oid {
			found = rule
			count++
		}
	}
	return found, count == 1
}

func uniqueSourceSafetyViewByRelationOID(
	inventory schema.CatalogInventory,
	oid uint32,
) (schema.CatalogView, bool) {
	var found schema.CatalogView
	count := 0
	for _, view := range inventory.Views {
		if view.RelationOID == oid {
			found = view
			count++
		}
	}
	return found, count == 1
}

func uniqueSourceSafetyTypeByOID(
	inventory schema.CatalogInventory,
	oid uint32,
) (schema.CatalogType, bool) {
	var found schema.CatalogType
	count := 0
	for _, catalogType := range inventory.Types {
		if catalogType.OID == oid {
			found = catalogType
			count++
		}
	}
	return found, count == 1
}

func countModeledViews(modeled schema.Schema, view schema.CatalogView) int {
	count := 0
	escapedName := schema.EscapeIdentifier(view.Name)
	if view.Kind == schema.RelKindMaterializedView {
		for _, candidate := range modeled.MaterializedViews {
			if candidate.SchemaName == view.SchemaName && candidate.EscapedName == escapedName {
				count++
			}
		}
		return count
	}
	for _, candidate := range modeled.Views {
		if candidate.SchemaName == view.SchemaName && candidate.EscapedName == escapedName {
			count++
		}
	}
	return count
}

func countModeledTables(modeled schema.Schema, schemaName, name string) int {
	count := 0
	escapedName := schema.EscapeIdentifier(name)
	for _, table := range modeled.Tables {
		if table.SchemaName == schemaName && table.EscapedName == escapedName {
			count++
		}
	}
	return count
}

func countModeledRoutines(modeled schema.Schema, routine schema.CatalogRoutine) int {
	count := 0
	escapedName := fmt.Sprintf("%s(%s)", schema.EscapeIdentifier(routine.Name), routine.IdentityArguments)
	switch routine.Kind {
	case "f":
		for _, function := range modeled.Functions {
			if function.SchemaName == routine.SchemaName && function.EscapedName == escapedName {
				count++
			}
		}
	case "p":
		for _, procedure := range modeled.Procedures {
			if procedure.SchemaName == routine.SchemaName && procedure.EscapedName == escapedName {
				count++
			}
		}
	}
	return count
}

func countModeledForeignKeys(modeled schema.Schema, foreignKey schema.CatalogForeignKey) int {
	count := 0
	escapedConstraintName := schema.EscapeIdentifier(foreignKey.Name)
	escapedOwningTableName := schema.EscapeIdentifier(foreignKey.OwningRelationName)
	for _, candidate := range modeled.ForeignKeyConstraints {
		if candidate.EscapedName == escapedConstraintName &&
			candidate.OwningTable.SchemaName == foreignKey.OwningSchemaName &&
			candidate.OwningTable.EscapedName == escapedOwningTableName {
			count++
		}
	}
	return count
}

func sourceSafetyIncomingDependencyKey(dependency sourceSafetyIncomingDependency) string {
	return fmt.Sprintf("%010d\x00%s\x00%s\x00%s\x00%d",
		dependency.ProposedTableRelationOID, dependency.Dependent.Kind,
		dependency.Dependent.SchemaName, dependency.Dependent.Identity,
		dependency.Dependent.CatalogClass)
}

func compareSourceSafetyIncomingDependencies(a, b sourceSafetyIncomingDependency) int {
	return cmp.Or(
		cmp.Compare(a.ProposedTableRelationOID, b.ProposedTableRelationOID),
		cmp.Compare(a.Dependent.Kind, b.Dependent.Kind),
		cmp.Compare(a.Dependent.SchemaName, b.Dependent.SchemaName),
		cmp.Compare(a.Dependent.Identity, b.Dependent.Identity),
		cmp.Compare(a.Dependent.CatalogClass, b.Dependent.CatalogClass),
	)
}

func compareSourceSafetyForeignKeys(a, b sourceSafetyForeignKey) int {
	return cmp.Or(
		cmp.Compare(a.ForeignKey.OwningSchemaName, b.ForeignKey.OwningSchemaName),
		cmp.Compare(a.ForeignKey.OwningRelationName, b.ForeignKey.OwningRelationName),
		cmp.Compare(a.ForeignKey.Name, b.ForeignKey.Name),
		cmp.Compare(a.ForeignKey.OID, b.ForeignKey.OID),
	)
}

func compareSourceSafetyCatalogDependencies(a, b schema.CatalogDependency) int {
	return cmp.Or(
		cmp.Compare(a.Referenced.SchemaName, b.Referenced.SchemaName),
		cmp.Compare(a.Referenced.Identity, b.Referenced.Identity),
		cmp.Compare(a.Referenced.ObjectType, b.Referenced.ObjectType),
		cmp.Compare(a.Dependent.SchemaName, b.Dependent.SchemaName),
		cmp.Compare(a.Dependent.Identity, b.Dependent.Identity),
		cmp.Compare(a.Dependent.ObjectType, b.Dependent.ObjectType),
		cmp.Compare(a.Dependent.ClassOID, b.Dependent.ClassOID),
		cmp.Compare(a.Dependent.SubObjectID, b.Dependent.SubObjectID),
		cmp.Compare(a.Dependent.ObjectOID, b.Dependent.ObjectOID),
		cmp.Compare(a.Type, b.Type),
	)
}

func sortedSourceSafetyRelations(
	relationsByOID map[uint32]schema.CatalogRelation,
) []schema.CatalogRelation {
	relations := make([]schema.CatalogRelation, 0, len(relationsByOID))
	for _, relation := range relationsByOID {
		relations = append(relations, relation)
	}
	slices.SortFunc(relations, func(a, b schema.CatalogRelation) int {
		return cmp.Or(
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.Kind, b.Kind),
			cmp.Compare(a.OID, b.OID),
		)
	})
	return relations
}

func catalogRelationDescription(relation schema.CatalogRelation) string {
	return fmt.Sprintf("%s.%s (OID %d)", relation.SchemaName, relation.Name, relation.OID)
}

func catalogRoutineDescription(routine schema.CatalogRoutine) string {
	return fmt.Sprintf("%s.%s(%s)", routine.SchemaName, routine.Name, routine.IdentityArguments)
}

func sourceSafetyDependentDescription(identity sourceSafetyDependentIdentity) string {
	if identity.Identity != "" {
		return fmt.Sprintf("%q", identity.Identity)
	}
	return fmt.Sprintf("%s.%s", identity.SchemaName, identity.Name)
}

func sourceSafetyCatalogObjectDescription(object schema.CatalogDependencyObject) string {
	parts := []string{object.ObjectType}
	if object.Identity != "" {
		parts = append(parts, fmt.Sprintf("%q", object.Identity))
	} else if object.SchemaName != "" || object.Name != "" {
		parts = append(parts, fmt.Sprintf("%q",
			strings.TrimPrefix(object.SchemaName+"."+object.Name, ".")))
	}
	parts = append(parts, fmt.Sprintf("(class OID %d, object OID %d, sub-object %d)",
		object.ClassOID, object.ObjectOID, object.SubObjectID))
	return strings.Join(parts, " ")
}
