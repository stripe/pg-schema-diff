package diff

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/stripe/pg-schema-diff/internal/pgidentifier"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

const (
	archivalNonceLength  = 8
	archivalTimestampLen = 22
)

type archivalGroupID string

// archivalGroupNameAllocationRequest describes only the physical relations that
// need names. Retention and dependency-closure decisions belong to later stages.
type archivalGroupNameAllocationRequest struct {
	RootRelationOID uint32
	Members         []archivalPhysicalMemberNameAllocationRequest
}

type archivalPhysicalMemberNameAllocationRequest struct {
	RelationOID uint32
	// ExplicitDependencyObjects contains schema-scoped objects assigned to this
	// otherwise-empty destination in addition to the table's expected move set.
	ExplicitDependencyObjects []schema.CatalogObjectIdentity
}

type archivalGroupNameAllocation struct {
	GroupID                     archivalGroupID
	Nonce                       string
	Timestamp                   string
	DependencySchemaName        string
	EscapedDependencySchemaName string
	Members                     []archivalPhysicalMemberNameAllocation
}

type archivalPhysicalMemberNameAllocation struct {
	RelationOID              uint32
	SourceSchemaName         string
	SourceTableName          string
	CleanupSchemaName        string
	EscapedCleanupSchemaName string
}

type preparedArchivalGroupNameRequest struct {
	root    schema.CatalogRelation
	members []preparedArchivalMemberNameRequest
}

type preparedArchivalMemberNameRequest struct {
	relation schema.CatalogRelation
	objects  []schema.CatalogObjectIdentity
}

// allocateArchivalNames builds and validates every allocation before returning.
// It is dormant until the archival graph is activated in a later delivery stage.
func allocateArchivalNames(
	planOpts *planOptions,
	currentInventory schema.CatalogInventory,
	targetInventory schema.CatalogInventory,
	requests []archivalGroupNameAllocationRequest,
) ([]archivalGroupNameAllocation, error) {
	if planOpts == nil {
		return nil, fmt.Errorf("allocating archival names: plan options are required")
	}
	if err := validateSchemaPartialArchivalPrefix(planOpts.schemaPartialArchivalPrefix); err != nil {
		return nil, err
	}
	if planOpts.randReader == nil {
		return nil, fmt.Errorf("allocating archival names: random reader is required")
	}

	timestamp := formatArchivalTimestamp(planOpts.generationTimestamp)
	if len(timestamp) != archivalTimestampLen {
		return nil, fmt.Errorf("formatting archival timestamp %q: expected %d bytes, got %d",
			planOpts.generationTimestamp, archivalTimestampLen, len(timestamp))
	}

	prepared, err := prepareArchivalNameRequests(currentInventory, requests)
	if err != nil {
		return nil, err
	}

	allocations := make([]archivalGroupNameAllocation, 0, len(prepared))
	for _, group := range prepared {
		nonce, err := pgidentifier.RandomIdentifierSuffix(planOpts.randReader, archivalNonceLength)
		if err != nil {
			return nil, fmt.Errorf("allocating archival group for %s.%s: %w",
				group.root.SchemaName, group.root.Name, err)
		}

		allocation := archivalGroupNameAllocation{
			GroupID:   archivalGroupID(timestamp + "_" + nonce),
			Nonce:     nonce,
			Timestamp: timestamp,
			Members:   make([]archivalPhysicalMemberNameAllocation, 0, len(group.members)),
		}
		dependencySchemaName, err := buildArchivalDependencySchemaName(
			planOpts.schemaPartialArchivalPrefix, timestamp, nonce,
		)
		if err != nil {
			return nil, fmt.Errorf("allocating dependency schema for relation %s.%s: %w",
				group.root.SchemaName, group.root.Name, err)
		}
		allocation.DependencySchemaName = dependencySchemaName
		allocation.EscapedDependencySchemaName =
			schema.EscapeIdentifier(dependencySchemaName)
		for _, member := range group.members {
			cleanupSchemaName, err := buildArchivalSchemaName(
				planOpts.schemaPartialArchivalPrefix,
				member.relation.SchemaName,
				member.relation.Name,
				timestamp,
				nonce,
			)
			if err != nil {
				return nil, fmt.Errorf("allocating cleanup schema for relation %s.%s: %w",
					member.relation.SchemaName, member.relation.Name, err)
			}
			allocation.Members = append(allocation.Members, archivalPhysicalMemberNameAllocation{
				RelationOID:              member.relation.OID,
				SourceSchemaName:         member.relation.SchemaName,
				SourceTableName:          member.relation.Name,
				CleanupSchemaName:        cleanupSchemaName,
				EscapedCleanupSchemaName: schema.EscapeIdentifier(cleanupSchemaName),
			})
		}
		allocations = append(allocations, allocation)
	}

	if err := validateArchivalNameAllocations(
		planOpts.schemaPartialArchivalPrefix,
		currentInventory,
		targetInventory,
		prepared,
		allocations,
	); err != nil {
		return nil, err
	}
	return allocations, nil
}

func buildArchivalDependencySchemaName(prefix, timestamp, nonce string) (string, error) {
	return buildArchivalSchemaName(prefix, "dep", "o", timestamp, nonce)
}

func prepareArchivalNameRequests(
	inventory schema.CatalogInventory,
	requests []archivalGroupNameAllocationRequest,
) ([]preparedArchivalGroupNameRequest, error) {
	prepared := make([]preparedArchivalGroupNameRequest, 0, len(requests))
	seenRelations := make(map[uint32]struct{})
	for _, request := range requests {
		root := catalogRelationWithOID(inventory, request.RootRelationOID)
		if root == nil {
			return nil, fmt.Errorf("archival group root relation OID %d is not present in the catalog inventory",
				request.RootRelationOID)
		}
		if len(request.Members) == 0 {
			return nil, fmt.Errorf("archival group rooted at %s.%s has no physical members",
				root.SchemaName, root.Name)
		}

		group := preparedArchivalGroupNameRequest{root: *root}
		memberOIDs := make(map[uint32]struct{}, len(request.Members))
		for _, memberRequest := range request.Members {
			if _, duplicate := memberOIDs[memberRequest.RelationOID]; duplicate {
				return nil, fmt.Errorf("archival group rooted at %s.%s contains relation OID %d more than once",
					root.SchemaName, root.Name, memberRequest.RelationOID)
			}
			memberOIDs[memberRequest.RelationOID] = struct{}{}
			if _, duplicate := seenRelations[memberRequest.RelationOID]; duplicate {
				return nil, fmt.Errorf("relation OID %d belongs to more than one archival group",
					memberRequest.RelationOID)
			}
			seenRelations[memberRequest.RelationOID] = struct{}{}

			relation := catalogRelationWithOID(inventory, memberRequest.RelationOID)
			if relation == nil {
				return nil, fmt.Errorf("archival member relation OID %d is not present in the catalog inventory",
					memberRequest.RelationOID)
			}
			kind, tableLike := inventory.ClassifyTable(relation.OID)
			if !tableLike || !isSupportedArchivalTableKind(kind) {
				return nil, fmt.Errorf("relation OID %d (%s.%s) has unsupported archival classification %q",
					relation.OID, relation.SchemaName, relation.Name, kind)
			}
			move, err := inventory.ExpectedTableMove(relation.OID)
			if err != nil {
				return nil, fmt.Errorf("building expected move for relation %s.%s: %w",
					relation.SchemaName, relation.Name, err)
			}
			objects := append(slices.Clone(move.CleanupSchemaObjects), move.ExplicitMoveObjects...)
			objects = append(objects, memberRequest.ExplicitDependencyObjects...)
			slices.SortFunc(objects, compareArchivalCatalogObjectIdentity)
			group.members = append(group.members, preparedArchivalMemberNameRequest{
				relation: *relation, objects: objects,
			})
		}
		if _, rootIncluded := memberOIDs[root.OID]; !rootIncluded {
			return nil, fmt.Errorf("archival group root %s.%s is not included in its physical members",
				root.SchemaName, root.Name)
		}
		if err := validateArchivalGroupTopology(inventory, group); err != nil {
			return nil, err
		}
		slices.SortFunc(group.members, comparePreparedArchivalMember)
		prepared = append(prepared, group)
	}
	slices.SortFunc(prepared, func(a, b preparedArchivalGroupNameRequest) int {
		return compareCatalogRelations(a.root, b.root)
	})
	return prepared, nil
}

func isSupportedArchivalTableKind(kind schema.CatalogTableKind) bool {
	return kind == schema.CatalogTableKindOrdinary ||
		kind == schema.CatalogTableKindPartitioned ||
		kind == schema.CatalogTableKindDeclarativePartition
}

func validateArchivalGroupTopology(
	inventory schema.CatalogInventory,
	group preparedArchivalGroupNameRequest,
) error {
	rootKind, _ := inventory.ClassifyTable(group.root.OID)
	switch rootKind {
	case schema.CatalogTableKindOrdinary:
		if len(group.members) != 1 || group.members[0].relation.OID != group.root.OID {
			return fmt.Errorf("ordinary archival group rooted at %s.%s must contain only its root relation",
				group.root.SchemaName, group.root.Name)
		}
		return nil
	case schema.CatalogTableKindPartitioned:
		expected, err := buildCompletePartitionTreeArchivalNameAllocationRequest(inventory, group.root.OID)
		if err != nil {
			return err
		}
		actualOIDs := make([]uint32, 0, len(group.members))
		for _, member := range group.members {
			actualOIDs = append(actualOIDs, member.relation.OID)
		}
		expectedOIDs := make([]uint32, 0, len(expected.Members))
		for _, member := range expected.Members {
			expectedOIDs = append(expectedOIDs, member.RelationOID)
		}
		slices.Sort(actualOIDs)
		slices.Sort(expectedOIDs)
		if !slices.Equal(actualOIDs, expectedOIDs) {
			return fmt.Errorf("archival group rooted at %s.%s does not contain its complete declarative partition tree",
				group.root.SchemaName, group.root.Name)
		}
		return nil
	default:
		return fmt.Errorf("relation OID %d (%s.%s) cannot root an archival allocation with classification %q",
			group.root.OID, group.root.SchemaName, group.root.Name, rootKind)
	}
}

// buildCompletePartitionTreeArchivalNameAllocationRequest deterministically
// groups one complete declarative partition tree without making removal choices.
func buildCompletePartitionTreeArchivalNameAllocationRequest(
	inventory schema.CatalogInventory,
	rootRelationOID uint32,
) (archivalGroupNameAllocationRequest, error) {
	root := catalogRelationWithOID(inventory, rootRelationOID)
	if root == nil {
		return archivalGroupNameAllocationRequest{}, fmt.Errorf(
			"partition tree root relation OID %d is not present in the catalog inventory", rootRelationOID,
		)
	}
	rootKind, tableLike := inventory.ClassifyTable(rootRelationOID)
	if !tableLike || rootKind != schema.CatalogTableKindPartitioned {
		return archivalGroupNameAllocationRequest{}, fmt.Errorf(
			"partition tree root relation OID %d (%s.%s) has unsupported classification %q",
			root.OID, root.SchemaName, root.Name, rootKind,
		)
	}

	childrenByParent := make(map[uint32][]uint32)
	parentsByChild := make(map[uint32][]uint32)
	for _, edge := range inventory.InheritanceEdges {
		childrenByParent[edge.ParentRelationOID] =
			append(childrenByParent[edge.ParentRelationOID], edge.ChildRelationOID)
		parentsByChild[edge.ChildRelationOID] = append(
			parentsByChild[edge.ChildRelationOID], edge.ParentRelationOID,
		)
	}
	for parentOID := range childrenByParent {
		slices.SortFunc(childrenByParent[parentOID], func(a, b uint32) int {
			aRelation := catalogRelationWithOID(inventory, a)
			bRelation := catalogRelationWithOID(inventory, b)
			if aRelation == nil || bRelation == nil {
				return cmp.Compare(a, b)
			}
			return compareCatalogRelations(*aRelation, *bRelation)
		})
	}

	request := archivalGroupNameAllocationRequest{RootRelationOID: rootRelationOID}
	visiting := make(map[uint32]bool)
	visited := make(map[uint32]bool)
	var visit func(uint32) error
	visit = func(relationOID uint32) error {
		if visiting[relationOID] {
			return fmt.Errorf("declarative partition tree rooted at %s.%s contains an inheritance cycle",
				root.SchemaName, root.Name)
		}
		if visited[relationOID] {
			return nil
		}
		visiting[relationOID] = true
		relation := catalogRelationWithOID(inventory, relationOID)
		if relation == nil {
			return fmt.Errorf("partition tree rooted at %s.%s references missing relation OID %d",
				root.SchemaName, root.Name, relationOID)
		}
		if relationOID != rootRelationOID {
			kind, tableLike := inventory.ClassifyTable(relationOID)
			if !tableLike || kind != schema.CatalogTableKindDeclarativePartition {
				return fmt.Errorf("partition relation OID %d (%s.%s) has unsupported classification %q",
					relation.OID, relation.SchemaName, relation.Name, kind)
			}
			if len(parentsByChild[relationOID]) != 1 {
				return fmt.Errorf("partition relation OID %d (%s.%s) has %d inheritance parents; exactly one is required",
					relation.OID, relation.SchemaName, relation.Name, len(parentsByChild[relationOID]))
			}
		}

		request.Members = append(request.Members, archivalPhysicalMemberNameAllocationRequest{
			RelationOID: relationOID,
		})
		for _, childOID := range childrenByParent[relationOID] {
			if err := visit(childOID); err != nil {
				return err
			}
		}
		visiting[relationOID] = false
		visited[relationOID] = true
		return nil
	}
	if err := visit(rootRelationOID); err != nil {
		return archivalGroupNameAllocationRequest{}, err
	}
	slices.SortFunc(request.Members, func(a, b archivalPhysicalMemberNameAllocationRequest) int {
		aRelation := catalogRelationWithOID(inventory, a.RelationOID)
		bRelation := catalogRelationWithOID(inventory, b.RelationOID)
		return compareCatalogRelations(*aRelation, *bRelation)
	})
	return request, nil
}

func formatArchivalTimestamp(timestamp time.Time) string {
	timestamp = timestamp.UTC()
	return timestamp.Format("20060102T150405") + fmt.Sprintf("%06d", timestamp.Nanosecond()/1_000) + "Z"
}

func buildArchivalSchemaName(prefix, sourceSchema, sourceTable, timestamp, nonce string) (string, error) {
	fixedSize := len(prefix) + len(timestamp) + len(nonce) + 4
	componentBudget := maxPostgresIdentifierSize - fixedSize
	truncatedSchema, truncatedTable, err := truncateArchivalSourceComponents(
		sourceSchema, sourceTable, componentBudget,
	)
	if err != nil {
		return "", err
	}
	name := strings.Join([]string{prefix, truncatedSchema, truncatedTable, timestamp, nonce}, "_")
	if len(name) > maxPostgresIdentifierSize {
		return "", fmt.Errorf("generated archival schema name is %d bytes; maximum is %d",
			len(name), maxPostgresIdentifierSize)
	}
	return name, nil
}

func truncateArchivalSourceComponents(sourceSchema, sourceTable string, byteBudget int) (string, string, error) {
	if !utf8.ValidString(sourceSchema) || !utf8.ValidString(sourceTable) {
		return "", "", fmt.Errorf("source schema and table names must contain valid UTF-8")
	}
	schemaRunes := []rune(sourceSchema)
	tableRunes := []rune(sourceTable)
	if len(schemaRunes) == 0 || len(tableRunes) == 0 {
		return "", "", fmt.Errorf("source schema and table names must each contain at least one rune")
	}
	minimumSize := utf8.RuneLen(schemaRunes[0]) + utf8.RuneLen(tableRunes[0])
	if minimumSize > byteBudget {
		return "", "", fmt.Errorf(
			"archival schema fixed components leave %d bytes, but one rune from each source component requires %d bytes",
			byteBudget, minimumSize,
		)
	}

	for len(string(schemaRunes))+len(string(tableRunes)) > byteBudget {
		canTrimSchema := len(schemaRunes) > 1
		canTrimTable := len(tableRunes) > 1
		if !canTrimSchema && !canTrimTable {
			return "", "", fmt.Errorf("source schema and table names cannot fit within %d bytes", byteBudget)
		}
		if canTrimSchema && (!canTrimTable || len(string(schemaRunes)) >= len(string(tableRunes))) {
			schemaRunes = schemaRunes[:len(schemaRunes)-1]
		} else {
			tableRunes = tableRunes[:len(tableRunes)-1]
		}
	}
	return string(schemaRunes), string(tableRunes), nil
}

func validateArchivalNameAllocations(
	prefix string,
	currentInventory schema.CatalogInventory,
	targetInventory schema.CatalogInventory,
	prepared []preparedArchivalGroupNameRequest,
	allocations []archivalGroupNameAllocation,
) error {
	currentSchemas := catalogSchemaNameSet(currentInventory)
	targetSchemas := catalogSchemaNameSet(targetInventory)
	generatedSchemas := make(map[string]struct{})
	groupIDs := make(map[archivalGroupID]struct{})
	for groupIdx, allocation := range allocations {
		for memberIdx, member := range allocation.Members {
			if err := validateArchivalDestinationNamespace(
				member.CleanupSchemaName, prepared[groupIdx].members[memberIdx].objects,
			); err != nil {
				return err
			}
		}
	}
	for _, allocation := range allocations {
		if err := validateGeneratedArchivalSchemaName(
			allocation.DependencySchemaName, currentSchemas, targetSchemas, generatedSchemas,
		); err != nil {
			return err
		}
		for _, member := range allocation.Members {
			if err := validateGeneratedArchivalSchemaName(
				member.CleanupSchemaName, currentSchemas, targetSchemas, generatedSchemas,
			); err != nil {
				return err
			}
		}
		if _, duplicate := groupIDs[allocation.GroupID]; duplicate {
			return fmt.Errorf("duplicate generated archival group ID %q", allocation.GroupID)
		}
		groupIDs[allocation.GroupID] = struct{}{}
	}
	return validateNoReservedArchivalTargetSchemas(prefix, targetInventory)
}

func validateGeneratedArchivalSchemaName(
	name string,
	currentSchemas map[string]struct{},
	targetSchemas map[string]struct{},
	generatedSchemas map[string]struct{},
) error {
	if _, duplicate := generatedSchemas[name]; duplicate {
		return fmt.Errorf("duplicate generated archival schema name %q", name)
	}
	generatedSchemas[name] = struct{}{}
	if _, collision := currentSchemas[name]; collision {
		return fmt.Errorf("generated archival schema name %q collides with a current schema", name)
	}
	if _, collision := targetSchemas[name]; collision {
		return fmt.Errorf("generated archival schema name %q collides with a target schema", name)
	}
	return nil
}

type archivalObjectNamespace string

const (
	archivalObjectNamespaceRelation          archivalObjectNamespace = "relation"
	archivalObjectNamespaceType              archivalObjectNamespace = "type"
	archivalObjectNamespaceExtendedStatistic archivalObjectNamespace = "extended statistic"
)

func validateArchivalDestinationNamespace(
	destinationSchema string,
	objects []schema.CatalogObjectIdentity,
) error {
	byNamespace := make(map[archivalObjectNamespace]map[string]schema.CatalogObjectIdentity)
	for _, object := range objects {
		namespace, participates := archivalNamespaceForObject(object.Kind)
		if !participates {
			continue
		}
		if object.Name == "" {
			return fmt.Errorf("%s object OID %d assigned to archival schema %q has an empty name",
				object.Kind, object.OID, destinationSchema)
		}
		if byNamespace[namespace] == nil {
			byNamespace[namespace] = make(map[string]schema.CatalogObjectIdentity)
		}
		if existing, duplicate := byNamespace[namespace][object.Name]; duplicate && existing.OID != object.OID {
			return fmt.Errorf("archival schema %q has conflicting %s objects named %q (OIDs %d and %d)",
				destinationSchema, namespace, object.Name, existing.OID, object.OID)
		}
		byNamespace[namespace][object.Name] = object
	}

	// pg_class and pg_type have separate namespaces. In particular, a table and
	// its linked row type intentionally have the same name.
	return nil
}

func archivalNamespaceForObject(kind schema.CatalogObjectKind) (archivalObjectNamespace, bool) {
	switch kind {
	case schema.CatalogObjectKindRelation,
		schema.CatalogObjectKindIndex,
		schema.CatalogObjectKindOwnedSequence:
		return archivalObjectNamespaceRelation, true
	case schema.CatalogObjectKindRowType,
		schema.CatalogObjectKindArrayType:
		return archivalObjectNamespaceType, true
	case schema.CatalogObjectKindExtendedStatistic:
		return archivalObjectNamespaceExtendedStatistic, true
	default:
		return "", false
	}
}

func isReservedArchivalSchemaName(name, prefix string) bool {
	if !strings.HasPrefix(name, prefix+"_") {
		return false
	}
	remainder := strings.TrimPrefix(name, prefix+"_")
	minimumRemainderSize := 1 + 1 + 1 + archivalTimestampLen + 1 + archivalNonceLength
	if len(remainder) < minimumRemainderSize {
		return false
	}

	nonceStart := len(remainder) - archivalNonceLength
	if nonceStart < 1 || remainder[nonceStart-1] != '_' ||
		!isArchivalNonce(remainder[nonceStart:]) {
		return false
	}
	timestampEnd := nonceStart - 1
	timestampStart := timestampEnd - archivalTimestampLen
	if timestampStart < 1 || remainder[timestampStart-1] != '_' ||
		!isArchivalTimestamp(remainder[timestampStart:timestampEnd]) {
		return false
	}
	sourceComponents := remainder[:timestampStart-1]
	for idx := 1; idx < len(sourceComponents)-1; idx++ {
		if sourceComponents[idx] == '_' {
			return true
		}
	}
	return false
}

func isArchivalTimestamp(value string) bool {
	if len(value) != archivalTimestampLen || value[8] != 'T' || value[len(value)-1] != 'Z' {
		return false
	}
	withFractionSeparator := value[:15] + "." + value[15:]
	_, err := time.Parse("20060102T150405.000000Z", withFractionSeparator)
	return err == nil
}

func isArchivalNonce(value string) bool {
	if len(value) != archivalNonceLength {
		return false
	}
	for _, char := range value {
		switch {
		case char >= 'A' && char <= 'Z':
		case char >= 'a' && char <= 'z':
		case char >= '0' && char <= '9':
		case char == '$' || char == '_':
		default:
			return false
		}
	}
	return true
}

func catalogSchemaNameSet(inventory schema.CatalogInventory) map[string]struct{} {
	result := make(map[string]struct{}, len(inventory.Schemas))
	for _, catalogSchema := range inventory.Schemas {
		result[catalogSchema.Name] = struct{}{}
	}
	return result
}

func catalogRelationWithOID(inventory schema.CatalogInventory, oid uint32) *schema.CatalogRelation {
	for idx := range inventory.Relations {
		if inventory.Relations[idx].OID == oid {
			return &inventory.Relations[idx]
		}
	}
	return nil
}

func compareCatalogRelations(a, b schema.CatalogRelation) int {
	return cmp.Or(
		cmp.Compare(a.SchemaName, b.SchemaName),
		cmp.Compare(a.Name, b.Name),
		cmp.Compare(a.OID, b.OID),
	)
}

func comparePreparedArchivalMember(a, b preparedArchivalMemberNameRequest) int {
	return compareCatalogRelations(a.relation, b.relation)
}

func compareArchivalCatalogObjectIdentity(a, b schema.CatalogObjectIdentity) int {
	return cmp.Or(
		cmp.Compare(a.Kind, b.Kind),
		cmp.Compare(a.Name, b.Name),
		cmp.Compare(a.OID, b.OID),
		cmp.Compare(a.RelationOID, b.RelationOID),
	)
}
