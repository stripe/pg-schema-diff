package diff

import (
	"bytes"
	"cmp"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

const (
	archivalMarkerVersion           = 1
	archivalMarkerEnvelopeNamespace = "pg-schema-diff:schema-partial-archival:"
	archivalMarkerEnvelopeV1        = archivalMarkerEnvelopeNamespace + "v1:"
)

type archivalMarkerObjectKind string

const (
	archivalMarkerObjectKindSchema            archivalMarkerObjectKind = "schema"
	archivalMarkerObjectKindTable             archivalMarkerObjectKind = "table"
	archivalMarkerObjectKindRowType           archivalMarkerObjectKind = "row_type"
	archivalMarkerObjectKindArrayType         archivalMarkerObjectKind = "array_type"
	archivalMarkerObjectKindIndex             archivalMarkerObjectKind = "index"
	archivalMarkerObjectKindConstraint        archivalMarkerObjectKind = "constraint"
	archivalMarkerObjectKindTrigger           archivalMarkerObjectKind = "trigger"
	archivalMarkerObjectKindRule              archivalMarkerObjectKind = "rule"
	archivalMarkerObjectKindPolicy            archivalMarkerObjectKind = "policy"
	archivalMarkerObjectKindOwnedSequence     archivalMarkerObjectKind = "owned_sequence"
	archivalMarkerObjectKindExtendedStatistic archivalMarkerObjectKind = "extended_statistic"
	archivalMarkerObjectKindToastRelation     archivalMarkerObjectKind = "toast_relation"
	archivalMarkerObjectKindSequence          archivalMarkerObjectKind = "sequence"
	archivalMarkerObjectKindFunction          archivalMarkerObjectKind = "function"
	archivalMarkerObjectKindType              archivalMarkerObjectKind = "type"
	archivalMarkerObjectKindCollation         archivalMarkerObjectKind = "collation"
	archivalMarkerObjectKindOperator          archivalMarkerObjectKind = "operator"
	archivalMarkerObjectKindView              archivalMarkerObjectKind = "view"
	archivalMarkerObjectKindMaterializedView  archivalMarkerObjectKind = "materialized_view"
)

// archivalMarkerObjectIdentity is deliberately narrower than the catalog
// inventory. Its fields are the complete v1 identity contract.
type archivalMarkerObjectIdentity struct {
	Kind              archivalMarkerObjectKind `json:"kind"`
	OID               uint32                   `json:"oid"`
	SchemaName        string                   `json:"schema_name"`
	Name              string                   `json:"name"`
	IdentityArguments []string                 `json:"identity_arguments"`
}

type archivalMarkerSchemaIdentity struct {
	Name string `json:"name"`
}

type archivalMarkerMemberV1 struct {
	MemberID                  string                         `json:"member_id"`
	SourceTable               archivalMarkerObjectIdentity   `json:"source_table"`
	CleanupTable              archivalMarkerObjectIdentity   `json:"cleanup_table"`
	AutomaticallyMovedObjects []archivalMarkerObjectIdentity `json:"automatically_moved_objects"`
	AttachedObjects           []archivalMarkerObjectIdentity `json:"attached_objects"`
	ExplicitlyMovedObjects    []archivalMarkerObjectIdentity `json:"explicitly_moved_objects"`
	InternalToastObjects      []archivalMarkerObjectIdentity `json:"internal_toast_objects"`
}

type archivalMarkerPartitionEdgeV1 struct {
	ParentMemberID string `json:"parent_member_id"`
	ChildMemberID  string `json:"child_member_id"`
}

type archivalMarkerSharedGroupEdgeV1 struct {
	FirstGroupID  archivalGroupID `json:"first_group_id"`
	SecondGroupID archivalGroupID `json:"second_group_id"`
}

type archivalMarkerACLRecordV1 struct {
	ObjectClass     string                       `json:"object_class"`
	Object          archivalMarkerObjectIdentity `json:"object"`
	ColumnName      string                       `json:"column_name"`
	OwnerName       string                       `json:"owner_name"`
	GrantorName     string                       `json:"grantor_name"`
	GranteeName     string                       `json:"grantee_name"`
	GranteeIsPublic bool                         `json:"grantee_is_public"`
	Privilege       string                       `json:"privilege"`
	IsGrantable     bool                         `json:"is_grantable"`
}

type archivalMarkerForeignKeyColumnV1 struct {
	OwningColumnName     string `json:"owning_column_name"`
	ReferencedColumnName string `json:"referenced_column_name"`
}

type archivalMarkerForeignKeyV1 struct {
	Name                string                             `json:"name"`
	OwningTable         archivalMarkerObjectIdentity       `json:"owning_table"`
	ReferencedTable     archivalMarkerObjectIdentity       `json:"referenced_table"`
	Columns             []archivalMarkerForeignKeyColumnV1 `json:"columns"`
	MatchType           string                             `json:"match_type"`
	UpdateAction        string                             `json:"update_action"`
	DeleteAction        string                             `json:"delete_action"`
	IsDeferrable        bool                               `json:"is_deferrable"`
	IsInitiallyDeferred bool                               `json:"is_initially_deferred"`
	IsValidated         bool                               `json:"is_validated"`
	Definition          string                             `json:"definition"`
}

type archivalMarkerPublicationMembershipV1 struct {
	PublicationName string                       `json:"publication_name"`
	Table           archivalMarkerObjectIdentity `json:"table"`
	ColumnNames     []string                     `json:"column_names"`
	RowFilter       string                       `json:"row_filter"`
}

type archivalMarkerV1 struct {
	Version                          int                                     `json:"version"`
	GroupID                          archivalGroupID                         `json:"group_id"`
	Members                          []archivalMarkerMemberV1                `json:"members"`
	PartitionEdges                   []archivalMarkerPartitionEdgeV1         `json:"partition_edges"`
	ExclusiveDependencySchemas       []archivalMarkerSchemaIdentity          `json:"exclusive_dependency_schemas"`
	ExclusiveDependencyObjects       []archivalMarkerObjectIdentity          `json:"exclusive_dependency_objects"`
	SharedCleanupComponentGroupEdges []archivalMarkerSharedGroupEdgeV1       `json:"shared_cleanup_component_group_edges"`
	OriginalACLs                     []archivalMarkerACLRecordV1             `json:"original_acls"`
	OriginalForeignKeys              []archivalMarkerForeignKeyV1            `json:"original_foreign_keys"`
	OriginalPublicationMemberships   []archivalMarkerPublicationMembershipV1 `json:"original_publication_memberships"`
	CleanupDigest                    cleanupOperationDigest                  `json:"cleanup_digest"`
}

func marshalArchivalMarker(payload archivalMarkerV1) (string, error) {
	if err := validateArchivalMarker(payload); err != nil {
		return "", fmt.Errorf("validating archival marker: %w", err)
	}
	payload = canonicalizeArchivalMarker(payload)
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshaling archival marker payload: %w", err)
	}
	return archivalMarkerEnvelopeV1 + base64.RawURLEncoding.EncodeToString(encoded), nil
}

func parseArchivalMarker(marker string) (archivalMarkerV1, error) {
	if !strings.HasPrefix(marker, archivalMarkerEnvelopeNamespace) {
		return archivalMarkerV1{}, fmt.Errorf("invalid archival marker envelope")
	}
	remainder := strings.TrimPrefix(marker, archivalMarkerEnvelopeNamespace)
	version, encoded, found := strings.Cut(remainder, ":")
	if !found {
		return archivalMarkerV1{}, fmt.Errorf("invalid archival marker envelope")
	}
	if version != "v1" {
		return archivalMarkerV1{}, fmt.Errorf("unsupported archival marker envelope version %q", version)
	}
	if encoded == "" || strings.Contains(encoded, "=") {
		return archivalMarkerV1{}, fmt.Errorf("invalid unpadded base64url archival marker payload")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return archivalMarkerV1{}, fmt.Errorf("decoding archival marker payload: %w", err)
	}
	if base64.RawURLEncoding.EncodeToString(decoded) != encoded {
		return archivalMarkerV1{}, fmt.Errorf("archival marker payload is not canonical unpadded base64url")
	}
	if err := validateRequiredJSONFields(decoded, reflect.TypeFor[archivalMarkerV1]()); err != nil {
		return archivalMarkerV1{}, fmt.Errorf("decoding archival marker JSON: %w", err)
	}

	var payload archivalMarkerV1
	if err := decodeStrictJSON(decoded, &payload); err != nil {
		return archivalMarkerV1{}, fmt.Errorf("decoding archival marker JSON: %w", err)
	}
	if err := validateArchivalMarker(payload); err != nil {
		return archivalMarkerV1{}, fmt.Errorf("validating archival marker: %w", err)
	}
	return canonicalizeArchivalMarker(payload), nil
}

func escapeArchivalMarkerSQLLiteral(marker string) string {
	return schema.EscapeLiteral(marker)
}

func decodeStrictJSON(encoded []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return fmt.Errorf("reading trailing JSON: %w", err)
	}
	return nil
}

func validateRequiredJSONFields(encoded []byte, expectedType reflect.Type) error {
	var value any
	if err := decodeStrictJSON(encoded, &value); err != nil {
		return err
	}
	return validateRequiredJSONValue(value, expectedType, "$")
}

func validateRequiredJSONValue(value any, expectedType reflect.Type, path string) error {
	if value == nil {
		return fmt.Errorf("required JSON field %s must not be null", path)
	}
	switch expectedType.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("JSON field %s must be an object", path)
		}
		for fieldIdx := range expectedType.NumField() {
			field := expectedType.Field(fieldIdx)
			fieldName := strings.Split(field.Tag.Get("json"), ",")[0]
			if fieldName == "" || fieldName == "-" {
				continue
			}
			fieldValue, present := object[fieldName]
			if !present {
				return fmt.Errorf("required JSON field %s.%s is missing", path, fieldName)
			}
			if err := validateRequiredJSONValue(fieldValue, field.Type, path+"."+fieldName); err != nil {
				return err
			}
		}
	case reflect.Slice:
		array, ok := value.([]any)
		if !ok {
			return fmt.Errorf("JSON field %s must be an array", path)
		}
		for idx, element := range array {
			if err := validateRequiredJSONValue(element, expectedType.Elem(),
				fmt.Sprintf("%s[%d]", path, idx)); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateArchivalMarker(payload archivalMarkerV1) error {
	if payload.Version != archivalMarkerVersion {
		return fmt.Errorf("unsupported payload version %d", payload.Version)
	}
	if err := validateArchivalCodecString("group ID", string(payload.GroupID)); err != nil {
		return err
	}
	if len(payload.Members) == 0 {
		return fmt.Errorf("at least one group member is required")
	}
	if _, err := parseCleanupOperationDigest(payload.CleanupDigest.String()); err != nil {
		return fmt.Errorf("cleanup digest: %w", err)
	}

	memberIDs := make(map[string]struct{}, len(payload.Members))
	memberSchemas := make(map[string]struct{}, len(payload.Members))
	sourceTables := make(map[string]struct{}, len(payload.Members))
	for memberIdx, member := range payload.Members {
		if err := validateArchivalMarkerMember(member); err != nil {
			return fmt.Errorf("member %d: %w", memberIdx, err)
		}
		if _, duplicate := memberIDs[member.MemberID]; duplicate {
			return fmt.Errorf("duplicate member ID %q", member.MemberID)
		}
		memberIDs[member.MemberID] = struct{}{}
		if _, duplicate := memberSchemas[member.CleanupTable.SchemaName]; duplicate {
			return fmt.Errorf("duplicate member cleanup schema %q", member.CleanupTable.SchemaName)
		}
		memberSchemas[member.CleanupTable.SchemaName] = struct{}{}
		sourceTableKey := markerObjectIdentityKey(member.SourceTable)
		if _, duplicate := sourceTables[sourceTableKey]; duplicate {
			return fmt.Errorf("duplicate member source table identity %s.%s",
				member.SourceTable.SchemaName, member.SourceTable.Name)
		}
		sourceTables[sourceTableKey] = struct{}{}
	}
	if err := validateArchivalMarkerTopology(memberIDs, payload.PartitionEdges); err != nil {
		return err
	}

	dependencySchemas := make(map[string]struct{}, len(payload.ExclusiveDependencySchemas))
	for _, dependencySchema := range payload.ExclusiveDependencySchemas {
		if err := validateArchivalCodecString("exclusive dependency schema", dependencySchema.Name); err != nil {
			return err
		}
		if _, duplicate := dependencySchemas[dependencySchema.Name]; duplicate {
			return fmt.Errorf("duplicate exclusive dependency schema %q", dependencySchema.Name)
		}
		if _, memberSchema := memberSchemas[dependencySchema.Name]; memberSchema {
			return fmt.Errorf("exclusive dependency schema %q is also a member cleanup schema", dependencySchema.Name)
		}
		dependencySchemas[dependencySchema.Name] = struct{}{}
	}
	if err := validateUniqueMarkerObjects("exclusive dependency",
		payload.ExclusiveDependencyObjects, nil); err != nil {
		return err
	}
	for _, object := range payload.ExclusiveDependencyObjects {
		if _, ok := dependencySchemas[object.SchemaName]; !ok {
			return fmt.Errorf("exclusive dependency object %s.%s references undeclared dependency schema %q",
				object.SchemaName, object.Name, object.SchemaName)
		}
	}
	if err := validateArchivalMarkerSharedEdges(payload.GroupID,
		payload.SharedCleanupComponentGroupEdges); err != nil {
		return err
	}
	if err := validateArchivalMarkerACLs(payload.OriginalACLs); err != nil {
		return err
	}
	if err := validateArchivalMarkerForeignKeys(payload.OriginalForeignKeys); err != nil {
		return err
	}
	return validateArchivalMarkerPublicationMemberships(payload.OriginalPublicationMemberships)
}

func validateArchivalMarkerMember(member archivalMarkerMemberV1) error {
	if err := validateArchivalCodecString("member ID", member.MemberID); err != nil {
		return err
	}
	if err := validateMarkerObjectKind(member.SourceTable, archivalMarkerObjectKindTable); err != nil {
		return fmt.Errorf("source table: %w", err)
	}
	if err := validateMarkerObjectKind(member.CleanupTable, archivalMarkerObjectKindTable); err != nil {
		return fmt.Errorf("cleanup table: %w", err)
	}
	if member.SourceTable.Name != member.CleanupTable.Name {
		return fmt.Errorf("source table %q and cleanup table %q must have the same name",
			member.SourceTable.Name, member.CleanupTable.Name)
	}
	if member.SourceTable.OID != member.CleanupTable.OID {
		return fmt.Errorf("source table OID %d and cleanup table OID %d must be the same",
			member.SourceTable.OID, member.CleanupTable.OID)
	}
	if len(member.AutomaticallyMovedObjects) == 0 {
		return fmt.Errorf("automatically moved objects are required")
	}
	allowedAutomatic := map[archivalMarkerObjectKind]struct{}{
		archivalMarkerObjectKindTable: {}, archivalMarkerObjectKindRowType: {},
		archivalMarkerObjectKindArrayType: {}, archivalMarkerObjectKindIndex: {},
		archivalMarkerObjectKindConstraint: {}, archivalMarkerObjectKindOwnedSequence: {},
	}
	allowedAttached := map[archivalMarkerObjectKind]struct{}{
		archivalMarkerObjectKindTrigger: {}, archivalMarkerObjectKindRule: {}, archivalMarkerObjectKindPolicy: {},
	}
	allowedExplicit := map[archivalMarkerObjectKind]struct{}{
		archivalMarkerObjectKindExtendedStatistic: {},
	}
	allowedToast := map[archivalMarkerObjectKind]struct{}{
		archivalMarkerObjectKindToastRelation: {},
	}

	allLocal := make(map[string]string)
	localOIDs := make(map[uint32]string)
	for _, category := range []struct {
		name    string
		values  []archivalMarkerObjectIdentity
		allowed map[archivalMarkerObjectKind]struct{}
	}{
		{name: "automatically moved", values: member.AutomaticallyMovedObjects, allowed: allowedAutomatic},
		{name: "attached", values: member.AttachedObjects, allowed: allowedAttached},
		{name: "explicitly moved", values: member.ExplicitlyMovedObjects, allowed: allowedExplicit},
		{name: "internal TOAST", values: member.InternalToastObjects, allowed: allowedToast},
	} {
		if err := validateUniqueMarkerObjects(category.name, category.values, category.allowed); err != nil {
			return err
		}
		for _, object := range category.values {
			key := markerObjectIdentityKey(object)
			if previous, duplicate := allLocal[key]; duplicate {
				return fmt.Errorf("duplicate local object identity in %s and %s objects: %s.%s",
					previous, category.name, object.SchemaName, object.Name)
			}
			allLocal[key] = category.name
			if previous, duplicate := localOIDs[object.OID]; duplicate {
				return fmt.Errorf("duplicate local object OID %d in %s and %s objects",
					object.OID, previous, category.name)
			}
			localOIDs[object.OID] = category.name
			if category.name != "internal TOAST" &&
				object.SchemaName != member.CleanupTable.SchemaName {
				return fmt.Errorf("%s object %s.%s is not in member cleanup schema %q",
					category.name, object.SchemaName, object.Name, member.CleanupTable.SchemaName)
			}
		}
	}
	foundCleanupTable := false
	for _, object := range member.AutomaticallyMovedObjects {
		if compareMarkerObjects(object, member.CleanupTable) == 0 {
			foundCleanupTable = true
			break
		}
	}
	if !foundCleanupTable {
		return fmt.Errorf("automatically moved objects do not contain cleanup table %s.%s",
			member.CleanupTable.SchemaName, member.CleanupTable.Name)
	}
	return nil
}

func validateArchivalMarkerTopology(
	members map[string]struct{},
	edges []archivalMarkerPartitionEdgeV1,
) error {
	parents := make(map[string]string, len(edges))
	children := make(map[string][]string, len(edges))
	seen := make(map[string]struct{}, len(edges))
	for _, edge := range edges {
		if _, ok := members[edge.ParentMemberID]; !ok {
			return fmt.Errorf("partition edge references missing parent member %q", edge.ParentMemberID)
		}
		if _, ok := members[edge.ChildMemberID]; !ok {
			return fmt.Errorf("partition edge references missing child member %q", edge.ChildMemberID)
		}
		if edge.ParentMemberID == edge.ChildMemberID {
			return fmt.Errorf("partition member %q cannot be its own parent", edge.ParentMemberID)
		}
		key := edge.ParentMemberID + "\x00" + edge.ChildMemberID
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate partition edge %q -> %q", edge.ParentMemberID, edge.ChildMemberID)
		}
		seen[key] = struct{}{}
		if existing, duplicateParent := parents[edge.ChildMemberID]; duplicateParent {
			return fmt.Errorf("partition member %q has multiple parents %q and %q",
				edge.ChildMemberID, existing, edge.ParentMemberID)
		}
		parents[edge.ChildMemberID] = edge.ParentMemberID
		children[edge.ParentMemberID] = append(children[edge.ParentMemberID], edge.ChildMemberID)
	}
	if len(edges) != len(members)-1 {
		return fmt.Errorf("partition topology for %d members must contain %d edges, got %d",
			len(members), len(members)-1, len(edges))
	}
	roots := make([]string, 0, 1)
	for memberID := range members {
		if _, hasParent := parents[memberID]; !hasParent {
			roots = append(roots, memberID)
		}
	}
	if len(roots) != 1 {
		return fmt.Errorf("partition topology must have exactly one root, got %d", len(roots))
	}
	visited := make(map[string]struct{}, len(members))
	var visit func(string) error
	visiting := make(map[string]struct{}, len(members))
	visit = func(memberID string) error {
		if _, cycle := visiting[memberID]; cycle {
			return fmt.Errorf("partition topology contains a cycle at member %q", memberID)
		}
		if _, done := visited[memberID]; done {
			return nil
		}
		visiting[memberID] = struct{}{}
		for _, childID := range children[memberID] {
			if err := visit(childID); err != nil {
				return err
			}
		}
		delete(visiting, memberID)
		visited[memberID] = struct{}{}
		return nil
	}
	if err := visit(roots[0]); err != nil {
		return err
	}
	if len(visited) != len(members) {
		return fmt.Errorf("partition topology is disconnected or cyclic")
	}
	return nil
}

func validateArchivalMarkerSharedEdges(
	groupID archivalGroupID,
	edges []archivalMarkerSharedGroupEdgeV1,
) error {
	seen := make(map[string]struct{}, len(edges))
	for _, edge := range edges {
		if err := validateArchivalCodecString("shared edge first group ID",
			string(edge.FirstGroupID)); err != nil {
			return err
		}
		if err := validateArchivalCodecString("shared edge second group ID",
			string(edge.SecondGroupID)); err != nil {
			return err
		}
		if edge.FirstGroupID == edge.SecondGroupID {
			return fmt.Errorf("shared cleanup-component edge cannot be a self-edge for group %q", edge.FirstGroupID)
		}
		first, second := canonicalGroupEdge(edge)
		if first != groupID && second != groupID {
			return fmt.Errorf("shared cleanup-component edge %q--%q does not contain marker group %q",
				first, second, groupID)
		}
		key := string(first) + "\x00" + string(second)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate shared cleanup-component edge %q--%q", first, second)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateArchivalMarkerACLs(records []archivalMarkerACLRecordV1) error {
	seen := make(map[string]struct{}, len(records))
	for idx, record := range records {
		expectedKind := archivalMarkerObjectKind("")
		switch record.ObjectClass {
		case "schema":
			expectedKind = archivalMarkerObjectKindSchema
		case "table":
			expectedKind = archivalMarkerObjectKindTable
		case "sequence":
			expectedKind = archivalMarkerObjectKindSequence
		case "function":
			expectedKind = archivalMarkerObjectKindFunction
		case "type":
			expectedKind = archivalMarkerObjectKindType
		default:
			return fmt.Errorf("original ACL %d has unsupported object class %q", idx, record.ObjectClass)
		}
		if err := validateMarkerObjectKind(record.Object, expectedKind); err != nil {
			return fmt.Errorf("original ACL %d object: %w", idx, err)
		}
		if record.ColumnName != "" {
			if record.ObjectClass != "table" {
				return fmt.Errorf("original ACL %d has a column on non-table object class %q", idx, record.ObjectClass)
			}
			if err := validateArchivalCodecString("ACL column", record.ColumnName); err != nil {
				return err
			}
		}
		for label, value := range map[string]string{
			"owner": record.OwnerName, "grantor": record.GrantorName,
			"grantee": record.GranteeName, "privilege": record.Privilege,
		} {
			if err := validateArchivalCodecString("ACL "+label, value); err != nil {
				return err
			}
		}
		if record.GranteeIsPublic != (record.GranteeName == schema.CatalogPublicRoleName) {
			return fmt.Errorf("original ACL %d has inconsistent PUBLIC grantee metadata", idx)
		}
		canonicalRecord := record
		canonicalRecord.Object = cloneMarkerObject(canonicalRecord.Object)
		keyBytes, _ := json.Marshal(canonicalRecord)
		key := string(keyBytes)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate original ACL record for %s.%s",
				record.Object.SchemaName, record.Object.Name)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateArchivalMarkerForeignKeys(foreignKeys []archivalMarkerForeignKeyV1) error {
	seen := make(map[string]struct{}, len(foreignKeys))
	for idx, foreignKey := range foreignKeys {
		if err := validateArchivalCodecString("foreign key name", foreignKey.Name); err != nil {
			return err
		}
		if err := validateMarkerObjectKind(foreignKey.OwningTable,
			archivalMarkerObjectKindTable); err != nil {
			return fmt.Errorf("original foreign key %d owning table: %w", idx, err)
		}
		if err := validateMarkerObjectKind(foreignKey.ReferencedTable,
			archivalMarkerObjectKindTable); err != nil {
			return fmt.Errorf("original foreign key %d referenced table: %w", idx, err)
		}
		if len(foreignKey.Columns) == 0 {
			return fmt.Errorf("original foreign key %q has no columns", foreignKey.Name)
		}
		columnPairs := make(map[string]struct{}, len(foreignKey.Columns))
		for _, column := range foreignKey.Columns {
			if err := validateArchivalCodecString("foreign key owning column", column.OwningColumnName); err != nil {
				return err
			}
			if err := validateArchivalCodecString("foreign key referenced column",
				column.ReferencedColumnName); err != nil {
				return err
			}
			key := column.OwningColumnName + "\x00" + column.ReferencedColumnName
			if _, duplicate := columnPairs[key]; duplicate {
				return fmt.Errorf("original foreign key %q contains duplicate column pair", foreignKey.Name)
			}
			columnPairs[key] = struct{}{}
		}
		for label, value := range map[string]string{
			"match type": foreignKey.MatchType, "update action": foreignKey.UpdateAction,
			"delete action": foreignKey.DeleteAction, "definition": foreignKey.Definition,
		} {
			if err := validateArchivalCodecString("foreign key "+label, value); err != nil {
				return err
			}
		}
		key := markerObjectIdentityKey(foreignKey.OwningTable) + "\x00" + foreignKey.Name
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate original foreign key %s.%s",
				foreignKey.OwningTable.SchemaName, foreignKey.Name)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateArchivalMarkerPublicationMemberships(
	memberships []archivalMarkerPublicationMembershipV1,
) error {
	seen := make(map[string]struct{}, len(memberships))
	for idx, membership := range memberships {
		if err := validateArchivalCodecString("publication name",
			membership.PublicationName); err != nil {
			return err
		}
		if err := validateMarkerObjectKind(membership.Table, archivalMarkerObjectKindTable); err != nil {
			return fmt.Errorf("original publication membership %d table: %w", idx, err)
		}
		columns := make(map[string]struct{}, len(membership.ColumnNames))
		for _, columnName := range membership.ColumnNames {
			if err := validateArchivalCodecString("publication column", columnName); err != nil {
				return err
			}
			if _, duplicate := columns[columnName]; duplicate {
				return fmt.Errorf("publication %q contains duplicate column %q", membership.PublicationName, columnName)
			}
			columns[columnName] = struct{}{}
		}
		key := membership.PublicationName + "\x00" +
			markerObjectIdentityKey(membership.Table)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate original publication membership for publication %q and table %s.%s",
				membership.PublicationName, membership.Table.SchemaName, membership.Table.Name)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateUniqueMarkerObjects(
	category string,
	objects []archivalMarkerObjectIdentity,
	allowed map[archivalMarkerObjectKind]struct{},
) error {
	seen := make(map[string]struct{}, len(objects))
	for _, object := range objects {
		if err := validateMarkerObjectIdentity(object); err != nil {
			return fmt.Errorf("%s object: %w", category, err)
		}
		if allowed != nil {
			if _, ok := allowed[object.Kind]; !ok {
				return fmt.Errorf("%s object has unsupported kind %q", category, object.Kind)
			}
		}
		key := markerObjectIdentityKey(object)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate %s object identity %s.%s", category, object.SchemaName, object.Name)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateMarkerObjectKind(identity archivalMarkerObjectIdentity, expected archivalMarkerObjectKind) error {
	if err := validateMarkerObjectIdentity(identity); err != nil {
		return err
	}
	if identity.Kind != expected {
		return fmt.Errorf("expected object kind %q, got %q", expected, identity.Kind)
	}
	return nil
}

func validateMarkerObjectIdentity(identity archivalMarkerObjectIdentity) error {
	switch identity.Kind {
	case archivalMarkerObjectKindSchema, archivalMarkerObjectKindTable,
		archivalMarkerObjectKindRowType, archivalMarkerObjectKindArrayType,
		archivalMarkerObjectKindIndex, archivalMarkerObjectKindConstraint,
		archivalMarkerObjectKindTrigger, archivalMarkerObjectKindRule,
		archivalMarkerObjectKindPolicy, archivalMarkerObjectKindOwnedSequence,
		archivalMarkerObjectKindExtendedStatistic, archivalMarkerObjectKindToastRelation,
		archivalMarkerObjectKindSequence, archivalMarkerObjectKindFunction,
		archivalMarkerObjectKindType, archivalMarkerObjectKindCollation,
		archivalMarkerObjectKindOperator, archivalMarkerObjectKindView,
		archivalMarkerObjectKindMaterializedView:
	default:
		return fmt.Errorf("unsupported object kind %q", identity.Kind)
	}
	if identity.Kind == archivalMarkerObjectKindSchema {
		if identity.OID != 0 {
			return fmt.Errorf("schema object identity must not contain a catalog OID")
		}
		if identity.SchemaName != "" {
			return fmt.Errorf("schema object identity must not have a containing schema")
		}
	} else {
		if identity.OID == 0 {
			return fmt.Errorf("object catalog OID is required")
		}
		if err := validateArchivalCodecString("object schema name", identity.SchemaName); err != nil {
			return err
		}
	}
	if err := validateArchivalCodecString("object name", identity.Name); err != nil {
		return err
	}
	for _, argument := range identity.IdentityArguments {
		if err := validateArchivalCodecString("object identity argument", argument); err != nil {
			return err
		}
	}
	if len(identity.IdentityArguments) > 0 && identity.Kind != archivalMarkerObjectKindFunction &&
		identity.Kind != archivalMarkerObjectKindOperator {
		return fmt.Errorf("object kind %q cannot have identity arguments", identity.Kind)
	}
	return nil
}

func validateArchivalCodecString(label, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", label)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must contain valid UTF-8", label)
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s must not contain a null byte", label)
	}
	return nil
}

func canonicalizeArchivalMarker(payload archivalMarkerV1) archivalMarkerV1 {
	payload.Members = cloneMarkerMembers(payload.Members)
	for idx := range payload.Members {
		member := &payload.Members[idx]
		member.SourceTable = cloneMarkerObject(member.SourceTable)
		member.CleanupTable = cloneMarkerObject(member.CleanupTable)
		member.AutomaticallyMovedObjects = canonicalMarkerObjects(member.AutomaticallyMovedObjects)
		member.AttachedObjects = canonicalMarkerObjects(member.AttachedObjects)
		member.ExplicitlyMovedObjects = canonicalMarkerObjects(member.ExplicitlyMovedObjects)
		member.InternalToastObjects = canonicalMarkerObjects(member.InternalToastObjects)
	}
	slices.SortFunc(payload.Members, func(a, b archivalMarkerMemberV1) int {
		return cmp.Compare(a.MemberID, b.MemberID)
	})
	payload.PartitionEdges = cloneOrEmpty(payload.PartitionEdges)
	slices.SortFunc(payload.PartitionEdges, func(a, b archivalMarkerPartitionEdgeV1) int {
		return cmp.Or(cmp.Compare(a.ParentMemberID, b.ParentMemberID),
			cmp.Compare(a.ChildMemberID, b.ChildMemberID))
	})
	payload.ExclusiveDependencySchemas = cloneOrEmpty(payload.ExclusiveDependencySchemas)
	slices.SortFunc(payload.ExclusiveDependencySchemas, func(a, b archivalMarkerSchemaIdentity) int {
		return cmp.Compare(a.Name, b.Name)
	})
	payload.ExclusiveDependencyObjects = canonicalMarkerObjects(payload.ExclusiveDependencyObjects)
	payload.SharedCleanupComponentGroupEdges = cloneOrEmpty(
		payload.SharedCleanupComponentGroupEdges,
	)
	for idx, edge := range payload.SharedCleanupComponentGroupEdges {
		first, second := canonicalGroupEdge(edge)
		payload.SharedCleanupComponentGroupEdges[idx] = archivalMarkerSharedGroupEdgeV1{
			FirstGroupID: first, SecondGroupID: second,
		}
	}
	slices.SortFunc(payload.SharedCleanupComponentGroupEdges, func(
		a, b archivalMarkerSharedGroupEdgeV1,
	) int {
		return cmp.Or(cmp.Compare(a.FirstGroupID, b.FirstGroupID),
			cmp.Compare(a.SecondGroupID, b.SecondGroupID))
	})
	payload.OriginalACLs = cloneOrEmpty(payload.OriginalACLs)
	for idx := range payload.OriginalACLs {
		payload.OriginalACLs[idx].Object = cloneMarkerObject(payload.OriginalACLs[idx].Object)
	}
	slices.SortFunc(payload.OriginalACLs, compareMarkerACLs)
	payload.OriginalForeignKeys = cloneMarkerForeignKeys(payload.OriginalForeignKeys)
	slices.SortFunc(payload.OriginalForeignKeys, func(a, b archivalMarkerForeignKeyV1) int {
		return cmp.Or(compareMarkerObjects(a.OwningTable, b.OwningTable), cmp.Compare(a.Name, b.Name))
	})
	payload.OriginalPublicationMemberships = cloneOrEmpty(payload.OriginalPublicationMemberships)
	for idx := range payload.OriginalPublicationMemberships {
		membership := &payload.OriginalPublicationMemberships[idx]
		membership.Table = cloneMarkerObject(membership.Table)
		membership.ColumnNames = cloneOrEmpty(membership.ColumnNames)
		slices.Sort(membership.ColumnNames)
	}
	slices.SortFunc(payload.OriginalPublicationMemberships,
		func(a, b archivalMarkerPublicationMembershipV1) int {
			return cmp.Or(cmp.Compare(a.PublicationName, b.PublicationName), compareMarkerObjects(a.Table, b.Table))
		})
	return payload
}

func cloneMarkerMembers(members []archivalMarkerMemberV1) []archivalMarkerMemberV1 {
	return cloneOrEmpty(members)
}

func cloneMarkerForeignKeys(foreignKeys []archivalMarkerForeignKeyV1) []archivalMarkerForeignKeyV1 {
	result := cloneOrEmpty(foreignKeys)
	for idx := range result {
		result[idx].OwningTable = cloneMarkerObject(result[idx].OwningTable)
		result[idx].ReferencedTable = cloneMarkerObject(result[idx].ReferencedTable)
		result[idx].Columns = cloneOrEmpty(result[idx].Columns)
	}
	return result
}

func canonicalMarkerObjects(objects []archivalMarkerObjectIdentity) []archivalMarkerObjectIdentity {
	result := cloneOrEmpty(objects)
	for idx := range result {
		result[idx] = cloneMarkerObject(result[idx])
	}
	slices.SortFunc(result, compareMarkerObjects)
	return result
}

func cloneMarkerObject(object archivalMarkerObjectIdentity) archivalMarkerObjectIdentity {
	object.IdentityArguments = cloneOrEmpty(object.IdentityArguments)
	return object
}

func compareMarkerObjects(a, b archivalMarkerObjectIdentity) int {
	return cmp.Or(
		cmp.Compare(a.Kind, b.Kind),
		cmp.Compare(a.OID, b.OID),
		cmp.Compare(a.SchemaName, b.SchemaName),
		cmp.Compare(a.Name, b.Name),
		slices.Compare(a.IdentityArguments, b.IdentityArguments),
	)
}

func compareMarkerACLs(a, b archivalMarkerACLRecordV1) int {
	return cmp.Or(
		cmp.Compare(a.ObjectClass, b.ObjectClass), compareMarkerObjects(a.Object, b.Object),
		cmp.Compare(a.ColumnName, b.ColumnName), cmp.Compare(a.OwnerName, b.OwnerName),
		cmp.Compare(a.GrantorName, b.GrantorName), cmp.Compare(a.GranteeName, b.GranteeName),
		cmp.Compare(a.Privilege, b.Privilege), compareMarkerBools(a.IsGrantable, b.IsGrantable),
	)
}

func compareMarkerBools(a, b bool) int {
	if a == b {
		return 0
	}
	if !a {
		return -1
	}
	return 1
}

func markerObjectIdentityKey(object archivalMarkerObjectIdentity) string {
	return strings.Join([]string{
		string(object.Kind), object.SchemaName, object.Name,
		strings.Join(object.IdentityArguments, "\x01"),
	}, "\x00")
}

func canonicalGroupEdge(edge archivalMarkerSharedGroupEdgeV1) (archivalGroupID, archivalGroupID) {
	if edge.FirstGroupID <= edge.SecondGroupID {
		return edge.FirstGroupID, edge.SecondGroupID
	}
	return edge.SecondGroupID, edge.FirstGroupID
}

func cloneOrEmpty[T any](values []T) []T {
	return append([]T{}, values...)
}
