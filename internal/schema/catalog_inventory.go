package schema

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	dbsqlc "github.com/stripe/pg-schema-diff/internal/queries"
)

type CatalogInventory struct {
	Schemas              []CatalogSchema
	Relations            []CatalogRelation
	Types                []CatalogType
	Tables               []CatalogTable
	Columns              []CatalogColumn
	Indexes              []CatalogIndex
	Constraints          []CatalogConstraint
	Triggers             []CatalogTrigger
	Rules                []CatalogRule
	Policies             []CatalogPolicy
	Sequences            []CatalogSequence
	OwnedSequences       []CatalogOwnedSequence
	ExtendedStatistics   []CatalogExtendedStatistic
	InheritanceEdges     []CatalogInheritanceEdge
	PartitionAttachments []CatalogPartitionAttachment
	SecurityLabels       []CatalogSecurityLabel
}

type CatalogSchema struct {
	OID       uint32
	Name      string
	OwnerOID  uint32
	OwnerName string
	Comment   string
}

type CatalogRelation struct {
	OID           uint32
	SchemaOID     uint32
	SchemaName    string
	Name          string
	Comment       string
	OwnerOID      uint32
	OwnerName     string
	Kind          RelKind
	Persistence   RelationPersistence
	IsPartition   bool
	RowTypeOID    uint32
	ArrayTypeOID  uint32
	ToastRelation *CatalogRelationIdentity
	Extension     *CatalogExtension
}

type CatalogRelationIdentity struct {
	OID        uint32
	SchemaOID  uint32
	SchemaName string
	Name       string
}

type CatalogType struct {
	OID         uint32
	SchemaOID   uint32
	SchemaName  string
	Name        string
	RelationOID uint32
	Kind        CatalogTypeKind
}

type CatalogTypeKind string

const (
	CatalogTypeKindRow   CatalogTypeKind = "row"
	CatalogTypeKindArray CatalogTypeKind = "array"
)

type CatalogIndex struct {
	OID               uint32
	SchemaOID         uint32
	SchemaName        string
	Name              string
	RelationOID       uint32
	Kind              RelKind
	ConstraintOID     uint32
	IsClustered       bool
	IsReplicaIdentity bool
	IsPrimary         bool
	IsUnique          bool
	IsExclusion       bool
	IsValid           bool
	IsReady           bool
	IsLive            bool
	Extension         *CatalogExtension
}

type CatalogConstraint struct {
	OID                   uint32
	SchemaOID             uint32
	SchemaName            string
	Name                  string
	Type                  string
	RelationOID           uint32
	IndexOID              uint32
	ParentConstraintOID   uint32
	ReferencedRelationOID uint32
	KeyColumnNumbers      []int16
	IsDeferrable          bool
	IsDeferred            bool
	IsValidated           bool
	IsLocal               bool
	InheritanceCount      int32
	IsNoInherit           bool
	CheckExpression       string
	Comment               string
	Extension             *CatalogExtension
}

type CatalogSequence struct {
	OID        uint32
	SchemaOID  uint32
	SchemaName string
	Name       string
	Extension  *CatalogExtension
}

type CatalogTable struct {
	RelationOID             uint32
	RLSEnabled              bool
	RLSForced               bool
	ReplicaIdentity         string
	ReplicaIdentityIndexOID uint32
	ClusteredIndexOID       uint32
	Options                 []string
	TablespaceOID           uint32
	TablespaceName          string
	AccessMethodOID         uint32
	AccessMethodName        string
}

type CatalogColumn struct {
	RelationOID         uint32
	Number              int16
	Name                string
	IsDropped           bool
	TypeOID             uint32
	TypeSchemaOID       uint32
	TypeSchemaName      string
	TypeName            string
	TypeModifier        int32
	FormattedType       string
	CollationOID        uint32
	CollationSchemaOID  uint32
	CollationSchemaName string
	CollationName       string
	IsNotNull           bool
	IdentityMode        string
	GeneratedMode       string
	DefaultExpression   string
	GeneratedExpression string
	HasMissingValue     bool
	MissingValueBinary  string
	StorageMode         string
	CompressionMode     string
	Options             []string
	StatisticsTarget    int32
	IsLocal             bool
	InheritanceCount    int32
	Comment             string
}

type CatalogTrigger struct {
	OID              uint32
	RelationOID      uint32
	Name             string
	FunctionOID      uint32
	Type             int16
	EnabledMode      string
	IsInternal       bool
	ParentTriggerOID uint32
	ConstraintOID    uint32
	Definition       string
	Comment          string
}

type CatalogRule struct {
	OID         uint32
	RelationOID uint32
	Name        string
	EventType   string
	EnabledMode string
	IsInstead   bool
	Definition  string
	Comment     string
}

type CatalogPolicy struct {
	OID             uint32
	RelationOID     uint32
	Name            string
	Command         string
	IsPermissive    bool
	RoleOIDs        []uint32
	RoleNames       []string
	UsingExpression string
	CheckExpression string
	Comment         string
}

type CatalogOwnedSequence struct {
	SequenceOID    uint32
	RelationOID    uint32
	ColumnNumber   int32
	ColumnName     string
	DependencyType CatalogSequenceOwnershipType
}

type CatalogSequenceOwnershipType string

const (
	CatalogSequenceOwnershipTypeSerial   CatalogSequenceOwnershipType = "a"
	CatalogSequenceOwnershipTypeIdentity CatalogSequenceOwnershipType = "i"
)

type CatalogExtendedStatistic struct {
	OID           uint32
	SchemaOID     uint32
	SchemaName    string
	Name          string
	OwnerOID      uint32
	OwnerName     string
	RelationOID   uint32
	ColumnNumbers []int16
	Expressions   []string
	Kinds         []string
	Comment       string
}

type CatalogPartitionAttachment struct {
	RelationOID       uint32
	ParentRelationOID uint32
	BoundExpression   string
}

type CatalogSecurityLabel struct {
	RelationOID  uint32
	ColumnNumber int32
	Provider     string
	Label        string
}

type CatalogInheritanceEdge struct {
	ChildRelationOID  uint32
	ParentRelationOID uint32
	SequenceNumber    int32
}

type CatalogExtension struct {
	OID  uint32
	Name string
}

type RelationPersistence string

const (
	RelationPersistencePermanent RelationPersistence = "p"
	RelationPersistenceUnlogged  RelationPersistence = "u"
	RelationPersistenceTemporary RelationPersistence = "t"
)

type CatalogTableKind string

const (
	CatalogTableKindOrdinary               CatalogTableKind = "ordinary"
	CatalogTableKindPartitioned            CatalogTableKind = "partitioned"
	CatalogTableKindDeclarativePartition   CatalogTableKind = "declarative_partition"
	CatalogTableKindForeign                CatalogTableKind = "foreign"
	CatalogTableKindForeignPartition       CatalogTableKind = "foreign_partition"
	CatalogTableKindTraditionalInheritance CatalogTableKind = "traditional_inheritance"
	CatalogTableKindMultipleInheritance    CatalogTableKind = "multiple_inheritance"
)

type CatalogObjectKind string

const (
	CatalogObjectKindRelation          CatalogObjectKind = "relation"
	CatalogObjectKindRowType           CatalogObjectKind = "row_type"
	CatalogObjectKindArrayType         CatalogObjectKind = "array_type"
	CatalogObjectKindIndex             CatalogObjectKind = "index"
	CatalogObjectKindConstraint        CatalogObjectKind = "constraint"
	CatalogObjectKindTrigger           CatalogObjectKind = "trigger"
	CatalogObjectKindRule              CatalogObjectKind = "rule"
	CatalogObjectKindPolicy            CatalogObjectKind = "policy"
	CatalogObjectKindOwnedSequence     CatalogObjectKind = "owned_sequence"
	CatalogObjectKindExtendedStatistic CatalogObjectKind = "extended_statistic"
	CatalogObjectKindToastRelation     CatalogObjectKind = "toast_relation"
)

type CatalogObjectIdentity struct {
	Kind        CatalogObjectKind
	OID         uint32
	SchemaOID   uint32
	SchemaName  string
	Name        string
	RelationOID uint32
}

// CatalogExpectedTableMove groups table-local identities by SET SCHEMA behavior.
// CleanupSchemaObjects acquire the table's new schema, AttachedObjects remain
// table subobjects, ExplicitMoveObjects need separate SQL, and InternalObjects
// remain in PostgreSQL-owned schemas.
type CatalogExpectedTableMove struct {
	RelationOID          uint32
	CleanupSchemaObjects []CatalogObjectIdentity
	AttachedObjects      []CatalogObjectIdentity
	ExplicitMoveObjects  []CatalogObjectIdentity
	InternalObjects      []CatalogObjectIdentity
}

// ExpectedTableMove returns the complete deterministic local identity set for a
// table-like relation. It does not make archival or validation decisions.
func (i CatalogInventory) ExpectedTableMove(relationOID uint32) (CatalogExpectedTableMove, error) {
	relation := i.relationByOID(relationOID)
	if relation == nil {
		return CatalogExpectedTableMove{}, fmt.Errorf(
			"relation OID %d is not present in the catalog inventory", relationOID,
		)
	}
	if _, tableLike := i.ClassifyTable(relationOID); !tableLike {
		return CatalogExpectedTableMove{}, fmt.Errorf(
			"relation OID %d (%s.%s) is not table-like",
			relationOID, relation.SchemaName, relation.Name,
		)
	}

	move := CatalogExpectedTableMove{RelationOID: relationOID}
	move.CleanupSchemaObjects = append(move.CleanupSchemaObjects, CatalogObjectIdentity{
		Kind:        CatalogObjectKindRelation,
		OID:         relation.OID,
		SchemaOID:   relation.SchemaOID,
		SchemaName:  relation.SchemaName,
		Name:        relation.Name,
		RelationOID: relationOID,
	})
	for _, catalogType := range i.Types {
		if catalogType.RelationOID != relationOID {
			continue
		}
		kind := CatalogObjectKindRowType
		if catalogType.Kind == CatalogTypeKindArray {
			kind = CatalogObjectKindArrayType
		}
		move.CleanupSchemaObjects = append(move.CleanupSchemaObjects, CatalogObjectIdentity{
			Kind:        kind,
			OID:         catalogType.OID,
			SchemaOID:   catalogType.SchemaOID,
			SchemaName:  catalogType.SchemaName,
			Name:        catalogType.Name,
			RelationOID: relationOID,
		})
	}
	for _, index := range i.Indexes {
		if index.RelationOID == relationOID {
			move.CleanupSchemaObjects = append(move.CleanupSchemaObjects, CatalogObjectIdentity{
				Kind: CatalogObjectKindIndex, OID: index.OID, SchemaOID: index.SchemaOID,
				SchemaName: index.SchemaName, Name: index.Name, RelationOID: relationOID,
			})
		}
	}
	for _, constraint := range i.Constraints {
		if constraint.RelationOID == relationOID {
			move.CleanupSchemaObjects = append(move.CleanupSchemaObjects, CatalogObjectIdentity{
				Kind: CatalogObjectKindConstraint, OID: constraint.OID, SchemaOID: constraint.SchemaOID,
				SchemaName: constraint.SchemaName, Name: constraint.Name, RelationOID: relationOID,
			})
		}
	}
	for _, ownership := range i.OwnedSequences {
		if ownership.RelationOID != relationOID {
			continue
		}
		sequence := i.sequenceByOID(ownership.SequenceOID)
		if sequence == nil {
			return CatalogExpectedTableMove{}, fmt.Errorf(
				"owned sequence OID %d for relation OID %d is not present in the catalog inventory",
				ownership.SequenceOID, relationOID,
			)
		}
		move.CleanupSchemaObjects = append(move.CleanupSchemaObjects, CatalogObjectIdentity{
			Kind: CatalogObjectKindOwnedSequence, OID: sequence.OID, SchemaOID: sequence.SchemaOID,
			SchemaName: sequence.SchemaName, Name: sequence.Name, RelationOID: relationOID,
		})
	}

	for _, trigger := range i.Triggers {
		if trigger.RelationOID == relationOID {
			move.AttachedObjects = append(move.AttachedObjects, relationSubObjectIdentity(
				relation, CatalogObjectKindTrigger, trigger.OID, trigger.Name,
			))
		}
	}
	for _, rule := range i.Rules {
		if rule.RelationOID == relationOID {
			move.AttachedObjects = append(move.AttachedObjects, relationSubObjectIdentity(
				relation, CatalogObjectKindRule, rule.OID, rule.Name,
			))
		}
	}
	for _, policy := range i.Policies {
		if policy.RelationOID == relationOID {
			move.AttachedObjects = append(move.AttachedObjects, relationSubObjectIdentity(
				relation, CatalogObjectKindPolicy, policy.OID, policy.Name,
			))
		}
	}
	for _, statistic := range i.ExtendedStatistics {
		if statistic.RelationOID == relationOID {
			move.ExplicitMoveObjects = append(move.ExplicitMoveObjects, CatalogObjectIdentity{
				Kind: CatalogObjectKindExtendedStatistic, OID: statistic.OID, SchemaOID: statistic.SchemaOID,
				SchemaName: statistic.SchemaName, Name: statistic.Name, RelationOID: relationOID,
			})
		}
	}
	if relation.ToastRelation != nil {
		move.InternalObjects = append(move.InternalObjects, CatalogObjectIdentity{
			Kind: CatalogObjectKindToastRelation, OID: relation.ToastRelation.OID,
			SchemaOID:  relation.ToastRelation.SchemaOID,
			SchemaName: relation.ToastRelation.SchemaName,
			Name:       relation.ToastRelation.Name, RelationOID: relationOID,
		})
	}

	move.CleanupSchemaObjects = sortedCatalogObjectIdentities(move.CleanupSchemaObjects)
	move.AttachedObjects = sortedCatalogObjectIdentities(move.AttachedObjects)
	move.ExplicitMoveObjects = sortedCatalogObjectIdentities(move.ExplicitMoveObjects)
	move.InternalObjects = sortedCatalogObjectIdentities(move.InternalObjects)
	return move, nil
}

func relationSubObjectIdentity(
	relation *CatalogRelation,
	kind CatalogObjectKind,
	oid uint32,
	name string,
) CatalogObjectIdentity {
	return CatalogObjectIdentity{
		Kind: kind, OID: oid, SchemaOID: relation.SchemaOID, SchemaName: relation.SchemaName,
		Name: name, RelationOID: relation.OID,
	}
}

func sortedCatalogObjectIdentities(objects []CatalogObjectIdentity) []CatalogObjectIdentity {
	return sortedCatalogRecords(objects, func(a, b CatalogObjectIdentity) int {
		return cmp.Or(
			cmp.Compare(a.Kind, b.Kind),
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.OID, b.OID),
		)
	})
}

// ClassifyTable classifies a table-like relation using only raw catalog facts.
// The boolean is false when the OID does not identify a table-like relation.
func (i CatalogInventory) ClassifyTable(relationOID uint32) (CatalogTableKind, bool) {
	relation := i.relationByOID(relationOID)
	if relation == nil || (relation.Kind != RelKindOrdinaryTable &&
		relation.Kind != RelKindPartitionedTable && relation.Kind != RelKindForeignTable) {
		return "", false
	}

	traditionalParentCount := 0
	hasTraditionalInheritance := false
	for _, edge := range i.InheritanceEdges {
		child := i.relationByOID(edge.ChildRelationOID)
		if child == nil || child.IsPartition || (child.Kind != RelKindOrdinaryTable &&
			child.Kind != RelKindPartitionedTable && child.Kind != RelKindForeignTable) {
			continue
		}
		if edge.ChildRelationOID == relationOID || edge.ParentRelationOID == relationOID {
			hasTraditionalInheritance = true
		}
		if edge.ChildRelationOID == relationOID {
			traditionalParentCount++
		}
	}
	if relation.IsPartition {
		if relation.Kind == RelKindForeignTable {
			return CatalogTableKindForeignPartition, true
		}
		return CatalogTableKindDeclarativePartition, true
	}
	if traditionalParentCount > 1 {
		return CatalogTableKindMultipleInheritance, true
	}
	if hasTraditionalInheritance {
		return CatalogTableKindTraditionalInheritance, true
	}
	if relation.Kind == RelKindPartitionedTable {
		return CatalogTableKindPartitioned, true
	}
	if relation.Kind == RelKindForeignTable {
		return CatalogTableKindForeign, true
	}
	return CatalogTableKindOrdinary, true
}

func (i CatalogInventory) relationByOID(oid uint32) *CatalogRelation {
	for idx := range i.Relations {
		if i.Relations[idx].OID == oid {
			return &i.Relations[idx]
		}
	}
	return nil
}

func (i CatalogInventory) sequenceByOID(oid uint32) *CatalogSequence {
	for idx := range i.Sequences {
		if i.Sequences[idx].OID == oid {
			return &i.Sequences[idx]
		}
	}
	return nil
}

// Normalize returns an inventory with every record in deterministic identity order.
func (i CatalogInventory) Normalize() CatalogInventory {
	i.Schemas = sortedCatalogRecords(i.Schemas, func(a, b CatalogSchema) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.OID, b.OID))
	})
	i.Relations = sortedCatalogRecords(i.Relations, func(a, b CatalogRelation) int {
		return cmp.Or(
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.Kind, b.Kind),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.Types = sortedCatalogRecords(i.Types, func(a, b CatalogType) int {
		return cmp.Or(
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.Kind, b.Kind),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.Tables = sortedCatalogRecords(i.Tables, func(a, b CatalogTable) int {
		return cmp.Compare(a.RelationOID, b.RelationOID)
	})
	for idx := range i.Tables {
		i.Tables[idx].Options = sortedStrings(i.Tables[idx].Options)
	}
	i.Columns = sortedCatalogRecords(i.Columns, func(a, b CatalogColumn) int {
		return cmp.Or(
			cmp.Compare(a.RelationOID, b.RelationOID),
			cmp.Compare(a.Number, b.Number),
		)
	})
	for idx := range i.Columns {
		i.Columns[idx].Options = sortedStrings(i.Columns[idx].Options)
	}
	i.Indexes = sortedCatalogRecords(i.Indexes, func(a, b CatalogIndex) int {
		return cmp.Or(
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.Constraints = sortedCatalogRecords(i.Constraints, func(a, b CatalogConstraint) int {
		return cmp.Or(
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.RelationOID, b.RelationOID),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.Triggers = sortedCatalogRecords(i.Triggers, func(a, b CatalogTrigger) int {
		return cmp.Or(
			cmp.Compare(a.RelationOID, b.RelationOID),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.Rules = sortedCatalogRecords(i.Rules, func(a, b CatalogRule) int {
		return cmp.Or(
			cmp.Compare(a.RelationOID, b.RelationOID),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.Policies = sortedCatalogRecords(i.Policies, func(a, b CatalogPolicy) int {
		return cmp.Or(
			cmp.Compare(a.RelationOID, b.RelationOID),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.Sequences = sortedCatalogRecords(i.Sequences, func(a, b CatalogSequence) int {
		return cmp.Or(
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.OwnedSequences = sortedCatalogRecords(i.OwnedSequences, func(a, b CatalogOwnedSequence) int {
		return cmp.Or(
			cmp.Compare(a.RelationOID, b.RelationOID),
			cmp.Compare(a.ColumnNumber, b.ColumnNumber),
			cmp.Compare(a.SequenceOID, b.SequenceOID),
		)
	})
	i.ExtendedStatistics = sortedCatalogRecords(i.ExtendedStatistics, func(a, b CatalogExtendedStatistic) int {
		return cmp.Or(
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.OID, b.OID),
		)
	})
	for idx := range i.ExtendedStatistics {
		i.ExtendedStatistics[idx].Kinds = sortedStrings(i.ExtendedStatistics[idx].Kinds)
	}
	i.InheritanceEdges = sortedCatalogRecords(i.InheritanceEdges, func(a, b CatalogInheritanceEdge) int {
		return cmp.Or(
			cmp.Compare(a.ChildRelationOID, b.ChildRelationOID),
			cmp.Compare(a.SequenceNumber, b.SequenceNumber),
			cmp.Compare(a.ParentRelationOID, b.ParentRelationOID),
		)
	})
	i.PartitionAttachments = sortedCatalogRecords(i.PartitionAttachments, func(a, b CatalogPartitionAttachment) int {
		return cmp.Or(
			cmp.Compare(a.RelationOID, b.RelationOID),
			cmp.Compare(a.ParentRelationOID, b.ParentRelationOID),
		)
	})
	i.SecurityLabels = sortedCatalogRecords(i.SecurityLabels, func(a, b CatalogSecurityLabel) int {
		return cmp.Or(
			cmp.Compare(a.RelationOID, b.RelationOID),
			cmp.Compare(a.ColumnNumber, b.ColumnNumber),
			cmp.Compare(a.Provider, b.Provider),
			cmp.Compare(a.Label, b.Label),
		)
	})
	return i
}

func sortedStrings(values []string) []string {
	return sortedCatalogRecords(values, cmp.Compare[string])
}

func sortedCatalogRecords[T any](records []T, compare func(T, T) int) []T {
	cloned := slices.Clone(records)
	slices.SortFunc(cloned, compare)
	return cloned
}

func fetchCatalogInventory(ctx context.Context, db dbsqlc.DBTX) (CatalogInventory, error) {
	queries := dbsqlc.New()
	var inventory CatalogInventory

	rawSchemas, err := queries.GetCatalogSchemas(ctx, db)
	if err != nil {
		return CatalogInventory{}, fmt.Errorf("GetCatalogSchemas: %w", err)
	}
	for _, rawSchema := range rawSchemas {
		inventory.Schemas = append(inventory.Schemas, CatalogSchema{
			OID:       catalogOID(rawSchema.SchemaOid),
			Name:      rawSchema.SchemaName,
			OwnerOID:  catalogOID(rawSchema.OwnerOid),
			OwnerName: rawSchema.OwnerName,
			Comment:   rawSchema.SchemaComment,
		})
	}

	rawRelations, err := queries.GetCatalogRelations(ctx, db)
	if err != nil {
		return CatalogInventory{}, fmt.Errorf("GetCatalogRelations: %w", err)
	}
	for _, rawRelation := range rawRelations {
		extension := catalogExtension(rawRelation.ExtensionOid, rawRelation.ExtensionName)
		relation := CatalogRelation{
			OID:          catalogOID(rawRelation.RelationOid),
			SchemaOID:    catalogOID(rawRelation.SchemaOid),
			SchemaName:   rawRelation.SchemaName,
			Name:         rawRelation.RelationName,
			Comment:      rawRelation.RelationComment,
			OwnerOID:     catalogOID(rawRelation.OwnerOid),
			OwnerName:    rawRelation.OwnerName,
			Kind:         RelKind(rawRelation.RelationKind),
			Persistence:  RelationPersistence(rawRelation.Persistence),
			IsPartition:  rawRelation.IsPartition,
			RowTypeOID:   catalogOID(rawRelation.RowTypeOid),
			ArrayTypeOID: catalogOID(rawRelation.ArrayTypeOid),
			Extension:    extension,
		}
		if rawRelation.ToastRelationOid != 0 {
			relation.ToastRelation = &CatalogRelationIdentity{
				OID:        catalogOID(rawRelation.ToastRelationOid),
				SchemaOID:  catalogOID(rawRelation.ToastSchemaOid),
				SchemaName: rawRelation.ToastSchemaName,
				Name:       rawRelation.ToastRelationName,
			}
		}
		inventory.Relations = append(inventory.Relations, relation)

		if rawRelation.RowTypeOid != 0 {
			inventory.Types = append(inventory.Types, CatalogType{
				OID:         catalogOID(rawRelation.RowTypeOid),
				SchemaOID:   catalogOID(rawRelation.RowTypeSchemaOid),
				SchemaName:  rawRelation.RowTypeSchemaName,
				Name:        rawRelation.RowTypeName,
				RelationOID: relation.OID,
				Kind:        CatalogTypeKindRow,
			})
		}
		if rawRelation.ArrayTypeOid != 0 {
			inventory.Types = append(inventory.Types, CatalogType{
				OID:         catalogOID(rawRelation.ArrayTypeOid),
				SchemaOID:   catalogOID(rawRelation.ArrayTypeSchemaOid),
				SchemaName:  rawRelation.ArrayTypeSchemaName,
				Name:        rawRelation.ArrayTypeName,
				RelationOID: relation.OID,
				Kind:        CatalogTypeKindArray,
			})
		}
		switch relation.Kind {
		case RelKindIndex, RelKindPartitionedIndex:
			inventory.Indexes = append(inventory.Indexes, CatalogIndex{
				OID:               relation.OID,
				SchemaOID:         relation.SchemaOID,
				SchemaName:        relation.SchemaName,
				Name:              relation.Name,
				RelationOID:       catalogOID(rawRelation.IndexedRelationOid),
				Kind:              relation.Kind,
				ConstraintOID:     catalogOID(rawRelation.IndexConstraintOid),
				IsClustered:       rawRelation.IndexIsClustered,
				IsReplicaIdentity: rawRelation.IndexIsReplicaIdentity,
				IsPrimary:         rawRelation.IndexIsPrimary,
				IsUnique:          rawRelation.IndexIsUnique,
				IsExclusion:       rawRelation.IndexIsExclusion,
				IsValid:           rawRelation.IndexIsValid,
				IsReady:           rawRelation.IndexIsReady,
				IsLive:            rawRelation.IndexIsLive,
				Extension:         extension,
			})
		case RelKindSequence:
			inventory.Sequences = append(inventory.Sequences, CatalogSequence{
				OID:        relation.OID,
				SchemaOID:  relation.SchemaOID,
				SchemaName: relation.SchemaName,
				Name:       relation.Name,
				Extension:  extension,
			})
		}
	}

	rawTables, err := queries.GetCatalogTables(ctx, db)
	if err != nil {
		return CatalogInventory{}, fmt.Errorf("GetCatalogTables: %w", err)
	}
	for _, rawTable := range rawTables {
		inventory.Tables = append(inventory.Tables, CatalogTable{
			RelationOID:             catalogOID(rawTable.RelationOid),
			RLSEnabled:              rawTable.RlsEnabled,
			RLSForced:               rawTable.RlsForced,
			ReplicaIdentity:         rawTable.ReplicaIdentity,
			ReplicaIdentityIndexOID: catalogOID(rawTable.ReplicaIdentityIndexOid),
			ClusteredIndexOID:       catalogOID(rawTable.ClusteredIndexOid),
			Options:                 slices.Clone(rawTable.Options),
			TablespaceOID:           catalogOID(rawTable.TablespaceOid),
			TablespaceName:          rawTable.TablespaceName,
			AccessMethodOID:         catalogOID(rawTable.AccessMethodOid),
			AccessMethodName:        rawTable.AccessMethodName,
		})
	}

	rawColumns, err := queries.GetCatalogColumns(ctx, db)
	if err != nil {
		return CatalogInventory{}, fmt.Errorf("GetCatalogColumns: %w", err)
	}
	for _, rawColumn := range rawColumns {
		inventory.Columns = append(inventory.Columns, CatalogColumn{
			RelationOID:         catalogOID(rawColumn.RelationOid),
			Number:              rawColumn.ColumnNumber,
			Name:                rawColumn.ColumnName,
			IsDropped:           rawColumn.IsDropped,
			TypeOID:             catalogOID(rawColumn.TypeOid),
			TypeSchemaOID:       catalogOID(rawColumn.TypeSchemaOid),
			TypeSchemaName:      rawColumn.TypeSchemaName,
			TypeName:            rawColumn.TypeName,
			TypeModifier:        rawColumn.TypeModifier,
			FormattedType:       rawColumn.FormattedType,
			CollationOID:        catalogOID(rawColumn.CollationOid),
			CollationSchemaOID:  catalogOID(rawColumn.CollationSchemaOid),
			CollationSchemaName: rawColumn.CollationSchemaName,
			CollationName:       rawColumn.CollationName,
			IsNotNull:           rawColumn.IsNotNull,
			IdentityMode:        rawColumn.IdentityMode,
			GeneratedMode:       rawColumn.GeneratedMode,
			DefaultExpression:   rawColumn.DefaultExpression,
			GeneratedExpression: rawColumn.GeneratedExpression,
			HasMissingValue:     rawColumn.HasMissingValue,
			MissingValueBinary:  rawColumn.MissingValueBinary,
			StorageMode:         rawColumn.StorageMode,
			CompressionMode:     rawColumn.CompressionMode,
			Options:             slices.Clone(rawColumn.Options),
			StatisticsTarget:    rawColumn.StatisticsTarget,
			IsLocal:             rawColumn.IsLocal,
			InheritanceCount:    rawColumn.InheritanceCount,
			Comment:             rawColumn.ColumnComment,
		})
	}

	rawConstraints, err := queries.GetCatalogConstraints(ctx, db)
	if err != nil {
		return CatalogInventory{}, fmt.Errorf("GetCatalogConstraints: %w", err)
	}
	for _, rawConstraint := range rawConstraints {
		inventory.Constraints = append(inventory.Constraints, CatalogConstraint{
			OID:                   catalogOID(rawConstraint.ConstraintOid),
			SchemaOID:             catalogOID(rawConstraint.SchemaOid),
			SchemaName:            rawConstraint.SchemaName,
			Name:                  rawConstraint.ConstraintName,
			Type:                  rawConstraint.ConstraintType,
			RelationOID:           catalogOID(rawConstraint.RelationOid),
			IndexOID:              catalogOID(rawConstraint.IndexOid),
			ParentConstraintOID:   catalogOID(rawConstraint.ParentConstraintOid),
			ReferencedRelationOID: catalogOID(rawConstraint.ReferencedRelationOid),
			KeyColumnNumbers:      slices.Clone(rawConstraint.KeyColumnNumbers),
			IsDeferrable:          rawConstraint.IsDeferrable,
			IsDeferred:            rawConstraint.IsDeferred,
			IsValidated:           rawConstraint.IsValidated,
			IsLocal:               rawConstraint.IsLocal,
			InheritanceCount:      rawConstraint.InheritanceCount,
			IsNoInherit:           rawConstraint.IsNoInherit,
			CheckExpression:       rawConstraint.CheckExpression,
			Comment:               rawConstraint.ConstraintComment,
			Extension: catalogExtension(
				rawConstraint.ExtensionOid,
				rawConstraint.ExtensionName,
			),
		})
	}

	rawTriggers, err := queries.GetCatalogTriggers(ctx, db)
	if err != nil {
		return CatalogInventory{}, fmt.Errorf("GetCatalogTriggers: %w", err)
	}
	for _, rawTrigger := range rawTriggers {
		inventory.Triggers = append(inventory.Triggers, CatalogTrigger{
			OID:              catalogOID(rawTrigger.TriggerOid),
			RelationOID:      catalogOID(rawTrigger.RelationOid),
			Name:             rawTrigger.TriggerName,
			FunctionOID:      catalogOID(rawTrigger.FunctionOid),
			Type:             rawTrigger.TriggerType,
			EnabledMode:      rawTrigger.EnabledMode,
			IsInternal:       rawTrigger.IsInternal,
			ParentTriggerOID: catalogOID(rawTrigger.ParentTriggerOid),
			ConstraintOID:    catalogOID(rawTrigger.ConstraintOid),
			Definition:       rawTrigger.TriggerDefinition,
			Comment:          rawTrigger.TriggerComment,
		})
	}

	rawRules, err := queries.GetCatalogRules(ctx, db)
	if err != nil {
		return CatalogInventory{}, fmt.Errorf("GetCatalogRules: %w", err)
	}
	for _, rawRule := range rawRules {
		inventory.Rules = append(inventory.Rules, CatalogRule{
			OID:         catalogOID(rawRule.RuleOid),
			RelationOID: catalogOID(rawRule.RelationOid),
			Name:        rawRule.RuleName,
			EventType:   rawRule.EventType,
			EnabledMode: rawRule.EnabledMode,
			IsInstead:   rawRule.IsInstead,
			Definition:  rawRule.RuleDefinition,
			Comment:     rawRule.RuleComment,
		})
	}

	rawPolicies, err := queries.GetCatalogPolicies(ctx, db)
	if err != nil {
		return CatalogInventory{}, fmt.Errorf("GetCatalogPolicies: %w", err)
	}
	for _, rawPolicy := range rawPolicies {
		inventory.Policies = append(inventory.Policies, CatalogPolicy{
			OID:             catalogOID(rawPolicy.PolicyOid),
			RelationOID:     catalogOID(rawPolicy.RelationOid),
			Name:            rawPolicy.PolicyName,
			Command:         rawPolicy.Command,
			IsPermissive:    rawPolicy.IsPermissive,
			RoleOIDs:        catalogOIDs(rawPolicy.RoleOids),
			RoleNames:       slices.Clone(rawPolicy.RoleNames),
			UsingExpression: rawPolicy.UsingExpression,
			CheckExpression: rawPolicy.CheckExpression,
			Comment:         rawPolicy.PolicyComment,
		})
	}

	rawOwnedSequences, err := queries.GetCatalogOwnedSequences(ctx, db)
	if err != nil {
		return CatalogInventory{}, fmt.Errorf("GetCatalogOwnedSequences: %w", err)
	}
	for _, rawOwnership := range rawOwnedSequences {
		inventory.OwnedSequences = append(inventory.OwnedSequences, CatalogOwnedSequence{
			SequenceOID:    catalogOID(rawOwnership.SequenceOid),
			RelationOID:    catalogOID(rawOwnership.RelationOid),
			ColumnNumber:   rawOwnership.ColumnNumber,
			ColumnName:     rawOwnership.ColumnName,
			DependencyType: CatalogSequenceOwnershipType(rawOwnership.DependencyType),
		})
	}

	rawStatistics, err := queries.GetCatalogExtendedStatistics(ctx, db)
	if err != nil {
		return CatalogInventory{}, fmt.Errorf("GetCatalogExtendedStatistics: %w", err)
	}
	for _, rawStatistic := range rawStatistics {
		inventory.ExtendedStatistics = append(inventory.ExtendedStatistics, CatalogExtendedStatistic{
			OID:           catalogOID(rawStatistic.StatisticOid),
			SchemaOID:     catalogOID(rawStatistic.SchemaOid),
			SchemaName:    rawStatistic.SchemaName,
			Name:          rawStatistic.StatisticName,
			OwnerOID:      catalogOID(rawStatistic.OwnerOid),
			OwnerName:     rawStatistic.OwnerName,
			RelationOID:   catalogOID(rawStatistic.RelationOid),
			ColumnNumbers: slices.Clone(rawStatistic.ColumnNumbers),
			Expressions:   slices.Clone(rawStatistic.Expressions),
			Kinds:         slices.Clone(rawStatistic.Kinds),
			Comment:       rawStatistic.StatisticComment,
		})
	}

	rawEdges, err := queries.GetCatalogInheritanceEdges(ctx, db)
	if err != nil {
		return CatalogInventory{}, fmt.Errorf("GetCatalogInheritanceEdges: %w", err)
	}
	for _, rawEdge := range rawEdges {
		inventory.InheritanceEdges = append(inventory.InheritanceEdges, CatalogInheritanceEdge{
			ChildRelationOID:  catalogOID(rawEdge.ChildRelationOid),
			ParentRelationOID: catalogOID(rawEdge.ParentRelationOid),
			SequenceNumber:    rawEdge.SequenceNumber,
		})
	}

	rawAttachments, err := queries.GetCatalogPartitionAttachments(ctx, db)
	if err != nil {
		return CatalogInventory{}, fmt.Errorf("GetCatalogPartitionAttachments: %w", err)
	}
	for _, rawAttachment := range rawAttachments {
		inventory.PartitionAttachments = append(inventory.PartitionAttachments, CatalogPartitionAttachment{
			RelationOID:       catalogOID(rawAttachment.RelationOid),
			ParentRelationOID: catalogOID(rawAttachment.ParentRelationOid),
			BoundExpression:   rawAttachment.BoundExpression,
		})
	}

	rawSecurityLabels, err := queries.GetCatalogSecurityLabels(ctx, db)
	if err != nil {
		return CatalogInventory{}, fmt.Errorf("GetCatalogSecurityLabels: %w", err)
	}
	for _, rawLabel := range rawSecurityLabels {
		inventory.SecurityLabels = append(inventory.SecurityLabels, CatalogSecurityLabel{
			RelationOID:  catalogOID(rawLabel.RelationOid),
			ColumnNumber: rawLabel.ColumnNumber,
			Provider:     rawLabel.Provider,
			Label:        rawLabel.Label,
		})
	}

	return inventory.Normalize(), nil
}

func catalogExtension(oid int64, name string) *CatalogExtension {
	if oid == 0 {
		return nil
	}
	return &CatalogExtension{OID: catalogOID(oid), Name: name}
}

func catalogOID(oid int64) uint32 {
	return uint32(oid)
}

func catalogOIDs(oids []int64) []uint32 {
	result := make([]uint32, len(oids))
	for idx, oid := range oids {
		result[idx] = catalogOID(oid)
	}
	return result
}
