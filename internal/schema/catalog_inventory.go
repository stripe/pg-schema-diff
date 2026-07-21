package schema

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	dbsqlc "github.com/stripe/pg-schema-diff/internal/queries"
)

type CatalogInventory struct {
	Schemas          []CatalogSchema
	Relations        []CatalogRelation
	Types            []CatalogType
	Indexes          []CatalogIndex
	Constraints      []CatalogConstraint
	Sequences        []CatalogSequence
	InheritanceEdges []CatalogInheritanceEdge
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
	OID         uint32
	SchemaOID   uint32
	SchemaName  string
	Name        string
	RelationOID uint32
	Kind        RelKind
	Extension   *CatalogExtension
}

type CatalogConstraint struct {
	OID                 uint32
	SchemaOID           uint32
	SchemaName          string
	Name                string
	Type                string
	RelationOID         uint32
	IndexOID            uint32
	ParentConstraintOID uint32
	Extension           *CatalogExtension
}

type CatalogSequence struct {
	OID        uint32
	SchemaOID  uint32
	SchemaName string
	Name       string
	Extension  *CatalogExtension
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
	i.Sequences = sortedCatalogRecords(i.Sequences, func(a, b CatalogSequence) int {
		return cmp.Or(
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.InheritanceEdges = sortedCatalogRecords(i.InheritanceEdges, func(a, b CatalogInheritanceEdge) int {
		return cmp.Or(
			cmp.Compare(a.ChildRelationOID, b.ChildRelationOID),
			cmp.Compare(a.SequenceNumber, b.SequenceNumber),
			cmp.Compare(a.ParentRelationOID, b.ParentRelationOID),
		)
	})
	return i
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
				OID:         relation.OID,
				SchemaOID:   relation.SchemaOID,
				SchemaName:  relation.SchemaName,
				Name:        relation.Name,
				RelationOID: catalogOID(rawRelation.IndexedRelationOid),
				Kind:        relation.Kind,
				Extension:   extension,
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

	rawConstraints, err := queries.GetCatalogConstraints(ctx, db)
	if err != nil {
		return CatalogInventory{}, fmt.Errorf("GetCatalogConstraints: %w", err)
	}
	for _, rawConstraint := range rawConstraints {
		inventory.Constraints = append(inventory.Constraints, CatalogConstraint{
			OID:                 catalogOID(rawConstraint.ConstraintOid),
			SchemaOID:           catalogOID(rawConstraint.SchemaOid),
			SchemaName:          rawConstraint.SchemaName,
			Name:                rawConstraint.ConstraintName,
			Type:                rawConstraint.ConstraintType,
			RelationOID:         catalogOID(rawConstraint.RelationOid),
			IndexOID:            catalogOID(rawConstraint.IndexOid),
			ParentConstraintOID: catalogOID(rawConstraint.ParentConstraintOid),
			Extension: catalogExtension(
				rawConstraint.ExtensionOid,
				rawConstraint.ExtensionName,
			),
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
