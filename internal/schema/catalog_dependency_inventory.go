package schema

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	dbsqlc "github.com/stripe/pg-schema-diff/internal/queries"
)

type CatalogDependency struct {
	IsShared   bool
	Dependent  CatalogDependencyObject
	Referenced CatalogDependencyObject
	Type       string
}

type CatalogDependencyObject struct {
	ClassOID    uint32
	ObjectOID   uint32
	SubObjectID int32
	ObjectType  string
	SchemaName  string
	Name        string
	Identity    string
}

type CatalogForeignKey struct {
	OID                    uint32
	SchemaOID              uint32
	SchemaName             string
	Name                   string
	OwningRelationOID      uint32
	OwningSchemaName       string
	OwningRelationName     string
	ReferencedRelationOID  uint32
	ReferencedSchemaName   string
	ReferencedRelationName string
	Columns                []CatalogForeignKeyColumn
	MatchType              string
	UpdateAction           string
	DeleteAction           string
	IsDeferrable           bool
	IsDeferred             bool
	IsValidated            bool
	ParentConstraintOID    uint32
	Definition             string
}

type CatalogForeignKeyColumn struct {
	OwningNumber     int16
	OwningName       string
	ReferencedNumber int16
	ReferencedName   string
}

type CatalogView struct {
	RelationOID uint32
	SchemaOID   uint32
	SchemaName  string
	Name        string
	Kind        RelKind
	IsPopulated bool
	Options     []string
	Definition  string
}

type CatalogRoutine struct {
	OID                   uint32
	SchemaOID             uint32
	SchemaName            string
	Name                  string
	OwnerOID              uint32
	OwnerName             string
	Kind                  string
	LanguageOID           uint32
	LanguageName          string
	IdentityArguments     string
	Arguments             string
	Result                string
	InputArgumentTypeOIDs []uint32
	AllArgumentTypeOIDs   []uint32
	ArgumentModes         []string
	ArgumentNames         []string
	ResultTypeOID         uint32
	ReturnsSet            bool
	Source                string
	Binary                string
	HasSQLBody            bool
	SQLBody               string
	Configuration         []string
	Definition            string
	BodyForm              CatalogRoutineBodyForm
	ReferenceTrackability CatalogRoutineReferenceTrackability
}

type CatalogRoutineBodyForm string

const (
	CatalogRoutineBodyFormOther       CatalogRoutineBodyForm = "other"
	CatalogRoutineBodyFormPLPGSQL     CatalogRoutineBodyForm = "plpgsql"
	CatalogRoutineBodyFormSQLStandard CatalogRoutineBodyForm = "sql_standard"
	CatalogRoutineBodyFormSQLString   CatalogRoutineBodyForm = "sql_string"
)

type CatalogRoutineReferenceTrackability string

const (
	CatalogRoutineReferenceTrackabilityCatalogTrackable CatalogRoutineReferenceTrackability = "catalog_trackable"
	CatalogRoutineReferenceTrackabilityUntrackable      CatalogRoutineReferenceTrackability = "untrackable"
)

// ClassifyCatalogRoutineReferenceTrackability classifies only whether PostgreSQL
// records body references in the catalogs. It does not make an archival safety decision.
func ClassifyCatalogRoutineReferenceTrackability(
	languageName string,
	hasSQLBody bool,
) (CatalogRoutineBodyForm, CatalogRoutineReferenceTrackability) {
	if languageName == "sql" {
		if hasSQLBody {
			return CatalogRoutineBodyFormSQLStandard,
				CatalogRoutineReferenceTrackabilityCatalogTrackable
		}
		return CatalogRoutineBodyFormSQLString, CatalogRoutineReferenceTrackabilityUntrackable
	}
	if languageName == "plpgsql" {
		return CatalogRoutineBodyFormPLPGSQL, CatalogRoutineReferenceTrackabilityUntrackable
	}
	return CatalogRoutineBodyFormOther, CatalogRoutineReferenceTrackabilityUntrackable
}

type CatalogEnumLabel struct {
	TypeOID   uint32
	OID       uint32
	SortOrder float64
	Label     string
}

type CatalogCompositeAttribute struct {
	TypeOID          uint32
	RelationOID      uint32
	Number           int16
	Name             string
	AttributeTypeOID uint32
	TypeModifier     int32
	CollationOID     uint32
}

type CatalogDomainConstraint struct {
	OID          uint32
	TypeOID      uint32
	Name         string
	IsDeferrable bool
	IsDeferred   bool
	IsValidated  bool
	Definition   string
}

type CatalogRange struct {
	TypeOID                uint32
	SubtypeOID             uint32
	MultirangeTypeOID      uint32
	CollationOID           uint32
	OperatorClassOID       uint32
	CanonicalFunctionOID   uint32
	SubtypeDiffFunctionOID uint32
}

type CatalogTypeSupportFunction struct {
	TypeOID                   uint32
	Role                      string
	FunctionOID               uint32
	FunctionSchemaName        string
	FunctionName              string
	FunctionIdentityArguments string
}

type CatalogCollation struct {
	OID             uint32
	SchemaOID       uint32
	SchemaName      string
	Name            string
	OwnerOID        uint32
	OwnerName       string
	Provider        string
	IsDeterministic bool
	Encoding        int32
	Collate         string
	CType           string
	Locale          string
	ICURules        string
	Version         string
}

type CatalogOperator struct {
	OID                    uint32
	SchemaOID              uint32
	SchemaName             string
	Name                   string
	OwnerOID               uint32
	OwnerName              string
	Kind                   string
	CanMerge               bool
	CanHash                bool
	LeftTypeOID            uint32
	RightTypeOID           uint32
	ResultTypeOID          uint32
	FunctionOID            uint32
	RestrictionFunctionOID uint32
	JoinFunctionOID        uint32
	CommutatorOperatorOID  uint32
	NegatorOperatorOID     uint32
	LeftType               string
	RightType              string
	ResultType             string
}

type CatalogExtensionIdentity struct {
	OID                       uint32
	Name                      string
	OwnerOID                  uint32
	OwnerName                 string
	SchemaOID                 uint32
	SchemaName                string
	IsRelocatable             bool
	Version                   string
	ConfigurationRelationOIDs []uint32
	ConfigurationConditions   []string
}

type CatalogExtensionMember struct {
	ExtensionOID  uint32
	ExtensionName string
	Object        CatalogDependencyObject
}

type CatalogEventTrigger struct {
	OID         uint32
	Name        string
	OwnerOID    uint32
	OwnerName   string
	Event       string
	FunctionOID uint32
	EnabledMode string
	Tags        []string
}

type CatalogPublication struct {
	OID                       uint32
	Name                      string
	OwnerOID                  uint32
	OwnerName                 string
	PublishesAllTables        bool
	PublishesInserts          bool
	PublishesUpdates          bool
	PublishesDeletes          bool
	PublishesTruncates        bool
	PublishesViaPartitionRoot bool
}

type CatalogPublicationRelation struct {
	OID                uint32
	PublicationOID     uint32
	PublicationName    string
	RelationOID        uint32
	RelationSchemaName string
	RelationName       string
	ColumnNumbers      []int16
	ColumnNames        []string
	RowFilter          string
}

type CatalogPublicationSchema struct {
	OID             uint32
	PublicationOID  uint32
	PublicationName string
	SchemaOID       uint32
	SchemaName      string
}

func normalizeCatalogDependencyInventory(i CatalogInventory) CatalogInventory {
	i.Dependencies = sortedCatalogRecords(i.Dependencies, func(a, b CatalogDependency) int {
		return cmp.Or(
			cmp.Compare(boolAsInt(a.IsShared), boolAsInt(b.IsShared)),
			compareCatalogDependencyObject(a.Dependent, b.Dependent),
			compareCatalogDependencyObject(a.Referenced, b.Referenced),
			cmp.Compare(a.Type, b.Type),
		)
	})
	i.ForeignKeys = sortedCatalogRecords(i.ForeignKeys, func(a, b CatalogForeignKey) int {
		return cmp.Or(
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.OwningRelationOID, b.OwningRelationOID),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.Views = sortedCatalogRecords(i.Views, func(a, b CatalogView) int {
		return cmp.Or(
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.Kind, b.Kind),
			cmp.Compare(a.RelationOID, b.RelationOID),
		)
	})
	for idx := range i.Views {
		i.Views[idx].Options = sortedStrings(i.Views[idx].Options)
	}
	i.Routines = sortedCatalogRecords(i.Routines, func(a, b CatalogRoutine) int {
		return cmp.Or(
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.IdentityArguments, b.IdentityArguments),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.EnumLabels = sortedCatalogRecords(i.EnumLabels, func(a, b CatalogEnumLabel) int {
		return cmp.Or(
			cmp.Compare(a.TypeOID, b.TypeOID),
			cmp.Compare(a.SortOrder, b.SortOrder),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.CompositeAttributes = sortedCatalogRecords(i.CompositeAttributes, func(a, b CatalogCompositeAttribute) int {
		return cmp.Or(cmp.Compare(a.TypeOID, b.TypeOID), cmp.Compare(a.Number, b.Number))
	})
	i.DomainConstraints = sortedCatalogRecords(i.DomainConstraints, func(a, b CatalogDomainConstraint) int {
		return cmp.Or(
			cmp.Compare(a.TypeOID, b.TypeOID),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.Ranges = sortedCatalogRecords(i.Ranges, func(a, b CatalogRange) int {
		return cmp.Compare(a.TypeOID, b.TypeOID)
	})
	i.TypeSupportFunctions = sortedCatalogRecords(i.TypeSupportFunctions, func(a, b CatalogTypeSupportFunction) int {
		return cmp.Or(
			cmp.Compare(a.TypeOID, b.TypeOID),
			cmp.Compare(a.Role, b.Role),
			cmp.Compare(a.FunctionOID, b.FunctionOID),
		)
	})
	i.Collations = sortedCatalogRecords(i.Collations, func(a, b CatalogCollation) int {
		return cmp.Or(
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.Operators = sortedCatalogRecords(i.Operators, func(a, b CatalogOperator) int {
		return cmp.Or(
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.LeftTypeOID, b.LeftTypeOID),
			cmp.Compare(a.RightTypeOID, b.RightTypeOID),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.Extensions = sortedCatalogRecords(i.Extensions, func(a, b CatalogExtensionIdentity) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.OID, b.OID))
	})
	i.ExtensionMembers = sortedCatalogRecords(i.ExtensionMembers, func(a, b CatalogExtensionMember) int {
		return cmp.Or(
			cmp.Compare(a.ExtensionName, b.ExtensionName),
			compareCatalogDependencyObject(a.Object, b.Object),
		)
	})
	i.EventTriggers = sortedCatalogRecords(i.EventTriggers, func(a, b CatalogEventTrigger) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.OID, b.OID))
	})
	for idx := range i.EventTriggers {
		i.EventTriggers[idx].Tags = sortedStrings(i.EventTriggers[idx].Tags)
	}
	i.Publications = sortedCatalogRecords(i.Publications, func(a, b CatalogPublication) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.OID, b.OID))
	})
	i.PublicationRelations = sortedCatalogRecords(i.PublicationRelations, func(a, b CatalogPublicationRelation) int {
		return cmp.Or(
			cmp.Compare(a.PublicationName, b.PublicationName),
			cmp.Compare(a.RelationSchemaName, b.RelationSchemaName),
			cmp.Compare(a.RelationName, b.RelationName),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.PublicationSchemas = sortedCatalogRecords(i.PublicationSchemas, func(a, b CatalogPublicationSchema) int {
		return cmp.Or(
			cmp.Compare(a.PublicationName, b.PublicationName),
			cmp.Compare(a.SchemaName, b.SchemaName),
			cmp.Compare(a.OID, b.OID),
		)
	})
	return i
}

func compareCatalogDependencyObject(a, b CatalogDependencyObject) int {
	return cmp.Or(
		cmp.Compare(a.ClassOID, b.ClassOID),
		cmp.Compare(a.ObjectOID, b.ObjectOID),
		cmp.Compare(a.SubObjectID, b.SubObjectID),
	)
}

func boolAsInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func fetchCatalogDependencyInventory(
	ctx context.Context,
	db dbsqlc.DBTX,
	inventory *CatalogInventory,
) error {
	queries := dbsqlc.New()

	rawDependencies, err := queries.GetCatalogDependencies(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogDependencies: %w", err)
	}
	for _, raw := range rawDependencies {
		inventory.Dependencies = append(inventory.Dependencies, CatalogDependency{
			IsShared: raw.IsShared,
			Dependent: CatalogDependencyObject{
				ClassOID: catalogOID(raw.ClassID), ObjectOID: catalogOID(raw.ObjectID),
				SubObjectID: raw.SubObjectID, ObjectType: raw.ObjectType,
				SchemaName: raw.ObjectSchemaName, Name: raw.ObjectName, Identity: raw.ObjectIdentity,
			},
			Referenced: CatalogDependencyObject{
				ClassOID:    catalogOID(raw.ReferencedClassID),
				ObjectOID:   catalogOID(raw.ReferencedObjectID),
				SubObjectID: raw.ReferencedSubObjectID, ObjectType: raw.ReferencedObjectType,
				SchemaName: raw.ReferencedObjectSchemaName, Name: raw.ReferencedObjectName,
				Identity: raw.ReferencedObjectIdentity,
			},
			Type: raw.DependencyType,
		})
	}

	rawForeignKeys, err := queries.GetCatalogForeignKeys(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogForeignKeys: %w", err)
	}
	for _, raw := range rawForeignKeys {
		columnCount := len(raw.OwningColumnNumbers)
		if len(raw.OwningColumnNames) != columnCount ||
			len(raw.ReferencedColumnNumbers) != columnCount ||
			len(raw.ReferencedColumnNames) != columnCount {
			return fmt.Errorf("foreign key OID %d has inconsistent column inventory", raw.ConstraintOid)
		}
		columns := make([]CatalogForeignKeyColumn, columnCount)
		for idx := range columns {
			columns[idx] = CatalogForeignKeyColumn{
				OwningNumber: raw.OwningColumnNumbers[idx], OwningName: raw.OwningColumnNames[idx],
				ReferencedNumber: raw.ReferencedColumnNumbers[idx],
				ReferencedName:   raw.ReferencedColumnNames[idx],
			}
		}
		inventory.ForeignKeys = append(inventory.ForeignKeys, CatalogForeignKey{
			OID: catalogOID(raw.ConstraintOid), SchemaOID: catalogOID(raw.SchemaOid),
			SchemaName: raw.SchemaName, Name: raw.ConstraintName,
			OwningRelationOID: catalogOID(raw.OwningRelationOid), OwningSchemaName: raw.OwningSchemaName,
			OwningRelationName:     raw.OwningRelationName,
			ReferencedRelationOID:  catalogOID(raw.ReferencedRelationOid),
			ReferencedSchemaName:   raw.ReferencedSchemaName,
			ReferencedRelationName: raw.ReferencedRelationName, Columns: columns,
			MatchType: raw.MatchType, UpdateAction: raw.UpdateAction, DeleteAction: raw.DeleteAction,
			IsDeferrable: raw.IsDeferrable, IsDeferred: raw.IsDeferred, IsValidated: raw.IsValidated,
			ParentConstraintOID: catalogOID(raw.ParentConstraintOid), Definition: raw.ConstraintDefinition,
		})
	}

	rawViews, err := queries.GetCatalogViews(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogViews: %w", err)
	}
	for _, raw := range rawViews {
		inventory.Views = append(inventory.Views, CatalogView{
			RelationOID: catalogOID(raw.RelationOid), SchemaOID: catalogOID(raw.SchemaOid),
			SchemaName: raw.SchemaName, Name: raw.ViewName, Kind: RelKind(raw.RelationKind),
			IsPopulated: raw.IsPopulated, Options: slices.Clone(raw.Options), Definition: raw.ViewDefinition,
		})
	}

	rawRoutines, err := queries.GetCatalogRoutines(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogRoutines: %w", err)
	}
	for _, raw := range rawRoutines {
		bodyForm, trackability := ClassifyCatalogRoutineReferenceTrackability(raw.LanguageName, raw.HasSqlBody)
		inventory.Routines = append(inventory.Routines, CatalogRoutine{
			OID: catalogOID(raw.RoutineOid), SchemaOID: catalogOID(raw.SchemaOid), SchemaName: raw.SchemaName,
			Name: raw.RoutineName, OwnerOID: catalogOID(raw.OwnerOid), OwnerName: raw.OwnerName,
			Kind: raw.RoutineKind, LanguageOID: catalogOID(raw.LanguageOid), LanguageName: raw.LanguageName,
			IdentityArguments: raw.IdentityArguments, Arguments: raw.Arguments, Result: raw.Result,
			InputArgumentTypeOIDs: catalogOIDs(raw.InputArgumentTypeOids),
			AllArgumentTypeOIDs:   catalogOIDs(raw.AllArgumentTypeOids),
			ArgumentModes:         slices.Clone(raw.ArgumentModes),
			ArgumentNames:         slices.Clone(raw.ArgumentNames),
			ResultTypeOID:         catalogOID(raw.ResultTypeOid), ReturnsSet: raw.ReturnsSet,
			Source: raw.Source, Binary: raw.Binary, HasSQLBody: raw.HasSqlBody, SQLBody: raw.SqlBody,
			Configuration: slices.Clone(raw.Configuration), Definition: raw.Definition,
			BodyForm: bodyForm, ReferenceTrackability: trackability,
		})
	}

	rawTypes, err := queries.GetCatalogTypes(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogTypes: %w", err)
	}
	for _, raw := range rawTypes {
		inventory.Types = append(inventory.Types, CatalogType{
			OID: catalogOID(raw.TypeOid), SchemaOID: catalogOID(raw.SchemaOid), SchemaName: raw.SchemaName,
			Name: raw.TypeName, OwnerOID: catalogOID(raw.OwnerOid), OwnerName: raw.OwnerName,
			Kind: CatalogTypeKind(raw.TypeKind), RawKind: raw.RawTypeKind, Category: raw.Category,
			IsPreferred: raw.IsPreferred, IsDefined: raw.IsDefined, InternalLength: raw.InternalLength,
			IsPassedByValue: raw.IsPassedByValue, Delimiter: raw.Delimiter, Alignment: raw.Alignment,
			Storage: raw.Storage, RelationOID: catalogOID(raw.RelationOid),
			ElementTypeOID: catalogOID(raw.ElementTypeOid),
			ArrayTypeOID:   catalogOID(raw.ArrayTypeOid),
			BaseTypeOID:    catalogOID(raw.BaseTypeOid), TypeModifier: raw.TypeModifier, Dimensions: raw.Dimensions,
			CollationOID: catalogOID(raw.CollationOid), IsNotNull: raw.IsNotNull, DefaultValue: raw.DefaultValue,
			Extension: catalogExtension(raw.ExtensionOid, raw.ExtensionName),
		})
	}

	rawEnumLabels, err := queries.GetCatalogEnumLabels(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogEnumLabels: %w", err)
	}
	for _, raw := range rawEnumLabels {
		inventory.EnumLabels = append(inventory.EnumLabels, CatalogEnumLabel{
			TypeOID: catalogOID(raw.TypeOid), OID: catalogOID(raw.LabelOid),
			SortOrder: raw.SortOrder, Label: raw.Label,
		})
	}

	rawCompositeAttributes, err := queries.GetCatalogCompositeAttributes(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogCompositeAttributes: %w", err)
	}
	for _, raw := range rawCompositeAttributes {
		inventory.CompositeAttributes = append(inventory.CompositeAttributes, CatalogCompositeAttribute{
			TypeOID: catalogOID(raw.TypeOid), RelationOID: catalogOID(raw.RelationOid), Number: raw.AttributeNumber,
			Name: raw.AttributeName, AttributeTypeOID: catalogOID(raw.AttributeTypeOid),
			TypeModifier: raw.TypeModifier, CollationOID: catalogOID(raw.CollationOid),
		})
	}

	rawDomainConstraints, err := queries.GetCatalogDomainConstraints(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogDomainConstraints: %w", err)
	}
	for _, raw := range rawDomainConstraints {
		inventory.DomainConstraints = append(inventory.DomainConstraints, CatalogDomainConstraint{
			OID: catalogOID(raw.ConstraintOid), TypeOID: catalogOID(raw.TypeOid), Name: raw.ConstraintName,
			IsDeferrable: raw.IsDeferrable, IsDeferred: raw.IsDeferred, IsValidated: raw.IsValidated,
			Definition: raw.ConstraintDefinition,
		})
	}

	rawRanges, err := queries.GetCatalogRanges(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogRanges: %w", err)
	}
	for _, raw := range rawRanges {
		inventory.Ranges = append(inventory.Ranges, CatalogRange{
			TypeOID: catalogOID(raw.RangeTypeOid), SubtypeOID: catalogOID(raw.SubtypeOid),
			MultirangeTypeOID:      catalogOID(raw.MultirangeTypeOid),
			CollationOID:           catalogOID(raw.CollationOid),
			OperatorClassOID:       catalogOID(raw.OperatorClassOid),
			CanonicalFunctionOID:   catalogOID(raw.CanonicalFunctionOid),
			SubtypeDiffFunctionOID: catalogOID(raw.SubtypeDiffFunctionOid),
		})
	}

	rawSupportFunctions, err := queries.GetCatalogTypeSupportFunctions(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogTypeSupportFunctions: %w", err)
	}
	for _, raw := range rawSupportFunctions {
		inventory.TypeSupportFunctions = append(inventory.TypeSupportFunctions, CatalogTypeSupportFunction{
			TypeOID: catalogOID(raw.TypeOid), Role: raw.SupportRole,
			FunctionOID:        catalogOID(raw.FunctionOid),
			FunctionSchemaName: raw.FunctionSchemaName, FunctionName: raw.FunctionName,
			FunctionIdentityArguments: raw.FunctionIdentityArguments,
		})
	}

	rawCollations, err := queries.GetCatalogCollations(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogCollations: %w", err)
	}
	for _, raw := range rawCollations {
		inventory.Collations = append(inventory.Collations, CatalogCollation{
			OID: catalogOID(raw.CollationOid), SchemaOID: catalogOID(raw.SchemaOid), SchemaName: raw.SchemaName,
			Name: raw.CollationName, OwnerOID: catalogOID(raw.OwnerOid), OwnerName: raw.OwnerName,
			Provider: raw.Provider, IsDeterministic: raw.IsDeterministic, Encoding: raw.Encoding,
			Collate: raw.Collate, CType: raw.Ctype, Locale: raw.Locale, ICURules: raw.IcuRules, Version: raw.Version,
		})
	}

	rawOperators, err := queries.GetCatalogOperators(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogOperators: %w", err)
	}
	for _, raw := range rawOperators {
		inventory.Operators = append(inventory.Operators, CatalogOperator{
			OID: catalogOID(raw.OperatorOid), SchemaOID: catalogOID(raw.SchemaOid), SchemaName: raw.SchemaName,
			Name: raw.OperatorName, OwnerOID: catalogOID(raw.OwnerOid), OwnerName: raw.OwnerName,
			Kind: raw.OperatorKind, CanMerge: raw.CanMerge, CanHash: raw.CanHash,
			LeftTypeOID: catalogOID(raw.LeftTypeOid), RightTypeOID: catalogOID(raw.RightTypeOid),
			ResultTypeOID:          catalogOID(raw.ResultTypeOid),
			FunctionOID:            catalogOID(raw.FunctionOid),
			RestrictionFunctionOID: catalogOID(raw.RestrictionFunctionOid),
			JoinFunctionOID:        catalogOID(raw.JoinFunctionOid),
			CommutatorOperatorOID:  catalogOID(raw.CommutatorOperatorOid),
			NegatorOperatorOID:     catalogOID(raw.NegatorOperatorOid), LeftType: raw.LeftType,
			RightType: raw.RightType, ResultType: raw.ResultType,
		})
	}

	rawSequences, err := queries.GetCatalogSequences(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogSequences: %w", err)
	}
	for _, raw := range rawSequences {
		inventory.Sequences = append(inventory.Sequences, CatalogSequence{
			OID: catalogOID(raw.SequenceOid), SchemaOID: catalogOID(raw.SchemaOid), SchemaName: raw.SchemaName,
			Name: raw.SequenceName, OwnerOID: catalogOID(raw.OwnerOid), OwnerName: raw.OwnerName,
			Persistence: RelationPersistence(raw.Persistence),
			DataTypeOID: catalogOID(raw.DataTypeOid),
			StartValue:  raw.StartValue, IncrementValue: raw.IncrementValue,
			MaxValue: raw.MaxValue, MinValue: raw.MinValue, CacheSize: raw.CacheSize, IsCycle: raw.IsCycle,
			Extension: catalogExtension(raw.ExtensionOid, raw.ExtensionName),
		})
	}

	rawExtensions, err := queries.GetCatalogExtensions(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogExtensions: %w", err)
	}
	for _, raw := range rawExtensions {
		inventory.Extensions = append(inventory.Extensions, CatalogExtensionIdentity{
			OID: catalogOID(raw.ExtensionOid), Name: raw.ExtensionName, OwnerOID: catalogOID(raw.OwnerOid),
			OwnerName: raw.OwnerName, SchemaOID: catalogOID(raw.SchemaOid), SchemaName: raw.SchemaName,
			IsRelocatable: raw.IsRelocatable, Version: raw.Version,
			ConfigurationRelationOIDs: catalogOIDs(raw.ConfigurationRelationOids),
			ConfigurationConditions:   slices.Clone(raw.ConfigurationConditions),
		})
	}

	rawExtensionMembers, err := queries.GetCatalogExtensionMembers(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogExtensionMembers: %w", err)
	}
	for _, raw := range rawExtensionMembers {
		inventory.ExtensionMembers = append(inventory.ExtensionMembers, CatalogExtensionMember{
			ExtensionOID: catalogOID(raw.ExtensionOid), ExtensionName: raw.ExtensionName,
			Object: CatalogDependencyObject{
				ClassOID: catalogOID(raw.ClassID), ObjectOID: catalogOID(raw.ObjectID), SubObjectID: raw.SubObjectID,
				ObjectType: raw.ObjectType, SchemaName: raw.ObjectSchemaName, Name: raw.ObjectName,
				Identity: raw.ObjectIdentity,
			},
		})
	}

	rawEventTriggers, err := queries.GetCatalogEventTriggers(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogEventTriggers: %w", err)
	}
	for _, raw := range rawEventTriggers {
		inventory.EventTriggers = append(inventory.EventTriggers, CatalogEventTrigger{
			OID: catalogOID(raw.EventTriggerOid), Name: raw.EventTriggerName,
			OwnerOID: catalogOID(raw.OwnerOid), OwnerName: raw.OwnerName, Event: raw.Event,
			FunctionOID: catalogOID(raw.FunctionOid), EnabledMode: raw.EnabledMode, Tags: slices.Clone(raw.Tags),
		})
	}

	rawPublications, err := queries.GetCatalogPublications(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogPublications: %w", err)
	}
	for _, raw := range rawPublications {
		inventory.Publications = append(inventory.Publications, CatalogPublication{
			OID: catalogOID(raw.PublicationOid), Name: raw.PublicationName,
			OwnerOID: catalogOID(raw.OwnerOid), OwnerName: raw.OwnerName,
			PublishesAllTables: raw.PublishesAllTables, PublishesInserts: raw.PublishesInserts,
			PublishesUpdates: raw.PublishesUpdates, PublishesDeletes: raw.PublishesDeletes,
			PublishesTruncates:        raw.PublishesTruncates,
			PublishesViaPartitionRoot: raw.PublishesViaPartitionRoot,
		})
	}

	rawPublicationRelations, err := queries.GetCatalogPublicationRelations(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogPublicationRelations: %w", err)
	}
	for _, raw := range rawPublicationRelations {
		inventory.PublicationRelations = append(inventory.PublicationRelations, CatalogPublicationRelation{
			OID: catalogOID(raw.MembershipOid), PublicationOID: catalogOID(raw.PublicationOid),
			PublicationName: raw.PublicationName, RelationOID: catalogOID(raw.RelationOid),
			RelationSchemaName: raw.RelationSchemaName, RelationName: raw.RelationName,
			ColumnNumbers: slices.Clone(raw.ColumnNumbers),
			ColumnNames:   slices.Clone(raw.ColumnNames),
			RowFilter:     raw.RowFilter,
		})
	}

	rawPublicationSchemas, err := queries.GetCatalogPublicationSchemas(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogPublicationSchemas: %w", err)
	}
	for _, raw := range rawPublicationSchemas {
		inventory.PublicationSchemas = append(inventory.PublicationSchemas, CatalogPublicationSchema{
			OID: catalogOID(raw.MembershipOid), PublicationOID: catalogOID(raw.PublicationOid),
			PublicationName: raw.PublicationName, SchemaOID: catalogOID(raw.SchemaOid), SchemaName: raw.SchemaName,
		})
	}

	return nil
}
