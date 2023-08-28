package schema

import (
	"context"
	"fmt"
	"regexp"
	"sort"

	"github.com/mitchellh/hashstructure/v2"
	"github.com/stripe/pg-schema-diff/internal/queries"
)

type (
	// Object represents a resource in a schema (table, column, index...)
	Object interface {
		// GetName is used to identify the old and new versions of a schema object between the old and new schemas
		// If the name is not present in the old schema objects list, then it is added
		// If the name is not present in the new schemas objects list, then it is removed
		// Otherwise, it has persisted across two schemas and is possibly altered
		GetName() string
	}

	// SchemaQualifiedName represents a schema object name scoped within a schema
	SchemaQualifiedName struct {
		SchemaName string
		// EscapedName is the name of the object. It should already be escaped
		// We take an escaped name because there are weird exceptions, like functions, where we can't just
		// surround the name in quotes
		EscapedName string
	}
)

func (o SchemaQualifiedName) GetName() string {
	return o.GetFQEscapedName()
}

// GetFQEscapedName gets the fully-qualified, escaped name of the schema object, including the schema name
func (o SchemaQualifiedName) GetFQEscapedName() string {
	return fmt.Sprintf("%s.%s", EscapeIdentifier(o.SchemaName), o.EscapedName)
}

func (o SchemaQualifiedName) IsEmpty() bool {
	return len(o.SchemaName) == 0
}

type Schema struct {
	Extensions            []Extension
	Tables                []Table
	Indexes               []Index
	ForeignKeyConstraints []ForeignKeyConstraint
	UniqueConstraints     []UniqueConstraint
	Sequences             []Sequence
	Functions             []Function
	Triggers              []Trigger
}

// Normalize normalizes the schema (alphabetically sorts tables and columns in tables)
// Useful for hashing and testing
func (s Schema) Normalize() Schema {
	var normTables []Table
	for _, table := range sortSchemaObjectsByName(s.Tables) {
		// Don't normalize columns order. their order is derived from the postgres catalogs
		// (relevant to data packing)
		var normCheckConstraints []CheckConstraint
		for _, checkConstraint := range sortSchemaObjectsByName(table.CheckConstraints) {
			checkConstraint.DependsOnFunctions = sortSchemaObjectsByName(checkConstraint.DependsOnFunctions)
			normCheckConstraints = append(normCheckConstraints, checkConstraint)
		}
		table.CheckConstraints = normCheckConstraints
		normTables = append(normTables, table)
	}
	s.Tables = normTables

	s.Indexes = sortSchemaObjectsByName(s.Indexes)
	s.ForeignKeyConstraints = sortSchemaObjectsByName(s.ForeignKeyConstraints)
	s.Sequences = sortSchemaObjectsByName(s.Sequences)
	s.Extensions = sortSchemaObjectsByName(s.Extensions)

	var normFunctions []Function
	for _, function := range sortSchemaObjectsByName(s.Functions) {
		function.DependsOnFunctions = sortSchemaObjectsByName(function.DependsOnFunctions)
		normFunctions = append(normFunctions, function)
	}
	s.Functions = normFunctions

	s.Triggers = sortSchemaObjectsByName(s.Triggers)

	return s
}

// sortSchemaObjectsByName returns a (copied) sorted list of schema objects.
func sortSchemaObjectsByName[S Object](vals []S) []S {
	clonedVals := make([]S, len(vals))
	copy(clonedVals, vals)
	sort.Slice(clonedVals, func(i, j int) bool {
		return clonedVals[i].GetName() < clonedVals[j].GetName()
	})
	return clonedVals
}

func (s Schema) Hash() (string, error) {
	// alternatively, we can print the struct as a string and hash it
	hashVal, err := hashstructure.Hash(s.Normalize(), hashstructure.FormatV2, nil)
	if err != nil {
		return "", fmt.Errorf("hashing schema: %w", err)
	}
	return fmt.Sprintf("%x", hashVal), nil
}

type Extension struct {
	SchemaQualifiedName
	Version string
}

type Table struct {
	Name             string
	Columns          []Column
	CheckConstraints []CheckConstraint

	// PartitionKeyDef is the output of Pg function pg_get_partkeydef:
	// PARTITION BY $PartitionKeyDef
	// If empty, then the table is not partitioned
	PartitionKeyDef string

	ParentTableName string
	ForValues       string
}

func (t Table) IsPartitioned() bool {
	return len(t.PartitionKeyDef) > 0
}

func (t Table) IsPartition() bool {
	return len(t.ForValues) > 0
}

func (t Table) GetName() string {
	return t.Name
}

type Column struct {
	Name      string
	Type      string
	Collation SchemaQualifiedName
	// If the column has a default value, this will be a SQL string representing that value.
	// Examples:
	//   ''::text
	//   CURRENT_TIMESTAMP
	// If empty, indicates that there is no default value.
	Default    string
	IsNullable bool

	// Size is the number of bytes required to store the value.
	// It is used for data-packing purposes
	Size int //
}

func (c Column) GetName() string {
	return c.Name
}

func (c Column) IsCollated() bool {
	return !c.Collation.IsEmpty()
}

var (
	// The first matching group is the "CREATE [UNIQUE] INDEX ". UNIQUE is an optional match
	// because only UNIQUE indices will have the UNIQUE keyword in their pg_get_indexdef statement
	//
	// The third matching group is the rest of the statement
	idxToConcurrentlyRegex = regexp.MustCompile("^(CREATE (UNIQUE )?INDEX )(.*)$")
)

// GetIndexDefStatement is the output of pg_getindexdef. It is a `CREATE INDEX` statement that will re-create
// the index. This statement does not contain `CONCURRENTLY`.
// For unique indexes, it does contain `UNIQUE`
// For partitioned tables, it does contain `ONLY`
type GetIndexDefStatement string

func (i GetIndexDefStatement) ToCreateIndexConcurrently() (string, error) {
	if !idxToConcurrentlyRegex.MatchString(string(i)) {
		return "", fmt.Errorf("%s follows an unexpected structure", i)
	}
	return idxToConcurrentlyRegex.ReplaceAllString(string(i), "${1}CONCURRENTLY ${3}"), nil
}

type Index struct {
	TableName string
	Name      string
	Columns   []string
	IsInvalid bool
	IsPk      bool
	IsUnique  bool
	// ConstraintName is the name of the constraint associated with an index. Empty string if no associated constraint.
	// Once we need support for constraints not associated with indexes, we'll add a
	// Constraint schema object and starting fetching constraints directly
	ConstraintName string

	// GetIndexDefStmt is the output of pg_getindexdef
	GetIndexDefStmt GetIndexDefStatement

	// ParentIdxName is the name of the parent index if the index is a partition of an index
	ParentIdxName string
}

func (i Index) GetName() string {
	return i.Name
}

func (i Index) IsPartitionOfIndex() bool {
	return len(i.ParentIdxName) > 0
}

type CheckConstraint struct {
	Name               string
	Expression         string
	IsValid            bool
	IsInheritable      bool
	DependsOnFunctions []SchemaQualifiedName
}

func (c CheckConstraint) GetName() string {
	return c.Name
}

type ForeignKeyConstraint struct {
	EscapedName string
	OwningTable SchemaQualifiedName
	// TableUnescapedName is a hackaround until we switch over Tables to use fully-qualified, escaped names
	OwningTableUnescapedName string
	ForeignTable             SchemaQualifiedName
	// ForeignTableUnescapedName is hackaround for the same as above
	ForeignTableUnescapedName string
	ConstraintDef             string
	IsValid                   bool
}

func (f ForeignKeyConstraint) GetName() string {
	return f.OwningTable.GetFQEscapedName() + "_" + f.EscapedName
}

type (
	ParentConstraint struct {
		EscapedName string
		OwningTable SchemaQualifiedName
		// OwningTableUnescapedName is a hackaround until we switch over Tables to use fully-qualified, escaped names
		OwningTableUnescapedName string
	}

	UniqueConstraint struct {
		EscapedName string
		OwningTable SchemaQualifiedName
		// TableUnescapedName is a hackaround until we switch over Tables to use fully-qualified, escaped names
		OwningTableUnescapedName string
		IndexEscapedName         string
		// IndexUnescapedName is a hackaround until we switch over Indexes to use fully-qualified, escaped names
		IndexUnescapedName string

		ParentConstraint *ParentConstraint
	}
)

func (u UniqueConstraint) GetName() string {
	return u.OwningTable.GetFQEscapedName() + "_" + u.EscapedName
}

type (
	// SequenceOwner represents the owner of a sequence. Once we remove TableUnescapedName, we can replace it with
	// ColumnIdentifier struct that can also be used in the Column struct
	SequenceOwner struct {
		TableName SchemaQualifiedName
		// TableUnescapedName is a hackaround until we switch over Tables to use fully-qualified, escaped names
		TableUnescapedName string
		ColumnName         string
	}

	Sequence struct {
		SchemaQualifiedName
		Owner      *SequenceOwner
		Type       string
		StartValue int64
		Increment  int64
		MaxValue   int64
		MinValue   int64
		CacheSize  int64
		Cycle      bool
	}
)

type Function struct {
	SchemaQualifiedName
	// FunctionDef is the statement required to completely (re)create
	// the function, as returned by `pg_get_functiondef`. It is a CREATE OR REPLACE
	// statement
	FunctionDef string
	// Language is the language of the function. This is relevant in determining if we
	// can track the dependencies of the function (or not)
	Language           string
	DependsOnFunctions []SchemaQualifiedName
}

var (
	// The first matching group is the "CREATE ". The second matching group is the rest of the statement
	triggerToOrReplaceRegex = regexp.MustCompile("^(CREATE )(.*)$")
)

// GetTriggerDefStatement is the output of pg_get_triggerdef. It is a `CREATE TRIGGER` statement that will create
// the trigger. This statement does not contain `OR REPLACE`
type GetTriggerDefStatement string

func (g GetTriggerDefStatement) ToCreateOrReplace() (string, error) {
	if !triggerToOrReplaceRegex.MatchString(string(g)) {
		return "", fmt.Errorf("%s follows an unexpected structure", g)
	}
	return triggerToOrReplaceRegex.ReplaceAllString(string(g), "${1}OR REPLACE ${2}"), nil
}

type Trigger struct {
	EscapedName string
	OwningTable SchemaQualifiedName
	// OwningTableUnescapedName lets us be backwards compatible with the TableSQLVertexGenerator, which
	// currently uses the unescaped name as the vertex id. This will be removed once the TableSQLVertexGenerator
	// is migrated to use SchemaQualifiedName
	OwningTableUnescapedName string
	Function                 SchemaQualifiedName
	// GetTriggerDefStmt is the statement required to completely (re)create the trigger, as returned
	// by pg_get_triggerdef
	GetTriggerDefStmt GetTriggerDefStatement
}

func (t Trigger) GetName() string {
	return t.OwningTable.GetFQEscapedName() + "_" + t.EscapedName
}

// GetPublicSchema fetches the "public" schema. It is a non-atomic operation.
func GetPublicSchema(ctx context.Context, db queries.DBTX) (Schema, error) {
	q := queries.New(db)

	extensions, err := fetchExtensions(ctx, q)
	if err != nil {
		return Schema{}, fmt.Errorf("fetchExtensions: %w", err)
	}

	tables, err := fetchTables(ctx, q)
	if err != nil {
		return Schema{}, fmt.Errorf("fetchTables: %w", err)
	}

	indexes, err := fetchIndexes(ctx, q)
	if err != nil {
		return Schema{}, fmt.Errorf("fetchIndexes: %w", err)
	}

	fkCons, err := fetchForeignKeyCons(ctx, q)
	if err != nil {
		return Schema{}, fmt.Errorf("fetchForeignKeyCons: %w", err)
	}

	sequences, err := fetchSequences(ctx, q)
	if err != nil {
		return Schema{}, fmt.Errorf("fetchSequences: %w", err)
	}

	functions, err := fetchFunctions(ctx, q)
	if err != nil {
		return Schema{}, fmt.Errorf("fetchFunctions: %w", err)
	}

	triggers, err := fetchTriggers(ctx, q)
	if err != nil {
		return Schema{}, fmt.Errorf("fetchTriggers: %w", err)
	}

	return Schema{
		Extensions:            extensions,
		Tables:                tables,
		Indexes:               indexes,
		ForeignKeyConstraints: fkCons,
		Sequences:             sequences,
		Functions:             functions,
		Triggers:              triggers,
	}, nil
}

func fetchExtensions(ctx context.Context, q *queries.Queries) ([]Extension, error) {
	rawExtensions, err := q.GetExtensions(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetExtensions(): %w", err)
	}

	var extensions []Extension
	for _, e := range rawExtensions {
		extensions = append(extensions, Extension{
			SchemaQualifiedName: SchemaQualifiedName{
				EscapedName: EscapeIdentifier(e.ExtensionName),
				SchemaName:  e.SchemaName,
			},
			Version: e.ExtensionVersion,
		})
	}
	return extensions, nil
}

func fetchTables(ctx context.Context, q *queries.Queries) ([]Table, error) {
	rawTables, err := q.GetTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetTables(): %w", err)
	}

	tablesToCheckConsMap, err := fetchCheckConsAndBuildTableToCheckConsMap(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("fetchCheckConsAndBuildTableToCheckConsMap: %w", err)
	}

	var tables []Table
	for _, table := range rawTables {
		if len(table.ParentTableName) > 0 && table.ParentTableSchemaName != "public" {
			return nil, fmt.Errorf(
				"table %s has parent table in schema %s. only parent tables in public schema are supported: %w",
				table.TableName,
				table.ParentTableSchemaName,
				err,
			)
		}

		rawColumns, err := q.GetColumnsForTable(ctx, table.Oid)
		if err != nil {
			return nil, fmt.Errorf("GetColumnsForTable(%s): %w", table.Oid, err)
		}
		var columns []Column
		for _, column := range rawColumns {
			collation := SchemaQualifiedName{}
			if len(column.CollationName) > 0 {
				collation = SchemaQualifiedName{
					EscapedName: EscapeIdentifier(column.CollationName),
					SchemaName:  column.CollationSchemaName,
				}
			}

			columns = append(columns, Column{
				Name:       column.ColumnName,
				Type:       column.ColumnType,
				Collation:  collation,
				IsNullable: !column.IsNotNull,
				// If the column has a default value, this will be a SQL string representing that value.
				// Examples:
				//   ''::text
				//   CURRENT_TIMESTAMP
				// If empty, indicates that there is no default value.
				Default: column.DefaultValue,
				Size:    int(column.ColumnSize),
			})
		}

		tables = append(tables, Table{
			Name:             table.TableName,
			Columns:          columns,
			CheckConstraints: tablesToCheckConsMap[table.TableName],

			PartitionKeyDef: table.PartitionKeyDef,

			ParentTableName: table.ParentTableName,
			ForValues:       table.PartitionForValues,
		})
	}
	return tables, nil
}

// fetchCheckConsAndBuildTableToCheckConsMap fetches the check constraints and builds a map of table name to the check
// constraints within the table
func fetchCheckConsAndBuildTableToCheckConsMap(ctx context.Context, q *queries.Queries) (map[string][]CheckConstraint, error) {
	rawCheckCons, err := q.GetCheckConstraints(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetCheckConstraints: %w", err)
	}

	result := make(map[string][]CheckConstraint)
	for _, cc := range rawCheckCons {
		dependsOnFunctions, err := fetchDependsOnFunctions(ctx, q, "pg_constraint", cc.Oid)
		if err != nil {
			return nil, fmt.Errorf("fetchDependsOnFunctions(%s): %w", cc.Oid, err)
		}

		checkCon := CheckConstraint{
			Name:               cc.ConstraintName,
			Expression:         cc.ConstraintExpression,
			IsValid:            cc.IsValid,
			IsInheritable:      !cc.IsNotInheritable,
			DependsOnFunctions: dependsOnFunctions,
		}
		result[cc.TableName] = append(result[cc.TableName], checkCon)
	}

	return result, nil
}

// fetchIndexes fetches the indexes We fetch all indexes at once to minimize number of queries, since each index needs
// to fetch columns
func fetchIndexes(ctx context.Context, q *queries.Queries) ([]Index, error) {
	rawIndexes, err := q.GetIndexes(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetColumnsInPublicSchema: %w", err)
	}

	var indexes []Index
	for _, rawIndex := range rawIndexes {
		rawColumns, err := q.GetColumnsForIndex(ctx, rawIndex.Oid)
		if err != nil {
			return nil, fmt.Errorf("GetColumnsForIndex(%s): %w", rawIndex.Oid, err)
		}

		indexes = append(indexes, Index{
			TableName:       rawIndex.TableName,
			Name:            rawIndex.IndexName,
			Columns:         rawColumns,
			GetIndexDefStmt: GetIndexDefStatement(rawIndex.DefStmt),
			IsInvalid:       !rawIndex.IndexIsValid,
			IsPk:            rawIndex.IndexIsPk,
			IsUnique:        rawIndex.IndexIsUnique,
			ConstraintName:  rawIndex.ConstraintName,
			ParentIdxName:   rawIndex.ParentIndexName,
		})
	}

	return indexes, nil
}

func fetchForeignKeyCons(ctx context.Context, q *queries.Queries) ([]ForeignKeyConstraint, error) {
	rawFkCons, err := q.GetForeignKeyConstraints(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetForeignKeyConstraints: %w", err)
	}

	var fkCons []ForeignKeyConstraint
	for _, rawFkCon := range rawFkCons {
		fkCons = append(fkCons, ForeignKeyConstraint{
			EscapedName: EscapeIdentifier(rawFkCon.ConstraintName),
			OwningTable: SchemaQualifiedName{
				SchemaName:  rawFkCon.OwningTableSchemaName,
				EscapedName: EscapeIdentifier(rawFkCon.OwningTableName),
			},
			OwningTableUnescapedName: rawFkCon.OwningTableName,
			ForeignTable: SchemaQualifiedName{
				SchemaName:  rawFkCon.ForeignTableSchemaName,
				EscapedName: EscapeIdentifier(rawFkCon.ForeignTableName),
			},
			ForeignTableUnescapedName: rawFkCon.ForeignTableName,
			ConstraintDef:             rawFkCon.ConstraintDef,
			IsValid:                   rawFkCon.IsValid,
		})
	}
	return fkCons, nil
}

func fetchSequences(ctx context.Context, q *queries.Queries) ([]Sequence, error) {
	rawSeqs, err := q.GetSequences(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetSequences: %w", err)
	}

	var seqs []Sequence
	for _, rawSeq := range rawSeqs {
		var owner *SequenceOwner
		if len(rawSeq.OwnerColumnName) > 0 {
			owner = &SequenceOwner{
				TableName: SchemaQualifiedName{
					SchemaName:  rawSeq.OwnerSchemaName,
					EscapedName: EscapeIdentifier(rawSeq.OwnerTableName),
				},
				TableUnescapedName: rawSeq.OwnerTableName,
				ColumnName:         rawSeq.OwnerColumnName,
			}
		}
		seqs = append(seqs, Sequence{
			SchemaQualifiedName: SchemaQualifiedName{
				SchemaName:  rawSeq.SequenceSchemaName,
				EscapedName: EscapeIdentifier(rawSeq.SequenceName),
			},
			Owner:      owner,
			Type:       rawSeq.DataType,
			StartValue: rawSeq.StartValue,
			Increment:  rawSeq.IncrementValue,
			MaxValue:   rawSeq.MaxValue,
			MinValue:   rawSeq.MinValue,
			CacheSize:  rawSeq.CacheSize,
			Cycle:      rawSeq.IsCycle,
		})
	}
	return seqs, nil
}

func fetchFunctions(ctx context.Context, q *queries.Queries) ([]Function, error) {
	rawFunctions, err := q.GetFunctions(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetFunctions: %w", err)
	}

	var functions []Function
	for _, rawFunction := range rawFunctions {
		dependsOnFunctions, err := fetchDependsOnFunctions(ctx, q, "pg_proc", rawFunction.Oid)
		if err != nil {
			return nil, fmt.Errorf("fetchDependsOnFunctions(%s): %w", rawFunction.Oid, err)
		}

		functions = append(functions, Function{
			SchemaQualifiedName: buildFuncName(rawFunction.FuncName, rawFunction.FuncIdentityArguments, rawFunction.FuncSchemaName),
			FunctionDef:         rawFunction.FuncDef,
			Language:            rawFunction.FuncLang,
			DependsOnFunctions:  dependsOnFunctions,
		})
	}

	return functions, nil
}

func fetchDependsOnFunctions(ctx context.Context, q *queries.Queries, systemCatalog string, oid any) ([]SchemaQualifiedName, error) {
	dependsOnFunctions, err := q.GetDependsOnFunctions(ctx, queries.GetDependsOnFunctionsParams{
		SystemCatalog: systemCatalog,
		ObjectID:      oid,
	})
	if err != nil {
		return nil, err
	}

	var functionNames []SchemaQualifiedName
	for _, rawFunction := range dependsOnFunctions {
		functionNames = append(functionNames, buildFuncName(rawFunction.FuncName, rawFunction.FuncIdentityArguments, rawFunction.FuncSchemaName))
	}

	return functionNames, nil
}

func fetchTriggers(ctx context.Context, q *queries.Queries) ([]Trigger, error) {
	rawTriggers, err := q.GetTriggers(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetTriggers: %w", err)
	}

	var triggers []Trigger
	for _, rawTrigger := range rawTriggers {
		triggers = append(triggers, Trigger{
			EscapedName:              EscapeIdentifier(rawTrigger.TriggerName),
			OwningTable:              buildNameFromUnescaped(rawTrigger.OwningTableName, rawTrigger.OwningTableSchemaName),
			OwningTableUnescapedName: rawTrigger.OwningTableName,
			Function:                 buildFuncName(rawTrigger.FuncName, rawTrigger.FuncIdentityArguments, rawTrigger.FuncSchemaName),
			GetTriggerDefStmt:        GetTriggerDefStatement(rawTrigger.TriggerDef),
		})
	}

	return triggers, nil
}

func buildFuncName(name, identityArguments, schemaName string) SchemaQualifiedName {
	return SchemaQualifiedName{
		SchemaName:  schemaName,
		EscapedName: fmt.Sprintf("\"%s\"(%s)", name, identityArguments),
	}
}

func buildNameFromUnescaped(unescapedName, schemaName string) SchemaQualifiedName {
	return SchemaQualifiedName{
		EscapedName: EscapeIdentifier(unescapedName),
		SchemaName:  schemaName,
	}
}

// FQEscapedColumnName builds a fully-qualified escape column name
func FQEscapedColumnName(table SchemaQualifiedName, columnName string) string {
	return fmt.Sprintf("%s.%s", table.GetFQEscapedName(), EscapeIdentifier(columnName))
}

func EscapeIdentifier(name string) string {
	return fmt.Sprintf("\"%s\"", name)
}
