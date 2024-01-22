package schema

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"

	"github.com/mitchellh/hashstructure/v2"
	"github.com/stripe/pg-schema-diff/internal/concurrent"
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
			checkConstraint.KeyColumns = sortByKey(checkConstraint.KeyColumns, func(s string) string {
				return s
			})
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
	return sortByKey(vals, func(v S) string {
		return v.GetName()
	})
}

func sortByKey[S any](vals []S, getValFn func(S) string) []S {
	clonedVals := make([]S, len(vals))
	copy(clonedVals, vals)
	sort.Slice(clonedVals, func(i, j int) bool {
		return getValFn(clonedVals[i]) < getValFn(clonedVals[j])
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

type ReplicaIdentity string

const (
	ReplicaIdentityDefault ReplicaIdentity = "d"
	ReplicaIdentityNothing ReplicaIdentity = "n"
	ReplicaIdentityFull    ReplicaIdentity = "f"
	ReplicaIdentityIndex   ReplicaIdentity = "i"
)

type Extension struct {
	SchemaQualifiedName
	Version string
}

type Table struct {
	Name             string
	Columns          []Column
	CheckConstraints []CheckConstraint
	ReplicaIdentity  ReplicaIdentity

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

type (
	IndexConstraintType string

	// IndexConstraint informally represents a constraint that is always 1:1 with an index, i.e.,
	// primary and unique constraints. It's easiest to just treat these like a property of the index rather than
	// a separate entity
	IndexConstraint struct {
		Type                  IndexConstraintType
		EscapedConstraintName string
		ConstraintDef         string
		IsLocal               bool
	}

	Index struct {
		TableName string
		Name      string
		Columns   []string
		IsInvalid bool
		IsUnique  bool

		Constraint *IndexConstraint

		// GetIndexDefStmt is the output of pg_getindexdef
		GetIndexDefStmt GetIndexDefStatement

		// ParentIdxName is the name of the parent index if the index is a partition of an index
		ParentIdxName string
	}
)

const (
	PkIndexConstraintType IndexConstraintType = "p"
)

func (i Index) GetName() string {
	return i.Name
}

func (i Index) IsPartitionOfIndex() bool {
	return len(i.ParentIdxName) > 0
}

func (i Index) IsPk() bool {
	return i.Constraint != nil && i.Constraint.Type == PkIndexConstraintType
}

type CheckConstraint struct {
	Name string
	// KeyColumns are the columns that the constraint applies to
	KeyColumns         []string
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
	// To allow backwards compatibility with connections, we will not use concurrency if passed in a db that is not a
	// *sql.DB. This is because not all implementations are thread safe, e.g., pgx.Connection.
	//
	// In the future, we should maybe create options where users can pass in a DB pool (WithPool(db) or WithConnection(db))
	// and we can set concurrency to 1 if the passed in db is not a *sql.DB.
	goRoutineRunnerFactory := concurrent.NewSynchronousGoRoutineRunner
	if _, ok := db.(*sql.DB); ok {
		goRoutineRunnerFactory = func() concurrent.GoRoutineRunner {
			return concurrent.NewGoroutineLimiter(50)
		}
	}

	return (&schemaFetcher{
		q:                      queries.New(db),
		goroutineRunnerFactory: goRoutineRunnerFactory,
	}).getPublicSchema(ctx)
}

type schemaFetcher struct {
	q *queries.Queries
	// goroutineRunnerFactory is a factory function that returns a GoRoutineRunner. We need to be able to construct
	// multiple GoRoutineRunners to avoid deadlock created by circular dependencies of submitted go routines.
	goroutineRunnerFactory func() concurrent.GoRoutineRunner
}

func (s *schemaFetcher) getPublicSchema(ctx context.Context) (Schema, error) {
	goroutineRunner := s.goroutineRunnerFactory()

	extensionsFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Extension, error) {
		return s.fetchExtensions(ctx)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting extensions future: %w", err)
	}

	tablesFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Table, error) {
		return s.fetchTables(ctx)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting tables future: %w", err)
	}

	indexesFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Index, error) {
		return s.fetchIndexes(ctx)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting indexes future: %w", err)
	}

	fkConsFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]ForeignKeyConstraint, error) {
		return s.fetchForeignKeyCons(ctx)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting foreign key constraints future: %w", err)
	}

	sequencesFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Sequence, error) {
		return s.fetchSequences(ctx)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting sequences future: %w", err)
	}

	functionsFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Function, error) {
		return s.fetchFunctions(ctx)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting functions future: %w", err)
	}

	triggersFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Trigger, error) {
		return s.fetchTriggers(ctx)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting triggers future: %w", err)
	}

	extensions, err := extensionsFuture.Get(ctx)
	if err != nil {
		return Schema{}, fmt.Errorf("getting extensions: %w", err)
	}

	tables, err := tablesFuture.Get(ctx)
	if err != nil {
		return Schema{}, fmt.Errorf("getting tables: %w", err)
	}

	indexes, err := indexesFuture.Get(ctx)
	if err != nil {
		return Schema{}, fmt.Errorf("getting indexes: %w", err)
	}

	fkCons, err := fkConsFuture.Get(ctx)
	if err != nil {
		return Schema{}, fmt.Errorf("getting foreign key constraints: %w", err)
	}

	sequences, err := sequencesFuture.Get(ctx)
	if err != nil {
		return Schema{}, fmt.Errorf("getting sequences: %w", err)
	}

	functions, err := functionsFuture.Get(ctx)
	if err != nil {
		return Schema{}, fmt.Errorf("getting functions: %w", err)
	}

	triggers, err := triggersFuture.Get(ctx)
	if err != nil {
		return Schema{}, fmt.Errorf("getting triggers: %w", err)
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

func (s *schemaFetcher) fetchExtensions(ctx context.Context) ([]Extension, error) {
	rawExtensions, err := s.q.GetExtensions(ctx)
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

func (s *schemaFetcher) fetchTables(ctx context.Context) ([]Table, error) {
	rawTables, err := s.q.GetTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetTables(): %w", err)
	}

	tablesToCheckConsMap, err := s.fetchCheckConsAndBuildTableToCheckConsMap(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetchCheckConsAndBuildTableToCheckConsMap: %w", err)
	}

	goroutineRunner := s.goroutineRunnerFactory()
	var tableFutures []concurrent.Future[Table]
	for _, _rawTable := range rawTables {
		rawTable := _rawTable // Capture loop variables for go routine
		tableFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() (Table, error) {
			return s.buildTable(ctx, rawTable, tablesToCheckConsMap)
		})
		if err != nil {
			return nil, fmt.Errorf("starting table future: %w", err)
		}
		tableFutures = append(tableFutures, tableFuture)
	}

	return concurrent.GetAll(ctx, tableFutures...)
}

func (s *schemaFetcher) buildTable(ctx context.Context, table queries.GetTablesRow, tablesToCheckConsMap map[string][]CheckConstraint) (Table, error) {
	if len(table.ParentTableName) > 0 && table.ParentTableSchemaName != "public" {
		return Table{}, fmt.Errorf(
			"table %s has parent table in schema %s. only parent tables in public schema are supported",
			table.TableName,
			table.ParentTableSchemaName,
		)
	}

	rawColumns, err := s.q.GetColumnsForTable(ctx, table.Oid)
	if err != nil {
		return Table{}, fmt.Errorf("GetColumnsForTable(%s): %w", table.Oid, err)
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

	return Table{
		Name:             table.TableName,
		Columns:          columns,
		CheckConstraints: tablesToCheckConsMap[table.TableName],
		ReplicaIdentity:  ReplicaIdentity(table.ReplicaIdentity),

		PartitionKeyDef: table.PartitionKeyDef,

		ParentTableName: table.ParentTableName,
		ForValues:       table.PartitionForValues,
	}, nil
}

// fetchCheckConsAndBuildTableToCheckConsMap fetches the check constraints and builds a map of table name to the check
// constraints within the table
func (s *schemaFetcher) fetchCheckConsAndBuildTableToCheckConsMap(ctx context.Context) (map[string][]CheckConstraint, error) {
	rawCheckCons, err := s.q.GetCheckConstraints(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetCheckConstraints: %w", err)
	}

	type checkConstraintAndTable struct {
		checkConstraint CheckConstraint
		tableName       string
	}

	goRoutineRunner := s.goroutineRunnerFactory()
	var ccFutures []concurrent.Future[checkConstraintAndTable]
	for _, _rawCC := range rawCheckCons {
		rawCC := _rawCC // Capture loop variable for go routine
		f, err := concurrent.SubmitFuture(ctx, goRoutineRunner, func() (checkConstraintAndTable, error) {
			cc, err := s.buildCheckConstraint(ctx, rawCC)
			if err != nil {
				return checkConstraintAndTable{}, fmt.Errorf("building check constraint: %w", err)
			}
			return checkConstraintAndTable{
				checkConstraint: cc,
				tableName:       rawCC.TableName,
			}, nil
		})
		if err != nil {
			return nil, fmt.Errorf("starting check constraint future: %w", err)
		}

		ccFutures = append(ccFutures, f)
	}

	ccs, err := concurrent.GetAll(ctx, ccFutures...)
	if err != nil {
		return nil, fmt.Errorf("getting check constraints: %w", err)
	}

	// Build a map of table name to check constraints
	tablesToCheckConsMap := make(map[string][]CheckConstraint)
	for _, cc := range ccs {
		tablesToCheckConsMap[cc.tableName] = append(tablesToCheckConsMap[cc.tableName], cc.checkConstraint)
	}

	return tablesToCheckConsMap, nil
}

func (s *schemaFetcher) buildCheckConstraint(ctx context.Context, cc queries.GetCheckConstraintsRow) (CheckConstraint, error) {
	dependsOnFunctions, err := s.fetchDependsOnFunctions(ctx, "pg_constraint", cc.Oid)
	if err != nil {
		return CheckConstraint{}, fmt.Errorf("fetchDependsOnFunctions(%s): %w", cc.Oid, err)
	}
	return CheckConstraint{
		Name:               cc.ConstraintName,
		KeyColumns:         cc.ColumnNames,
		Expression:         cc.ConstraintExpression,
		IsValid:            cc.IsValid,
		IsInheritable:      !cc.IsNotInheritable,
		DependsOnFunctions: dependsOnFunctions,
	}, nil
}

// fetchIndexes fetches the indexes We fetch all indexes at once to minimize number of queries, since each index needs
// to fetch columns
func (s *schemaFetcher) fetchIndexes(ctx context.Context) ([]Index, error) {
	rawIndexes, err := s.q.GetIndexes(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetIndexes: %w", err)
	}

	goroutineRunner := s.goroutineRunnerFactory()
	var idxFutures []concurrent.Future[Index]
	for _, _rawIndex := range rawIndexes {
		rawIndex := _rawIndex // Capture loop variable for go routine
		f, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() (Index, error) {
			return s.buildIndex(ctx, rawIndex)
		})
		if err != nil {
			return nil, fmt.Errorf("starting index future: %w", err)
		}

		idxFutures = append(idxFutures, f)
	}

	return concurrent.GetAll(ctx, idxFutures...)
}

func (s *schemaFetcher) buildIndex(ctx context.Context, rawIndex queries.GetIndexesRow) (Index, error) {
	rawColumns, err := s.q.GetColumnsForIndex(ctx, rawIndex.Oid)
	if err != nil {
		return Index{}, fmt.Errorf("GetColumnsForIndex(%s): %w", rawIndex.Oid, err)
	}

	var indexConstraint *IndexConstraint
	if rawIndex.ConstraintName != "" {
		indexConstraint = &IndexConstraint{
			Type:                  IndexConstraintType(rawIndex.ConstraintType),
			EscapedConstraintName: EscapeIdentifier(rawIndex.ConstraintName),
			ConstraintDef:         rawIndex.ConstraintDef,
			IsLocal:               rawIndex.ConstraintIsLocal,
		}
	}

	return Index{
		TableName:       rawIndex.TableName,
		Name:            rawIndex.IndexName,
		Columns:         rawColumns,
		GetIndexDefStmt: GetIndexDefStatement(rawIndex.DefStmt),
		IsInvalid:       !rawIndex.IndexIsValid,
		IsUnique:        rawIndex.IndexIsUnique,

		Constraint: indexConstraint,

		ParentIdxName: rawIndex.ParentIndexName,
	}, nil
}

func (s *schemaFetcher) fetchForeignKeyCons(ctx context.Context) ([]ForeignKeyConstraint, error) {
	rawFkCons, err := s.q.GetForeignKeyConstraints(ctx)
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

func (s *schemaFetcher) fetchSequences(ctx context.Context) ([]Sequence, error) {
	rawSeqs, err := s.q.GetSequences(ctx)
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

func (s *schemaFetcher) fetchFunctions(ctx context.Context) ([]Function, error) {
	rawFunctions, err := s.q.GetFunctions(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetFunctions: %w", err)
	}

	goroutineRunner := s.goroutineRunnerFactory()
	var functionFutures []concurrent.Future[Function]
	for _, _rawFunction := range rawFunctions {
		rawFunction := _rawFunction // Capture loop variable for go routine
		f, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() (Function, error) {
			return s.buildFunction(ctx, rawFunction)
		})
		if err != nil {
			return nil, fmt.Errorf("starting function future: %w", err)
		}
		functionFutures = append(functionFutures, f)
	}

	return concurrent.GetAll(ctx, functionFutures...)
}

func (s *schemaFetcher) buildFunction(ctx context.Context, rawFunction queries.GetFunctionsRow) (Function, error) {
	dependsOnFunctions, err := s.fetchDependsOnFunctions(ctx, "pg_proc", rawFunction.Oid)
	if err != nil {
		return Function{}, fmt.Errorf("fetchDependsOnFunctions(%s): %w", rawFunction.Oid, err)
	}

	return Function{
		SchemaQualifiedName: buildFuncName(rawFunction.FuncName, rawFunction.FuncIdentityArguments, rawFunction.FuncSchemaName),
		FunctionDef:         rawFunction.FuncDef,
		Language:            rawFunction.FuncLang,
		DependsOnFunctions:  dependsOnFunctions,
	}, nil
}

func (s *schemaFetcher) fetchDependsOnFunctions(ctx context.Context, systemCatalog string, oid any) ([]SchemaQualifiedName, error) {
	dependsOnFunctions, err := s.q.GetDependsOnFunctions(ctx, queries.GetDependsOnFunctionsParams{
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

func (s *schemaFetcher) fetchTriggers(ctx context.Context) ([]Trigger, error) {
	rawTriggers, err := s.q.GetTriggers(ctx)
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
