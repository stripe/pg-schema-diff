package schema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

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
		//
		// GetName should be qualified with the schema name.
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

// Schema is the schema of the database, not just a single Postgres schema.
type Schema struct {
	NamedSchemas          []NamedSchema
	Extensions            []Extension
	Enums                 []Enum
	Tables                []Table
	Indexes               []Index
	ForeignKeyConstraints []ForeignKeyConstraint
	Sequences             []Sequence
	Functions             []Function
	Procedures            []Procedure
	Triggers              []Trigger
	Views                 []View
	MaterializedViews     []MaterializedView
}

// Normalize normalizes the schema (alphabetically sorts tables and columns in tables).
// Useful for hashing and testing.
func (s Schema) Normalize() Schema {
	s.NamedSchemas = sortSchemaObjectsByName(s.NamedSchemas)
	s.Extensions = sortSchemaObjectsByName(s.Extensions)
	s.Enums = sortSchemaObjectsByName(s.Enums)

	var normTables []Table
	for _, t := range sortSchemaObjectsByName(s.Tables) {
		normTables = append(normTables, normalizeTable(t))
	}
	s.Tables = normTables

	s.Indexes = sortSchemaObjectsByName(s.Indexes)
	s.ForeignKeyConstraints = sortSchemaObjectsByName(s.ForeignKeyConstraints)
	s.Sequences = sortSchemaObjectsByName(s.Sequences)

	var normFunctions []Function
	for _, function := range sortSchemaObjectsByName(s.Functions) {
		function.DependsOnFunctions = sortSchemaObjectsByName(function.DependsOnFunctions)
		normFunctions = append(normFunctions, function)
	}
	s.Functions = normFunctions

	s.Procedures = sortSchemaObjectsByName(s.Procedures)
	s.Triggers = sortSchemaObjectsByName(s.Triggers)

	var normViews []View
	for _, v := range sortSchemaObjectsByName(s.Views) {
		normViews = append(normViews, normalizeView(v))
	}
	s.Views = normViews

	var normMaterializedViews []MaterializedView
	for _, mv := range sortSchemaObjectsByName(s.MaterializedViews) {
		normMaterializedViews = append(normMaterializedViews, normalizeMaterializedView(mv))
	}
	s.MaterializedViews = normMaterializedViews

	return s
}

func normalizeTable(t Table) Table {
	// Don't normalize columns order. their order is derived from the postgres catalogs
	// (relevant to data packing)
	var normCheckConstraints []CheckConstraint
	for _, checkConstraint := range sortSchemaObjectsByName(t.CheckConstraints) {
		checkConstraint.DependsOnFunctions = sortSchemaObjectsByName(checkConstraint.DependsOnFunctions)
		checkConstraint.KeyColumns = sortByKey(checkConstraint.KeyColumns, func(s string) string {
			return s
		})
		normCheckConstraints = append(normCheckConstraints, checkConstraint)
	}
	t.CheckConstraints = normCheckConstraints

	var normPolicies []Policy
	for _, p := range sortSchemaObjectsByName(t.Policies) {
		p.AppliesTo = sortByKey(p.AppliesTo, func(s string) string {
			return s
		})
		p.Columns = sortByKey(p.Columns, func(s string) string {
			return s
		})
		normPolicies = append(normPolicies, p)
	}
	t.Policies = normPolicies
	return t
}

func normalizeView(v View) View {
	var normTableDeps []TableDependency
	for _, d := range sortSchemaObjectsByName(v.TableDependencies) {
		d.Columns = sortByKey(d.Columns, func(s string) string { return s })
		normTableDeps = append(normTableDeps, d)
	}
	v.TableDependencies = normTableDeps
	return v
}

func normalizeMaterializedView(mv MaterializedView) MaterializedView {
	var normTableDeps []TableDependency
	for _, d := range sortSchemaObjectsByName(mv.TableDependencies) {
		d.Columns = sortByKey(d.Columns, func(s string) string { return s })
		normTableDeps = append(normTableDeps, d)
	}
	mv.TableDependencies = normTableDeps
	return mv
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

// NamedSchema represents a schema in the database. We call it NamedSchema to distinguish it from the Postgres Database
// schema
type NamedSchema struct {
	Name string
}

func (n NamedSchema) GetName() string {
	return n.Name
}

type Extension struct {
	SchemaQualifiedName
	Version string
}

type Enum struct {
	SchemaQualifiedName
	Labels []string
}

type Table struct {
	SchemaQualifiedName
	Columns          []Column
	CheckConstraints []CheckConstraint
	Policies         []Policy
	ReplicaIdentity  ReplicaIdentity
	RLSEnabled       bool
	RLSForced        bool

	// PartitionKeyDef is the output of Pg function pg_get_partkeydef:
	// PARTITION BY $PartitionKeyDef
	// If empty, then the table is not partitioned
	PartitionKeyDef string

	ParentTable *SchemaQualifiedName
	ForValues   string
}

func (t Table) IsPartitioned() bool {
	return len(t.PartitionKeyDef) > 0
}

// IsPartition returns whether the table is a partition.
// It represents a mismatch in modeling because the ForValues and ParentTable are stored separately.
// Instead, the fields should be stored under the same struct as a nilable pointer, and this function should be deleted.
func (t Table) IsPartition() bool {
	return t.ParentTable != nil
}

type ColumnIdentityType string

const (
	ColumnIdentityTypeAlways    = "a"
	ColumnIdentityTypeByDefault = "d"
)

type (
	ColumnIdentity struct {
		Type       ColumnIdentityType
		MinValue   int64
		MaxValue   int64
		StartValue int64
		Increment  int64
		CacheSize  int64
		Cycle      bool
	}

	Column struct {
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
		Size     int
		Identity *ColumnIdentity
	}
)

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
		// Name is the name of the index. We don't store the schema because the schema is just the schema of the table.
		// Referencing the name is an anti-pattern because it is not qualified. Use should use GetSchemaQualifiedName instead.
		Name        string
		OwningTable SchemaQualifiedName
		Columns     []string
		IsInvalid   bool
		IsUnique    bool

		Constraint *IndexConstraint

		// GetIndexDefStmt is the output of pg_getindexdef
		GetIndexDefStmt GetIndexDefStatement

		ParentIdx *SchemaQualifiedName
	}
)

const (
	PkIndexConstraintType IndexConstraintType = "p"
)

func (i Index) GetName() string {
	return i.GetSchemaQualifiedName().GetFQEscapedName()
}

func (i Index) GetSchemaQualifiedName() SchemaQualifiedName {
	return SchemaQualifiedName{
		SchemaName:  i.OwningTable.SchemaName,
		EscapedName: EscapeIdentifier(i.Name),
	}
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
	EscapedName   string
	OwningTable   SchemaQualifiedName
	ForeignTable  SchemaQualifiedName
	ConstraintDef string
	IsValid       bool
}

func (f ForeignKeyConstraint) GetName() string {
	return f.OwningTable.GetFQEscapedName() + "-" + f.EscapedName
}

type (
	// SequenceOwner represents the owner of a sequence.
	SequenceOwner struct {
		TableName  SchemaQualifiedName
		ColumnName string
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

type Procedure struct {
	SchemaQualifiedName
	// Def is the statement required to completely (re)create
	// the procedure, as returned by `pg_get_functiondef`. It is a CREATE OR REPLACE
	// statement.
	Def string
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

// PolicyCmd represents the polcmd value in the pg_policy system catalog.
// See docs for possible values: https://www.postgresql.org/docs/current/catalog-pg-policy.html#CATALOG-PG-POLICY
type PolicyCmd string

const (
	SelectPolicyCmd PolicyCmd = "r"
	InsertPolicyCmd PolicyCmd = "a"
	UpdatePolicyCmd PolicyCmd = "w"
	DeletePolicyCmd PolicyCmd = "d"
	AllPolicyCmd    PolicyCmd = "*"
)

type Policy struct {
	EscapedName     string
	IsPermissive    bool
	AppliesTo       []string
	Cmd             PolicyCmd
	CheckExpression string
	UsingExpression string
	// Columns are the columns that the policy applies to.
	Columns []string
}

func (p Policy) GetName() string {
	return p.EscapedName
}

type Trigger struct {
	EscapedName string
	OwningTable SchemaQualifiedName
	Function    SchemaQualifiedName
	// GetTriggerDefStmt is the statement required to completely (re)create the trigger, as returned
	// by pg_get_triggerdef
	GetTriggerDefStmt GetTriggerDefStatement
	IsConstraint      bool
}

func (t Trigger) GetName() string {
	return t.OwningTable.GetFQEscapedName() + "-" + t.EscapedName
}

// TableDependency represents a (view's) dependency on a table.
type TableDependency struct {
	SchemaQualifiedName
	Columns []string
}

type View struct {
	SchemaQualifiedName
	// ViewDefinition is the select query that defines the view. It is derived from pg_get_viewdef.
	ViewDefinition string
	// Options represents key value map of view options, i.e., pg_class.reloptions.
	Options map[string]string

	// TableDependencies is a list of tables the view depends on.
	TableDependencies []TableDependency
}

type MaterializedView struct {
	SchemaQualifiedName
	// ViewDefinition is the select query that defines the materialized view. It is derived from pg_get_viewdef.
	ViewDefinition string
	// Options represents key value map of materialized view options, i.e., pg_class.reloptions.
	Options map[string]string
	// Tablespace is the tablespace where the materialized view is stored. Empty string means default tablespace.
	Tablespace string

	// TableDependencies is a list of tables the materialized view depends on.
	TableDependencies []TableDependency
}

type (
	GetSchemaOpt func(*getSchemaOptions)
)

// WithIncludeSchemas filters the schema to only include the given schemas. This unions with any schemas that are already included
// via WithIncludeSchemas. If empty, then all schemas are included.
func WithIncludeSchemas(schemas ...string) GetSchemaOpt {
	return func(o *getSchemaOptions) {
		for _, schema := range schemas {
			o.includeSchemas = append(o.includeSchemas, schema)
		}
	}
}

// WithExcludeSchemas filters the schema to exclude the given schemas. This unions with any schemas that are already excluded
// via WithExcludeSchemas. If empty, then no schemas are excluded.
func WithExcludeSchemas(schemas ...string) GetSchemaOpt {
	return func(o *getSchemaOptions) {
		o.excludeSchemas = append(o.excludeSchemas, schemas...)
	}
}

type getSchemaOptions struct {
	// includeSchemas is a list of schemas to include in the schema. If empty, then all schemas are included.
	// We could have built a more complex set of options using the nameFilter system (nested unions and intersections);
	// however, I felt it could expose some weird behaviors that we don't want to have to worry about just yet,
	includeSchemas []string
	// excludeSchemas is the exclude analog of includeSchemas.
	excludeSchemas []string
}

// GetSchema fetches the database schema. It is a non-atomic operation.
func GetSchema(ctx context.Context, db queries.DBTX, opts ...GetSchemaOpt) (Schema, error) {
	// To allow backwards compatibility with connections, we will not use concurrency if passed in a db that is not a
	// *sql.DB. This is because not all implementations are thread safe, e.g., pgx.Connection.
	//
	// In the future, we should maybe create options where users can pass in a DB pool (WithPool(db) or WithConnection(db))
	// and we can set concurrency to 1 if the passed in db is not a *sql.DB.
	goroutineRunnerFactory := concurrent.NewSynchronousGoroutineRunner
	if _, ok := db.(*sql.DB); ok {
		goroutineRunnerFactory = func() concurrent.GoroutineRunner {
			return concurrent.NewGoroutineLimiter(50)
		}
	}

	options := getSchemaOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	nameFilter, err := buildNameFilter(options)
	if err != nil {
		return Schema{}, fmt.Errorf("building name filter: %w", err)
	}

	return (&schemaFetcher{
		q:                      queries.New(db),
		goroutineRunnerFactory: goroutineRunnerFactory,
		nameFilter:             nameFilter,
	}).getSchema(ctx)
}

func buildNameFilter(options getSchemaOptions) (nameFilter, error) {
	if intersection := intersect(options.includeSchemas, options.excludeSchemas); len(intersection) > 0 {
		return nil, fmt.Errorf("schemas %v are both included and excluded", intersection)
	}

	includeSchemasFilter := buildIncludeSchemasFilter(options.includeSchemas)
	excludeSchemasFilter := buildExcludeSchemasFilter(options.excludeSchemas)
	return andNameFilter(includeSchemasFilter, excludeSchemasFilter), nil
}

func intersect(a, b []string) []string {
	inAByA := make(map[string]bool)
	for _, s := range a {
		inAByA[s] = true
	}
	intersection := make([]string, 0, len(b))
	for _, s := range b {
		if inAByA[s] {
			intersection = append(intersection, s)
		}
	}
	return intersection
}

func buildIncludeSchemasFilter(schemas []string) nameFilter {
	if len(schemas) == 0 {
		return func(name SchemaQualifiedName) bool {
			return true
		}
	}

	var filters []nameFilter
	for _, schema := range schemas {
		filters = append(filters, schemaNameFilter(schema))
	}
	return orNameFilter(filters...)
}

func buildExcludeSchemasFilter(schemas []string) nameFilter {
	if len(schemas) == 0 {
		return func(name SchemaQualifiedName) bool {
			return true
		}
	}

	var filters []nameFilter
	for _, schema := range schemas {
		filters = append(filters, notSchemaNameFilter(schema))
	}
	return andNameFilter(filters...)
}

type (
	schemaFetcher struct {
		q *queries.Queries
		// goroutineRunnerFactory is a factory function that returns a GoroutineRunner. We need to be able to construct
		// multiple GoroutineRunners to avoid deadlock created by circular dependencies of submitted go routines.
		goroutineRunnerFactory func() concurrent.GoroutineRunner
		// nameFilter is a filter that determines which schema objects to include in the schema via their
		// schema name and object name.
		//
		// Currently, we don't do any sort of validation to ensure that all dependencies are included, so users might
		// experience unexpected outcomes if they accidentally filter out a dependency of an object they are trying to
		// diff. In the future, we might want to add some sort of validation layer to ensure that all dependencies are included
		// and error out otherwise.
		//
		// Examples of dependencies that could be filtered out include the functions used by triggers and the parent
		// tables of partitions.
		nameFilter nameFilter
	}
)

func (s *schemaFetcher) getSchema(ctx context.Context) (Schema, error) {
	goroutineRunner := s.goroutineRunnerFactory()

	namedSchemasFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]NamedSchema, error) {
		return s.fetchNamedSchemas(ctx)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting named schemas future: %w", err)
	}

	extensionsFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Extension, error) {
		return s.fetchExtensions(ctx)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting extensions future: %w", err)
	}

	enumsFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Enum, error) {
		return s.fetchEnums(ctx)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting enums future: %w", err)
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

	proceduresFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Procedure, error) {
		return s.fetchProcedures(ctx)
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

	viewsFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]View, error) {
		return s.fetchViews(ctx)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting views future: %w", err)
	}

	materializedViewsFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]MaterializedView, error) {
		return s.fetchMaterializedViews(ctx)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting materialized views future: %w", err)
	}

	schemas, err := namedSchemasFuture.Get(ctx)
	if err != nil {
		return Schema{}, fmt.Errorf("getting named schemas: %w", err)
	}

	extensions, err := extensionsFuture.Get(ctx)
	if err != nil {
		return Schema{}, fmt.Errorf("getting extensions: %w", err)
	}

	enums, err := enumsFuture.Get(ctx)
	if err != nil {
		return Schema{}, fmt.Errorf("getting enums: %w", err)
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

	procedures, err := proceduresFuture.Get(ctx)
	if err != nil {
		return Schema{}, fmt.Errorf("getting procedures: %w", err)
	}

	triggers, err := triggersFuture.Get(ctx)
	if err != nil {
		return Schema{}, fmt.Errorf("getting triggers: %w", err)
	}

	views, err := viewsFuture.Get(ctx)
	if err != nil {
		return Schema{}, fmt.Errorf("getting views: %w", err)
	}

	materializedViews, err := materializedViewsFuture.Get(ctx)
	if err != nil {
		return Schema{}, fmt.Errorf("getting materialized views: %w", err)
	}

	return Schema{
		NamedSchemas:          schemas,
		Extensions:            extensions,
		Enums:                 enums,
		Tables:                tables,
		Indexes:               indexes,
		ForeignKeyConstraints: fkCons,
		Sequences:             sequences,
		Functions:             functions,
		Procedures:            procedures,
		Triggers:              triggers,
		Views:                 views,
		MaterializedViews:     materializedViews,
	}, nil
}

func (s *schemaFetcher) fetchNamedSchemas(ctx context.Context) ([]NamedSchema, error) {
	schemaNames, err := s.q.GetSchemas(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetSchemas(): %w", err)
	}

	var schemas []NamedSchema
	for _, schemaName := range schemaNames {
		schemas = append(schemas, NamedSchema{
			Name: schemaName,
		})
	}

	schemas = filterSliceByName(
		schemas,
		func(s NamedSchema) SchemaQualifiedName {
			return SchemaQualifiedName{
				SchemaName:  s.Name,
				EscapedName: EscapeIdentifier(s.Name),
			}
		},
		s.nameFilter,
	)

	return schemas, nil
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

	extensions = filterSliceByName(
		extensions,
		func(e Extension) SchemaQualifiedName {
			return e.SchemaQualifiedName
		},
		s.nameFilter,
	)

	return extensions, nil
}

func (s *schemaFetcher) fetchEnums(ctx context.Context) ([]Enum, error) {
	rawEnums, err := s.q.GetEnums(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetEnums: %w", err)
	}

	var enums []Enum
	for _, rawEnum := range rawEnums {
		enums = append(enums, Enum{
			SchemaQualifiedName: SchemaQualifiedName{
				SchemaName:  rawEnum.EnumSchemaName,
				EscapedName: EscapeIdentifier(rawEnum.EnumName),
			},
			Labels: rawEnum.EnumLabels,
		})
	}

	enums = filterSliceByName(
		enums,
		func(enum Enum) SchemaQualifiedName {
			return enum.SchemaQualifiedName
		},
		s.nameFilter,
	)

	return enums, nil
}

func (s *schemaFetcher) fetchTables(ctx context.Context) ([]Table, error) {
	rawTables, err := s.q.GetTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetTables(): %w", err)
	}

	checkCons, err := s.fetchCheckCons(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetchCheckCons(): %w", err)
	}
	checkConsByTable := make(map[string][]CheckConstraint)
	for _, cc := range checkCons {
		checkConsByTable[cc.table.GetFQEscapedName()] = append(checkConsByTable[cc.table.GetFQEscapedName()], cc.checkConstraint)
	}

	policies, err := s.fetchPolicies(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetchPolicies(): %w", err)
	}
	policiesByTable := make(map[string][]Policy)
	for _, p := range policies {
		policiesByTable[p.table.GetFQEscapedName()] = append(policiesByTable[p.table.GetFQEscapedName()], p.policy)
	}

	goroutineRunner := s.goroutineRunnerFactory()
	var tableFutures []concurrent.Future[Table]
	for _, _rawTable := range rawTables {
		rawTable := _rawTable // Capture loop variables for go routine
		tableFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() (Table, error) {
			return s.buildTable(ctx, rawTable, checkConsByTable, policiesByTable)
		})
		if err != nil {
			return nil, fmt.Errorf("starting table future: %w", err)
		}
		tableFutures = append(tableFutures, tableFuture)
	}
	tables, err := concurrent.GetAll(ctx, tableFutures...)
	if err != nil {
		return nil, fmt.Errorf("getting tables: %w", err)
	}

	tables = filterSliceByName(
		tables,
		func(t Table) SchemaQualifiedName {
			return t.SchemaQualifiedName
		},
		s.nameFilter,
	)

	return tables, nil
}

func (s *schemaFetcher) buildTable(
	ctx context.Context,
	table queries.GetTablesRow,
	checkConsByTable map[string][]CheckConstraint,
	policiesByTable map[string][]Policy,
) (Table, error) {
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

		var identity *ColumnIdentity
		if len(column.IdentityType) > 0 && table.ParentTableName == "" {
			// Exclude identity columns from table partitions because they are owned by the parent.
			identity = &ColumnIdentity{
				Type:       ColumnIdentityType(column.IdentityType),
				StartValue: column.StartValue.Int64,
				Increment:  column.IncrementValue.Int64,
				MaxValue:   column.MaxValue.Int64,
				MinValue:   column.MinValue.Int64,
				CacheSize:  column.CacheSize.Int64,
				Cycle:      column.IsCycle.Bool,
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
			Default:  column.DefaultValue,
			Size:     int(column.ColumnSize),
			Identity: identity,
		})
	}

	var parentTable *SchemaQualifiedName
	if table.ParentTableName != "" {
		parentTable = &SchemaQualifiedName{
			SchemaName:  table.ParentTableSchemaName,
			EscapedName: EscapeIdentifier(table.ParentTableName),
		}
	}
	schemaQualifiedName := SchemaQualifiedName{
		SchemaName:  table.TableSchemaName,
		EscapedName: EscapeIdentifier(table.TableName),
	}
	return Table{
		SchemaQualifiedName: schemaQualifiedName,
		Columns:             columns,
		CheckConstraints:    checkConsByTable[schemaQualifiedName.GetFQEscapedName()],
		Policies:            policiesByTable[schemaQualifiedName.GetFQEscapedName()],
		ReplicaIdentity:     ReplicaIdentity(table.ReplicaIdentity),
		RLSEnabled:          table.RlsEnabled,
		RLSForced:           table.RlsForced,

		PartitionKeyDef: table.PartitionKeyDef,

		ParentTable: parentTable,
		ForValues:   table.PartitionForValues,
	}, nil
}

type checkConstraintAndTable struct {
	checkConstraint CheckConstraint
	table           SchemaQualifiedName
}

// fetchCheckCons fetches the check constraints
func (s *schemaFetcher) fetchCheckCons(ctx context.Context) ([]checkConstraintAndTable, error) {
	rawCheckCons, err := s.q.GetCheckConstraints(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetCheckConstraints: %w", err)
	}

	goroutineRunner := s.goroutineRunnerFactory()
	var ccFutures []concurrent.Future[checkConstraintAndTable]
	for _, _rawCC := range rawCheckCons {
		rawCC := _rawCC // Capture loop variable for go routine
		f, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() (checkConstraintAndTable, error) {
			cc, err := s.buildCheckConstraint(ctx, rawCC)
			if err != nil {
				return checkConstraintAndTable{}, fmt.Errorf("building check constraint: %w", err)
			}
			return checkConstraintAndTable{
				checkConstraint: cc,
				table:           buildNameFromUnescaped(rawCC.TableName, rawCC.TableSchemaName),
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

	ccs = filterSliceByName(
		ccs,
		func(cc checkConstraintAndTable) SchemaQualifiedName {
			return SchemaQualifiedName{
				SchemaName:  cc.table.SchemaName,
				EscapedName: EscapeIdentifier(cc.checkConstraint.Name),
			}
		},
		s.nameFilter,
	)

	return ccs, nil
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

// fetchIndexes fetches the indexes. We fetch all the indexes at once to minimize the number of queries.
func (s *schemaFetcher) fetchIndexes(ctx context.Context) ([]Index, error) {
	rawIndexes, err := s.q.GetIndexes(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetIndexes: %w", err)
	}

	var idxs []Index
	for _, idx := range rawIndexes {
		idxs = append(idxs, s.buildIndex(idx))
	}

	idxs = filterSliceByName(
		idxs,
		func(idx Index) SchemaQualifiedName {
			return idx.GetSchemaQualifiedName()
		},
		s.nameFilter,
	)

	return idxs, nil
}

func (s *schemaFetcher) buildIndex(rawIndex queries.GetIndexesRow) Index {
	var indexConstraint *IndexConstraint
	if rawIndex.ConstraintName != "" {
		indexConstraint = &IndexConstraint{
			Type:                  IndexConstraintType(rawIndex.ConstraintType),
			EscapedConstraintName: EscapeIdentifier(rawIndex.ConstraintName),
			ConstraintDef:         rawIndex.ConstraintDef,
			IsLocal:               rawIndex.ConstraintIsLocal,
		}
	}

	var parentIdx *SchemaQualifiedName
	if rawIndex.ParentIndexName != "" {
		parentIdx = &SchemaQualifiedName{
			SchemaName:  rawIndex.ParentIndexSchemaName,
			EscapedName: EscapeIdentifier(rawIndex.ParentIndexName),
		}
	}

	return Index{
		OwningTable: SchemaQualifiedName{
			SchemaName:  rawIndex.TableSchemaName,
			EscapedName: EscapeIdentifier(rawIndex.TableName),
		},
		Name:            rawIndex.IndexName,
		Columns:         rawIndex.ColumnNames,
		GetIndexDefStmt: GetIndexDefStatement(rawIndex.DefStmt),
		IsInvalid:       !rawIndex.IndexIsValid,
		IsUnique:        rawIndex.IndexIsUnique,

		Constraint: indexConstraint,

		ParentIdx: parentIdx,
	}
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
			ForeignTable: SchemaQualifiedName{
				SchemaName:  rawFkCon.ForeignTableSchemaName,
				EscapedName: EscapeIdentifier(rawFkCon.ForeignTableName),
			},
			ConstraintDef: rawFkCon.ConstraintDef,
			IsValid:       rawFkCon.IsValid,
		})
	}

	fkCons = filterSliceByName(
		fkCons,
		func(fkCon ForeignKeyConstraint) SchemaQualifiedName {
			return SchemaQualifiedName{
				SchemaName:  fkCon.OwningTable.SchemaName,
				EscapedName: fkCon.EscapedName,
			}
		},
		s.nameFilter,
	)

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
				ColumnName: rawSeq.OwnerColumnName,
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

	seqs = filterSliceByName(
		seqs,
		func(seq Sequence) SchemaQualifiedName {
			return SchemaQualifiedName{
				SchemaName:  seq.SchemaName,
				EscapedName: seq.EscapedName,
			}
		},
		s.nameFilter,
	)

	return seqs, nil
}

func (s *schemaFetcher) fetchFunctions(ctx context.Context) ([]Function, error) {
	rawFunctions, err := s.q.GetProcs(ctx, 'f')
	if err != nil {
		return nil, fmt.Errorf("GetProcs: %w", err)
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

	functions, err := concurrent.GetAll(ctx, functionFutures...)
	if err != nil {
		return nil, fmt.Errorf("getting functions: %w", err)
	}

	functions = filterSliceByName(
		functions,
		func(function Function) SchemaQualifiedName {
			return function.SchemaQualifiedName
		},
		s.nameFilter,
	)

	return functions, nil
}

func (s *schemaFetcher) buildFunction(ctx context.Context, rawFunction queries.GetProcsRow) (Function, error) {
	dependsOnFunctions, err := s.fetchDependsOnFunctions(ctx, "pg_proc", rawFunction.Oid)
	if err != nil {
		return Function{}, fmt.Errorf("fetchDependsOnFunctions(%s): %w", rawFunction.Oid, err)
	}

	return Function{
		SchemaQualifiedName: buildProcName(rawFunction.FuncName, rawFunction.FuncIdentityArguments, rawFunction.FuncSchemaName),
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
		functionNames = append(functionNames, buildProcName(rawFunction.FuncName, rawFunction.FuncIdentityArguments, rawFunction.FuncSchemaName))
	}

	return functionNames, nil
}

func (s *schemaFetcher) fetchProcedures(ctx context.Context) ([]Procedure, error) {
	rawProcedures, err := s.q.GetProcs(ctx, 'p')
	if err != nil {
		return nil, fmt.Errorf("GetProcs: %w", err)
	}

	var procedures []Procedure
	for _, rawProcedure := range rawProcedures {
		p := Procedure{
			SchemaQualifiedName: buildProcName(rawProcedure.FuncName, rawProcedure.FuncIdentityArguments, rawProcedure.FuncSchemaName),
			Def:                 rawProcedure.FuncDef,
		}
		procedures = append(procedures, p)
	}

	procedures = filterSliceByName(
		procedures,
		func(function Procedure) SchemaQualifiedName {
			return function.SchemaQualifiedName
		},
		s.nameFilter,
	)

	return procedures, nil
}

type policyAndTable struct {
	policy Policy
	table  SchemaQualifiedName
}

func (s *schemaFetcher) fetchPolicies(ctx context.Context) ([]policyAndTable, error) {
	rawPolicies, err := s.q.GetPolicies(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetPolicies: %w", err)
	}

	var policies []policyAndTable
	for _, rp := range rawPolicies {
		policies = append(policies, policyAndTable{
			policy: Policy{
				EscapedName:     EscapeIdentifier(rp.PolicyName),
				IsPermissive:    rp.IsPermissive,
				AppliesTo:       rp.AppliesTo,
				Cmd:             PolicyCmd(rp.Cmd),
				CheckExpression: rp.CheckExpression,
				UsingExpression: rp.UsingExpression,
				Columns:         rp.ColumnNames,
			},
			table: buildNameFromUnescaped(rp.OwningTableName, rp.OwningTableSchemaName),
		})
	}

	policies = filterSliceByName(
		policies,
		func(p policyAndTable) SchemaQualifiedName {
			return SchemaQualifiedName{
				SchemaName:  p.table.SchemaName,
				EscapedName: p.policy.EscapedName,
			}
		},
		s.nameFilter,
	)

	return policies, nil
}

func (s *schemaFetcher) fetchTriggers(ctx context.Context) ([]Trigger, error) {
	rawTriggers, err := s.q.GetTriggers(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetTriggers: %w", err)
	}

	var triggers []Trigger
	for _, rawTrigger := range rawTriggers {
		triggers = append(triggers, Trigger{
			EscapedName:       EscapeIdentifier(rawTrigger.TriggerName),
			OwningTable:       buildNameFromUnescaped(rawTrigger.OwningTableName, rawTrigger.OwningTableSchemaName),
			Function:          buildProcName(rawTrigger.FuncName, rawTrigger.FuncIdentityArguments, rawTrigger.FuncSchemaName),
			GetTriggerDefStmt: GetTriggerDefStatement(rawTrigger.TriggerDef),
			IsConstraint:      rawTrigger.IsConstraint,
		})
	}

	triggers = filterSliceByName(
		triggers,
		func(trigger Trigger) SchemaQualifiedName {
			return SchemaQualifiedName{
				SchemaName:  trigger.OwningTable.SchemaName,
				EscapedName: trigger.EscapedName,
			}
		},
		s.nameFilter,
	)

	return triggers, nil
}

func (s *schemaFetcher) fetchViews(ctx context.Context) ([]View, error) {
	rawViews, err := s.q.GetViews(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetViews: %w", err)
	}

	var views []View
	for _, v := range rawViews {
		options, err := relOptionsToMap(v.RelOptions)
		if err != nil {
			return nil, fmt.Errorf("view (%q): %w", v.ViewName, err)
		}

		tableDependencies, err := parseJSONTableDependencies(v.TableDependencies)
		if err != nil {
			return nil, fmt.Errorf("parsing schema qualified names JSON: %w", err)
		}

		views = append(views, View{
			SchemaQualifiedName: buildNameFromUnescaped(v.ViewName, v.SchemaName),
			ViewDefinition:      v.ViewDefinition,
			Options:             options,

			TableDependencies: tableDependencies,
		})
	}

	views = filterSliceByName(
		views,
		func(view View) SchemaQualifiedName {
			return view.SchemaQualifiedName
		},
		s.nameFilter,
	)

	return views, nil
}

func (s *schemaFetcher) fetchMaterializedViews(ctx context.Context) ([]MaterializedView, error) {
	rawMaterializedViews, err := s.q.GetMaterializedViews(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetMaterializedViews: %w", err)
	}

	var materializedViews []MaterializedView
	for _, mv := range rawMaterializedViews {
		options, err := relOptionsToMap(mv.RelOptions)
		if err != nil {
			return nil, fmt.Errorf("materialized view (%q): %w", mv.ViewName, err)
		}

		tableDependencies, err := parseJSONTableDependencies(mv.TableDependencies)
		if err != nil {
			return nil, fmt.Errorf("parsing schema qualified names JSON: %w", err)
		}

		materializedViews = append(materializedViews, MaterializedView{
			SchemaQualifiedName: buildNameFromUnescaped(mv.ViewName, mv.SchemaName),
			ViewDefinition:      mv.ViewDefinition,
			Options:             options,
			Tablespace:          mv.TablespaceName,

			TableDependencies: tableDependencies,
		})
	}

	materializedViews = filterSliceByName(
		materializedViews,
		func(materializedView MaterializedView) SchemaQualifiedName {
			return materializedView.SchemaQualifiedName
		},
		s.nameFilter,
	)

	return materializedViews, nil
}

// parseViewJSONTableDependencies takes an slice of JSON values with schema,
// `schema: string; table: string, columns: []string` and unmarshals them into a go struct.
func parseJSONTableDependencies(vals []string) ([]TableDependency, error) {
	var out []TableDependency
	for _, v := range vals {
		var s struct {
			Schema  string   `json:"schema"`
			Name    string   `json:"name"`
			Columns []string `json:"columns"`
		}
		if err := json.Unmarshal([]byte(v), &s); err != nil {
			return nil, fmt.Errorf("json.Unmarshal(%q, SchemaQualifiedName): %w", string(v), err)
		}
		out = append(out, TableDependency{
			SchemaQualifiedName: buildNameFromUnescaped(s.Name, s.Schema),
			Columns:             s.Columns,
		})
	}
	return out, nil
}

// buildProcName is used to build the schema qualified name for a proc (function, procedure), i.e., anything
// identified by a name AND its arguments.
func buildProcName(name, identityArguments, schemaName string) SchemaQualifiedName {
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

// relOptionsToMap converts pg_catalog.pg_class.reloptions to a map.
func relOptionsToMap(vals []string) (map[string]string, error) {
	out := make(map[string]string)
	for i, v := range vals {
		kv := strings.SplitN(v, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("val[%d] (%q): expected 2 values when splitting by \"=\" but found %d", i, v, len(kv))
		}
		out[kv[0]] = kv[1]
	}
	return out, nil
}
