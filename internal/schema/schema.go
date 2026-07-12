package schema

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mitchellh/hashstructure/v2"

	"github.com/stripe/pg-schema-diff/internal/concurrent"
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

	t.Privileges = sortSchemaObjectsByName(t.Privileges)

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
	Privileges       []TablePrivilege
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

// TablePrivilege represents a privilege granted on a table
type TablePrivilege struct {
	// Grantee is the role that has the privilege. Empty string means PUBLIC.
	Grantee string
	// Privilege is the type of privilege (SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER)
	Privilege string
	// IsGrantable indicates if the grantee can grant this privilege to others (WITH GRANT OPTION)
	IsGrantable bool
}

func (p TablePrivilege) GetName() string {
	grantee := p.Grantee
	if grantee == "" {
		grantee = "PUBLIC"
	}
	return fmt.Sprintf("%s:%s", grantee, p.Privilege)
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
		Default string
		// If the column is a generated column, this will be true.
		IsGenerated bool
		// If the column is a generated column, this will be the generation expression.
		// Examples:
		//   to_tsvector('simple', title || ' ' || coalesce(artist, ''))
		//   (price * 1.1)
		// Only populated if IsGenerated is true.
		GenerationExpression string
		IsNullable           bool
		// HasMissingValOptimization refers to the 'attmissingval' optimization for adding columns with a default.
		HasMissingValOptimization bool
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

// The first matching group is the "CREATE [UNIQUE] INDEX ". UNIQUE is an optional match
// because only UNIQUE indices will have the UNIQUE keyword in their pg_get_indexdef statement
//
// The third matching group is the rest of the statement
var idxToConcurrentlyRegex = regexp.MustCompile("^(CREATE (UNIQUE )?INDEX )(.*)$")

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
	RelKind             string

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
		Name string
		// OwningRelName refers to the owning table or materialized view.
		OwningRelName SchemaQualifiedName
		// OwningRelKind is the relkind of the owning relation.
		OwningRelKind RelKind
		Columns       []string
		IsInvalid     bool
		IsUnique      bool

		Constraint *IndexConstraint

		// GetIndexDefStmt is the output of pg_getindexdef
		GetIndexDefStmt GetIndexDefStatement

		ParentIdx *SchemaQualifiedName
	}
)

const (
	PkIndexConstraintType IndexConstraintType = "p"

	RelKindOrdinaryTable    RelKind = "r"
	RelKindPartitionedTable RelKind = "p"
	RelKindMaterializedView RelKind = "m"
)

func (i Index) GetName() string {
	return i.GetSchemaQualifiedName().GetFQEscapedName()
}

func (i Index) GetSchemaQualifiedName() SchemaQualifiedName {
	return SchemaQualifiedName{
		SchemaName:  i.OwningRelName.SchemaName,
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

// The first matching group is the "CREATE ". The second matching group is the rest of the statement
var triggerToOrReplaceRegex = regexp.MustCompile("^(CREATE )(.*)$")

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
		o.includeSchemas = append(o.includeSchemas, schemas...)
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
func GetSchema(ctx context.Context, db *pgxpool.Pool, opts ...GetSchemaOpt) (Schema, error) {
	goroutineRunnerFactory := func() concurrent.GoroutineRunner {
		return concurrent.NewGoroutineLimiter(50)
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
		goroutineRunnerFactory: goroutineRunnerFactory,
		nameFilter:             nameFilter,
	}).getSchema(ctx, db)
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

func (s *schemaFetcher) getSchema(ctx context.Context, db *pgxpool.Pool) (Schema, error) {
	goroutineRunner := s.goroutineRunnerFactory()

	namedSchemasFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]NamedSchema, error) {
		return fetchNamedSchemas(ctx, db)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting named schemas future: %w", err)
	}

	extensionsFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Extension, error) {
		return fetchExtensions(ctx, db)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting extensions future: %w", err)
	}

	enumsFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Enum, error) {
		return fetchEnums(ctx, db)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting enums future: %w", err)
	}

	tablesFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Table, error) {
		return fetchTables(ctx, db, s.goroutineRunnerFactory)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting tables future: %w", err)
	}

	indexesFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Index, error) {
		return fetchIndexes(ctx, db)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting indexes future: %w", err)
	}

	fkConsFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]ForeignKeyConstraint, error) {
		return fetchForeignKeyCons(ctx, db)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting foreign key constraints future: %w", err)
	}

	sequencesFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Sequence, error) {
		return fetchSequences(ctx, db)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting sequences future: %w", err)
	}

	functionsFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Function, error) {
		return fetchFunctions(ctx, db, s.goroutineRunnerFactory)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting functions future: %w", err)
	}

	proceduresFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Procedure, error) {
		return fetchProcedures(ctx, db)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting functions future: %w", err)
	}

	triggersFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]Trigger, error) {
		return fetchTriggers(ctx, db)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting triggers future: %w", err)
	}

	viewsFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]View, error) {
		return fetchViews(ctx, db)
	})
	if err != nil {
		return Schema{}, fmt.Errorf("starting views future: %w", err)
	}

	materializedViewsFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() ([]MaterializedView, error) {
		return fetchMaterializedViews(ctx, db)
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
	extensions = filterSliceByName(
		extensions,
		func(e Extension) SchemaQualifiedName {
			return e.SchemaQualifiedName
		},
		s.nameFilter,
	)
	enums = filterSliceByName(
		enums,
		func(enum Enum) SchemaQualifiedName {
			return enum.SchemaQualifiedName
		},
		s.nameFilter,
	)

	var filteredTables []Table
	for _, table := range tables {
		table.CheckConstraints = filterSliceByName(
			table.CheckConstraints,
			func(cc CheckConstraint) SchemaQualifiedName {
				return SchemaQualifiedName{
					SchemaName:  table.SchemaName,
					EscapedName: EscapeIdentifier(cc.Name),
				}
			},
			s.nameFilter,
		)
		table.Policies = filterSliceByName(
			table.Policies,
			func(p Policy) SchemaQualifiedName {
				return SchemaQualifiedName{
					SchemaName:  table.SchemaName,
					EscapedName: p.EscapedName,
				}
			},
			s.nameFilter,
		)
		table.Privileges = filterSliceByName(
			table.Privileges,
			func(TablePrivilege) SchemaQualifiedName {
				return table.SchemaQualifiedName
			},
			s.nameFilter,
		)
		filteredTables = append(filteredTables, table)
	}
	tables = filterSliceByName(
		filteredTables,
		func(t Table) SchemaQualifiedName {
			return t.SchemaQualifiedName
		},
		s.nameFilter,
	)
	indexes = filterSliceByName(
		indexes,
		func(idx Index) SchemaQualifiedName {
			return idx.GetSchemaQualifiedName()
		},
		s.nameFilter,
	)
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
	sequences = filterSliceByName(
		sequences,
		func(seq Sequence) SchemaQualifiedName {
			return SchemaQualifiedName{
				SchemaName:  seq.SchemaName,
				EscapedName: seq.EscapedName,
			}
		},
		s.nameFilter,
	)
	functions = filterSliceByName(
		functions,
		func(function Function) SchemaQualifiedName {
			return function.SchemaQualifiedName
		},
		s.nameFilter,
	)
	procedures = filterSliceByName(
		procedures,
		func(procedure Procedure) SchemaQualifiedName {
			return procedure.SchemaQualifiedName
		},
		s.nameFilter,
	)
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
	views = filterSliceByName(
		views,
		func(view View) SchemaQualifiedName {
			return view.SchemaQualifiedName
		},
		s.nameFilter,
	)
	materializedViews = filterSliceByName(
		materializedViews,
		func(materializedView MaterializedView) SchemaQualifiedName {
			return materializedView.SchemaQualifiedName
		},
		s.nameFilter,
	)

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

// FQEscapedColumnName builds a fully-qualified escape column name
func FQEscapedColumnName(table SchemaQualifiedName, columnName string) string {
	return fmt.Sprintf("%s.%s", table.GetFQEscapedName(), EscapeIdentifier(columnName))
}

func EscapeIdentifier(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

// EscapeLiteral returns a safely escaped SQL string literal, enclosed in single
// quotes. Single quotes within the value are doubled per the SQL standard, and
// null bytes are stripped as they are not valid in PostgreSQL string literals.
func EscapeLiteral(val string) string {
	val = strings.ReplaceAll(val, string([]byte{0}), "")
	return "'" + strings.ReplaceAll(val, "'", "''") + "'"
}
