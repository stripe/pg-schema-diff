package diff

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/kr/pretty"
	"github.com/stripe/pg-schema-diff/internal/schema"
	externalschema "github.com/stripe/pg-schema-diff/pkg/schema"

	"github.com/stripe/pg-schema-diff/pkg/log"
	"github.com/stripe/pg-schema-diff/pkg/sqldb"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

const (
	tempDbMaxConnections = 5
)

var (
	errTempDbFactoryRequired = fmt.Errorf("tempDbFactory is required. include the option WithTempDbFactory")
)

type DoSomethingOnTempdb interface {
	Do(*sql.DB)
}

func WithdoSomethingOnTempdb(doSomethingOnTempdb DoSomethingOnTempdb) PlanOpt {
	return func(opts *planOptions) {
		opts.doSomethingOnTempdb = doSomethingOnTempdb
	}
}

type (
	planOptions struct {
		tempDbFactory           tempdb.Factory
		dataPackNewTables       bool
		ignoreChangesToColOrder bool
		logger                  log.Logger
		validatePlan            bool
		getSchemaOpts           []schema.GetSchemaOpt
		doSomethingOnTempdb     DoSomethingOnTempdb
	}

	PlanOpt func(opts *planOptions)
)

func WithTempDbFactory(factory tempdb.Factory) PlanOpt {
	return func(opts *planOptions) {
		opts.tempDbFactory = factory
	}
}

// WithDataPackNewTables configures the plan generation such that it packs the columns in the new tables to minimize
// padding. It will help minimize the storage used by the tables
func WithDataPackNewTables() PlanOpt {
	return func(opts *planOptions) {
		opts.dataPackNewTables = true
	}
}

// WithRespectColumnOrder configures the plan generation to respect any changes to the ordering of columns in
// existing tables. You will most likely want this disabled, since column ordering changes are common
func WithRespectColumnOrder() PlanOpt {
	return func(opts *planOptions) {
		opts.ignoreChangesToColOrder = false
	}
}

// WithDoNotValidatePlan disables plan validation, where the migration plan is tested against a temporary database
// instance.
func WithDoNotValidatePlan() PlanOpt {
	return func(opts *planOptions) {
		opts.validatePlan = false
	}
}

// WithLogger configures plan generation to use the provided logger instead of the default
func WithLogger(logger log.Logger) PlanOpt {
	return func(opts *planOptions) {
		opts.logger = logger
	}
}

func WithIncludeSchemas(schemas ...string) PlanOpt {
	return func(opts *planOptions) {
		opts.getSchemaOpts = append(opts.getSchemaOpts, schema.WithIncludeSchemas(schemas...))
	}
}

func WithExcludeSchemas(schemas ...string) PlanOpt {
	return func(opts *planOptions) {
		opts.getSchemaOpts = append(opts.getSchemaOpts, schema.WithExcludeSchemas(schemas...))
	}
}

func WithGetSchemaOpts(getSchemaOpts ...externalschema.GetSchemaOpt) PlanOpt {
	return func(opts *planOptions) {
		opts.getSchemaOpts = append(opts.getSchemaOpts, getSchemaOpts...)
	}
}

// deprecated: GeneratePlan generates a migration plan to migrate the database to the target schema. This function only
// diffs the public schemas.
//
// Use Generate instead with the DDLSchemaSource(newDDL) and WithIncludeSchemas("public") and WithTempDbFactory options.
//
// Parameters:
// queryable: 	The target database to generate the diff for. It is recommended to pass in *sql.DB of the db you
// wish to migrate. If using a connection pool, it is RECOMMENDED to set a maximum number of connections.
// tempDbFactory:  	used to create a temporary database instance to extract the schema from the new DDL and validate the
// migration plan. It is recommended to use tempdb.NewOnInstanceFactory, or you can provide your own.
// newDDL:  		DDL encoding the new schema
// opts:  			Additional options to configure the plan generation
func GeneratePlan(ctx context.Context, queryable sqldb.Queryable, tempdbFactory tempdb.Factory, newDDL []string, opts ...PlanOpt) (Plan, error) {
	return Generate(ctx, queryable, DDLSchemaSource(newDDL), append(opts, WithTempDbFactory(tempdbFactory), WithIncludeSchemas("public"))...)
}

// Generate generates a migration plan to migrate the database to the target schema
//
// Parameters:
// fromDB:			The target database to generate the diff for. It is recommended to pass in *sql.DB of the db you
// wish to migrate. If using a connection pool, it is RECOMMENDED to set a maximum number of connections.
// targetSchema:	The (source of the) schema you want to migrate the database to. Use DDLSchemaSource if the new
// schema is encoded in DDL.
// opts: 			Additional options to configure the plan generation
func Generate(
	ctx context.Context,
	fromDB sqldb.Queryable,
	targetSchema SchemaSource,
	opts ...PlanOpt,
) (Plan, error) {
	planOptions := &planOptions{
		validatePlan:            true,
		ignoreChangesToColOrder: true,
		logger:                  log.SimpleLogger(),
	}
	for _, opt := range opts {
		opt(planOptions)
	}

	currentSchema, err := schema.GetSchema(ctx, fromDB, planOptions.getSchemaOpts...)
	if err != nil {
		return Plan{}, fmt.Errorf("getting current schema: %w", err)
	}
	newSchema, err := targetSchema.GetSchema(ctx, schemaSourcePlanDeps{
		tempDBFactory:       planOptions.tempDbFactory,
		logger:              planOptions.logger,
		getSchemaOpts:       planOptions.getSchemaOpts,
		doSomethingOnTempdb: planOptions.doSomethingOnTempdb,
	})
	if err != nil {
		return Plan{}, fmt.Errorf("getting new schema: %w", err)
	}

	statements, err := generateMigrationStatements(currentSchema, newSchema, planOptions)
	if err != nil {
		return Plan{}, fmt.Errorf("generating plan statements: %w", err)
	}

	hash, err := currentSchema.Hash()
	if err != nil {
		return Plan{}, fmt.Errorf("generating current schema hash: %w", err)
	}

	plan := Plan{
		Statements:        statements,
		CurrentSchemaHash: hash,
	}

	if planOptions.validatePlan {
		if planOptions.tempDbFactory == nil {
			return Plan{}, fmt.Errorf("cannot validate plan without a tempDbFactory: %w", errTempDbFactoryRequired)
		}
		if err := assertValidPlan(ctx, planOptions.tempDbFactory, currentSchema, newSchema, plan, planOptions); err != nil {
			return Plan{}, fmt.Errorf("validating migration plan: %w \n%# v", err, pretty.Formatter(plan))
		}
	}

	return plan, nil
}

func generateMigrationStatements(oldSchema, newSchema schema.Schema, planOptions *planOptions) ([]Statement, error) {
	diff, _, err := buildSchemaDiff(oldSchema, newSchema)
	if err != nil {
		return nil, err
	}

	if planOptions.dataPackNewTables {
		// Instead of enabling ignoreChangesToColOrder by default, force the user to enable ignoreChangesToColOrder.
		// This ensures the user knows what's going on behind-the-scenes
		if !planOptions.ignoreChangesToColOrder {
			return nil, fmt.Errorf("cannot data pack new tables without also ignoring changes to column order")
		}
		diff = dataPackNewTables(diff)
	}
	if planOptions.ignoreChangesToColOrder {
		diff = removeChangesToColumnOrdering(diff)
	}

	statements, err := diff.resolveToSQL()
	if err != nil {
		return nil, fmt.Errorf("generating migration statements: %w", err)
	}
	return statements, nil
}

func assertValidPlan(ctx context.Context,
	tempDbFactory tempdb.Factory,
	currentSchema, newSchema schema.Schema,
	plan Plan,
	planOptions *planOptions,
) error {
	tempDb, err := tempDbFactory.Create(ctx)
	if err != nil {
		return err
	}
	defer func(closer tempdb.ContextualCloser) {
		if err := closer.Close(ctx); err != nil {
			planOptions.logger.Errorf("an error occurred while dropping the temp database: %s", err)
		}
	}(tempDb.ContextualCloser)
	// Set a max connections if a user has not set one. This is to prevent us from exploding the number of connections
	// on the database.
	setMaxConnectionsIfNotSet(tempDb.ConnPool, tempDbMaxConnections)

	if err := setSchemaForEmptyDatabase(ctx, tempDb, currentSchema, planOptions); err != nil {
		return fmt.Errorf("inserting schema in temporary database: %w", err)
	}

	if err := executeStatementsIgnoreTimeouts(ctx, tempDb.ConnPool, plan.Statements); err != nil {
		return fmt.Errorf("running migration plan: %w", err)
	}

	migratedSchema, err := schemaFromTempDb(ctx, tempDb, planOptions)
	if err != nil {
		return fmt.Errorf("fetching schema from migrated database: %w", err)
	}

	return assertMigratedSchemaMatchesTarget(migratedSchema, newSchema, planOptions)
}

func setMaxConnectionsIfNotSet(db *sql.DB, defaultMax int) {
	if db.Stats().MaxOpenConnections <= 0 {
		db.SetMaxOpenConns(defaultMax)
	}
}

func setSchemaForEmptyDatabase(ctx context.Context, emptyDb *tempdb.Database, targetSchema schema.Schema, options *planOptions) error {
	// We can't create invalid indexes. We'll mark them valid in the schema, which should be functionally
	// equivalent for the sake of DDL and other statements.
	//
	// Make a new array, so we don't mutate the underlying array of the original schema. Ideally, we have a clone function
	// in the future
	var validIndexes []schema.Index
	for _, idx := range targetSchema.Indexes {
		idx.IsInvalid = false
		validIndexes = append(validIndexes, idx)
	}
	targetSchema.Indexes = validIndexes

	// An empty database doesn't necessarily have an empty schema, so we should fetch it.
	startingSchema, err := schemaFromTempDb(ctx, emptyDb, options)
	if err != nil {
		return fmt.Errorf("getting schema from empty database: %w", err)
	}

	statements, err := generateMigrationStatements(startingSchema, targetSchema, &planOptions{})
	if err != nil {
		return fmt.Errorf("building schema diff: %w", err)
	}
	if err := executeStatementsIgnoreTimeouts(ctx, emptyDb.ConnPool, statements); err != nil {
		return fmt.Errorf("executing statements: %w\n%# v", err, pretty.Formatter(statements))
	}
	return nil
}

func schemaFromTempDb(ctx context.Context, db *tempdb.Database, plan *planOptions) (schema.Schema, error) {
	return schema.GetSchema(ctx, db.ConnPool, append(plan.getSchemaOpts, db.ExcludeMetadataOptions...)...)
}

func assertMigratedSchemaMatchesTarget(migratedSchema, targetSchema schema.Schema, planOptions *planOptions) error {
	toTargetSchemaStmts, err := generateMigrationStatements(migratedSchema, targetSchema, planOptions)
	if err != nil {
		return fmt.Errorf("building schema diff between migrated database and new schema: %w", err)
	}

	if len(toTargetSchemaStmts) > 0 {
		var stmtsStrs []string
		for _, stmt := range toTargetSchemaStmts {
			stmtsStrs = append(stmtsStrs, stmt.DDL)
		}
		return fmt.Errorf("validating plan failed. diff detected:\n%s", strings.Join(stmtsStrs, "\n"))
	}

	return nil
}

// executeStatementsIgnoreTimeouts executes the statements using the sql connection but ignores any provided timeouts.
// This function is currently used to validate migration plans.
func executeStatementsIgnoreTimeouts(ctx context.Context, connPool *sql.DB, statements []Statement) error {
	conn, err := connPool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("getting connection from pool: %w", err)
	}
	defer conn.Close()

	// Set a session-level statement_timeout to bound the execution of the migration plan.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION statement_timeout = %d", (10*time.Second).Milliseconds())); err != nil {
		return fmt.Errorf("setting statement timeout: %w", err)
	}
	// Due to the way *sql.Db works, when a statement_timeout is set for the session, it will NOT reset
	// by default when it's returned to the pool.
	//
	// We can't set the timeout at the TRANSACTION-level (for each transaction) because `ADD INDEX CONCURRENTLY`
	// must be executed within its own transaction block. Postgres will error if you try to set a TRANSACTION-level
	// timeout for it. SESSION-level statement_timeouts are respected by `ADD INDEX CONCURRENTLY`
	for _, stmt := range statements {
		if _, err := conn.ExecContext(ctx, stmt.ToSQL()); err != nil {
			return fmt.Errorf("executing migration statement: %s: %w", stmt, err)
		}
	}
	return nil
}
