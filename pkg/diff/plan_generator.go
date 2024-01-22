package diff

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/kr/pretty"
	"github.com/stripe/pg-schema-diff/internal/schema"

	"github.com/stripe/pg-schema-diff/internal/queries"
	"github.com/stripe/pg-schema-diff/pkg/log"
	"github.com/stripe/pg-schema-diff/pkg/sqldb"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

type (
	planOptions struct {
		dataPackNewTables       bool
		ignoreChangesToColOrder bool
		logger                  log.Logger
		validatePlan            bool
	}

	PlanOpt func(opts *planOptions)
)

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
// instance
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

// GeneratePlan generates a migration plan to migrate the database to the target schema.
//
// Parameters:
// queryable: 	The target database to generate the diff for. It is recommended to pass in *sql.DB of the db you
// wish to migrate.
// tempDbFactory:  	used to create a temporary database instance to extract the schema from the new DDL and validate the
// migration plan. It is recommended to use tempdb.NewOnInstanceFactory, or you can provide your own.
// newDDL:  		DDL encoding the new schema
// opts:  			Additional options to configure the plan generation
func GeneratePlan(ctx context.Context, queryable sqldb.Queryable, tempDbFactory tempdb.Factory, newDDL []string, opts ...PlanOpt) (Plan, error) {
	planOptions := &planOptions{
		validatePlan:            true,
		ignoreChangesToColOrder: true,
		logger:                  log.SimpleLogger(),
	}
	for _, opt := range opts {
		opt(planOptions)
	}

	currentSchema, err := schema.GetPublicSchema(ctx, queryable)
	if err != nil {
		return Plan{}, fmt.Errorf("getting current schema: %w", err)
	}
	newSchema, err := deriveSchemaFromDDLOnTempDb(ctx, planOptions.logger, tempDbFactory, newDDL)
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
		if err := assertValidPlan(ctx, tempDbFactory, currentSchema, newSchema, plan, planOptions); err != nil {
			return Plan{}, fmt.Errorf("validating migration plan: %w \n%# v", err, pretty.Formatter(plan))
		}
	}

	return plan, nil
}

func deriveSchemaFromDDLOnTempDb(ctx context.Context, logger log.Logger, tempDbFactory tempdb.Factory, ddl []string) (schema.Schema, error) {
	tempDb, dropTempDb, err := tempDbFactory.Create(ctx)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("creating temp database: %w", err)
	}
	defer func(drop tempdb.Dropper) {
		if err := drop(ctx); err != nil {
			logger.Errorf("an error occurred while dropping the temp database: %s", err)
		}
	}(dropTempDb)

	for _, stmt := range ddl {
		if _, err := tempDb.ExecContext(ctx, stmt); err != nil {
			return schema.Schema{}, fmt.Errorf("running DDL: %w", err)
		}
	}

	return schema.GetPublicSchema(ctx, tempDb)
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
	tempDb, dropTempDb, err := tempDbFactory.Create(ctx)
	if err != nil {
		return err
	}
	defer func(drop tempdb.Dropper) {
		if err := drop(ctx); err != nil {
			planOptions.logger.Errorf("an error occurred while dropping the temp database: %s", err)
		}
	}(dropTempDb)

	tempDbConn, err := tempDb.Conn(ctx)
	if err != nil {
		return fmt.Errorf("opening database connection: %w", err)
	}
	defer tempDbConn.Close()

	if err := setSchemaForEmptyDatabase(ctx, tempDbConn, currentSchema); err != nil {
		return fmt.Errorf("inserting schema in temporary database: %w", err)
	}

	if err := executeStatements(ctx, tempDbConn, plan.Statements); err != nil {
		return fmt.Errorf("running migration plan: %w", err)
	}

	migratedSchema, err := schema.GetPublicSchema(ctx, tempDbConn)
	if err != nil {
		return fmt.Errorf("fetching schema from migrated database: %w", err)
	}

	return assertMigratedSchemaMatchesTarget(migratedSchema, newSchema, planOptions)
}

func setSchemaForEmptyDatabase(ctx context.Context, conn *sql.Conn, dbSchema schema.Schema) error {
	// We can't create invalid indexes. We'll mark them valid in the schema, which should be functionally
	// equivalent for the sake of DDL and other statements.
	//
	// Make a new array, so we don't mutate the underlying array of the original schema. Ideally, we have a clone function
	// in the future
	var validIndexes []schema.Index
	for _, idx := range dbSchema.Indexes {
		idx.IsInvalid = false
		validIndexes = append(validIndexes, idx)
	}
	dbSchema.Indexes = validIndexes

	statements, err := generateMigrationStatements(schema.Schema{
		Tables:    nil,
		Indexes:   nil,
		Functions: nil,
		Triggers:  nil,
	}, dbSchema, &planOptions{})
	if err != nil {
		return fmt.Errorf("building schema diff: %w", err)
	}
	if err := executeStatements(ctx, conn, statements); err != nil {
		return fmt.Errorf("executing statements: %w\n%# v", err, pretty.Formatter(statements))
	}
	return nil
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
		return fmt.Errorf("diff detected:\n%s", strings.Join(stmtsStrs, "\n"))
	}

	return nil
}

// executeStatements executes the statements using the sql connection. It will modify the session-level
// statement timeout of the underlying connection.
func executeStatements(ctx context.Context, conn queries.DBTX, statements []Statement) error {
	// Due to the way *sql.Db works, when a statement_timeout is set for the session, it will NOT reset
	// by default when it's returned to the pool.
	//
	// We can't set the timeout at the TRANSACTION-level (for each transaction) because `ADD INDEX CONCURRENTLY`
	// must be executed within its own transaction block. Postgres will error if you try to set a TRANSACTION-level
	// timeout for it. SESSION-level statement_timeouts are respected by `ADD INDEX CONCURRENTLY`
	for _, stmt := range statements {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION statement_timeout = %d", stmt.Timeout.Milliseconds())); err != nil {
			return fmt.Errorf("setting statement timeout: %w", err)
		}
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_timeout = %d", stmt.LockTimeout.Milliseconds())); err != nil {
			return fmt.Errorf("setting lock timeout: %w", err)
		}
		if _, err := conn.ExecContext(ctx, stmt.ToSQL()); err != nil {
			// could the migration statement contain sensitive information?
			return fmt.Errorf("executing migration statement: %s: %w", stmt, err)
		}
	}
	return nil
}
