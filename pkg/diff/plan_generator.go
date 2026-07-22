package diff

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kr/pretty"
	"github.com/stripe/pg-schema-diff/internal/pgidentifier"
	"github.com/stripe/pg-schema-diff/internal/schema"

	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

var errTempDbFactoryRequired = fmt.Errorf("tempDbFactory is required. include the option WithTempDbFactory")

const (
	defaultSchemaPartialArchivalPrefix = schema.DefaultCleanupSchemaPrefix
	maxSchemaPartialArchivalPrefixSize = 21
)

type (
	planOptions struct {
		tempDbFactory               tempdb.Factory
		logger                      *slog.Logger
		validatePlan                bool
		getSchemaOpts               []schema.GetSchemaOpt
		randReader                  io.Reader
		noConcurrentIndexOps        bool
		schemaPartialArchivalPrefix string
		now                         func() time.Time
		generationTimestamp         time.Time
	}

	PlanOpt func(opts *planOptions)
)

func WithTempDbFactory(factory tempdb.Factory) PlanOpt {
	return func(opts *planOptions) {
		opts.tempDbFactory = factory
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
func WithLogger(logger *slog.Logger) PlanOpt {
	return func(opts *planOptions) {
		opts.logger = logger
	}
}

func WithIncludeSchemaPatterns(patterns ...string) PlanOpt {
	return func(opts *planOptions) {
		opts.getSchemaOpts = append(opts.getSchemaOpts,
			schema.WithIncludeSchemaPatterns(patterns...))
	}
}

func WithExcludeSchemaPatterns(patterns ...string) PlanOpt {
	return func(opts *planOptions) {
		opts.getSchemaOpts = append(opts.getSchemaOpts,
			schema.WithExcludeSchemaPatterns(patterns...))
	}
}

func WithGetSchemaOpts(getSchemaOpts ...schema.GetSchemaOpt) PlanOpt {
	return func(opts *planOptions) {
		opts.getSchemaOpts = append(opts.getSchemaOpts, getSchemaOpts...)
	}
}

// WithRandReader seeds the random used to generate random SQL identifiers, e.g., temporary not-null check constraints.
func WithRandReader(randReader io.Reader) PlanOpt {
	return func(opts *planOptions) {
		opts.randReader = randReader
	}
}

// WithNoConcurrentIndexOps disables the use of CONCURRENTLY in CREATE INDEX and DROP INDEX statements.
// This can be useful when you need simpler DDL statements or when working in environments that don't support
// concurrent index operations. Note that disabling concurrent operations may result in longer lock times
// and potential downtime during migrations.
func WithNoConcurrentIndexOps() PlanOpt {
	return func(opts *planOptions) {
		opts.noConcurrentIndexOps = true
	}
}

// WithSchemaPartialArchivalPrefix configures generated archival schema names and
// strict discovery of existing marked archival schemas. A custom prefix replaces
// the default prefix.
func WithSchemaPartialArchivalPrefix(prefix string) PlanOpt {
	return func(opts *planOptions) {
		opts.schemaPartialArchivalPrefix = prefix
	}
}

// Generate generates a migration plan to migrate the database to the target schema
//
// Parameters:
// fromSchema:		The target schema to generate the diff for.
// targetSchema:	The (source of the) schema you want to migrate the database to. Use DDLSchemaSource if the new
// schema is encoded in DDL.
// opts: 			Additional options to configure the plan generation
func Generate(
	ctx context.Context,
	fromSchema SchemaSource,
	targetSchema SchemaSource,
	opts ...PlanOpt,
) (Plan, error) {
	planOptions := &planOptions{
		validatePlan:                true,
		logger:                      slog.Default(),
		randReader:                  rand.Reader,
		schemaPartialArchivalPrefix: defaultSchemaPartialArchivalPrefix,
		now:                         time.Now,
	}
	for _, opt := range opts {
		opt(planOptions)
	}
	planOptions.generationTimestamp = planOptions.now().UTC()
	if planOptions.logger == nil {
		planOptions.logger = slog.Default()
	}
	if err := validateSchemaPartialArchivalPrefix(planOptions.schemaPartialArchivalPrefix); err != nil {
		return Plan{}, err
	}
	currentSnapshot, err := fromSchema.GetSchemaSnapshot(ctx, schemaSourcePlanDeps{
		tempDBFactory: planOptions.tempDbFactory,
		logger:        planOptions.logger,
		getSchemaOpts: planOptions.getSchemaOpts,
	})
	if err != nil {
		return Plan{}, fmt.Errorf("getting current schema: %w", err)
	}
	newSnapshot, err := targetSchema.GetSchemaSnapshot(ctx, schemaSourcePlanDeps{
		tempDBFactory: planOptions.tempDbFactory,
		logger:        planOptions.logger,
		getSchemaOpts: planOptions.getSchemaOpts,
	})
	if err != nil {
		return Plan{}, fmt.Errorf("getting new schema: %w", err)
	}
	if err := validateNoChangedExtensionOwnsTable(currentSnapshot, newSnapshot); err != nil {
		return Plan{}, err
	}

	archival, err := orchestrateArchivalGeneration(currentSnapshot, newSnapshot, planOptions)
	if err != nil {
		return Plan{}, fmt.Errorf("generating archival migration plan: %w", err)
	}
	currentHash, err := buildCandidatePlanSnapshotHash(archival.current, archival.cleanup)
	if err != nil {
		return Plan{}, fmt.Errorf("hashing current schema snapshot: %w", err)
	}

	plan := Plan{
		Statements:        archival.statements,
		CleanupStatements: archival.cleanup.CleanupStatements,
		CurrentSchemaHash: currentHash,
	}

	archivalValidation := archivalPlanValidationRequest{
		TempDBFactory: planOptions.tempDbFactory, Logger: planOptions.logger,
		Prefix:          planOptions.schemaPartialArchivalPrefix,
		CurrentSnapshot: archival.current, TargetSnapshot: archival.target,
		OrdinaryStatements: plan.Statements, Cleanup: archival.cleanup,
		SourcePreflight: archival.preflight, DependencyClosure: archival.dependencyClosure,
		Isolation: archival.isolation, ManagedSchemaOptions: planOptions.getSchemaOpts,
	}
	if len(archival.cleanup.FinalizedMarkers) > 0 {
		if err := validateArchivalPlanSourceFacts(archivalValidation); err != nil {
			return Plan{}, fmt.Errorf("validating archival source safety: %w", err)
		}
	}
	if planOptions.validatePlan {
		if planOptions.tempDbFactory == nil {
			return Plan{}, fmt.Errorf("cannot validate plan without a tempDbFactory: %w", errTempDbFactoryRequired)
		}
		if len(archival.cleanup.FinalizedMarkers) > 0 {
			err = validateArchivalPlanTwoPhase(ctx, archivalValidation)
		} else {
			err = assertValidPlan(ctx, planOptions.tempDbFactory, archival.current.Schema,
				archival.target.Schema, plan, planOptions)
		}
		if err != nil {
			return Plan{}, fmt.Errorf("validating migration plan: %w \n%# v", err, pretty.Formatter(plan))
		}
	}

	return plan, nil
}

func validateSchemaPartialArchivalPrefix(prefix string) error {
	if !pgidentifier.IsSimpleIdentifier(prefix) {
		return fmt.Errorf("schema partial archival prefix %q must be a simple PostgreSQL identifier", prefix)
	}
	if prefix == "pg" || strings.HasPrefix(prefix, "pg_") {
		return fmt.Errorf("schema partial archival prefix %q uses PostgreSQL's reserved pg_ prefix", prefix)
	}
	if len(prefix) > maxSchemaPartialArchivalPrefixSize {
		return fmt.Errorf("schema partial archival prefix %q must be at most %d bytes",
			prefix, maxSchemaPartialArchivalPrefixSize)
	}
	return nil
}

func generateMigrationStatements(oldSchema, newSchema schema.Schema, planOptions *planOptions) ([]Statement, error) {
	diff, _, err := buildSchemaDiff(oldSchema, newSchema)
	if err != nil {
		return nil, err
	}

	statements, err := newSchemaSQLGenerator(planOptions.randReader, planOptions).Alter(diff)
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
	defer func() {
		if err := tempDb.Close(ctx); err != nil {
			planOptions.logger.ErrorContext(
				ctx, "failed to drop temporary database",
				slog.Any("error", err),
			)
		}
	}()
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

	return assertMigratedSchemaMatchesTarget(*migratedSchema, newSchema, planOptions)
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

	statements, err := generateMigrationStatements(*startingSchema, targetSchema, &planOptions{})
	if err != nil {
		return fmt.Errorf("building schema diff: %w", err)
	}
	if err := executeStatementsIgnoreTimeouts(ctx, emptyDb.ConnPool, statements); err != nil {
		return fmt.Errorf("executing statements: %w\n%# v", err, pretty.Formatter(statements))
	}
	return nil
}

func schemaFromTempDb(ctx context.Context, db *tempdb.Database, plan *planOptions) (*schema.Schema, error) {
	snapshot, err := schema.GetSchemaSnapshot(ctx, db.ConnPool, plan.getSchemaOpts...)
	if err != nil {
		return nil, err
	}
	return &snapshot.Schema, nil
}

// clearTablePrivileges returns a copy of the schema with all table privileges cleared.
// This is used during plan validation because privilege statements are skipped (roles don't exist in temp DB).
func clearTablePrivileges(s schema.Schema) schema.Schema {
	tables := make([]schema.Table, len(s.Tables))
	for i, t := range s.Tables {
		t.Privileges = nil
		tables[i] = t
	}
	s.Tables = tables
	return s
}

func assertMigratedSchemaMatchesTarget(migratedSchema, targetSchema schema.Schema, planOptions *planOptions) error {
	// Clear privileges from both schemas since privilege statements are skipped during validation
	// (roles don't exist in temp DB). We make copies to avoid modifying the original schemas.
	migratedSchema = clearTablePrivileges(migratedSchema)
	targetSchema = clearTablePrivileges(targetSchema)

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

// executeStatementsIgnoreTimeouts executes the statements using a single connection but ignores any provided timeouts.
// This function is currently used to validate migration plans.
func executeStatementsIgnoreTimeouts(ctx context.Context, connPool *pgxpool.Pool, statements []Statement) error {
	conn, err := connPool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("getting connection from pool: %w", err)
	}
	defer conn.Release()

	// Set a session-level statement_timeout to bound the execution of the migration plan.
	if _, err := conn.Exec(ctx, fmt.Sprintf("SET SESSION statement_timeout = %d",
		(10*time.Second).Milliseconds())); err != nil {
		return fmt.Errorf("setting statement timeout: %w", err)
	}
	// When a statement_timeout is set for the session, it will not reset
	// by default when it's returned to the pool.
	//
	// We can't set the timeout at the TRANSACTION-level (for each transaction) because `ADD INDEX CONCURRENTLY`
	// must be executed within its own transaction block. Postgres will error if you try to set a TRANSACTION-level
	// timeout for it. SESSION-level statement_timeouts are respected by `ADD INDEX CONCURRENTLY`
	for _, stmt := range statements {
		if stmt.SkipValidation {
			// Skip statements that cannot be validated in temp DB (e.g., GRANT/REVOKE which reference roles
			// that don't exist in the temp DB)
			continue
		}
		if _, err := conn.Exec(ctx, stmt.ToSQL()); err != nil {
			return fmt.Errorf("executing migration statement: %s: %w", stmt.DDL, err)
		}
	}
	return nil
}
