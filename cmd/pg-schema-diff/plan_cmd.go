package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/spf13/cobra"
	"github.com/stripe/pg-schema-diff/internal/util"
	"github.com/stripe/pg-schema-diff/pkg/diff"
	"github.com/stripe/pg-schema-diff/pkg/log"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

const (
	defaultMaxConnections = 5

	patternTimeoutModifierKey = "pattern"
	timeoutTimeoutModifierKey = "timeout"

	indexInsertStatementKey            = "index"
	statementInsertStatementKey        = "statement"
	statementTimeoutInsertStatementKey = "timeout"
	lockTimeoutInsertStatementKey      = "lock_timeout"
)

func buildPlanCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "plan",
		Aliases: []string{"diff"},
		Short:   "Generate the diff between two databases and the SQL to get from one to the other",
	}

	fromSchemaFlags := createSchemaSourceFlags(cmd, "from-")
	toSchemaFlags := createSchemaSourceFlags(cmd, "to-")
	tempDbConnFlags := createConnectionFlags(cmd, "temp-db-", "The temporary database to use for schema extraction. This is optional if diffing to/from a Postgres instance")
	planOptsFlags := createPlanOptionsFlags(cmd)
	outputFmt := outputFormatSql
	cmd.Flags().Var(
		&outputFmt,
		"output-format",
		fmt.Sprintf("Change the output format for what is printed. Defaults to %v. (options: %s)", outputFmt.identifier, strings.Join(outputFormatStrings(), ", ")),
	)
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		logger := log.SimpleLogger()

		fromSchema, err := parseSchemaSource(*fromSchemaFlags)
		if err != nil {
			return err
		}

		toSchema, err := parseSchemaSource(*toSchemaFlags)
		if err != nil {
			return err
		}

		if !tempDbConnFlags.IsSet() {
			// A temporary database must be provided. Attempt to pull it from the from or to schema source.
			if fromSchemaFlags.connFlags.IsSet() {
				tempDbConnFlags = fromSchemaFlags.connFlags
			} else if toSchemaFlags.connFlags.IsSet() {
				tempDbConnFlags = toSchemaFlags.connFlags
			} else {
				// In the future, we may allow folks to plumb in a postgres binary that we start for them OR a separate
				// flag that allows them to specify a temporary database DSN>
				//
				// Notably, a temporary database is NOT required if both databases are DSNs..., but inherently that means
				// we can derive a tempdDbDsn (this case is never hit).
				return fmt.Errorf("at least one Postgres server must be provided to generate a plan. either --%s, --%s or --%s must be set. Without a temporary Postgres database, pg-schema-diff cannot extract the schema from DDL", tempDbConnFlags.dsnFlagName, fromSchemaFlags.connFlags.dsnFlagName, toSchemaFlags.connFlags.dsnFlagName)
			}
		}
		tempDbConnConfig, err := parseConnectionFlags(tempDbConnFlags)
		if err != nil {
			return err
		}

		planOpts, err := parsePlanOptions(*planOptsFlags)
		if err != nil {
			return err
		}

		cmd.SilenceUsage = true

		plan, err := generatePlan(cmd.Context(), generatePlanParameters{
			fromSchema:       fromSchema,
			toSchema:         toSchema,
			tempDbConnConfig: tempDbConnConfig,
			planOptions:      planOpts,
			logger:           logger,
		})
		if err != nil {
			return err
		}

		cmd.Println(outputFmt.convertToOutputString(plan))
		return nil
	}

	return cmd
}

type (
	// parsePlanOptionsFlags stores the flags that are parsed into planOptions.
	planOptionsFlags struct {
		includeSchemas []string
		excludeSchemas []string

		dataPackNewTables     bool
		disablePlanValidation bool
		noConcurrentIndexOps  bool

		statementTimeoutModifiers []string
		lockTimeoutModifiers      []string
		insertStatements          []string
	}

	outputFormat struct {
		identifier            string
		convertToOutputString func(diff.Plan) string
	}

	timeoutModifier struct {
		regex   *regexp.Regexp
		timeout time.Duration
	}

	insertStatement struct {
		ddl         string
		index       int
		timeout     time.Duration
		lockTimeout time.Duration
	}

	// planOptions stores options that are plumbed into plan generation process and dictate post-plan processing.
	planOptions struct {
		opts                      []diff.PlanOpt
		statementTimeoutModifiers []timeoutModifier
		lockTimeoutModifiers      []timeoutModifier
		insertStatements          []insertStatement
	}

	// schemaSourceFactoryFlags stores the flags that are parsed into a schemaSourceFactory.
	schemaSourceFactoryFlags struct {
		// schemaDirs should be provided if the schema is defined via SQL files.
		schemaDirs        []string
		schemaDirFlagName string

		// connFlags should be provided if the schema is defined through a database.
		connFlags *connectionFlags
	}

	// schemaSourceFactory provides a layer of indirection such that all database opening and closing can be done
	// in a single place, i.e., in the plan generation function. It also enables  schema source flag parsing to return
	// errors while SilenceUsage=true, and database connection opening to have SilenceUsage=false.
	schemaSourceFactory func() (diff.SchemaSource, io.Closer, error)
)

var (
	outputFormatJson = outputFormat{
		identifier:            "json",
		convertToOutputString: planToJsonS,
	}

	outputFormatSql = outputFormat{
		identifier:            "sql",
		convertToOutputString: planToSql,
	}

	outputFormatPretty = outputFormat{
		identifier:            "pretty",
		convertToOutputString: planToPrettyS,
	}

	outputFormats = []outputFormat{
		outputFormatSql,
		outputFormatPretty,
		outputFormatJson,
	}

	outputFormatStrings = func() []string {
		var options []string
		for _, format := range outputFormats {
			options = append(options, format.identifier)
		}
		return options

	}
)

func (e *outputFormat) String() string {
	return e.identifier
}

func (e *outputFormat) Set(v string) error {
	var options []string
	for _, format := range outputFormats {
		if format.identifier == v {
			*e = format
			return nil
		}
		options = append(options, format.identifier)
	}
	return fmt.Errorf("invalid output format %q. Options are: %s", v, strings.Join(options, ", "))
}

func (e *outputFormat) Type() string {
	return "outputFormat"
}

func createPlanOptionsFlags(cmd *cobra.Command) *planOptionsFlags {
	var flags planOptionsFlags

	cmd.Flags().StringArrayVar(&flags.includeSchemas, "include-schema", nil, "Include the specified schema in the plan")
	cmd.Flags().StringArrayVar(&flags.excludeSchemas, "exclude-schema", nil, "Exclude the specified schema in the plan")

	cmd.Flags().BoolVar(&flags.dataPackNewTables, "data-pack-new-tables", true, "If set, will data pack new tables in the plan to minimize table size (re-arranges columns).")
	cmd.Flags().BoolVar(&flags.disablePlanValidation, "disable-plan-validation", false, "If set, will disable plan validation. Plan validation runs the migration against a temporary"+
		"database with an identical schema to the original, asserting that the generated plan actually migrates the schema to the desired target.")
	cmd.Flags().BoolVar(&flags.noConcurrentIndexOps, "no-concurrent-index-ops", false, "If set, will disable the use of CONCURRENTLY in CREATE INDEX and DROP INDEX statements. "+
		"This may result in longer lock times and potential downtime during migrations.")

	timeoutModifierFlagVar(cmd, &flags.statementTimeoutModifiers, "statement", "t")
	timeoutModifierFlagVar(cmd, &flags.lockTimeoutModifiers, "lock", "l")
	cmd.Flags().StringArrayVarP(
		&flags.insertStatements,
		"insert-statement", "s", nil,
		fmt.Sprintf(
			"'%s=<index> %s=\"<statement>\" %s=<duration> %s=<duration>' values. Will insert the statement at the index in the "+
				"generated plan. This follows normal insert semantics. Example: -s '%s=1 %s=\"SELECT pg_sleep(5)\" %s=5s %s=1s'",
			indexInsertStatementKey, statementInsertStatementKey, statementTimeoutInsertStatementKey, lockTimeoutInsertStatementKey,
			indexInsertStatementKey, statementInsertStatementKey, statementTimeoutInsertStatementKey, lockTimeoutInsertStatementKey,
		),
	)

	return &flags
}

func createSchemaSourceFlags(cmd *cobra.Command, prefix string) *schemaSourceFactoryFlags {
	var p schemaSourceFactoryFlags

	p.schemaDirFlagName = prefix + "dir"
	cmd.Flags().StringArrayVar(&p.schemaDirs, p.schemaDirFlagName, nil, "Directory of .SQL files to use as the schema source (can be multiple).")
	if err := cmd.MarkFlagDirname(p.schemaDirFlagName); err != nil {
		panic(err)
	}

	p.connFlags = createConnectionFlags(cmd, prefix, " The database to use as the schema source")

	return &p
}

func timeoutModifierFlagVar(cmd *cobra.Command, p *[]string, timeoutType string, shorthand string) {
	flagName := fmt.Sprintf("%s-timeout-modifier", timeoutType)
	description := fmt.Sprintf("list of '%s=\"<regex>\" %s=<duration>', where if a statement matches "+
		"the regex, the statement will have the target %s timeout. If multiple regexes match, the latest regex will "+
		"take priority. Example: -t '%s=\"CREATE TABLE\" %s=5m'",
		patternTimeoutModifierKey, timeoutTimeoutModifierKey,
		timeoutType,
		patternTimeoutModifierKey, timeoutTimeoutModifierKey,
	)
	cmd.Flags().StringArrayVarP(p, flagName, shorthand, nil, description)
}

func parseSchemaSource(p schemaSourceFactoryFlags) (schemaSourceFactory, error) {
	// Store result in a var instead of returning early to ensure only one option is set.
	var ssf schemaSourceFactory

	if len(p.schemaDirs) > 0 {
		ssf = func() (diff.SchemaSource, io.Closer, error) {
			schemaSource, err := diff.DirSchemaSource(p.schemaDirs)
			if err != nil {
				return nil, nil, err
			}
			return schemaSource, util.NoOpCloser(), nil
		}
	}

	if p.connFlags.IsSet() {
		if ssf != nil {
			return nil, fmt.Errorf("only one of --%s or --%s can be set", p.schemaDirFlagName, p.connFlags.dsnFlagName)
		}
		connConfig, err := parseConnectionFlags(p.connFlags)
		if err != nil {
			return nil, err
		}
		ssf = dsnSchemaSource(connConfig)
	}

	if ssf == nil {
		return nil, fmt.Errorf("either --%s or --%s must be set", p.schemaDirFlagName, p.connFlags.dsnFlagName)
	}
	return ssf, nil
}

// dsnSchemaSource returns a schema source factory that connects to a database using the provided DSN.
// This exists in its own function to allow for the plan cmd to call it.
func dsnSchemaSource(connConfig *pgx.ConnConfig) schemaSourceFactory {
	return func() (diff.SchemaSource, io.Closer, error) {
		connPool, err := openDbWithPgxConfig(connConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("opening db with pgx config: %w", err)
		}
		connPool.SetMaxOpenConns(defaultMaxConnections)
		return diff.DBSchemaSource(connPool), connPool, nil
	}
}

func parsePlanOptions(p planOptionsFlags) (planOptions, error) {
	opts := []diff.PlanOpt{
		diff.WithIncludeSchemas(p.includeSchemas...),
		diff.WithExcludeSchemas(p.excludeSchemas...),
	}

	if p.dataPackNewTables {
		opts = append(opts, diff.WithDataPackNewTables())
	}
	if p.disablePlanValidation {
		opts = append(opts, diff.WithDoNotValidatePlan())
	}
	if p.noConcurrentIndexOps {
		opts = append(opts, diff.WithNoConcurrentIndexOps())
	}

	var statementTimeoutModifiers []timeoutModifier
	for _, s := range p.statementTimeoutModifiers {
		stm, err := parseTimeoutModifier(s)
		if err != nil {
			return planOptions{}, fmt.Errorf("parsing statement timeout modifier from %q: %w", s, err)
		}
		statementTimeoutModifiers = append(statementTimeoutModifiers, stm)
	}

	var lockTimeoutModifiers []timeoutModifier
	for _, s := range p.lockTimeoutModifiers {
		ltm, err := parseTimeoutModifier(s)
		if err != nil {
			return planOptions{}, fmt.Errorf("parsing statement timeout modifier from %q: %w", s, err)
		}
		lockTimeoutModifiers = append(lockTimeoutModifiers, ltm)
	}

	var insertStatements []insertStatement
	for _, i := range p.insertStatements {
		is, err := parseInsertStatementStr(i)
		if err != nil {
			return planOptions{}, fmt.Errorf("parsing insert statement from %q: %w", i, err)
		}
		insertStatements = append(insertStatements, is)
	}

	return planOptions{
		opts:                      opts,
		statementTimeoutModifiers: statementTimeoutModifiers,
		lockTimeoutModifiers:      lockTimeoutModifiers,
		insertStatements:          insertStatements,
	}, nil
}

// parseTimeoutModifier attempts to parse an option representing a statement timeout modifier in the
// form of regex=duration where duration could be a decimal number and ends with a unit
func parseTimeoutModifier(val string) (timeoutModifier, error) {
	fm, err := logFmtToMap(val)
	if err != nil {
		return timeoutModifier{}, fmt.Errorf("could not parse %q into logfmt: %w", val, err)
	}

	regexStr, err := mustGetAndDeleteKey(fm, patternTimeoutModifierKey)
	if err != nil {
		return timeoutModifier{}, err
	}

	timeoutStr, err := mustGetAndDeleteKey(fm, timeoutTimeoutModifierKey)
	if err != nil {
		return timeoutModifier{}, err
	}

	if len(fm) > 0 {
		return timeoutModifier{}, fmt.Errorf("unknown keys %s", keys(fm))
	}

	duration, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return timeoutModifier{}, fmt.Errorf("duration could not be parsed from %q: %w", timeoutStr, err)
	}

	re, err := regexp.Compile(regexStr)
	if err != nil {
		return timeoutModifier{}, fmt.Errorf("pattern regex could not be compiled from %q: %w", regexStr, err)
	}

	return timeoutModifier{
		regex:   re,
		timeout: duration,
	}, nil
}

func parseInsertStatementStr(val string) (insertStatement, error) {
	fm, err := logFmtToMap(val)
	if err != nil {
		return insertStatement{}, fmt.Errorf("could not parse into logfmt: %w", err)
	}

	indexStr, err := mustGetAndDeleteKey(fm, indexInsertStatementKey)
	if err != nil {
		return insertStatement{}, err
	}

	statementStr, err := mustGetAndDeleteKey(fm, statementInsertStatementKey)
	if err != nil {
		return insertStatement{}, err
	}

	statementTimeoutStr, err := mustGetAndDeleteKey(fm, statementTimeoutInsertStatementKey)
	if err != nil {
		return insertStatement{}, err
	}

	lockTimeoutStr, err := mustGetAndDeleteKey(fm, lockTimeoutInsertStatementKey)
	if err != nil {
		return insertStatement{}, err
	}

	if len(fm) > 0 {
		return insertStatement{}, fmt.Errorf("unknown keys %s", keys(fm))
	}

	index, err := strconv.Atoi(indexStr)
	if err != nil {
		return insertStatement{}, fmt.Errorf("index could not be parsed from %q: %w", indexStr, err)
	}

	statementTimeout, err := time.ParseDuration(statementTimeoutStr)
	if err != nil {
		return insertStatement{}, fmt.Errorf("statement timeout duration could not be parsed from %q: %w", statementTimeoutStr, err)
	}

	lockTimeout, err := time.ParseDuration(lockTimeoutStr)
	if err != nil {
		return insertStatement{}, fmt.Errorf("lock timeout duration could not be parsed from %q: %w", lockTimeoutStr, err)
	}

	return insertStatement{
		index:       index,
		ddl:         statementStr,
		timeout:     statementTimeout,
		lockTimeout: lockTimeout,
	}, nil
}

type generatePlanParameters struct {
	fromSchema       schemaSourceFactory
	toSchema         schemaSourceFactory
	tempDbConnConfig *pgx.ConnConfig
	planOptions      planOptions
	logger           log.Logger
}

func generatePlan(
	ctx context.Context,
	params generatePlanParameters,
) (diff.Plan, error) {
	tempDbFactory, err := tempdb.NewOnInstanceFactory(ctx, func(ctx context.Context, dbName string) (*sql.DB, error) {
		cfg := params.tempDbConnConfig.Copy()
		cfg.Database = dbName
		return openDbWithPgxConfig(cfg)
	}, tempdb.WithRootDatabase(params.tempDbConnConfig.Database))
	if err != nil {
		return diff.Plan{}, fmt.Errorf("creating temp db factory: %w", err)
	}
	defer func() {
		err := tempDbFactory.Close()
		if err != nil {
			params.logger.Errorf("error shutting down temp db factory: %v", err)
		}
	}()

	fromSchema, fromSchemaSourceCloser, err := params.fromSchema()
	if err != nil {
		return diff.Plan{}, fmt.Errorf("creating schema source: %w", err)
	}
	defer fromSchemaSourceCloser.Close()

	toSchema, toSchemaSourceCloser, err := params.toSchema()
	if err != nil {
		return diff.Plan{}, fmt.Errorf("creating schema source: %w", err)
	}
	defer toSchemaSourceCloser.Close()

	plan, err := diff.Generate(ctx, fromSchema, toSchema,
		append(
			params.planOptions.opts,
			diff.WithTempDbFactory(tempDbFactory),
		)...,
	)
	if err != nil {
		return diff.Plan{}, fmt.Errorf("generating plan: %w", err)
	}

	modifiedPlan, err := applyPlanModifiers(
		plan,
		params.planOptions,
	)
	if err != nil {
		return diff.Plan{}, fmt.Errorf("applying plan modifiers: %w", err)
	}

	return modifiedPlan, nil
}

func applyPlanModifiers(
	plan diff.Plan,
	config planOptions,
) (diff.Plan, error) {
	for _, stm := range config.statementTimeoutModifiers {
		plan = plan.ApplyStatementTimeoutModifier(stm.regex, stm.timeout)
	}
	for _, ltm := range config.lockTimeoutModifiers {
		plan = plan.ApplyLockTimeoutModifier(ltm.regex, ltm.timeout)
	}
	for _, is := range config.insertStatements {
		var err error
		plan, err = plan.InsertStatement(is.index, diff.Statement{
			DDL:         is.ddl,
			Timeout:     is.timeout,
			LockTimeout: is.lockTimeout,
			Hazards: []diff.MigrationHazard{{
				Type:    diff.MigrationHazardTypeIsUserGenerated,
				Message: "This statement is user-generated",
			}},
		})
		if err != nil {
			return diff.Plan{}, fmt.Errorf("inserting %+v: %w", is, err)
		}
	}
	return plan, nil
}

func planToPrettyS(plan diff.Plan) string {
	sb := strings.Builder{}

	if len(plan.Statements) == 0 {
		sb.WriteString("Schema matches expected. No plan generated")
		return sb.String()
	}

	sb.WriteString(fmt.Sprintf("%s\n", header("Generated plan")))

	// We are going to put a statement index before each statement. To do that,
	// we need to find how many characters are in the largest index, so we can provide the appropriate amount
	// of padding before the statements to align all of them
	// E.g.
	// 1.  ALTER TABLE foobar ADD COLUMN foo BIGINT
	// ....
	// 22. ADD INDEX some_idx ON some_other_table(some_column)
	stmtNumPadding := len(strconv.Itoa(len(plan.Statements))) // find how much padding is required for the statement index
	fmtString := fmt.Sprintf("%%0%dd. %%s", stmtNumPadding)   // supply custom padding

	var stmtStrs []string
	for i, stmt := range plan.Statements {
		stmtStr := fmt.Sprintf(fmtString, getDisplayableStmtIdx(i), statementToPrettyS(stmt))
		stmtStrs = append(stmtStrs, stmtStr)
	}
	sb.WriteString(strings.Join(stmtStrs, "\n\n"))

	return sb.String()
}

func statementToPrettyS(stmt diff.Statement) string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("%s;", stmt.DDL))
	sb.WriteString(fmt.Sprintf("\n\t-- Statement Timeout: %s", stmt.Timeout))
	if stmt.LockTimeout > 0 && stmt.LockTimeout < stmt.Timeout {
		// If LockTimeout is 0, it's effectively not set. If it's >= to Timeout, it's redundant to print
		sb.WriteString(fmt.Sprintf("\n\t-- Lock Timeout: %s", stmt.LockTimeout))
	}
	if len(stmt.Hazards) > 0 {
		for _, hazard := range stmt.Hazards {
			sb.WriteString(fmt.Sprintf("\n\t-- Hazard %s", hazardToPrettyS(hazard)))
		}
	}
	return sb.String()
}

func hazardToPrettyS(hazard diff.MigrationHazard) string {
	if len(hazard.Message) > 0 {
		return fmt.Sprintf("%s: %s", hazard.Type, hazard.Message)
	} else {
		return hazard.Type
	}
}

// planToJsonS converts the plan to JSON.
func planToJsonS(plan diff.Plan) string {
	jsonData, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(jsonData)
}

// planToSql converts the plan to one large runnable SQL script.
func planToSql(plan diff.Plan) string {
	sb := strings.Builder{}
	for i, stmt := range plan.Statements {
		sb.WriteString("/*\n")
		sb.WriteString(fmt.Sprintf("Statement %d\n", i))
		if len(stmt.Hazards) > 0 {
			for _, hazard := range stmt.Hazards {
				sb.WriteString(fmt.Sprintf("  - %s\n", hazardToPrettyS(hazard)))
			}
		}
		sb.WriteString("*/\n")
		sb.WriteString(fmt.Sprintf("SET SESSION statement_timeout = %d;\n", stmt.Timeout.Milliseconds()))
		sb.WriteString(fmt.Sprintf("SET SESSION lock_timeout = %d;\n", stmt.LockTimeout.Milliseconds()))
		sb.WriteString(fmt.Sprintf("%s;", stmt.DDL))
		if i < len(plan.Statements)-1 {
			sb.WriteString("\n\n")
		}
	}
	return sb.String()
}
