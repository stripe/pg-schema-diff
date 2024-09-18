package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/spf13/cobra"
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

type outputFormat string

const (
	outputFormatPretty outputFormat = "pretty"
	outputFormatJson   outputFormat = "json"
)

func (e *outputFormat) String() string {
	return string(*e)
}

func (e *outputFormat) Set(v string) error {
	switch v {
	case "pretty", "json":
		*e = outputFormat(v)
		return nil
	default:
		return errors.New(`must be one of "pretty" or "json"`)
	}
}

func (e *outputFormat) Type() string {
	return "outputFormat"
}

func buildPlanCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "plan",
		Aliases: []string{"diff"},
		Short:   "Generate the diff between two databases and the SQL to get from one to the other",
	}

	connFlags := createConnFlags(cmd)
	planFlags := createPlanFlags(cmd)
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		logger := log.SimpleLogger()
		connConfig, err := parseConnConfig(*connFlags, logger)
		if err != nil {
			return err
		}

		planConfig, err := parsePlanConfig(*planFlags)
		if err != nil {
			return err
		}

		cmd.SilenceUsage = true

		plan, err := generatePlan(context.Background(), logger, connConfig, planConfig)
		if err != nil {
			return err
		} else if len(plan.Statements) == 0 {
			fmt.Println("Schema matches expected. No plan generated")
			return nil
		}

		if planFlags.outputFormat == "" || planFlags.outputFormat == outputFormatPretty {
			fmt.Printf("\n%s\n", header("Generated plan"))
			fmt.Println(planToPrettyS(plan))
		} else if planFlags.outputFormat == outputFormatJson {
			fmt.Println(planToJsonS(plan))
		}
		return nil
	}

	return cmd
}

type (
	schemaFlags struct {
		includeSchemas []string
		excludeSchemas []string
	}

	schemaSourceFlags struct {
		schemaDirs        []string
		targetDatabaseDSN string
	}

	planFlags struct {
		dbSchemaSourceFlags schemaSourceFlags

		schemaFlags schemaFlags

		dataPackNewTables     bool
		disablePlanValidation bool

		statementTimeoutModifiers []string
		lockTimeoutModifiers      []string
		insertStatements          []string
		outputFormat              outputFormat
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

	schemaSourceFactory func() (diff.SchemaSource, io.Closer, error)

	planConfig struct {
		schemaSourceFactory schemaSourceFactory
		opts                []diff.PlanOpt

		statementTimeoutModifiers []timeoutModifier
		lockTimeoutModifiers      []timeoutModifier
		insertStatements          []insertStatement
	}
)

func createPlanFlags(cmd *cobra.Command) *planFlags {
	flags := &planFlags{}

	schemaSourceFlagsVar(cmd, &flags.dbSchemaSourceFlags)

	schemaFlagsVar(cmd, &flags.schemaFlags)

	cmd.Flags().BoolVar(&flags.dataPackNewTables, "data-pack-new-tables", true, "If set, will data pack new tables in the plan to minimize table size (re-arranges columns).")
	cmd.Flags().BoolVar(&flags.disablePlanValidation, "disable-plan-validation", false, "If set, will disable plan validation. Plan validation runs the migration against a temporary"+
		"database with an identical schema to the original, asserting that the generated plan actually migrates the schema to the desired target.")

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

	cmd.Flags().Var(&flags.outputFormat, "output-format", "Change the output format for what is printed. Defaults to pretty-printed human-readable output. (options: pretty, json)")

	return flags
}

func schemaSourceFlagsVar(cmd *cobra.Command, p *schemaSourceFlags) {
	cmd.Flags().StringArrayVar(&p.schemaDirs, "schema-dir", nil, "Directory of .SQL files to use as the schema source (can be multiple). Use to generate a diff between the target database and the schema in this directory.")
	if err := cmd.MarkFlagDirname("schema-dir"); err != nil {
		panic(err)
	}
	cmd.Flags().StringVar(&p.targetDatabaseDSN, "schema-source-dsn", "", "DSN for the database to use as the schema source. Use to generate a diff between the target database and the schema in this database.")

	cmd.MarkFlagsMutuallyExclusive("schema-dir", "schema-source-dsn")
}

func schemaFlagsVar(cmd *cobra.Command, p *schemaFlags) {
	cmd.Flags().StringArrayVar(&p.includeSchemas, "include-schema", nil, "Include the specified schema in the plan")
	cmd.Flags().StringArrayVar(&p.excludeSchemas, "exclude-schema", nil, "Exclude the specified schema in the plan")
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

func parsePlanConfig(p planFlags) (planConfig, error) {
	schemaSourceFactory, err := parseSchemaSource(p.dbSchemaSourceFlags)
	if err != nil {
		return planConfig{}, err
	}

	opts := parseSchemaConfig(p.schemaFlags)
	if p.dataPackNewTables {
		opts = append(opts, diff.WithDataPackNewTables())
	}
	if p.disablePlanValidation {
		opts = append(opts, diff.WithDoNotValidatePlan())
	}

	var statementTimeoutModifiers []timeoutModifier
	for _, s := range p.statementTimeoutModifiers {
		stm, err := parseTimeoutModifier(s)
		if err != nil {
			return planConfig{}, fmt.Errorf("parsing statement timeout modifier from %q: %w", s, err)
		}
		statementTimeoutModifiers = append(statementTimeoutModifiers, stm)
	}

	var lockTimeoutModifiers []timeoutModifier
	for _, s := range p.lockTimeoutModifiers {
		ltm, err := parseTimeoutModifier(s)
		if err != nil {
			return planConfig{}, fmt.Errorf("parsing statement timeout modifier from %q: %w", s, err)
		}
		lockTimeoutModifiers = append(lockTimeoutModifiers, ltm)
	}

	var insertStatements []insertStatement
	for _, i := range p.insertStatements {
		is, err := parseInsertStatementStr(i)
		if err != nil {
			return planConfig{}, fmt.Errorf("parsing insert statement from %q: %w", i, err)
		}
		insertStatements = append(insertStatements, is)
	}

	return planConfig{
		schemaSourceFactory:       schemaSourceFactory,
		opts:                      opts,
		statementTimeoutModifiers: statementTimeoutModifiers,
		lockTimeoutModifiers:      lockTimeoutModifiers,
		insertStatements:          insertStatements,
	}, nil
}

func parseSchemaSource(p schemaSourceFlags) (schemaSourceFactory, error) {
	if len(p.schemaDirs) > 0 {
		return func() (diff.SchemaSource, io.Closer, error) {
			schemaSource, err := diff.DirSchemaSource(p.schemaDirs)
			if err != nil {
				return nil, nil, err
			}
			return schemaSource, nil, nil
		}, nil
	}

	if p.targetDatabaseDSN != "" {
		connConfig, err := pgx.ParseConfig(p.targetDatabaseDSN)
		if err != nil {
			return nil, fmt.Errorf("parsing DSN %q: %w", p.targetDatabaseDSN, err)
		}
		return func() (diff.SchemaSource, io.Closer, error) {
			connPool, err := openDbWithPgxConfig(connConfig)
			if err != nil {
				return nil, nil, fmt.Errorf("opening db with pgx config: %w", err)
			}
			return diff.DBSchemaSource(connPool), connPool, nil
		}, nil
	}

	return nil, fmt.Errorf("either --schema-dir or --schema-source-dsn must be set")
}

func parseSchemaConfig(p schemaFlags) []diff.PlanOpt {
	return []diff.PlanOpt{
		diff.WithIncludeSchemas(p.includeSchemas...),
		diff.WithExcludeSchemas(p.excludeSchemas...),
	}
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

func generatePlan(ctx context.Context, logger log.Logger, connConfig *pgx.ConnConfig, planConfig planConfig) (diff.Plan, error) {
	tempDbFactory, err := tempdb.NewOnInstanceFactory(ctx, func(ctx context.Context, dbName string) (*sql.DB, error) {
		copiedConfig := connConfig.Copy()
		copiedConfig.Database = dbName
		return openDbWithPgxConfig(copiedConfig)
	}, tempdb.WithRootDatabase(connConfig.Database))
	if err != nil {
		return diff.Plan{}, err
	}
	defer func() {
		err := tempDbFactory.Close()
		if err != nil {
			logger.Errorf("error shutting down temp db factory: %v", err)
		}
	}()

	connPool, err := openDbWithPgxConfig(connConfig)
	if err != nil {
		return diff.Plan{}, err
	}
	defer connPool.Close()
	connPool.SetMaxOpenConns(defaultMaxConnections)

	schemaSource, schemaSourceCloser, err := planConfig.schemaSourceFactory()
	if err != nil {
		return diff.Plan{}, fmt.Errorf("creating schema source: %w", err)
	}
	if schemaSourceCloser != nil {
		defer schemaSourceCloser.Close()
	}

	plan, err := diff.Generate(ctx, connPool, schemaSource,
		append(
			planConfig.opts,
			diff.WithTempDbFactory(tempDbFactory),
		)...,
	)
	if err != nil {
		return diff.Plan{}, fmt.Errorf("generating plan: %w", err)
	}

	modifiedPlan, err := applyPlanModifiers(
		plan,
		planConfig,
	)
	if err != nil {
		return diff.Plan{}, fmt.Errorf("applying plan modifiers: %w", err)
	}

	return modifiedPlan, nil
}

func applyPlanModifiers(
	plan diff.Plan,
	config planConfig,
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

func planToJsonS(plan diff.Plan) string {
	jsonData, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(jsonData)
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
