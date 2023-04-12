package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
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

var (
	// Match arguments in the format "regex=duration" where duration is any duration valid in time.ParseDuration
	// We'll let time.ParseDuration handle the complexity of parsing invalid duration, so the regex we're extracting is
	// all characters greedily up to the rightmost "="
	statementTimeoutModifierRegex = regexp.MustCompile(`^(?P<regex>.+)=(?P<duration>.+)$`)
	regexSTMRegexIndex            = statementTimeoutModifierRegex.SubexpIndex("regex")
	durationSTMRegexIndex         = statementTimeoutModifierRegex.SubexpIndex("duration")

	// Match arguments in the format "index duration:statement" where duration is any duration valid in
	// time.ParseDuration. In order to prevent matching on ":" in the duration, limit the character to just letters
	// and numbers. To keep the regex simple, we won't bother matching on a more specific pattern for durations.
	// time.ParseDuration can handle the complexity of parsing invalid durations
	insertStatementRegex              = regexp.MustCompile(`^(?P<index>\d+) (?P<duration>[a-zA-Z0-9\.]+):(?P<ddl>.+?);?$`)
	indexInsertStatementRegexIndex    = insertStatementRegex.SubexpIndex("index")
	durationInsertStatementRegexIndex = insertStatementRegex.SubexpIndex("duration")
	ddlInsertStatementRegexIndex      = insertStatementRegex.SubexpIndex("ddl")
)

func buildPlanCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "plan",
		Aliases: []string{"diff"},
		Short:   "Generate the diff between two databases and the SQL to get from one to the other",
	}

	connFlags := createConnFlags(cmd)
	planFlags := createPlanFlags(cmd)
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		connConfig, err := connFlags.parseConnConfig()
		if err != nil {
			return err
		}

		planConfig, err := planFlags.parsePlanConfig()
		if err != nil {
			return err
		}

		cmd.SilenceUsage = true

		plan, err := generatePlan(context.Background(), log.SimpleLogger(), connConfig, planConfig)
		if err != nil {
			return err
		} else if len(plan.Statements) == 0 {
			fmt.Println("Schema matches expected. No plan generated")
			return nil
		}
		fmt.Printf("\n%s\n", header("Generated plan"))
		fmt.Println(planToPrettyS(plan))
		return nil
	}

	return cmd
}

type (
	planFlags struct {
		schemaDir                 *string
		statementTimeoutModifiers *[]string
		insertStatements          *[]string
	}

	statementTimeoutModifier struct {
		regex   *regexp.Regexp
		timeout time.Duration
	}

	insertStatement struct {
		ddl     string
		index   int
		timeout time.Duration
	}

	planConfig struct {
		schemaDir                 string
		statementTimeoutModifiers []statementTimeoutModifier
		insertStatements          []insertStatement
	}
)

func createPlanFlags(cmd *cobra.Command) planFlags {
	schemaDir := cmd.Flags().String("schema-dir", "", "Directory containing schema files")
	mustMarkFlagAsRequired(cmd, "schema-dir")

	statementTimeoutModifiers := cmd.Flags().StringArrayP("statement-timeout-modifier", "t", nil,
		"regex=timeout key-value pairs, where if a statement matches the regex, the statement will have the target"+
			" timeout. If multiple regexes match, the latest regex will take priority. Example: -t 'CREATE TABLE=5m' -t 'CONCURRENTLY=10s'")
	insertStatements := cmd.Flags().StringArrayP("insert-statement", "s", nil,
		"<index>_<timeout>:<statement> values. Will insert the statement at the index in the "+
			"generated plan with the specified timeout. This follows normal insert semantics. Example: -s '0 5s:SELECT 1''")

	return planFlags{
		schemaDir:                 schemaDir,
		statementTimeoutModifiers: statementTimeoutModifiers,
		insertStatements:          insertStatements,
	}
}

func (p planFlags) parsePlanConfig() (planConfig, error) {
	var statementTimeoutModifiers []statementTimeoutModifier
	for _, s := range *p.statementTimeoutModifiers {
		stm, err := parseStatementTimeoutModifierStr(s)
		if err != nil {
			return planConfig{}, fmt.Errorf("parsing statement timeout modifier from %q: %w", s, err)
		}
		statementTimeoutModifiers = append(statementTimeoutModifiers, stm)
	}

	var insertStatements []insertStatement
	for _, i := range *p.insertStatements {
		is, err := parseInsertStatementStr(i)
		if err != nil {
			return planConfig{}, fmt.Errorf("parsing insert statement from %q: %w", i, err)
		}
		insertStatements = append(insertStatements, is)
	}

	return planConfig{
		schemaDir:                 *p.schemaDir,
		statementTimeoutModifiers: statementTimeoutModifiers,
		insertStatements:          insertStatements,
	}, nil
}

func parseStatementTimeoutModifierStr(val string) (statementTimeoutModifier, error) {
	submatches := statementTimeoutModifierRegex.FindStringSubmatch(val)
	if len(submatches) <= regexSTMRegexIndex || len(submatches) <= durationSTMRegexIndex {
		return statementTimeoutModifier{}, fmt.Errorf("could not parse regex and duration from arg. expected to be in the format of " +
			"'Some.*Regex=<duration>'. Example durations include: 2s, 5m, 10.5h")
	}
	regexStr := submatches[regexSTMRegexIndex]
	durationStr := submatches[durationSTMRegexIndex]

	regex, err := regexp.Compile(regexStr)
	if err != nil {
		return statementTimeoutModifier{}, fmt.Errorf("regex could not be compiled from %q: %w", regexStr, err)
	}

	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		return statementTimeoutModifier{}, fmt.Errorf("duration could not be parsed from %q: %w", durationStr, err)
	}

	return statementTimeoutModifier{
		regex:   regex,
		timeout: duration,
	}, nil
}

func parseInsertStatementStr(val string) (insertStatement, error) {
	submatches := insertStatementRegex.FindStringSubmatch(val)
	if len(submatches) <= indexInsertStatementRegexIndex ||
		len(submatches) <= durationInsertStatementRegexIndex ||
		len(submatches) <= ddlInsertStatementRegexIndex {
		return insertStatement{}, fmt.Errorf("could not parse index, duration, and statement from arg. expected to be in the " +
			"format of '<index> <duration>:<statement>'. Example durations include: 2s, 5m, 10.5h")
	}
	indexStr := submatches[indexInsertStatementRegexIndex]
	index, err := strconv.Atoi(indexStr)
	if err != nil {
		return insertStatement{}, fmt.Errorf("could not parse index (an int) from \"%q\"", indexStr)
	}

	durationStr := submatches[durationInsertStatementRegexIndex]
	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		return insertStatement{}, fmt.Errorf("duration could not be parsed from \"%q\": %w", durationStr, err)
	}

	return insertStatement{
		index:   index,
		ddl:     submatches[ddlInsertStatementRegexIndex],
		timeout: duration,
	}, nil
}

func generatePlan(ctx context.Context, logger log.Logger, connConfig *pgx.ConnConfig, planConfig planConfig) (diff.Plan, error) {
	ddl, err := getDDLFromPath(planConfig.schemaDir)
	if err != nil {
		return diff.Plan{}, nil
	}

	tempDbFactory, err := tempdb.NewOnInstanceFactory(ctx, func(ctx context.Context, dbName string) (*sql.DB, error) {
		copiedConfig := connConfig.Copy()
		copiedConfig.Database = dbName
		return openDbWithPgxConfig(copiedConfig)
	})
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

	conn, err := connPool.Conn(ctx)
	if err != nil {
		return diff.Plan{}, err
	}
	defer conn.Close()

	plan, err := diff.GeneratePlan(ctx, conn, tempDbFactory, ddl,
		diff.WithDataPackNewTables(),
		diff.WithIgnoreChangesToColOrder(),
	)
	if err != nil {
		return diff.Plan{}, fmt.Errorf("generating plan: %w", err)
	}

	modifiedPlan, err := applyPlanModifiers(plan, planConfig.statementTimeoutModifiers, planConfig.insertStatements)
	if err != nil {
		return diff.Plan{}, fmt.Errorf("applying plan modifiers: %w", err)
	}

	return modifiedPlan, nil
}

func applyPlanModifiers(
	plan diff.Plan,
	statementTimeoutModifiers []statementTimeoutModifier,
	insertStatements []insertStatement,
) (diff.Plan, error) {
	for _, stm := range statementTimeoutModifiers {
		plan = plan.ApplyStatementTimeoutModifier(stm.regex, stm.timeout)
	}
	for _, is := range insertStatements {
		var err error
		plan, err = plan.InsertStatement(is.index, diff.Statement{
			DDL:     is.ddl,
			Timeout: is.timeout,
			Hazards: []diff.MigrationHazard{{
				Type:    diff.MigrationHazardTypeIsUserGenerated,
				Message: "This statement is user-generated",
			}},
		})
		if err != nil {
			return diff.Plan{}, fmt.Errorf("inserting statement %q with timeout %s at index %d: %w",
				is.ddl, is.timeout, is.index, err)
		}
	}
	return plan, nil
}

func getDDLFromPath(path string) ([]string, error) {
	fileEntries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var ddl []string
	for _, entry := range fileEntries {
		if filepath.Ext(entry.Name()) == ".sql" {
			if stmts, err := os.ReadFile(filepath.Join(path, entry.Name())); err != nil {
				return nil, err
			} else {
				ddl = append(ddl, string(stmts))
			}
		}
	}
	return ddl, nil
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

func statementToPrettyS(stmt diff.Statement) string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("%s;", stmt.DDL))
	sb.WriteString(fmt.Sprintf("\n\t-- Timeout: %s", stmt.Timeout))
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
