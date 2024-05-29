package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/spf13/cobra"
	"github.com/stripe/pg-schema-diff/pkg/diff"
	"github.com/stripe/pg-schema-diff/pkg/log"
)

func buildApplyCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Migrate your database to the match the inputted schema (apply the schema to the database)",
	}

	connFlags := createConnFlags(cmd)
	planFlags := createPlanFlags(cmd)
	allowedHazardsTypesStrs := cmd.Flags().StringSlice("allow-hazards", nil,
		"Specify the hazards that are allowed. Order does not matter, and duplicates are ignored. If the"+
			" migration plan contains unwanted hazards (hazards not in this list), then the migration will fail to run"+
			" (example: --allowed-hazards DELETES_DATA,INDEX_BUILD)")
	skipConfirmPrompt := cmd.Flags().Bool("skip-confirm-prompt", false, "Skips prompt asking for user to confirm before applying")
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

		fmt.Println(header("Review plan"))
		fmt.Print(planToPrettyS(plan), "\n\n")

		if err := failIfHazardsNotAllowed(plan, *allowedHazardsTypesStrs); err != nil {
			return err
		}

		if !*skipConfirmPrompt {
			if err := mustContinuePrompt(
				fmt.Sprintf(
					"Apply migration with the following hazards: %s?",
					strings.Join(*allowedHazardsTypesStrs, ", "),
				),
			); err != nil {
				return err
			}
		}

		if err := runPlan(context.Background(), connConfig, plan); err != nil {
			return err
		}
		fmt.Println("Schema applied successfully")
		return nil
	}

	return cmd
}

func failIfHazardsNotAllowed(plan diff.Plan, allowedHazardsTypesStrs []string) error {
	isAllowedByHazardType := make(map[diff.MigrationHazardType]bool)
	for _, val := range allowedHazardsTypesStrs {
		isAllowedByHazardType[strings.ToUpper(val)] = true
	}
	var disallowedHazardMsgs []string
	for i, stmt := range plan.Statements {
		var disallowedTypes []diff.MigrationHazardType
		for _, hzd := range stmt.Hazards {
			if !isAllowedByHazardType[hzd.Type] {
				disallowedTypes = append(disallowedTypes, hzd.Type)
			}
		}
		if len(disallowedTypes) > 0 {
			disallowedHazardMsgs = append(disallowedHazardMsgs,
				fmt.Sprintf("- Statement %d: %s", getDisplayableStmtIdx(i), strings.Join(disallowedTypes, ", ")),
			)
		}

	}
	if len(disallowedHazardMsgs) > 0 {
		return errors.New(fmt.Sprintf(
			"Prohited hazards found\n"+
				"These hazards must be allowed via the allow-hazards flag, e.g., --allow-hazards %s\n"+
				"Prohibited hazards in the following statements:\n%s",
			strings.Join(getHazardTypes(plan), ","),
			strings.Join(disallowedHazardMsgs, "\n"),
		))
	}
	return nil
}

func runPlan(ctx context.Context, connConfig *pgx.ConnConfig, plan diff.Plan) error {
	connPool, err := openDbWithPgxConfig(connConfig)
	if err != nil {
		return err
	}
	defer connPool.Close()

	conn, err := connPool.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Due to the way *sql.Db works, when a statement_timeout is set for the session, it will NOT reset
	// by default when it's returned to the pool.
	//
	// We can't set the timeout at the TRANSACTION-level (for each transaction) because `ADD INDEX CONCURRENTLY`
	// must be executed within its own transaction block. Postgres will error if you try to set a TRANSACTION-level
	// timeout for it. SESSION-level statement_timeouts are respected by `ADD INDEX CONCURRENTLY`
	for i, stmt := range plan.Statements {
		fmt.Println(header(fmt.Sprintf("Executing statement %d", getDisplayableStmtIdx(i))))
		fmt.Printf("%s\n\n", statementToPrettyS(stmt))
		start := time.Now()
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION statement_timeout = %d", stmt.Timeout.Milliseconds())); err != nil {
			return fmt.Errorf("setting statement timeout: %w", err)
		}
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_timeout = %d", stmt.Timeout.Milliseconds())); err != nil {
			return fmt.Errorf("setting lock timeout: %w", err)
		}
		if _, err := conn.ExecContext(ctx, stmt.ToSQL()); err != nil {
			return fmt.Errorf("executing migration statement. the database maybe be in a dirty state: %s: %w", stmt, err)
		}
		fmt.Printf("Finished executing statement. Duration: %s\n", time.Since(start))
	}
	fmt.Println(header("Complete"))

	return nil
}

func getHazardTypes(plan diff.Plan) []diff.MigrationHazardType {
	seenHazardTypes := make(map[diff.MigrationHazardType]bool)
	var hazardTypes []diff.MigrationHazardType
	for _, stmt := range plan.Statements {
		for _, hazard := range stmt.Hazards {
			if !seenHazardTypes[hazard.Type] {
				seenHazardTypes[hazard.Type] = true
				hazardTypes = append(hazardTypes, hazard.Type)
			}
		}
	}
	sort.Slice(hazardTypes, func(i, j int) bool {
		return hazardTypes[i] < hazardTypes[j]
	})
	return hazardTypes
}

func getDisplayableStmtIdx(i int) int {
	return i + 1
}
