package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/spf13/cobra"
	"github.com/stripe/pg-schema-diff/pkg/diff"
	"github.com/stripe/pg-schema-diff/pkg/log"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

func buildDumpCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dump",
		Short: "Dump the schema of a database as SQL DDL statements (effectively pg_dump)",
	}

	connFlags := createConnectionFlags(cmd, "", "The database to dump")

	var includeSchemas []string
	var excludeSchemas []string
	cmd.Flags().StringArrayVar(&includeSchemas, "include-schema", nil, "Include the specified schema in the dump")
	cmd.Flags().StringArrayVar(&excludeSchemas, "exclude-schema", nil, "Exclude the specified schema from the dump")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		connConfig, err := parseConnectionFlags(connFlags)
		if err != nil {
			return err
		}

		cmd.SilenceUsage = true

		plan, err := generateDump(cmd.Context(), generateDumpParams{
			connConfig:     connConfig,
			includeSchemas: includeSchemas,
			excludeSchemas: excludeSchemas,
		})
		if err != nil {
			return err
		}

		cmdPrintln(cmd, dumpToSql(plan))
		return nil
	}

	return cmd
}

type generateDumpParams struct {
	connConfig     *pgx.ConnConfig
	includeSchemas []string
	excludeSchemas []string
}

func generateDump(ctx context.Context, params generateDumpParams) (diff.Plan, error) {
	connPool, err := openDbWithPgxConfig(params.connConfig)
	if err != nil {
		return diff.Plan{}, fmt.Errorf("opening database connection: %w", err)
	}
	defer connPool.Close()
	connPool.SetMaxOpenConns(defaultMaxConnections)

	tempDbFactory, err := tempdb.NewOnInstanceFactory(ctx, func(ctx context.Context, dbName string) (*sql.DB, error) {
		cfg := params.connConfig.Copy()
		cfg.Database = dbName
		return openDbWithPgxConfig(cfg)
	}, tempdb.WithRootDatabase(params.connConfig.Database))
	if err != nil {
		return diff.Plan{}, fmt.Errorf("creating temp db factory: %w", err)
	}
	defer func() {
		if err := tempDbFactory.Close(); err != nil {
			log.SimpleLogger().Errorf("error shutting down temp db factory: %v", err)
		}
	}()

	plan, err := diff.Generate(ctx, diff.DDLSchemaSource([]string{}), diff.DBSchemaSource(connPool),
		diff.WithTempDbFactory(tempDbFactory),
		diff.WithIncludeSchemas(params.includeSchemas...),
		diff.WithExcludeSchemas(params.excludeSchemas...),
		diff.WithDoNotValidatePlan(),
		diff.WithNoConcurrentIndexOps(),
	)
	if err != nil {
		return diff.Plan{}, fmt.Errorf("generating plan: %w", err)
	}
	return plan, nil
}

// dumpToSql converts the plan to clean SQL without metadata
func dumpToSql(plan diff.Plan) string {
	sb := strings.Builder{}
	for i, stmt := range plan.Statements {
		sb.WriteString(stmt.DDL)
		sb.WriteString(";")
		if i < len(plan.Statements)-1 {
			sb.WriteString("\n\n")
		}
	}
	return sb.String()
}
