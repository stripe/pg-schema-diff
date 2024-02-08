package main

import (
	"github.com/jackc/pgx/v4"
	"github.com/spf13/cobra"
	"github.com/stripe/pg-schema-diff/pkg/log"
)

type connFlags struct {
	dsn string
}

func createConnFlags(cmd *cobra.Command) *connFlags {
	flags := &connFlags{}

	cmd.Flags().StringVar(&flags.dsn, "dsn", "", "Connection string for the database (DB password can be specified through PGPASSWORD environment variable)")
	// Don't mark dsn as a required flag.
	// Allow users to use the "PGHOST" etc environment variables like `psql`.

	return flags
}

func parseConnConfig(c connFlags, logger log.Logger) (*pgx.ConnConfig, error) {
	if c.dsn == "" {
		logger.Warnf("DSN flag not set. Using libpq environment variables and default values.")
	}

	return pgx.ParseConfig(c.dsn)
}
