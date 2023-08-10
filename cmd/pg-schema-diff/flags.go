package main

import (
	"github.com/jackc/pgx/v4"
	"github.com/spf13/cobra"
	"github.com/stripe/pg-schema-diff/pkg/log"
)

type connFlags struct {
	dsn *string
}

func createConnFlags(cmd *cobra.Command) connFlags {
	dsn := cmd.Flags().String("dsn", "", "Connection string for the database (DB password can be specified through PGPASSWORD environment variable)")
	// Don't mark dsn as a required flag.
	// Allow users to use the "PGHOST" etc environment variables like `psql`.
	return connFlags{
		dsn: dsn,
	}
}

func (c connFlags) parseConnConfig(logger log.Logger) (*pgx.ConnConfig, error) {
	if c.dsn == nil || *c.dsn == "" {
		logger.Warnf("DSN flag not set. Using libpq environment variables and default values.")
	}

	return pgx.ParseConfig(*c.dsn)
}

func mustMarkFlagAsRequired(cmd *cobra.Command, flagName string) {
	if err := cmd.MarkFlagRequired(flagName); err != nil {
		panic(err)
	}
}
