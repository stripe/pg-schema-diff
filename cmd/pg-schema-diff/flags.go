package main

import (
	"os"

	"github.com/jackc/pgx/v4"
	"github.com/spf13/cobra"
)

type connFlags struct {
	dsn *string
}

func createConnFlags(cmd *cobra.Command) connFlags {
	schemaDir := cmd.Flags().String("dsn", "", "Connection string for the database (DB password can be specified through PGPASSWORD environment variable)")
	mustMarkFlagAsRequired(cmd, "dsn")

	return connFlags{
		dsn: schemaDir,
	}
}

func (c connFlags) parseConnConfig() (*pgx.ConnConfig, error) {
	config, err := pgx.ParseConfig(*c.dsn)
	if err != nil {
		return nil, err
	}

	if config.Password == "" {
		if pgPassword := os.Getenv("PGPASSWORD"); pgPassword != "" {
			config.Password = pgPassword
		}
	}

	return config, nil
}

func mustMarkFlagAsRequired(cmd *cobra.Command, flagName string) {
	if err := cmd.MarkFlagRequired(flagName); err != nil {
		panic(err)
	}
}
