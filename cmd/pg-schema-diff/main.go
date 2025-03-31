package main

import (
	"os"

	"github.com/spf13/cobra"
)

func buildRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "pg-schema-diff",
		Short: "Diff two Postgres schemas and generate the SQL to get from one to the other",
	}
	rootCmd.AddCommand(buildPlanCmd())
	rootCmd.AddCommand(buildApplyCmd())
	rootCmd.AddCommand(buildVersionCmd())
	return rootCmd
}

func main() {
	if err := buildRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
