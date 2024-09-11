package main

import (
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "pg-schema-diff",
	Short: "Diff two Postgres schemas and generate the SQL to get from one to the other",
}

func init() {
	rootCmd.AddCommand(buildPlanCmd())
	rootCmd.AddCommand(buildApplyCmd())
	rootCmd.AddCommand(buildVersionCmd())
}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
