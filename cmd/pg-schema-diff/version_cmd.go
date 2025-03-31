package main

import (
	"fmt"
	"runtime/debug"

	"github.com/spf13/cobra"
)

func buildVersionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Get the version of pg-schema-diff",
	}
	cmd.RunE = func(_ *cobra.Command, _ []string) error {
		buildInfo, ok := debug.ReadBuildInfo()
		if !ok {
			return fmt.Errorf("build information not available")
		}
		cmd.Printf("version=%s\n", buildInfo.Main.Version)

		return nil
	}
	return cmd
}
