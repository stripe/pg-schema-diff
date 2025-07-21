package main

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "multiline-sql-strings-lint",
		Short: "Lints multiline sql strings, replacing tabs with spaces",
	}
	dirPath := rootCmd.Flags().String("dir", "./internal/migration_acceptance_tests", "Directory path containing files to process")
	fix := rootCmd.Flags().Bool("fix", false, "Apply changes (without this flag, only shows what would change)")
	rootCmd.RunE = func(cmd *cobra.Command, args []string) error {
		if !*fix {
			fmt.Println("Running in dry-run mode. Use --fix to apply changes.")
		}

		var filesRequiringChanges []string
		if err := filepath.Walk(*dirPath, func(path string, info fs.FileInfo, err error) error {
			if err != nil {
				return err
			}

			fi, err := os.Stat(path)
			if err != nil {
				return fmt.Errorf("os.Stat: %w", err)
			}

			if fi.IsDir() {
				return nil
			}

			cr, err := processFile(path, *fix)
			if err != nil {
				return fmt.Errorf("processFile: %w", err)
			}
			if cr {
				filesRequiringChanges = append(filesRequiringChanges, path)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("filepath.Walk: %w", err)
		}

		if len(filesRequiringChanges) == 0 {
			fmt.Println("No changes required!")
			return nil
		}

		verb := "require changes"
		if *fix {
			verb = "fixed"
		}
		fmt.Printf("The following files %s:\n", verb)
		for _, f := range filesRequiringChanges {
			fmt.Printf(" - %s\n", f)
		}
		if !*fix {
			//	Return a non-zero exit code, since the linter failed, and fixes could not be applied.
			os.Exit(1)
		}
		return nil
	}
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// processFile replaces tabs with spaces in multi-line comments (sql strings). It will return if changes need to be made
// and an error if any occurred. If fix is false, the files won't be updated.
func processFile(filePath string, fix bool) (bool, error) {
	// Create a temporary file
	tempFile, err := os.CreateTemp(filepath.Dir(filePath), "temp_*")
	if err != nil {
		return false, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tempFile.Name()) // Clean up in case of failure
	defer tempFile.Close()

	// Open source file for reading
	srcFile, err := os.Open(filePath)
	if err != nil {
		return false, fmt.Errorf("failed to open source file: %w", err)
	}
	defer srcFile.Close()

	scanner := bufio.NewScanner(srcFile)
	writer := bufio.NewWriter(tempFile)

	inMultilineString := false
	changesRequired := false
	for scanner.Scan() {
		line := scanner.Text()
		newLine := line
		if inMultilineString && !strings.Contains(line, "`") && strings.Contains(line, "\t") {
			changesRequired = true
			newLine = strings.ReplaceAll(line, "\t", "    ")
		}
		if _, err = writer.WriteString(newLine + "\n"); err != nil {
			return false, fmt.Errorf("writing new line: %w", err)
		}

		if strings.Contains(line, "`") {
			inMultilineString = !inMultilineString
		}
	}

	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("reading source file: %w", err)
	}

	if err := writer.Flush(); err != nil {
		return false, fmt.Errorf("lush buffer: %w", err)
	}

	return changesRequired, os.Rename(tempFile.Name(), filePath)
}
