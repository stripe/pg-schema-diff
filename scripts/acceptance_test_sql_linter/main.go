package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

var (
	dirPath string
	fixMode bool
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "replace-spaces",
		Short: "Replace tabs with spaces in SQL strings within files",
		Long:  `This tool replaces tab characters with four spaces in SQL strings across files in a specified directory.`,
		Run: func(cmd *cobra.Command, args []string) {
			replaceSpaces()
		},
	}

	rootCmd.Flags().StringVarP(&dirPath, "dir", "d", "./internal/migration_acceptance_tests", "Directory path containing files to process")
	rootCmd.Flags().BoolVar(&fixMode, "fix", false, "Apply changes (without this flag, only shows what would change)")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func replaceSpaces() {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		fmt.Printf("Error reading directory: %v\n", err)
		return
	}

	if !fixMode {
		fmt.Println("Running in dry-run mode. Use --fix to apply changes.")
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filePath := filepath.Join(dirPath, file.Name())
		needsChanges, err := checkFileNeedsChanges(filePath)

		if err != nil {
			fmt.Printf("Error checking file %s: %v\n", filePath, err)
			continue
		}

		if needsChanges {
			if fixMode {
				if err := processFile(filePath); err != nil {
					fmt.Printf("Error processing file %s: %v\n", filePath, err)
				} else {
					fmt.Printf("Updated file: %s\n", filePath)
				}
			} else {
				fmt.Printf("Would update file: %s\n", filePath)
			}
		}
	}
}

// Check if the file would need changes without modifying it
func checkFileNeedsChanges(filePath string) (bool, error) {
	srcFile, err := os.Open(filePath)
	if err != nil {
		return false, fmt.Errorf("failed to open source file: %w", err)
	}
	defer srcFile.Close()

	scanner := bufio.NewScanner(srcFile)
	inSQLString := false
	needsChanges := false

	for scanner.Scan() {
		line := scanner.Text()

		if inSQLString && !strings.Contains(line, "`") && strings.Contains(line, "\t") {
			// Found a line that needs changes
			needsChanges = true
			break
		}

		// Toggle SQL string flag if line contains backtick
		if strings.Contains(line, "`") {
			inSQLString = !inSQLString
		}
	}

	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("error reading source file: %w", err)
	}

	return needsChanges, nil
}

func processFile(filePath string) error {
	// Create a temporary file
	tempFile, err := os.CreateTemp(filepath.Dir(filePath), "temp_*")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tempFilePath := tempFile.Name()
	defer os.Remove(tempFilePath) // Clean up in case of failure

	// Open source file for reading
	srcFile, err := os.Open(filePath)
	if err != nil {
		tempFile.Close()
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer srcFile.Close()

	// Process the file
	inSQLString := false
	scanner := bufio.NewScanner(srcFile)
	writer := bufio.NewWriter(tempFile)

	for scanner.Scan() {
		line := scanner.Text()

		if inSQLString && !strings.Contains(line, "`") {
			// Replace tabs with spaces in SQL strings
			newLine := strings.ReplaceAll(line, "\t", "    ")
			_, err = writer.WriteString(newLine + "\n")
		} else {
			_, err = writer.WriteString(line + "\n")
		}

		if err != nil {
			tempFile.Close()
			return fmt.Errorf("failed to write to temp file: %w", err)
		}

		// Toggle SQL string flag if line contains backtick
		if strings.Contains(line, "`") {
			inSQLString = !inSQLString
		}
	}

	if err := scanner.Err(); err != nil {
		tempFile.Close()
		return fmt.Errorf("error reading source file: %w", err)
	}

	// Ensure all buffered data is written
	if err := writer.Flush(); err != nil {
		tempFile.Close()
		return fmt.Errorf("failed to flush buffer: %w", err)
	}
	tempFile.Close()

	// Replace the original file with the temporary file
	return os.Rename(tempFilePath, filePath)
}
