package pgdump

import (
	"errors"
	"fmt"
	"os/exec"

	"github.com/stripe/pg-schema-diff/internal/pgengine"
)

// Parameter represents a parameter to be pg_dump. Don't use a type alias for a string slice
// because all parameters for pgdump should be explicitly added here
type Parameter struct {
	values []string `explicit:"always"`
}

func WithExcludeSchema(pattern string) Parameter {
	return Parameter{
		values: []string{"--exclude-schema", pattern},
	}
}

func WithSchemaOnly() Parameter {
	return Parameter{
		values: []string{"--schema-only"},
	}
}

// GetDump gets the pg_dump of the inputted database.
// It is only intended to be used for testing. You cannot securely pass passwords with this implementation, so it will
// only accept databases created for unit tests (spun up with the pgengine package)
// "pgdump" must be on the system's PATH
func GetDump(db *pgengine.DB, additionalParams ...Parameter) (string, error) {
	pgDumpBinaryPath, err := exec.LookPath("pg_dump")
	if err != nil {
		return "", errors.New("pg_dump executable not found in path")
	}
	return GetDumpUsingBinary(pgDumpBinaryPath, db, additionalParams...)
}

func GetDumpUsingBinary(pgDumpBinaryPath string, db *pgengine.DB, additionalParams ...Parameter) (string, error) {
	params := []string{
		db.GetDSN(),
	}
	for _, param := range additionalParams {
		params = append(params, param.values...)
	}

	output, err := exec.Command(pgDumpBinaryPath, params...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("running pg dump \noutput=%s\n: %w", output, err)
	}

	return string(output), nil
}
