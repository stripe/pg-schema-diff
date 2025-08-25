package pgdump

import (
	"errors"
	"fmt"
	"os/exec"
	"regexp"

	"github.com/hashicorp/go-version"
	"github.com/stripe/pg-schema-diff/internal/pgengine"
)

const (
	// FixedRestrictKey is a constant restricted key that can be used for tests and other use cases where
	// a constant restrict key is needed.
	FixedRestrictKey = "pgschemadiffrestrict"
)

var (
	// versionRe matches the version returned by pg_dump.
	versionRe = regexp.MustCompile(`pg_dump \(PostgreSQL\) (\d+(?:\.\d+)?)`)

	version15 = version.Must(version.NewSemver("15.0"))
)

// Parameter represents a parameter to be pg_dump. Don't use a type alias for a string slice
// because all parameters for pgdump should be explicitly added here
type Parameter struct {
	values []string
	// minimumVersion is the minimum required version pg_dump must return for the parameter to be added. If
	// pg_dump is an older version, it will not be added. If nil, there is no restriction.
	minimumVersion *version.Version
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

// WithRestrictKey is used by PSQL to prevent injection of "meta" commands. If not explicitly provided,
// a random one will be generated for each pg_dump run. This most likely needs to be fixed for any
// usages of pg_dump in tests.
func WithRestrictKey(restrictKey string) Parameter {
	return Parameter{
		values: []string{"--restrict-key", restrictKey},
		// Added in 17.6. https://www.postgresql.org/docs/release/17.6/.
		minimumVersion: version15,
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
	version, err := getVersion(pgDumpBinaryPath)
	if err != nil {
		return "", fmt.Errorf("getVersion: %w", err)
	}

	params := []string{
		db.GetDSN(),
	}
	for _, param := range additionalParams {
		if param.minimumVersion != nil && param.minimumVersion.GreaterThan(version) {
			// Exclude the parameter if the minimum version is not satisfied.
			continue
		}
		params = append(params, param.values...)
	}
	return runPgDumpCmd(pgDumpBinaryPath, params...)
}

// ParseVersion parses a version string from pg_dump output and returns a Version object.
// This function is exported to make it testable.
func ParseVersion(versionString string) (*version.Version, error) {
	matches := versionRe.FindStringSubmatch(versionString)
	if len(matches) < 2 {
		return nil, fmt.Errorf("could not extract version from string: %s", versionString)
	}

	// Parse the extracted version string
	v, err := version.NewVersion(matches[1])
	if err != nil {
		return nil, fmt.Errorf("could not parse version %s: %w", matches[1], err)
	}
	return v, nil
}

func getVersion(pgDumpBinaryPath string) (*version.Version, error) {
	versionString, err := runPgDumpCmd(pgDumpBinaryPath, "--version")
	if err != nil {
		return nil, err
	}

	return ParseVersion(versionString)
}

func runPgDumpCmd(pgDumpBinaryPath string, params ...string) (string, error) {
	output, err := exec.Command(pgDumpBinaryPath, params...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("running pg dump \noutput=%s\n: %w", output, err)
	}

	return string(output), nil
}
