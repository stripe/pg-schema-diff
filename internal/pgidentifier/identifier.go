package pgidentifier

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

var (
	// SimpleIdentifierRegex matches identifiers in Postgres that require no quotes
	SimpleIdentifierRegex = regexp.MustCompile("^[a-z_][a-z0-9_$]*$")
)

func IsSimpleIdentifier(val string) bool {
	return SimpleIdentifierRegex.MatchString(val)
}

const encodePostgresIdentifier = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789$_"

var postgresIdentifierEncoding = base64.NewEncoding(encodePostgresIdentifier).WithPadding(base64.NoPadding)

// RandomUUID builds a RandomUUID to be used in Postgres identifiers. This RandomUUID cannot be used directly as an identifier
// and must be prefixed with a letter
func RandomUUID() (string, error) {
	uuid, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("generating RandomUUID: %w", err)
	}

	// Encode in base64 to make the RandomUUID smaller
	binary, err := uuid.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("marshaling RandomUUID: %w", err)
	}

	var sb strings.Builder
	encoder := base64.NewEncoder(postgresIdentifierEncoding, &sb)
	if _, err := encoder.Write(binary); err != nil {
		return "", fmt.Errorf("encoding RandomUUID: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return "", fmt.Errorf("closing encoder: %w", err)
	}

	return sb.String(), nil
}
