package pgidentifier

import "regexp"

var (
	// SimpleIdentifierRegex matches identifiers in Postgres that require no quotes
	SimpleIdentifierRegex = regexp.MustCompile("^[a-z_][a-z0-9_$]*$")
)

func IsSimpleIdentifier(val string) bool {
	return SimpleIdentifierRegex.MatchString(val)
}
