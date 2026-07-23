package diff

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

var (
	unqualifiedToTimestampInUsing = regexp.MustCompile(`(?i)\busing\s+to_timestamp\s*\(`)
	qualifiedToTimestampInUsing   = regexp.MustCompile(`(?i)\busing\s+pg_catalog\.to_timestamp\s*\(`)
)

func TestTypeTransformationQualifiesToTimestampBuiltin(t *testing.T) {
	csg := &columnSQLVertexGenerator{
		tableName: schema.SchemaQualifiedName{
			SchemaName:  "app",
			EscapedName: `"events"`,
		},
	}

	stmt := csg.generateTypeTransformationStatement(
		schema.Column{Name: "ts"},
		"bigint",
		"timestamp without time zone",
		schema.SchemaQualifiedName{},
	)

	assert.NotRegexp(t, unqualifiedToTimestampInUsing, stmt.DDL)
	assert.Regexp(t, qualifiedToTimestampInUsing, stmt.DDL)
	assert.Contains(t, stmt.DDL, `::pg_catalog.float8`)
}
