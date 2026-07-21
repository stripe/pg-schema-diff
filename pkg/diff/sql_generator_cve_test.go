package diff

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

// Contract tests derived from SAT-28189 / CVE-2018-1058: emitted ALTER COLUMN ... USING
// clauses must not rely on search_path for built-in function or cast target resolution.

var (
	cveUnqualifiedToTimestamp = regexp.MustCompile(`(?i)\busing\s+to_timestamp\s*\(`)
	cveQualifiedToTimestamp   = regexp.MustCompile(`(?i)\busing\s+pg_catalog\.to_timestamp\s*\(`)
	cveUnqualifiedCast        = regexp.MustCompile(`(?i)\busing\s+[^;]*::`)
)

func TestCVE20181058_BigintToTimestampStatementQualifiesBuiltin(t *testing.T) {
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

	assert.NotRegexp(t, cveUnqualifiedToTimestamp, stmt.DDL)
	assert.Regexp(t, cveQualifiedToTimestamp, stmt.DDL)
	assert.Contains(t, stmt.DDL, `::pg_catalog.float8)`)
}

func TestCVE20181058_GenericCastStatementQualifiesTargetType(t *testing.T) {
	csg := &columnSQLVertexGenerator{
		tableName: schema.SchemaQualifiedName{
			SchemaName:  "app2",
			EscapedName: `"metrics"`,
		},
	}

	for _, tc := range []struct {
		name     string
		oldType  string
		newType  string
		wantCast string
	}{
		{
			name:     "varchar to integer",
			oldType:  "character varying(32)",
			newType:  "integer",
			wantCast: `CAST("val" AS pg_catalog.int4)`,
		},
		{
			name:     "varchar typmod widen",
			oldType:  "character varying(32)",
			newType:  "character varying(255)",
			wantCast: `CAST("val" AS pg_catalog.varchar(255))`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt := csg.generateTypeTransformationStatement(
				schema.Column{Name: "val"},
				tc.oldType,
				tc.newType,
				schema.SchemaQualifiedName{},
			)

			assert.NotRegexp(t, cveUnqualifiedCast, stmt.DDL)
			assert.Contains(t, stmt.DDL, tc.wantCast)
		})
	}
}

func TestCVE20181058_UserDefinedCastTargetRemainsSchemaQualified(t *testing.T) {
	csg := &columnSQLVertexGenerator{
		tableName: schema.SchemaQualifiedName{
			SchemaName:  "public",
			EscapedName: `"orders"`,
		},
	}

	stmt := csg.generateTypeTransformationStatement(
		schema.Column{Name: "status"},
		"text",
		`"public"."order_status"`,
		schema.SchemaQualifiedName{},
	)

	assert.Contains(t, stmt.DDL, `CAST("status" AS "public"."order_status")`)
	assert.NotContains(t, stmt.DDL, `"pg_catalog"."order_status"`)
}
