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
	unqualifiedCastInUsing        = regexp.MustCompile(`(?i)\busing\s+[^;]*::`)
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
	assert.Contains(t, stmt.DDL, `::pg_catalog.float8)`)
}

func TestTypeTransformationQualifiesCastTargetType(t *testing.T) {
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

			assert.NotRegexp(t, unqualifiedCastInUsing, stmt.DDL)
			assert.Contains(t, stmt.DDL, tc.wantCast)
		})
	}
}

func TestTypeTransformationPreservesUserDefinedCastTarget(t *testing.T) {
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
