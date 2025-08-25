package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

func TestMaterializedViewSQLGenerator_Add_WithTablespace(t *testing.T) {
	generator := newMaterializedViewSQLVertexGenerator()

	mv := schema.MaterializedView{
		SchemaQualifiedName: schema.SchemaQualifiedName{
			SchemaName:  "public",
			EscapedName: "\"test_view\"",
		},
		ViewDefinition: "SELECT id, name FROM users",
		Tablespace:     "custom_tablespace",
		Options:        map[string]string{},
		TableDependencies: []schema.TableDependency{
			{
				SchemaQualifiedName: schema.SchemaQualifiedName{
					SchemaName:  "public",
					EscapedName: "\"users\"",
				},
			},
		},
	}

	result, err := generator.Add(mv)
	assert.NoError(t, err)
	assert.Len(t, result.vertices, 1)

	expectedDDL := `CREATE MATERIALIZED VIEW "public"."test_view" TABLESPACE "custom_tablespace" AS
SELECT id, name FROM users`
	assert.Equal(t, expectedDDL, result.vertices[0].statements[0].DDL)
}

func TestMaterializedViewSQLGenerator_Add_WithoutTablespace(t *testing.T) {
	generator := newMaterializedViewSQLVertexGenerator()

	mv := schema.MaterializedView{
		SchemaQualifiedName: schema.SchemaQualifiedName{
			SchemaName:  "public",
			EscapedName: "\"test_view\"",
		},
		ViewDefinition: "SELECT id, name FROM users",
		Tablespace:     "", // No tablespace
		Options:        map[string]string{},
		TableDependencies: []schema.TableDependency{
			{
				SchemaQualifiedName: schema.SchemaQualifiedName{
					SchemaName:  "public",
					EscapedName: "\"users\"",
				},
			},
		},
	}

	result, err := generator.Add(mv)
	assert.NoError(t, err)
	assert.Len(t, result.vertices, 1)

	expectedDDL := `CREATE MATERIALIZED VIEW "public"."test_view" AS
SELECT id, name FROM users`
	assert.Equal(t, expectedDDL, result.vertices[0].statements[0].DDL)
}
