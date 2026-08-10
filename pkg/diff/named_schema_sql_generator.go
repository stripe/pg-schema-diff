package diff

import (
	"fmt"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

// namedSchemaSQLGenerator generates SQL statements for named schemas. It's much easier to make this a SQLGenerator
// rather than SQLVertexGenerator and setup the dependency for each schema entity that may depend on the named schema.
type namedSchemaSQLGenerator struct{}

func (n *namedSchemaSQLGenerator) Add(s schema.NamedSchema) ([]Statement, error) {
	stmts := []Statement{{
		DDL:         fmt.Sprintf("CREATE SCHEMA %s", schema.EscapeIdentifier(s.Name)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}
	stmts = append(stmts, commentDDLForAdd(commentTargetSchema(s.Name), s.Description)...)
	return stmts, nil
}

func (n *namedSchemaSQLGenerator) Delete(s schema.NamedSchema) ([]Statement, error) {
	return []Statement{{
		DDL:         fmt.Sprintf("DROP SCHEMA %s", schema.EscapeIdentifier(s.Name)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (n *namedSchemaSQLGenerator) Alter(d namedSchemaDiff) ([]Statement, error) {
	return commentDDLForAlter(commentTargetSchema(d.new.Name), d.old.Description, d.new.Description), nil
}
