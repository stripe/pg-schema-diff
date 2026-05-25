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

	privilegeGenerator := &schemaPrivilegeSQLGenerator{schemaName: s.Name}
	for _, privilege := range s.Privileges {
		addPrivilegeStmts, err := privilegeGenerator.Add(privilege)
		if err != nil {
			return nil, fmt.Errorf("generating add schema privilege statements for privilege %s: %w", privilege.GetName(), err)
		}
		// Remove hazards from statements since the schema is brand new.
		stmts = append(stmts, stripMigrationHazards(addPrivilegeStmts...)...)
	}

	return stmts, nil
}

func (n *namedSchemaSQLGenerator) Delete(s schema.NamedSchema) ([]Statement, error) {
	return []Statement{{
		DDL:         fmt.Sprintf("DROP SCHEMA %s", schema.EscapeIdentifier(s.Name)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (n *namedSchemaSQLGenerator) Alter(diff namedSchemaDiff) ([]Statement, error) {
	privilegeGenerator := &schemaPrivilegeSQLGenerator{schemaName: diff.new.Name}
	privilegeStatements, err := diff.privilegesDiff.resolveToSQLGroupedByEffect(privilegeGenerator)
	if err != nil {
		return nil, fmt.Errorf("resolving schema privilege sql: %w", err)
	}

	var stmts []Statement
	stmts = append(stmts, privilegeStatements.Deletes...)
	stmts = append(stmts, privilegeStatements.Alters...)
	stmts = append(stmts, privilegeStatements.Adds...)
	return stmts, nil
}
