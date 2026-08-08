package diff

import (
	"fmt"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

type schemaPrivilegeSQLGenerator struct {
	schemaName string
}

func (spg *schemaPrivilegeSQLGenerator) Add(p schema.SchemaPrivilege) ([]Statement, error) {
	grantee := p.Grantee
	if grantee == "" {
		grantee = "PUBLIC"
	} else {
		grantee = schema.EscapeIdentifier(grantee)
	}

	ddl := fmt.Sprintf("GRANT %s ON SCHEMA %s TO %s", p.Privilege, schema.EscapeIdentifier(spg.schemaName), grantee)
	if p.IsGrantable {
		ddl += " WITH GRANT OPTION"
	}

	return []Statement{{
		DDL:            ddl,
		Timeout:        statementTimeoutDefault,
		LockTimeout:    lockTimeoutDefault,
		Hazards:        []MigrationHazard{migrationHazardPrivilegeGranted},
		SkipValidation: true,
	}}, nil
}

func (spg *schemaPrivilegeSQLGenerator) Delete(p schema.SchemaPrivilege) ([]Statement, error) {
	grantee := p.Grantee
	if grantee == "" {
		grantee = "PUBLIC"
	} else {
		grantee = schema.EscapeIdentifier(grantee)
	}

	ddl := fmt.Sprintf("REVOKE %s ON SCHEMA %s FROM %s", p.Privilege, schema.EscapeIdentifier(spg.schemaName), grantee)

	return []Statement{{
		DDL:            ddl,
		Timeout:        statementTimeoutDefault,
		LockTimeout:    lockTimeoutDefault,
		Hazards:        []MigrationHazard{migrationHazardPrivilegeRevoked},
		SkipValidation: true,
	}}, nil
}

func (spg *schemaPrivilegeSQLGenerator) Alter(_ schemaPrivilegeDiff) ([]Statement, error) {
	// Privileges don't support ALTER - if IsGrantable changes, we need to recreate
	// (handled via requiresRecreation in buildSchemaDiff).
	return nil, nil
}
