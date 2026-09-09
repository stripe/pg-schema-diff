package diff

import (
	"fmt"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

// defaultPrivilegeSQLGenerator generates `ALTER DEFAULT PRIVILEGES ... IN SCHEMA ... GRANT/REVOKE`
// statements. Like the other privilege generators it is a plain sqlGenerator: the statements are
// emitted after every schema is created and before any schema is dropped, and they reference roles
// rather than schema objects, so there is nothing to order them against inside the SQL graph.
type defaultPrivilegeSQLGenerator struct{}

func (d *defaultPrivilegeSQLGenerator) Add(p schema.DefaultPrivilege) ([]Statement, error) {
	ddl := fmt.Sprintf("%s GRANT %s ON %s TO %s",
		alterDefaultPrivilegesPrefix(p), p.Privilege, p.ObjectType, escapedGrantee(p.Grantee))
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

func (d *defaultPrivilegeSQLGenerator) Delete(p schema.DefaultPrivilege) ([]Statement, error) {
	return []Statement{{
		DDL: fmt.Sprintf("%s REVOKE %s ON %s FROM %s",
			alterDefaultPrivilegesPrefix(p), p.Privilege, p.ObjectType, escapedGrantee(p.Grantee)),
		Timeout:        statementTimeoutDefault,
		LockTimeout:    lockTimeoutDefault,
		Hazards:        []MigrationHazard{migrationHazardPrivilegeRevoked},
		SkipValidation: true,
	}}, nil
}

func (d *defaultPrivilegeSQLGenerator) Alter(_ defaultPrivilegeDiff) ([]Statement, error) {
	// Default privileges don't support ALTER — if IsGrantable changes, the privilege is
	// re-created (handled via requiresRecreation in buildSchemaDiff).
	return nil, nil
}

func alterDefaultPrivilegesPrefix(p schema.DefaultPrivilege) string {
	return fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s",
		schema.EscapeIdentifier(p.TargetRole), schema.EscapeIdentifier(p.SchemaName))
}

func escapedGrantee(grantee string) string {
	if grantee == "" {
		return "PUBLIC"
	}
	return schema.EscapeIdentifier(grantee)
}
