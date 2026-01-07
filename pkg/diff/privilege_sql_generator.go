package diff

import (
	"fmt"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

var (
	migrationHazardPrivilegeGranted = MigrationHazard{
		Type:    MigrationHazardTypeAuthzUpdate,
		Message: "Granting privileges could allow unauthorized access to data.",
	}
	migrationHazardPrivilegeRevoked = MigrationHazard{
		Type:    MigrationHazardTypeAuthzUpdate,
		Message: "Revoking privileges could cause queries to fail if not correctly configured.",
	}
)

type privilegeSQLVertexGenerator struct {
	tableName schema.SchemaQualifiedName
}

func newPrivilegeSQLVertexGenerator(tableName schema.SchemaQualifiedName) sqlVertexGenerator[schema.TablePrivilege, privilegeDiff] {
	return legacyToNewSqlVertexGenerator[schema.TablePrivilege, privilegeDiff](&privilegeSQLVertexGenerator{
		tableName: tableName,
	})
}

func (psg *privilegeSQLVertexGenerator) Add(p schema.TablePrivilege) ([]Statement, error) {
	grantee := p.Grantee
	if grantee == "" {
		grantee = "PUBLIC"
	} else {
		grantee = schema.EscapeIdentifier(grantee)
	}

	ddl := fmt.Sprintf("GRANT %s ON %s TO %s", p.Privilege, psg.tableName.GetFQEscapedName(), grantee)
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

func (psg *privilegeSQLVertexGenerator) Delete(p schema.TablePrivilege) ([]Statement, error) {
	grantee := p.Grantee
	if grantee == "" {
		grantee = "PUBLIC"
	} else {
		grantee = schema.EscapeIdentifier(grantee)
	}

	ddl := fmt.Sprintf("REVOKE %s ON %s FROM %s", p.Privilege, psg.tableName.GetFQEscapedName(), grantee)

	return []Statement{{
		DDL:            ddl,
		Timeout:        statementTimeoutDefault,
		LockTimeout:    lockTimeoutDefault,
		Hazards:        []MigrationHazard{migrationHazardPrivilegeRevoked},
		SkipValidation: true,
	}}, nil
}

func (psg *privilegeSQLVertexGenerator) Alter(diff privilegeDiff) ([]Statement, error) {
	// Privileges don't support ALTER - if IsGrantable changes, we need to recreate
	// (handled via requiresRecreation in buildTableDiff)
	// This should not normally be called since only IsGrantable can change and that
	// triggers recreation.
	return nil, nil
}

func (psg *privilegeSQLVertexGenerator) GetSQLVertexId(p schema.TablePrivilege, diffType diffType) sqlVertexId {
	return buildSchemaObjVertexId("privilege", fmt.Sprintf("%s.%s", psg.tableName.GetFQEscapedName(), p.GetName()), diffType)
}

func (psg *privilegeSQLVertexGenerator) GetAddAlterDependencies(newPriv, _ schema.TablePrivilege) ([]dependency, error) {
	// Ensure delete runs before add/alter (for recreate scenarios)
	return []dependency{
		mustRun(psg.GetSQLVertexId(newPriv, diffTypeDelete)).before(psg.GetSQLVertexId(newPriv, diffTypeAddAlter)),
	}, nil
}

func (psg *privilegeSQLVertexGenerator) GetDeleteDependencies(_ schema.TablePrivilege) ([]dependency, error) {
	return nil, nil
}
