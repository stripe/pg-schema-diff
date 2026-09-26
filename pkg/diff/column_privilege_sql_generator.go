package diff

import (
	"fmt"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

// columnPrivilegeSQLVertexGenerator generates GRANT/REVOKE statements for privileges granted on individual
// columns of a single table.
type columnPrivilegeSQLVertexGenerator struct {
	tableName schema.SchemaQualifiedName
}

func newColumnPrivilegeSQLVertexGenerator(tableName schema.SchemaQualifiedName) sqlVertexGenerator[schema.ColumnPrivilege, columnPrivilegeDiff] {
	return legacyToNewSqlVertexGenerator[schema.ColumnPrivilege, columnPrivilegeDiff](&columnPrivilegeSQLVertexGenerator{
		tableName: tableName,
	})
}

func (cpg *columnPrivilegeSQLVertexGenerator) Add(p schema.ColumnPrivilege) ([]Statement, error) {
	ddl := fmt.Sprintf("GRANT %s (%s) ON %s TO %s",
		p.Privilege,
		schema.EscapeIdentifier(p.ColumnName),
		cpg.tableName.GetFQEscapedName(),
		privilegeGranteeSQL(p.Grantee),
	)
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

func (cpg *columnPrivilegeSQLVertexGenerator) Delete(p schema.ColumnPrivilege) ([]Statement, error) {
	ddl := fmt.Sprintf("REVOKE %s (%s) ON %s FROM %s",
		p.Privilege,
		schema.EscapeIdentifier(p.ColumnName),
		cpg.tableName.GetFQEscapedName(),
		privilegeGranteeSQL(p.Grantee),
	)

	return []Statement{{
		DDL:            ddl,
		Timeout:        statementTimeoutDefault,
		LockTimeout:    lockTimeoutDefault,
		Hazards:        []MigrationHazard{migrationHazardPrivilegeRevoked},
		SkipValidation: true,
	}}, nil
}

func (cpg *columnPrivilegeSQLVertexGenerator) Alter(diff columnPrivilegeDiff) ([]Statement, error) {
	// Column privileges don't support ALTER - if IsGrantable changes, the privilege is recreated
	// (handled via requiresRecreation in buildTableDiff).
	return nil, nil
}

func (cpg *columnPrivilegeSQLVertexGenerator) GetSQLVertexId(p schema.ColumnPrivilege, diffType diffType) sqlVertexId {
	return buildSchemaObjVertexId("column_privilege",
		fmt.Sprintf("%s.%s", cpg.tableName.GetFQEscapedName(), p.GetName()), diffType)
}

func (cpg *columnPrivilegeSQLVertexGenerator) GetAddAlterDependencies(newPriv, _ schema.ColumnPrivilege) ([]dependency, error) {
	return []dependency{
		// Ensure delete runs before add/alter (for recreate scenarios)
		mustRun(cpg.GetSQLVertexId(newPriv, diffTypeDelete)).before(cpg.GetSQLVertexId(newPriv, diffTypeAddAlter)),
		// The column must exist before a privilege can be granted on it
		mustRun(buildColumnVertexId(newPriv.ColumnName, diffTypeAddAlter)).before(cpg.GetSQLVertexId(newPriv, diffTypeAddAlter)),
	}, nil
}

func (cpg *columnPrivilegeSQLVertexGenerator) GetDeleteDependencies(_ schema.ColumnPrivilege) ([]dependency, error) {
	return nil, nil
}
