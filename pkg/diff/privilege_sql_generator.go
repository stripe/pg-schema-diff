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

func newPrivilegeSQLVertexGenerator(tableName schema.SchemaQualifiedName) *privilegeSQLVertexGenerator {
	return &privilegeSQLVertexGenerator{
		tableName: tableName,
	}
}

func (psg *privilegeSQLVertexGenerator) Add(p schema.TablePrivilege) (partialSQLGraph, error) {
	return buildPartialSQLGraph(
		psg.vertexID(p, diffTypeAddAlter),
		sqlPrioritySooner,
		psg.addStatements(p),
		psg.addAlterDependencies(p),
	), nil
}

func (psg *privilegeSQLVertexGenerator) addStatements(p schema.TablePrivilege) []Statement {
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
		Hazards:        []MigrationHazard{migrationHazardPrivilegeGranted},
		SkipValidation: true,
	}}
}

func (psg *privilegeSQLVertexGenerator) Delete(p schema.TablePrivilege) (partialSQLGraph, error) {
	return buildPartialSQLGraph(
		psg.vertexID(p, diffTypeDelete),
		sqlPriorityLater,
		psg.deleteStatements(p),
		nil,
	), nil
}

func (psg *privilegeSQLVertexGenerator) deleteStatements(p schema.TablePrivilege) []Statement {
	grantee := p.Grantee
	if grantee == "" {
		grantee = "PUBLIC"
	} else {
		grantee = schema.EscapeIdentifier(grantee)
	}

	ddl := fmt.Sprintf("REVOKE %s ON %s FROM %s", p.Privilege, psg.tableName.GetFQEscapedName(), grantee)

	return []Statement{{
		DDL:            ddl,
		Hazards:        []MigrationHazard{migrationHazardPrivilegeRevoked},
		SkipValidation: true,
	}}
}

func (psg *privilegeSQLVertexGenerator) Alter(diff privilegeDiff) (partialSQLGraph, error) {
	// Privileges don't support ALTER. IsGrantable changes are recreated by buildTableDiff;
	// otherwise, Alter emits a no-op vertex to preserve graph dependencies.
	return buildPartialSQLGraph(
		psg.vertexID(diff.new, diffTypeAddAlter),
		sqlPrioritySooner,
		nil,
		psg.addAlterDependencies(diff.new),
	), nil
}

func (psg *privilegeSQLVertexGenerator) vertexID(p schema.TablePrivilege, diffType diffType) sqlVertexId {
	return buildSchemaObjVertexId("privilege", fmt.Sprintf("%s.%s",
		psg.tableName.GetFQEscapedName(), p.GetName()), diffType)
}

func (psg *privilegeSQLVertexGenerator) addAlterDependencies(newPriv schema.TablePrivilege) []dependency {
	// Ensure delete runs before add/alter (for recreate scenarios)
	return []dependency{
		mustRun(psg.vertexID(newPriv, diffTypeDelete)).before(
			psg.vertexID(newPriv, diffTypeAddAlter),
		),
	}
}
