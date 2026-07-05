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
	objType   string
	objName   schema.SchemaQualifiedName
	sqlTarget string
}

func newPrivilegeSQLVertexGenerator(tableName schema.SchemaQualifiedName) sqlVertexGenerator[schema.TablePrivilege, privilegeDiff] {
	return newPrivilegeSQLVertexGeneratorForObject("privilege", tableName, tableName.GetFQEscapedName())
}

func newPrivilegeSQLVertexGeneratorForObject(
	objType string,
	objName schema.SchemaQualifiedName,
	sqlTarget string,
) sqlVertexGenerator[schema.Privilege, privilegeDiff] {
	return legacyToNewSqlVertexGenerator[schema.Privilege, privilegeDiff](&privilegeSQLVertexGenerator{
		objType:   objType,
		objName:   objName,
		sqlTarget: sqlTarget,
	})
}

func newFunctionPrivilegeSQLVertexGenerator(functionName schema.SchemaQualifiedName) sqlVertexGenerator[schema.Privilege, privilegeDiff] {
	return newPrivilegeSQLVertexGeneratorForObject(
		"function_privilege",
		functionName,
		fmt.Sprintf("FUNCTION %s", functionName.GetFQEscapedName()),
	)
}

func newProcedurePrivilegeSQLVertexGenerator(procedureName schema.SchemaQualifiedName) sqlVertexGenerator[schema.Privilege, privilegeDiff] {
	return newPrivilegeSQLVertexGeneratorForObject(
		"procedure_privilege",
		procedureName,
		fmt.Sprintf("PROCEDURE %s", procedureName.GetFQEscapedName()),
	)
}

func exactRoutinePrivilegeStatements(
	generator sqlVertexGenerator[schema.Privilege, privilegeDiff],
	privileges []schema.Privilege,
) ([]Statement, error) {
	var stmts []Statement
	hasDefaultPublicExecute := false
	for _, p := range privileges {
		if p.Grantee == "" && p.Privilege == "EXECUTE" && !p.IsGrantable {
			hasDefaultPublicExecute = true
			continue
		}
		addPartialGraph, err := generator.Add(p)
		if err != nil {
			return nil, fmt.Errorf("generating grant statement for privilege %s: %w", p.GetName(), err)
		}
		stmts = append(stmts, stripMigrationHazards(addPartialGraph.statements()...)...)
	}
	if !hasDefaultPublicExecute {
		deletePartialGraph, err := generator.Delete(schema.Privilege{Privilege: "EXECUTE"})
		if err != nil {
			return nil, fmt.Errorf("generating revoke statement for default PUBLIC EXECUTE: %w", err)
		}
		stmts = append(stripMigrationHazards(deletePartialGraph.statements()...), stmts...)
	}
	return stmts, nil
}

func (psg *privilegeSQLVertexGenerator) Add(p schema.Privilege) ([]Statement, error) {
	grantee := p.Grantee
	if grantee == "" {
		grantee = "PUBLIC"
	} else {
		grantee = schema.EscapeIdentifier(grantee)
	}

	ddl := fmt.Sprintf("GRANT %s ON %s TO %s", p.Privilege, psg.sqlTarget, grantee)
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

func (psg *privilegeSQLVertexGenerator) Delete(p schema.Privilege) ([]Statement, error) {
	grantee := p.Grantee
	if grantee == "" {
		grantee = "PUBLIC"
	} else {
		grantee = schema.EscapeIdentifier(grantee)
	}

	ddl := fmt.Sprintf("REVOKE %s ON %s FROM %s", p.Privilege, psg.sqlTarget, grantee)

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

func (psg *privilegeSQLVertexGenerator) GetSQLVertexId(p schema.Privilege, diffType diffType) sqlVertexId {
	return buildSchemaObjVertexId(psg.objType, fmt.Sprintf("%s.%s", psg.objName.GetFQEscapedName(), p.GetName()), diffType)
}

func (psg *privilegeSQLVertexGenerator) GetAddAlterDependencies(newPriv, _ schema.Privilege) ([]dependency, error) {
	// Ensure delete runs before add/alter (for recreate scenarios)
	return []dependency{
		mustRun(psg.GetSQLVertexId(newPriv, diffTypeDelete)).before(psg.GetSQLVertexId(newPriv, diffTypeAddAlter)),
	}, nil
}

func (psg *privilegeSQLVertexGenerator) GetDeleteDependencies(_ schema.Privilege) ([]dependency, error) {
	return nil, nil
}
