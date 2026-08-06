package diff

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type functionSQLVertexGenerator struct {
	// functionsInNewSchemaByName is a map of function name to functions in the new schema.
	// These functions are not necessarily new
	functionsInNewSchemaByName map[string]schema.Function
}

func newFunctionSqlVertexGenerator(functionsInNewSchemaByName map[string]schema.Function) sqlVertexGenerator[schema.Function, functionDiff] {
	return legacyToNewSqlVertexGenerator[schema.Function, functionDiff](&functionSQLVertexGenerator{
		functionsInNewSchemaByName: functionsInNewSchemaByName,
	})
}

func (f *functionSQLVertexGenerator) Add(function schema.Function) ([]Statement, error) {
	var hazards []MigrationHazard
	if !canFunctionDependenciesBeTracked(function) {
		hazards = append(hazards, MigrationHazard{
			Type: MigrationHazardTypeHasUntrackableDependencies,
			Message: "Dependencies, i.e. other functions used in the function body, of non-sql functions cannot be tracked. " +
				"As a result, we cannot guarantee that function dependencies are ordered properly relative to this " +
				"statement. For adds, this means you need to ensure that all functions this function depends on are " +
				"created/altered before this statement.",
		})
	}
	stmts := []Statement{{
		DDL:         function.FunctionDef,
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     hazards,
	}}
	privilegeStmts, err := exactRoutinePrivilegeStatements(
		newFunctionPrivilegeSQLVertexGenerator(function.SchemaQualifiedName),
		function.Privileges,
	)
	if err != nil {
		return nil, fmt.Errorf("generating function privilege statements: %w", err)
	}
	stmts = append(stmts, privilegeStmts...)
	return stmts, nil
}

func (f *functionSQLVertexGenerator) Delete(function schema.Function) ([]Statement, error) {
	var hazards []MigrationHazard
	if !canFunctionDependenciesBeTracked(function) {
		hazards = append(hazards, MigrationHazard{
			Type: MigrationHazardTypeHasUntrackableDependencies,
			Message: "Dependencies, i.e. other functions used in the function body, of non-sql functions cannot be " +
				"tracked. As a result, we cannot guarantee that function dependencies are ordered properly relative to " +
				"this statement. For drops, this means you need to ensure that all functions this function depends on " +
				"are dropped after this statement.",
		})
	}
	return []Statement{{
		DDL:         fmt.Sprintf("DROP FUNCTION %s", function.GetFQEscapedName()),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     hazards,
	}}, nil
}

func (f *functionSQLVertexGenerator) Alter(diff functionDiff) ([]Statement, error) {
	// We are assuming the function has been normalized, i.e., we don't have to worry DependsOnFunctions ordering
	// causing a false positive diff detected.
	oldWithoutPrivileges := diff.old
	oldWithoutPrivileges.Privileges = nil
	newWithoutPrivileges := diff.new
	newWithoutPrivileges.Privileges = nil

	var stmts []Statement
	if !cmp.Equal(oldWithoutPrivileges, newWithoutPrivileges) {
		addStmts, err := f.Add(diff.new)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, addStmts...)
	} else {
		privilegesPartialGraph, err := generatePartialGraph(
			newFunctionPrivilegeSQLVertexGenerator(diff.new.SchemaQualifiedName),
			diff.privilegesDiff,
		)
		if err != nil {
			return nil, fmt.Errorf("resolving function privilege sql: %w", err)
		}
		privilegeStmts, err := graphStatements(privilegesPartialGraph)
		if err != nil {
			return nil, fmt.Errorf("ordering function privilege sql: %w", err)
		}
		stmts = append(stmts, privilegeStmts...)
	}
	return stmts, nil
}

func canFunctionDependenciesBeTracked(function schema.Function) bool {
	return function.Language == "sql"
}

func (f *functionSQLVertexGenerator) GetSQLVertexId(function schema.Function, diffType diffType) sqlVertexId {
	return buildFunctionVertexId(function.SchemaQualifiedName, diffType)
}

func buildFunctionVertexId(name schema.SchemaQualifiedName, diffType diffType) sqlVertexId {
	return buildSchemaObjVertexId("function", name.GetFQEscapedName(), diffType)
}

func (f *functionSQLVertexGenerator) GetAddAlterDependencies(newFunction, oldFunction schema.Function) ([]dependency, error) {
	// Since functions can just be `CREATE OR REPLACE`, there will never be a case where a function is
	// added and dropped in the same migration. Thus, we don't need a dependency on the delete vertex of a function
	// because there won't be one if it is being added/altered
	var deps []dependency
	for _, depFunction := range newFunction.DependsOnFunctions {
		deps = append(deps, mustRun(f.GetSQLVertexId(newFunction, diffTypeAddAlter)).after(buildFunctionVertexId(depFunction, diffTypeAddAlter)))
	}

	if !cmp.Equal(oldFunction, schema.Function{}) {
		// If the function is being altered:
		// If the old version of the function calls other functions that are being deleted come, those deletions
		// must come after the function is altered, so it is no longer dependent on those dropped functions
		for _, depFunction := range oldFunction.DependsOnFunctions {
			deps = append(deps, mustRun(f.GetSQLVertexId(newFunction, diffTypeAddAlter)).before(buildFunctionVertexId(depFunction, diffTypeDelete)))
		}
	}

	return deps, nil
}

func (f *functionSQLVertexGenerator) GetDeleteDependencies(function schema.Function) ([]dependency, error) {
	var deps []dependency
	for _, depFunction := range function.DependsOnFunctions {
		deps = append(deps, mustRun(f.GetSQLVertexId(function, diffTypeDelete)).before(buildFunctionVertexId(depFunction, diffTypeDelete)))
	}
	return deps, nil
}
