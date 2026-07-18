package diff

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type functionSQLVertexGenerator struct{}

func (f *functionSQLVertexGenerator) Add(function schema.Function) (partialSQLGraph, error) {
	return buildPartialSQLGraph(
		buildFunctionVertexId(function.SchemaQualifiedName, diffTypeAddAlter),
		sqlPrioritySooner,
		f.addStatements(function),
		f.addDependencies(function),
	), nil
}

func (f *functionSQLVertexGenerator) addStatements(function schema.Function) []Statement {
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
	return []Statement{{
		DDL:     function.FunctionDef,
		Hazards: hazards,
	}}
}

func (f *functionSQLVertexGenerator) Delete(function schema.Function) (partialSQLGraph, error) {
	return buildPartialSQLGraph(
		buildFunctionVertexId(function.SchemaQualifiedName, diffTypeDelete),
		sqlPriorityLater,
		f.deleteStatements(function),
		f.deleteDependencies(function),
	), nil
}

func (f *functionSQLVertexGenerator) deleteStatements(function schema.Function) []Statement {
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
		DDL:     fmt.Sprintf("DROP FUNCTION %s", function.GetFQEscapedName()),
		Hazards: hazards,
	}}
}

func (f *functionSQLVertexGenerator) Alter(diff functionDiff) (partialSQLGraph, error) {
	return buildPartialSQLGraph(
		buildFunctionVertexId(diff.new.SchemaQualifiedName, diffTypeAddAlter),
		sqlPrioritySooner,
		f.alterStatements(diff),
		f.alterDependencies(diff.new, diff.old),
	), nil
}

func (f *functionSQLVertexGenerator) alterStatements(diff functionDiff) []Statement {
	// We are assuming the function has been normalized, i.e., we don't have to worry DependsOnFunctions ordering
	// causing a false positive diff detected.
	if cmp.Equal(diff.old, diff.new) {
		return nil
	}
	return f.addStatements(diff.new)
}

func canFunctionDependenciesBeTracked(function schema.Function) bool {
	return function.Language == "sql"
}

func buildFunctionVertexId(name schema.SchemaQualifiedName, diffType diffType) sqlVertexId {
	return buildSchemaObjVertexId("function", name.GetFQEscapedName(), diffType)
}

func (f *functionSQLVertexGenerator) addDependencies(function schema.Function) []dependency {
	// Since functions can just be `CREATE OR REPLACE`, there will never be a case where a function is
	// added and dropped in the same migration. Thus, we don't need a dependency on the delete vertex of a function
	// because there won't be one if it is being added/altered
	var deps []dependency
	for _, depFunction := range function.DependsOnFunctions {
		deps = append(deps, mustRun(buildFunctionVertexId(function.SchemaQualifiedName, diffTypeAddAlter)).after(
			buildFunctionVertexId(depFunction, diffTypeAddAlter),
		))
	}
	return deps
}

func (f *functionSQLVertexGenerator) alterDependencies(newFunction, oldFunction schema.Function) []dependency {
	deps := f.addDependencies(newFunction)
	// If the old version of the function calls other functions that are being deleted, those deletions
	// must come after the function is altered, so it is no longer dependent on those dropped functions.
	for _, depFunction := range oldFunction.DependsOnFunctions {
		deps = append(deps, mustRun(buildFunctionVertexId(newFunction.SchemaQualifiedName, diffTypeAddAlter)).before(
			buildFunctionVertexId(depFunction, diffTypeDelete),
		))
	}
	return deps
}

func (f *functionSQLVertexGenerator) deleteDependencies(function schema.Function) []dependency {
	var deps []dependency
	for _, depFunction := range function.DependsOnFunctions {
		deps = append(deps, mustRun(buildFunctionVertexId(function.SchemaQualifiedName, diffTypeDelete)).before(
			buildFunctionVertexId(depFunction, diffTypeDelete),
		))
	}
	return deps
}
