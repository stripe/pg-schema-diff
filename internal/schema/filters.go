package schema

import (
	"fmt"
	"regexp"
	"strings"
)

// nameFilter is one of the most generic of filters. We can use it to filter objects by their schema name or name.
// In the future, it might be expanded to include a "type" field, e.g., to filter down to specific tables.
type nameFilter func(name SchemaQualifiedName) bool

func schemaNameFilter(schema string) nameFilter {
	return func(obj SchemaQualifiedName) bool {
		return obj.SchemaName == schema
	}
}

func notSchemaNameFilter(schema string) nameFilter {
	return func(obj SchemaQualifiedName) bool {
		return obj.SchemaName != schema
	}
}

func orNameFilter(filters ...nameFilter) nameFilter {
	return func(obj SchemaQualifiedName) bool {
		for _, filter := range filters {
			if filter(obj) {
				return true
			}
		}
		return false
	}
}

func andNameFilter(filters ...nameFilter) nameFilter {
	return func(obj SchemaQualifiedName) bool {
		if len(filters) == 0 {
			return false
		}

		for _, filter := range filters {
			if !filter(obj) {
				return false
			}
		}
		return true
	}
}

func filterSliceByName[T any](objs []T, getNameFn func(T) SchemaQualifiedName, filter nameFilter) []T {
	var filteredObjs []T
	for _, obj := range objs {
		if filter(getNameFn(obj)) {
			filteredObjs = append(filteredObjs, obj)
		}
	}
	return filteredObjs
}

// unescapeIdentifier converts an escaped identifier (as produced by EscapeIdentifier) back to its raw form.
// Identifiers that are not wrapped in double quotes are returned as-is.
func unescapeIdentifier(escaped string) string {
	if len(escaped) >= 2 && strings.HasPrefix(escaped, `"`) && strings.HasSuffix(escaped, `"`) {
		return strings.ReplaceAll(escaped[1:len(escaped)-1], `""`, `"`)
	}
	return escaped
}

// buildExcludeTablesFilter builds a nameFilter that excludes (returns false for) any table whose unescaped name or
// unescaped schema-qualified name (e.g., "public.foobar") fully matches any of the given regex patterns. Patterns
// are anchored, i.e., wrapped in ^(?:...)$, so "users" matches only a table named exactly "users". Returns nil if no
// patterns are provided.
func buildExcludeTablesFilter(patterns []string) (nameFilter, error) {
	if len(patterns) == 0 {
		return nil, nil
	}

	var regexes []*regexp.Regexp
	for _, pattern := range patterns {
		regex, err := regexp.Compile(fmt.Sprintf("^(?:%s)$", pattern))
		if err != nil {
			return nil, fmt.Errorf("compiling exclude table pattern %q: %w", pattern, err)
		}
		regexes = append(regexes, regex)
	}

	return func(table SchemaQualifiedName) bool {
		name := unescapeIdentifier(table.EscapedName)
		qualifiedName := fmt.Sprintf("%s.%s", table.SchemaName, name)
		for _, regex := range regexes {
			if regex.MatchString(name) || regex.MatchString(qualifiedName) {
				return false
			}
		}
		return true
	}, nil
}

// excludeTables removes tables for which keepTable returns false from the schema, along with partitions of excluded
// tables (transitively) and any objects owned by excluded tables (indexes, foreign key constraints, triggers). Check
// constraints, policies, and privileges are stored on the Table struct, so they are removed with their table.
//
// Foreign keys owned by kept tables that reference an excluded table are kept, consistent with how cross-schema
// foreign keys behave with WithExcludeSchemas (see the nameFilter docstring on schemaFetcher about dependency
// validation).
func excludeTables(s Schema, keepTable nameFilter) Schema {
	excludedTables := make(map[string]bool)
	// Iterate until a fixed point is reached to handle multi-level partitioning, where a partition's parent is
	// itself a partition of an excluded table.
	for {
		changed := false
		for _, table := range s.Tables {
			fqName := table.GetFQEscapedName()
			if excludedTables[fqName] {
				continue
			}
			parentIsExcluded := table.ParentTable != nil && excludedTables[table.ParentTable.GetFQEscapedName()]
			if parentIsExcluded || !keepTable(table.SchemaQualifiedName) {
				excludedTables[fqName] = true
				changed = true
			}
		}
		if !changed {
			break
		}
	}

	keepOwningRel := func(owningRel SchemaQualifiedName) bool {
		return !excludedTables[owningRel.GetFQEscapedName()]
	}
	s.Tables = filterSliceByName(s.Tables, func(t Table) SchemaQualifiedName { return t.SchemaQualifiedName }, keepOwningRel)
	s.Indexes = filterSliceByName(s.Indexes, func(idx Index) SchemaQualifiedName { return idx.OwningRelName }, keepOwningRel)
	s.ForeignKeyConstraints = filterSliceByName(s.ForeignKeyConstraints, func(fk ForeignKeyConstraint) SchemaQualifiedName { return fk.OwningTable }, keepOwningRel)
	s.Triggers = filterSliceByName(s.Triggers, func(t Trigger) SchemaQualifiedName { return t.OwningTable }, keepOwningRel)
	return s
}
