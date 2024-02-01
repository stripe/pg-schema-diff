package schema

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
