package schema

import "strings"

// QualifyTypeForCast returns a type name suitable for CAST(... AS type) in generated
// migration SQL. Unqualified built-in types from pg_catalog.format_type are qualified
// with pg_catalog to prevent search_path shadowing (CVE-2018-1058).
func QualifyTypeForCast(typeName string) string {
	typeName = strings.TrimSpace(typeName)
	if isSchemaQualifiedTypeName(typeName) {
		return typeName
	}

	base, typmod := splitTypeNameAndTypmod(typeName)
	if strings.Contains(base, " ") {
		return `"pg_catalog".` + EscapeIdentifier(base) + typmod
	}
	return "pg_catalog." + base + typmod
}

func isSchemaQualifiedTypeName(typeName string) bool {
	if strings.HasPrefix(typeName, `"`) {
		return true
	}

	parenIdx := strings.Index(typeName, "(")
	dotIdx := strings.Index(typeName, ".")
	if dotIdx >= 0 && (parenIdx < 0 || dotIdx < parenIdx) {
		return true
	}

	return false
}

func splitTypeNameAndTypmod(typeName string) (base, typmod string) {
	idx := strings.Index(typeName, "(")
	if idx < 0 {
		return typeName, ""
	}
	return typeName[:idx], typeName[idx:]
}
