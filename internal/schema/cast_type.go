package schema

import "strings"

// formatTypeBaseToPgCatalogTypname maps pg_catalog.format_type base names to pg_catalog
// typnames suitable for CAST(... AS type). format_type returns human-readable names
// (e.g. "integer") but CAST requires typnames (e.g. int4) when schema-qualified.
var formatTypeBaseToPgCatalogTypname = map[string]string{
	"bigint":                      "int8",
	"bit":                         "bit",
	"bit varying":                 "varbit",
	"boolean":                     "bool",
	"bytea":                       "bytea",
	"character":                   "bpchar",
	"character varying":           "varchar",
	"date":                        "date",
	"double precision":            "float8",
	"integer":                     "int4",
	"interval":                    "interval",
	"json":                        "json",
	"jsonb":                       "jsonb",
	"jsonpath":                    "jsonpath",
	"money":                       "money",
	"name":                        "name",
	"numeric":                     "numeric",
	"real":                        "float4",
	"smallint":                    "int2",
	"text":                        "text",
	"time without time zone":      "time",
	"time with time zone":         "timetz",
	"timestamp without time zone": "timestamp",
	"timestamp with time zone":    "timestamptz",
	"tsquery":                     "tsquery",
	"tsvector":                    "tsvector",
	"uuid":                        "uuid",
	"xml":                         "xml",
	"cidr":                        "cidr",
	"inet":                        "inet",
	"macaddr":                     "macaddr",
	"macaddr8":                    "macaddr8",
}

// QualifyTypeForCast returns a type name suitable for CAST(... AS type) in generated
// migration SQL. Unqualified built-in types from pg_catalog.format_type are qualified
// with pg_catalog to prevent search_path shadowing of cast targets.
func QualifyTypeForCast(typeName string) string {
	typeName = strings.TrimSpace(typeName)
	if isSchemaQualifiedTypeName(typeName) {
		return typeName
	}

	base, typmod := splitTypeNameAndTypmod(typeName)
	base, arraySuffix := splitArraySuffix(base)
	if typname, ok := formatTypeBaseToPgCatalogTypname[strings.ToLower(base)]; ok {
		return "pg_catalog." + typname + typmod + arraySuffix
	}

	// User-defined types from introspection are schema-qualified (see isSchemaQualifiedTypeName).
	// This fallback covers remaining pg_catalog builtins whose format_type name is not yet mapped.
	return `"pg_catalog".` + EscapeIdentifier(base) + typmod + arraySuffix
}

func splitArraySuffix(typeName string) (base, arraySuffix string) {
	for strings.HasSuffix(typeName, "[]") {
		arraySuffix = "[]" + arraySuffix
		typeName = strings.TrimSuffix(typeName, "[]")
	}
	return typeName, arraySuffix
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
