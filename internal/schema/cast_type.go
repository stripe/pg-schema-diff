package schema

import "strings"

// formatTypeBaseToPgCatalogTypname maps pg_catalog.format_type base names to pg_catalog
// typnames suitable for CAST(... AS type). format_type returns human-readable names
// (e.g. "integer") but CAST requires typnames (e.g. int4) when schema-qualified.
var formatTypeBaseToPgCatalogTypname = map[string]string{
	"bigint":                      "int8",
	"boolean":                     "bool",
	"character":                   "bpchar",
	"character varying":           "varchar",
	"double precision":            "float8",
	"integer":                     "int4",
	"numeric":                     "numeric",
	"real":                        "float4",
	"smallint":                    "int2",
	"time without time zone":      "time",
	"time with time zone":         "timetz",
	"timestamp without time zone": "timestamp",
	"timestamp with time zone":    "timestamptz",
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
	if typname, ok := formatTypeBaseToPgCatalogTypname[strings.ToLower(base)]; ok {
		return "pg_catalog." + typname + typmod
	}

	return `"pg_catalog".` + EscapeIdentifier(base) + typmod
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
