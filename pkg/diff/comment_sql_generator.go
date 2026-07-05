package diff

import (
	"fmt"
	"strings"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

// escapeCommentLiteral escapes a string for use as a single-quoted PostgreSQL literal in
// `COMMENT ON ... IS '...'`.
func escapeCommentLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// commentOnStatement builds a `COMMENT ON <kind> <target> IS ...` statement. If description
// is empty, the statement uses `IS NULL` to remove any existing comment.
//
// kindAndTarget is the part between `COMMENT ON ` and ` IS ...`, e.g.
//
//	COLUMN "public"."t"."c"
//	TRIGGER "trg" ON "public"."t"
//	CONSTRAINT "ck" ON "public"."t"
//	FUNCTION "public"."f"(int)
//	TABLE "public"."t"
//	SCHEMA "public"
//
// Callers are responsible for assembling that string with the correct kind keyword.
func commentOnStatement(kindAndTarget, description string) Statement {
	var literal string
	if description == "" {
		literal = "NULL"
	} else {
		literal = fmt.Sprintf("'%s'", escapeCommentLiteral(description))
	}
	return Statement{
		DDL:         fmt.Sprintf("COMMENT ON %s IS %s", kindAndTarget, literal),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}
}

// commentDDLForAdd returns a slice that contains a single COMMENT statement when
// description is non-empty, or nil otherwise. Use this from a SQL generator's `Add`
// path: a freshly-created object has no comment in PG, so we only need to emit when
// the new schema specifies one.
func commentDDLForAdd(kindAndTarget, description string) []Statement {
	if description == "" {
		return nil
	}
	return []Statement{commentOnStatement(kindAndTarget, description)}
}

// commentDDLForAlter returns a slice that contains a single COMMENT statement when
// the description has changed (added, modified, or removed), or nil when both are
// equal. Use this from a SQL generator's `Alter` path.
func commentDDLForAlter(kindAndTarget, oldDescription, newDescription string) []Statement {
	if oldDescription == newDescription {
		return nil
	}
	return []Statement{commentOnStatement(kindAndTarget, newDescription)}
}

// commentTargetTable formats `TABLE <fq>`.
func commentTargetTable(name schema.SchemaQualifiedName) string {
	return fmt.Sprintf("TABLE %s", name.GetFQEscapedName())
}

// commentTargetColumn formats `COLUMN <fq>.<column>`.
func commentTargetColumn(table schema.SchemaQualifiedName, columnName string) string {
	return fmt.Sprintf("COLUMN %s", schema.FQEscapedColumnName(table, columnName))
}

// commentTargetView formats `VIEW <fq>`.
func commentTargetView(name schema.SchemaQualifiedName) string {
	return fmt.Sprintf("VIEW %s", name.GetFQEscapedName())
}

// commentTargetMaterializedView formats `MATERIALIZED VIEW <fq>`.
func commentTargetMaterializedView(name schema.SchemaQualifiedName) string {
	return fmt.Sprintf("MATERIALIZED VIEW %s", name.GetFQEscapedName())
}

// commentTargetSequence formats `SEQUENCE <fq>`.
func commentTargetSequence(name schema.SchemaQualifiedName) string {
	return fmt.Sprintf("SEQUENCE %s", name.GetFQEscapedName())
}

// commentTargetIndex formats `INDEX <fq>`.
func commentTargetIndex(name schema.SchemaQualifiedName) string {
	return fmt.Sprintf("INDEX %s", name.GetFQEscapedName())
}

// commentTargetSchema formats `SCHEMA <name>`.
func commentTargetSchema(unescapedName string) string {
	return fmt.Sprintf("SCHEMA %s", schema.EscapeIdentifier(unescapedName))
}

// commentTargetExtension formats `EXTENSION <escapedName>`.
func commentTargetExtension(extension schema.Extension) string {
	return fmt.Sprintf("EXTENSION %s", extension.EscapedName)
}

// commentTargetType formats `TYPE <fq>`. Used for enum/domain/composite types.
func commentTargetType(name schema.SchemaQualifiedName) string {
	return fmt.Sprintf("TYPE %s", name.GetFQEscapedName())
}

// commentTargetFunction formats `FUNCTION <fq-with-args>`. SchemaQualifiedName.EscapedName
// for a Function already includes the argument signature (see schema.buildProcName).
func commentTargetFunction(name schema.SchemaQualifiedName) string {
	return fmt.Sprintf("FUNCTION %s", name.GetFQEscapedName())
}

// commentTargetProcedure formats `PROCEDURE <fq-with-args>`.
func commentTargetProcedure(name schema.SchemaQualifiedName) string {
	return fmt.Sprintf("PROCEDURE %s", name.GetFQEscapedName())
}

// commentTargetTrigger formats `TRIGGER <name> ON <fq-table>`.
func commentTargetTrigger(escapedTriggerName string, owningTable schema.SchemaQualifiedName) string {
	return fmt.Sprintf("TRIGGER %s ON %s", escapedTriggerName, owningTable.GetFQEscapedName())
}

// commentTargetPolicy formats `POLICY <name> ON <fq-table>`.
func commentTargetPolicy(escapedPolicyName string, owningTable schema.SchemaQualifiedName) string {
	return fmt.Sprintf("POLICY %s ON %s", escapedPolicyName, owningTable.GetFQEscapedName())
}

// commentTargetConstraint formats `CONSTRAINT <name> ON <fq-table>`. Use this for
// CHECK / FOREIGN KEY / PRIMARY KEY / UNIQUE constraints — comments live on the
// constraint, not the underlying index.
func commentTargetConstraint(escapedConstraintName string, owningTable schema.SchemaQualifiedName) string {
	return fmt.Sprintf("CONSTRAINT %s ON %s", escapedConstraintName, owningTable.GetFQEscapedName())
}
