package diff

import (
	"fmt"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

var migrationHazardOwnerChanged = MigrationHazard{
	Type:    MigrationHazardTypeAuthzUpdate,
	Message: "Changing object ownership changes implicit privileges for the old and new owners.",
}

func ownerDDLForAdd(kindAndTarget, owner string) []Statement {
	if owner == "" {
		return nil
	}
	return stripMigrationHazards(buildAlterOwnerStatement(kindAndTarget, owner))
}

func ownerDDLForAlter(kindAndTarget, oldOwner, newOwner string) []Statement {
	if oldOwner == newOwner || newOwner == "" {
		return nil
	}
	return []Statement{buildAlterOwnerStatement(kindAndTarget, newOwner)}
}

func ownershipTarget(kind string, name schema.SchemaQualifiedName) string {
	return fmt.Sprintf("%s %s", kind, name.GetFQEscapedName())
}

func buildAlterOwnerStatement(kindAndTarget, owner string) Statement {
	return Statement{
		DDL:         fmt.Sprintf("ALTER %s OWNER TO %s", kindAndTarget, schema.EscapeIdentifier(owner)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     []MigrationHazard{migrationHazardOwnerChanged},
	}
}
