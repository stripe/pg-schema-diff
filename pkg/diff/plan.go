package diff

import (
	"encoding/json"
	"fmt"
)

type MigrationHazardType = string

const (
	MigrationHazardTypeAcquiresAccessExclusiveLock   MigrationHazardType = "ACQUIRES_ACCESS_EXCLUSIVE_LOCK"
	MigrationHazardTypeAcquiresShareLock             MigrationHazardType = "ACQUIRES_SHARE_LOCK"
	MigrationHazardTypeAcquiresShareRowExclusiveLock MigrationHazardType = "ACQUIRES_SHARE_ROW_EXCLUSIVE_LOCK"
	MigrationHazardTypeCorrectness                   MigrationHazardType = "CORRECTNESS"
	MigrationHazardTypeDeletesData                   MigrationHazardType = "DELETES_DATA"
	MigrationHazardTypeHasUntrackableDependencies    MigrationHazardType = "HAS_UNTRACKABLE_DEPENDENCIES"
	MigrationHazardTypeIndexBuild                    MigrationHazardType = "INDEX_BUILD"
	MigrationHazardTypeIndexDropped                  MigrationHazardType = "INDEX_DROPPED"
	MigrationHazardTypeImpactsDatabasePerformance    MigrationHazardType = "IMPACTS_DATABASE_PERFORMANCE"
	MigrationHazardTypeIsUserGenerated               MigrationHazardType = "IS_USER_GENERATED"
	MigrationHazardTypeExtensionVersionUpgrade       MigrationHazardType = "UPGRADING_EXTENSION_VERSION"
	MigrationHazardTypeAuthzUpdate                   MigrationHazardType = "AUTHZ_UPDATE"
)

// MigrationHazard represents a hazard that a statement poses to a database
type MigrationHazard struct {
	Type    MigrationHazardType `json:"type"`
	Message string              `json:"message"`
}

func (p MigrationHazard) String() string {
	return fmt.Sprintf("%s: %s", p.Type, p.Message)
}

type Statement struct {
	DDL string
	// The hazards this statement poses
	Hazards []MigrationHazard
	// SkipValidation indicates that this statement should be skipped during plan validation against a temporary
	// database instance. This is useful for statements that depend on entities (like roles) that won't exist
	// in the temp DB.
	SkipValidation bool
}

func (s Statement) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		DDL     string            `json:"ddl"`
		Hazards []MigrationHazard `json:"hazards"`
	}{
		DDL:     s.DDL,
		Hazards: s.Hazards,
	})
}

func (s Statement) ToSQL() string {
	return s.DDL + ";"
}

// Plan represents an ordinary migration and its optional deferred cleanup.
// Apply Statements to reach the managed target while retaining removed table
// data. CleanupStatements are destructive, are never applied automatically, and
// must not be concatenated with Statements.
type Plan struct {
	// Statements is the ordinary migration from schema A to schema B.
	Statements []Statement `json:"statements"`
	// CleanupStatements physically delete retained objects when executed separately at a later time.
	CleanupStatements []Statement `json:"cleanup_statements,omitempty"`
	// CurrentSchemaHash is the versioned hash of the source snapshot. Before applying a serialized plan, compare it
	// with schema.GetSchemaHash or schema.GetSchemaHashWithArchivalPrefix using the same prefix and schema filters.
	CurrentSchemaHash string `json:"current_schema_hash"`
}

// InsertStatement inserts the given statement at the given index. If index is equal to the length of the statements,
// it will append the statement to the end of the statement in the plan
func (p Plan) InsertStatement(index int, statement Statement) (Plan, error) {
	if index < 0 || index > len(p.Statements) {
		return Plan{}, fmt.Errorf("index must be >= 0 and <= %d", len(p.Statements))
	}
	if index == len(p.Statements) {
		p.Statements = append(p.Statements, statement)
		return p, nil
	}
	p.Statements = append(p.Statements[:index+1], p.Statements[index:]...)
	p.Statements[index] = statement
	return p, nil
}
