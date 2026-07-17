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

// Plan represents a set of statements to be executed in order to migrate a database from schema A to schema B
type Plan struct {
	// Statements is the set of statements to be executed in order to migrate a database from schema A to schema B
	Statements []Statement `json:"statements"`
	// CurrentSchemaHash is the hash of the current schema, schema A. If you serialize this plans somewhere and
	// plan on running them later, you should verify that the current schema hash matches the current schema hash.
	// To get the current schema hash, you can use schema.GetPublicSchemaHash(ctx, conn)
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
