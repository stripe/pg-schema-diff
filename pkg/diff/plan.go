package diff

import (
	"fmt"
	"regexp"
	"time"
)

type MigrationHazardType = string

const (
	MigrationHazardTypeAcquiresAccessExclusiveLock MigrationHazardType = "ACQUIRES_ACCESS_EXCLUSIVE_LOCK"
	MigrationHazardTypeAcquiresShareLock           MigrationHazardType = "ACQUIRES_SHARE_LOCK"
	MigrationHazardTypeDeletesData                 MigrationHazardType = "DELETES_DATA"
	MigrationHazardTypeHasUntrackableDependencies  MigrationHazardType = "HAS_UNTRACKABLE_DEPENDENCIES"
	MigrationHazardTypeIndexBuild                  MigrationHazardType = "INDEX_BUILD"
	MigrationHazardTypeIndexDropped                MigrationHazardType = "INDEX_DROPPED"
	MigrationHazardTypeImpactsDatabasePerformance  MigrationHazardType = "IMPACTS_DATABASE_PERFORMANCE"
	MigrationHazardTypeIsUserGenerated             MigrationHazardType = "IS_USER_GENERATED"
)

type MigrationHazard struct {
	Type    MigrationHazardType
	Message string
}

func (p MigrationHazard) String() string {
	return fmt.Sprintf("%s: %s", p.Type, p.Message)
}

type Statement struct {
	DDL     string
	Timeout time.Duration
	Hazards []MigrationHazard
}

func (s Statement) ToSQL() string {
	return s.DDL + ";"
}

type Plan struct {
	Statements        []Statement
	CurrentSchemaHash string
}

func (p Plan) ApplyStatementTimeoutModifier(regex *regexp.Regexp, timeout time.Duration) Plan {
	var modifiedStmts []Statement
	for _, stmt := range p.Statements {
		if regex.MatchString(stmt.DDL) {
			stmt.Timeout = timeout
		}
		modifiedStmts = append(modifiedStmts, stmt)
	}
	p.Statements = modifiedStmts
	return p
}

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
