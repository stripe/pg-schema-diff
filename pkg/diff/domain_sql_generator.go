package diff

import (
	"fmt"
	"strings"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

// domainSQLGenerator is a SQL generator for domains. Like enums, domains are created before anything that can
// reference them and dropped after the last column that did, so a dependency web is not needed yet.
type domainSQLGenerator struct{}

func (d *domainSQLGenerator) Add(domain schema.Domain) ([]Statement, error) {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("CREATE DOMAIN %s AS %s", domain.GetFQEscapedName(), domain.BaseType))
	if len(domain.Default) > 0 {
		sb.WriteString(fmt.Sprintf(" DEFAULT %s", domain.Default))
	}
	if domain.NotNull {
		sb.WriteString(" NOT NULL")
	}
	for _, constraint := range domain.Constraints {
		sb.WriteString(fmt.Sprintf("\n\tCONSTRAINT %s %s", constraint.EscapedName, constraint.Expression))
	}
	return []Statement{
		{
			DDL:         sb.String(),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		},
	}, nil
}

func (d *domainSQLGenerator) Delete(domain schema.Domain) ([]Statement, error) {
	return []Statement{
		{
			DDL:         fmt.Sprintf("DROP DOMAIN %s", domain.GetFQEscapedName()),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		},
	}, nil
}

func (d *domainSQLGenerator) Alter(diff domainDiff) ([]Statement, error) {
	if diff.old.BaseType != diff.new.BaseType {
		// A domain in use cannot be dropped and re-created, and Postgres has no ALTER DOMAIN ... TYPE.
		return nil, fmt.Errorf("changing the base type of domain %s from %s to %s: %w", diff.new.GetFQEscapedName(), diff.old.BaseType, diff.new.BaseType, ErrNotImplemented)
	}

	var stmts []Statement
	alter := func(ddl string) {
		stmts = append(stmts, Statement{
			DDL:         fmt.Sprintf("ALTER DOMAIN %s %s", diff.new.GetFQEscapedName(), ddl),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		})
	}

	if diff.old.Default != diff.new.Default {
		if len(diff.new.Default) == 0 {
			alter("DROP DEFAULT")
		} else {
			alter(fmt.Sprintf("SET DEFAULT %s", diff.new.Default))
		}
	}

	if diff.old.NotNull != diff.new.NotNull {
		if diff.new.NotNull {
			alter("SET NOT NULL")
		} else {
			alter("DROP NOT NULL")
		}
	}

	oldConstraints := make(map[string]string)
	for _, constraint := range diff.old.Constraints {
		oldConstraints[constraint.EscapedName] = constraint.Expression
	}
	newConstraints := make(map[string]string)
	for _, constraint := range diff.new.Constraints {
		newConstraints[constraint.EscapedName] = constraint.Expression
	}
	// A changed constraint is dropped and added again under the same name.
	for _, constraint := range diff.old.Constraints {
		if expression, ok := newConstraints[constraint.EscapedName]; !ok || expression != constraint.Expression {
			alter(fmt.Sprintf("DROP CONSTRAINT %s", constraint.EscapedName))
		}
	}
	for _, constraint := range diff.new.Constraints {
		if expression, ok := oldConstraints[constraint.EscapedName]; !ok || expression != constraint.Expression {
			alter(fmt.Sprintf("ADD CONSTRAINT %s %s", constraint.EscapedName, constraint.Expression))
		}
	}

	return stmts, nil
}
