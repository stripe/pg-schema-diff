package diff

import (
	"fmt"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/internal/set"
)

// enumSQLGenerator is a SQL generator for enums. In the future, we might want to convert this to a sqlVertexGenerator
// with dependencies on (table) columns that use this enum. It is much easier to implement this as a sqlGenerator for
// now.
type enumSQLGenerator struct{}

func (e *enumSQLGenerator) Add(enum schema.Enum) ([]Statement, error) {
	var escapedEnumVals []string
	for _, val := range enum.Labels {
		escapedEnumVals = append(escapedEnumVals, fmt.Sprintf("'%s'", val))
	}
	return []Statement{
		{
			DDL:         fmt.Sprintf("CREATE TYPE %s AS ENUM (%s)", enum.GetFQEscapedName(), strings.Join(escapedEnumVals, ", ")),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		},
	}, nil
}

func (e *enumSQLGenerator) Delete(enum schema.Enum) ([]Statement, error) {
	return []Statement{
		{
			DDL:         fmt.Sprintf("DROP TYPE %s", enum.GetFQEscapedName()),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		},
	}, nil
}

func (e *enumSQLGenerator) Alter(diff enumDiff) ([]Statement, error) {
	oldCopy := diff.old
	oldVals := set.NewSet(diff.old.Labels...)
	newVals := set.NewSet(diff.new.Labels...)
	if len(set.Difference(oldVals, newVals)) > 0 {
		// Old values cannot be deleted, so we will try to re-create the enum. Normally, we wouldn't try this
		// in sqlGenerator.Alter, and we would rely on the forceRecreate functionality of diff. However, if we tried the
		// normal delete -> add -> alter -> {all other generated SQL}, migrations involving deleting an enum would
		// fail because tables would still be using the enum. As a result, we must push re-creating the enum into the alter statement.
		//
		// 99% of the time this will fail for the user because they are doing something wrong, i.e., removing an enum value on an enum still in use.
		// We could spot this for the user while we generate the plan, but that would add complexity to the plan generation.
		// For now, we will identify this in plan validation, which should still fail.
		//
		// In the future, we might want to either:
		// (1) Convert this to a sqlVertexGenerator and build a SQL dependency web (a cyclic dependency error would be thrown in the case where the enum is impossible to drop and re-create)
		// (2) Try to make a best effort at identifying if any column in the new schema is using this enum (worse than SQL web)
		deletes, err := e.Delete(diff.old)
		if err != nil {
			return nil, fmt.Errorf("generating delete statements: %w", err)
		}
		adds, err := e.Add(diff.new)
		if err != nil {
			return nil, fmt.Errorf("generating add statements: %w", err)
		}
		return append(deletes, adds...), nil
	}

	var stmts []Statement

	// Add new values. It's easiest to add values from the end of the list to start beginning because the default ALTER
	// DDL adds values to the end of the enum.
	for i := len(diff.new.Labels) - 1; i >= 0; i-- {
		val := diff.new.Labels[i]
		if oldVals.Has(val) {
			continue
		}
		sb := strings.Builder{}
		sb.WriteString(fmt.Sprintf("ALTER TYPE %s ADD VALUE '%s'", diff.new.GetFQEscapedName(), val))
		if i < len(diff.new.Labels)-1 {
			sb.WriteString(fmt.Sprintf(" BEFORE '%s'", diff.new.Labels[i+1]))
		}
		stmts = append(stmts, Statement{
			DDL:         sb.String(),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		})
	}
	oldCopy.Labels = diff.new.Labels

	if diff := cmp.Diff(oldCopy, diff.new); diff != "" {
		return nil, fmt.Errorf("unable to resolve the diff %s: %w", diff, ErrNotImplemented)
	}

	return stmts, nil
}
