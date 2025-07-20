package diff

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type triggerDiff struct {
	oldAndNew[schema.Trigger]
}

type triggerSQLVertexGenerator struct {
	// functionsInNewSchemaByName is a map of function new to functions in the new schema.
	// These functions are not necessarily new
	functionsInNewSchemaByName map[string]schema.Function
}

func newTriggerSqlVertexGenerator(functionsInNewSchemaByName map[string]schema.Function) sqlVertexGenerator[schema.Trigger, triggerDiff] {
	return legacyToNewSqlVertexGenerator[schema.Trigger, triggerDiff](&triggerSQLVertexGenerator{
		functionsInNewSchemaByName: functionsInNewSchemaByName,
	})
}

func (t *triggerSQLVertexGenerator) Add(trigger schema.Trigger) ([]Statement, error) {
	return t.addStatements(trigger), nil
}

func (t *triggerSQLVertexGenerator) addStatements(trigger schema.Trigger) []Statement {
	return []Statement{{
		DDL:         string(trigger.GetTriggerDefStmt),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}
}

func (t *triggerSQLVertexGenerator) Delete(trigger schema.Trigger) ([]Statement, error) {
	return t.deleteStatements(trigger), nil
}

func (t *triggerSQLVertexGenerator) deleteStatements(trigger schema.Trigger) []Statement {
	return []Statement{{
		DDL:         fmt.Sprintf("DROP TRIGGER %s ON %s", trigger.EscapedName, trigger.OwningTable.GetFQEscapedName()),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}
}

func (t *triggerSQLVertexGenerator) Alter(diff triggerDiff) ([]Statement, error) {
	if cmp.Equal(diff.old, diff.new) {
		return nil, nil
	}

	if diff.old.IsConstraint || diff.new.IsConstraint {
		// Constraint triggers do not support "CREATE OR REPLACE", so just drop the original trigger and
		// create the new one.
		return append(t.deleteStatements(diff.old), t.addStatements(diff.new)...), nil

	}

	createOrReplaceStmt, err := diff.new.GetTriggerDefStmt.ToCreateOrReplace()
	if err != nil {
		return nil, fmt.Errorf("modifying get trigger def statement to create or replace: %w", err)
	}
	return []Statement{{
		DDL:         createOrReplaceStmt,
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (t *triggerSQLVertexGenerator) GetSQLVertexId(trigger schema.Trigger, diffType diffType) sqlVertexId {
	return buildSchemaObjVertexId("trigger", trigger.GetName(), diffType)
}

func (t *triggerSQLVertexGenerator) GetAddAlterDependencies(newTrigger, oldTrigger schema.Trigger) ([]dependency, error) {
	// Since a trigger can just be `CREATE OR REPLACE`, there will never be a case where a trigger is
	// added and dropped in the same migration. Thus, we don't need a dependency on the delete node of a function
	// because there won't be one if it is being added/altered
	deps := []dependency{
		mustRun(t.GetSQLVertexId(newTrigger, diffTypeAddAlter)).after(buildFunctionVertexId(newTrigger.Function, diffTypeAddAlter)),
		mustRun(t.GetSQLVertexId(newTrigger, diffTypeAddAlter)).after(buildTableVertexId(newTrigger.OwningTable, diffTypeAddAlter)),
	}

	if !cmp.Equal(oldTrigger, schema.Trigger{}) {
		// If the trigger is being altered:
		// If the old version of the trigger called a function being deleted, the function deletion must come after the
		// trigger is altered, so the trigger no longer has a dependency on the function
		deps = append(deps,
			mustRun(t.GetSQLVertexId(newTrigger, diffTypeAddAlter)).before(buildFunctionVertexId(oldTrigger.Function, diffTypeDelete)),
		)
	}

	return deps, nil
}

func (t *triggerSQLVertexGenerator) GetDeleteDependencies(trigger schema.Trigger) ([]dependency, error) {
	return []dependency{
		mustRun(t.GetSQLVertexId(trigger, diffTypeDelete)).before(buildFunctionVertexId(trigger.Function, diffTypeDelete)),
		mustRun(t.GetSQLVertexId(trigger, diffTypeDelete)).before(buildTableVertexId(trigger.OwningTable, diffTypeDelete)),
	}, nil
}
