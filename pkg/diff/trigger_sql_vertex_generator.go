package diff

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type triggerDiff struct {
	oldAndNew[schema.Trigger]
}

type triggerSQLVertexGenerator struct{}

func (t *triggerSQLVertexGenerator) Add(trigger schema.Trigger) (partialSQLGraph, error) {
	return buildPartialSQLGraph(
		buildTriggerVertexId(trigger, diffTypeAddAlter),
		sqlPrioritySooner,
		t.addStatements(trigger),
		t.addDependencies(trigger),
	), nil
}

func (t *triggerSQLVertexGenerator) addStatements(trigger schema.Trigger) []Statement {
	return []Statement{{
		DDL: string(trigger.GetTriggerDefStmt),
	}}
}

func (t *triggerSQLVertexGenerator) Delete(trigger schema.Trigger) (partialSQLGraph, error) {
	return buildPartialSQLGraph(
		buildTriggerVertexId(trigger, diffTypeDelete),
		sqlPriorityLater,
		t.deleteStatements(trigger),
		t.deleteDependencies(trigger),
	), nil
}

func (t *triggerSQLVertexGenerator) deleteStatements(trigger schema.Trigger) []Statement {
	return []Statement{{
		DDL: fmt.Sprintf("DROP TRIGGER %s ON %s", trigger.EscapedName,
			trigger.OwningTable.GetFQEscapedName()),
	}}
}

func (t *triggerSQLVertexGenerator) Alter(diff triggerDiff) (partialSQLGraph, error) {
	statements, err := t.alterStatements(diff)
	if err != nil {
		return partialSQLGraph{}, fmt.Errorf("generating sql: %w", err)
	}

	return buildPartialSQLGraph(
		buildTriggerVertexId(diff.new, diffTypeAddAlter),
		sqlPrioritySooner,
		statements,
		t.alterDependencies(diff.new, diff.old),
	), nil
}

func (t *triggerSQLVertexGenerator) alterStatements(diff triggerDiff) ([]Statement, error) {
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
		DDL: createOrReplaceStmt,
	}}, nil
}

func buildTriggerVertexId(trigger schema.Trigger, diffType diffType) sqlVertexId {
	return buildSchemaObjVertexId("trigger", trigger.GetName(), diffType)
}

func (t *triggerSQLVertexGenerator) addDependencies(trigger schema.Trigger) []dependency {
	// Since a trigger can just be `CREATE OR REPLACE`, there will never be a case where a trigger is
	// added and dropped in the same migration. Thus, we don't need a dependency on the trigger delete vertex
	// because there won't be one if it is being added/altered
	return []dependency{
		mustRun(buildTriggerVertexId(trigger, diffTypeAddAlter)).after(
			buildFunctionVertexId(trigger.Function, diffTypeAddAlter),
		),
		mustRun(buildTriggerVertexId(trigger, diffTypeAddAlter)).after(
			buildTableVertexId(trigger.OwningTable, diffTypeAddAlter),
		),
	}
}

func (t *triggerSQLVertexGenerator) alterDependencies(newTrigger, oldTrigger schema.Trigger) []dependency {
	return append(
		t.addDependencies(newTrigger),
		// If the old version of the trigger called a function being deleted, the function deletion must come after the
		// trigger is altered, so the trigger no longer has a dependency on the function.
		mustRun(buildTriggerVertexId(newTrigger, diffTypeAddAlter)).before(
			buildFunctionVertexId(oldTrigger.Function, diffTypeDelete),
		),
	)
}

func (t *triggerSQLVertexGenerator) deleteDependencies(trigger schema.Trigger) []dependency {
	return []dependency{
		mustRun(buildTriggerVertexId(trigger, diffTypeDelete)).before(
			buildFunctionVertexId(trigger.Function, diffTypeDelete),
		),
		mustRun(buildTriggerVertexId(trigger, diffTypeDelete)).before(
			buildTableVertexId(trigger.OwningTable, diffTypeDelete),
		),
	}
}
