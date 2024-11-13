package diff

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/pgidentifier"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

const (
	maxPostgresIdentifierSize = 63

	statementTimeoutDefault = 3 * time.Second
	lockTimeoutDefault      = statementTimeoutDefault

	// statementTimeoutConcurrentIndexBuild is the statement timeout for index builds. It may take a while to build
	// the index. Since it doesn't take out locks, this shouldn't be a concern
	statementTimeoutConcurrentIndexBuild = 20 * time.Minute
	// statementTimeoutConcurrentIndexDrop is the statement timeout for concurrent index drops. This operation shouldn't
	// take out locks except when changing table metadata, but it may take a while to complete, so give it a long
	// timeout
	statementTimeoutConcurrentIndexDrop = 20 * time.Minute
	// statementTimeoutTableDrop is the statement timeout for table drops. It may a take a while to delete the data
	// Since the table is being dropped, locks shouldn't be a concern
	statementTimeoutTableDrop = 20 * time.Minute
	// statementTimeoutAnalyzeColumn is the statement timeout for analyzing the column of a table
	statementTimeoutAnalyzeColumn = 20 * time.Minute

	tmpObjNamePrefix = "pgschemadiff_tmp"
)

var (
	// ErrColumnOrderingChanged is returned when the ordering of columns changes and column ordering is not ignored.
	// It is recommended to ignore column ordering changes to column order
	ErrColumnOrderingChanged = fmt.Errorf("column ordering changed: %w", ErrNotImplemented)

	migrationHazardAddAlterFunctionCannotTrackDependencies = MigrationHazard{
		Type: MigrationHazardTypeHasUntrackableDependencies,
		Message: "Dependencies, i.e. other functions used in the function body, of non-sql functions cannot be tracked. " +
			"As a result, we cannot guarantee that function dependencies are ordered properly relative to this " +
			"statement. For adds, this means you need to ensure that all functions this function depends on are " +
			"created/altered before this statement.",
	}
	migrationHazardIndexDroppedQueryPerf = MigrationHazard{
		Type: MigrationHazardTypeIndexDropped,
		Message: "Dropping this index means queries that use this index might perform worse because " +
			"they will no longer will be able to leverage it.",
	}
	migrationHazardIndexDroppedAcquiresLock = MigrationHazard{
		Type:    MigrationHazardTypeAcquiresAccessExclusiveLock,
		Message: "Index drops will lock out all accesses to the table. They should be fast",
	}
	migrationHazardSequenceCannotTrackDependencies = MigrationHazard{
		Type:    MigrationHazardTypeHasUntrackableDependencies,
		Message: "This sequence has no owner, so it cannot be tracked. It may be in use by a table or function.",
	}
	migrationHazardExtensionDroppedCannotTrackDependencies = MigrationHazard{
		Type:    MigrationHazardTypeHasUntrackableDependencies,
		Message: "This extension may be in use by tables, indexes, functions, triggers, etc. Tihs statement will be ran last, so this may be OK.",
	}
	migrationHazardExtensionAlteredVersionUpgraded = MigrationHazard{
		Type:    MigrationHazardTypeExtensionVersionUpgrade,
		Message: "This extension's version is being upgraded. Be sure the newer version is backwards compatible with your use case.",
	}
)

type oldAndNew[S any] struct {
	old S
	new S
}

func (o oldAndNew[S]) GetNew() S {
	return o.new
}

func (o oldAndNew[S]) GetOld() S {
	return o.old
}

type (
	namedSchemaDiff struct {
		oldAndNew[schema.NamedSchema]
	}

	enumDiff struct {
		oldAndNew[schema.Enum]
	}

	extensionDiff struct {
		oldAndNew[schema.Extension]
	}

	columnDiff struct {
		oldAndNew[schema.Column]
		oldOrdering int
		newOrdering int
	}

	checkConstraintDiff struct {
		oldAndNew[schema.CheckConstraint]
	}

	tableDiff struct {
		oldAndNew[schema.Table]
		columnsDiff         listDiff[schema.Column, columnDiff]
		checkConstraintDiff listDiff[schema.CheckConstraint, checkConstraintDiff]
		policiesDiff        listDiff[schema.Policy, policyDiff]
	}

	indexDiff struct {
		oldAndNew[schema.Index]
	}

	foreignKeyConstraintDiff struct {
		oldAndNew[schema.ForeignKeyConstraint]
	}

	sequenceDiff struct {
		oldAndNew[schema.Sequence]
	}

	functionDiff struct {
		oldAndNew[schema.Function]
	}

	triggerDiff struct {
		oldAndNew[schema.Trigger]
	}
)

type schemaDiff struct {
	oldAndNew[schema.Schema]
	namedSchemaDiffs          listDiff[schema.NamedSchema, namedSchemaDiff]
	extensionDiffs            listDiff[schema.Extension, extensionDiff]
	enumDiffs                 listDiff[schema.Enum, enumDiff]
	tableDiffs                listDiff[schema.Table, tableDiff]
	indexDiffs                listDiff[schema.Index, indexDiff]
	foreignKeyConstraintDiffs listDiff[schema.ForeignKeyConstraint, foreignKeyConstraintDiff]
	sequenceDiffs             listDiff[schema.Sequence, sequenceDiff]
	functionDiffs             listDiff[schema.Function, functionDiff]
	triggerDiffs              listDiff[schema.Trigger, triggerDiff]
}

func (sd schemaDiff) resolveToSQL() ([]Statement, error) {
	return schemaSQLGenerator{}.Alter(sd)
}

// The procedure for DIFFING schemas and GENERATING/RESOLVING the SQL required to migrate the old schema to the new schema is
// described below:
//
// A schema follows a hierarchy: Schemas -> Tables -> Columns and Indexes
// Every level of the hierarchy can depend on other items at the same level (indexes depend on columns).
// A similar idea applies with constraints, including Foreign key constraints. Because constraints can have cross-table
// dependencies, they can be viewed at the same level as tables. This hierarchy becomes interwoven with partitions
//
// Diffing two sets of schema objects follows a common pattern:
// (DIFFING)
// 1. Diff two lists of schema objects (e.g., schemas, tables). An item is new if it's name is not present in the old list.
// An item is deleted if it's name is not present in the new list. Otherwise, an item might have been altered
// 2. For each potentially altered item, generate the diff between the old and new. This might involve diffing lists if they have
// nested items (recursing into step 1)
// (GENERATING/RESOLVING)
// 3. Generate the SQL required for the deleted items (ADDS), the new items, and the altered items.
// These items might have interwoven dependencies
// 4. Topologically sort the diffed items
//
// The diffing is handled by diffLists. Diffs lists takes two lists of schema objects identifies which are
// added, deleted, or potentially altered. If the items are potentially altered, it will pass the items
// to a callback which handles diffing the old and new versions. This callback might call into diff lists
// for items nested inside its hierarchy
//
// Generating the SQL for the resulting diff from the two lists of items is handled by the SQL(Vertex)Generators.
// Every schema object defines a SQL(Vertex)Generator. A SQL(Vertex)Generator generates the SQL required to add, delete,
// or alter a schema object. If altering a schema object, the SQL(Vertex)Generator is passed the diff generated by the callback in diffLists.
// The sqlGenerator just generates SQL, while the sqlVertexGenerator also defines dependencies that a schema object has
// on other schema objects

func buildSchemaDiff(old, new schema.Schema) (schemaDiff, bool, error) {
	// Normalize the schemas, so we get a consistent ordering for statements.
	old = old.Normalize()
	new = new.Normalize()

	schemaDiffs, err := diffLists(
		old.NamedSchemas,
		new.NamedSchemas,
		func(old, new schema.NamedSchema, _, _ int) (namedSchemaDiff, bool, error) {
			return namedSchemaDiff{
				oldAndNew[schema.NamedSchema]{
					old: old,
					new: new,
				},
			}, false, nil
		})
	if err != nil {
		return schemaDiff{}, false, fmt.Errorf("diffing schemas: %w", err)
	}

	extensionDiffs, err := diffLists(
		old.Extensions,
		new.Extensions,
		func(old, new schema.Extension, _, _ int) (extensionDiff, bool, error) {
			return extensionDiff{
				oldAndNew[schema.Extension]{
					old: old,
					new: new,
				},
			}, false, nil
		})
	if err != nil {
		return schemaDiff{}, false, fmt.Errorf("diffing extensions: %w", err)
	}

	enumDiffs, err := diffLists(old.Enums, new.Enums, func(old, new schema.Enum, _, _ int) (enumDiff, bool, error) {
		return enumDiff{
			oldAndNew[schema.Enum]{
				old: old,
				new: new,
			},
		}, false, nil
	})
	if err != nil {
		return schemaDiff{}, false, fmt.Errorf("diffing enums: %w", err)
	}

	tableDiffs, err := diffLists(old.Tables, new.Tables, buildTableDiff)
	if err != nil {
		return schemaDiff{}, false, fmt.Errorf("diffing tables: %w", err)
	}

	newSchemaTablesByName := buildSchemaObjByNameMap(new.Tables)
	addedTablesByName := buildSchemaObjByNameMap(tableDiffs.adds)
	indexesDiff, err := diffLists(old.Indexes, new.Indexes, func(oldIndex, newIndex schema.Index, _, _ int) (indexDiff, bool, error) {
		return buildIndexDiff(indexDiffConfig{
			newSchemaTablesByName:  newSchemaTablesByName,
			addedTablesByName:      addedTablesByName,
			oldSchemaIndexesByName: buildSchemaObjByNameMap(old.Indexes),
			newSchemaIndexesByName: buildSchemaObjByNameMap(new.Indexes),
		}, oldIndex, newIndex)
	})
	if err != nil {
		return schemaDiff{}, false, fmt.Errorf("diffing indexes: %w", err)
	}

	fsg := newForeignKeyConstraintSQLVertexGenerator(oldAndNew[schema.Schema]{old: old, new: new}, tableDiffs)
	foreignKeyConstraintDiffs, err := diffLists(old.ForeignKeyConstraints, new.ForeignKeyConstraints, func(old, new schema.ForeignKeyConstraint, _, _ int) (foreignKeyConstraintDiff, bool, error) {
		return buildForeignKeyConstraintDiff(fsg, addedTablesByName, old, new)
	})
	if err != nil {
		return schemaDiff{}, false, fmt.Errorf("diffing foreign key constraints: %w", err)
	}

	sequencesDiffs, err := diffLists(old.Sequences, new.Sequences, func(old, new schema.Sequence, oldIndex, newIndex int) (diff sequenceDiff, requiresRecreation bool, error error) {
		seqDiff := sequenceDiff{
			oldAndNew[schema.Sequence]{
				old: old,
				new: new,
			},
		}
		if new.Owner != nil && cmp.Equal(old.Owner, new.Owner) {
			if _, isOnNewTable := addedTablesByName[new.Owner.TableName.GetName()]; isOnNewTable {
				// Recreate the sequence if the owning table is recreated. This simplifies ownership changes, since we
				// don't need to change the owner to none and then change it back to the new owner
				// We could alternatively move this into the Alter block of the SequenceSQLVertexGenerator
				return sequenceDiff{}, true, nil
			}
		}
		return seqDiff, false, nil
	})
	if err != nil {
		return schemaDiff{}, false, fmt.Errorf("diffing sequences: %w", err)
	}

	functionDiffs, err := diffLists(old.Functions, new.Functions, func(old, new schema.Function, _, _ int) (functionDiff, bool, error) {
		return functionDiff{
			oldAndNew[schema.Function]{
				old: old,
				new: new,
			},
		}, false, nil
	})
	if err != nil {
		return schemaDiff{}, false, fmt.Errorf("diffing functions: %w", err)
	}

	triggerDiffs, err := diffLists(old.Triggers, new.Triggers, func(old, new schema.Trigger, _, _ int) (triggerDiff, bool, error) {
		if _, isOnNewTable := addedTablesByName[new.OwningTable.GetName()]; isOnNewTable {
			// If the table is new, then it must be re-created (this occurs if the base table has been
			// re-created). In other words, a trigger must be re-created if the owning table is re-created
			return triggerDiff{}, true, nil
		}
		return triggerDiff{
			oldAndNew[schema.Trigger]{
				old: old,
				new: new,
			},
		}, false, nil
	})
	if err != nil {
		return schemaDiff{}, false, fmt.Errorf("diffing triggers: %w", err)
	}

	return schemaDiff{
		oldAndNew: oldAndNew[schema.Schema]{
			old: old,
			new: new,
		},
		namedSchemaDiffs:          schemaDiffs,
		extensionDiffs:            extensionDiffs,
		enumDiffs:                 enumDiffs,
		tableDiffs:                tableDiffs,
		indexDiffs:                indexesDiff,
		foreignKeyConstraintDiffs: foreignKeyConstraintDiffs,
		sequenceDiffs:             sequencesDiffs,
		functionDiffs:             functionDiffs,
		triggerDiffs:              triggerDiffs,
	}, false, nil
}

func buildTableDiff(oldTable, newTable schema.Table, _, _ int) (diff tableDiff, requiresRecreation bool, err error) {
	if oldTable.IsPartitioned() != newTable.IsPartitioned() {
		return tableDiff{}, true, nil
	} else if oldTable.PartitionKeyDef != newTable.PartitionKeyDef {
		// We won't support changing partition key def due to issues with requiresRecreation.
		//
		// BLUF of the problem: If you have a flattened hierarchy (partitions, materialized views) and the parent
		// is re-created but the children are unchanged, the children need to be re-created.
		//
		// If we want to add support, then we need diffLists to identify if a parent has been re-created (or if parents have changed),
		// so it knows to re-create the child. This problem becomes more acute when a child can belong to
		// multiple parents, e.g., materialized views. Ultimately, it's a graph problem in diffLists that can
		// be solved through a `getParents` function
		//
		// Until the above is implemented, we can't support requiresRecreation on any flattened hierarchies
		return tableDiff{}, false, fmt.Errorf("changing partition key def: %w", ErrNotImplemented)
	}

	if !cmp.Equal(oldTable.ParentTable, newTable.ParentTable) {
		// Since diffLists doesn't handle re-creating hierarchies that change, we need to manually
		// identify if the hierarchy has changed. This approach will NOT work if we support multiple layers
		// of partitioning because it's possible the parent's parent changed but the parent remained the same
		return tableDiff{}, true, nil
	}

	columnsDiff, err := diffLists(
		oldTable.Columns,
		newTable.Columns,
		func(old, new schema.Column, oldIndex, newIndex int) (columnDiff, bool, error) {
			return columnDiff{
				oldAndNew:   oldAndNew[schema.Column]{old: old, new: new},
				oldOrdering: oldIndex,
				newOrdering: newIndex,
			}, false, nil
		},
	)
	if err != nil {
		return tableDiff{}, false, fmt.Errorf("diffing columns: %w", err)
	}

	checkConsDiff, err := diffLists(
		oldTable.CheckConstraints,
		newTable.CheckConstraints,
		func(old, new schema.CheckConstraint, _, _ int) (checkConstraintDiff, bool, error) {
			recreateConstraint := (old.Expression != new.Expression) ||
				(old.IsValid && !new.IsValid) ||
				(old.IsInheritable != new.IsInheritable)
			return checkConstraintDiff{oldAndNew[schema.CheckConstraint]{old: old, new: new}},
				recreateConstraint,
				nil
		},
	)
	if err != nil {
		return tableDiff{}, false, fmt.Errorf("diffing check cons: %w", err)
	}

	var nilableOldTable *schema.Table
	if !cmp.Equal(oldTable, schema.Table{}) {
		nilableOldTable = &oldTable
	}
	psg, err := newPolicySQLVertexGenerator(nilableOldTable, newTable)
	if err != nil {
		return tableDiff{}, false, fmt.Errorf("creating policy sql vertex generator: %w", err)
	}
	policiesDiff, err := buildPolicyDiffs(psg, oldTable.Policies, newTable.Policies)
	if err != nil {
		return tableDiff{}, false, fmt.Errorf("diffing policies: %w", err)

	}

	return tableDiff{
		oldAndNew: oldAndNew[schema.Table]{
			old: oldTable,
			new: newTable,
		},
		columnsDiff:         columnsDiff,
		checkConstraintDiff: checkConsDiff,
		policiesDiff:        policiesDiff,
	}, false, nil
}

type indexDiffConfig struct {
	newSchemaTablesByName map[string]schema.Table
	addedTablesByName     map[string]schema.Table

	// oldSchemaIndexesByName and newSchemaIndexesByName by name are hackaround because the diff function does not yet support hierarchies
	oldSchemaIndexesByName map[string]schema.Index
	newSchemaIndexesByName map[string]schema.Index

	// seenIndexByName is used to prevent infinite recursion when diffing indexes
	seenIndexesByName map[string]bool
}

// buildIndexDiff builds the index diff
func buildIndexDiff(deps indexDiffConfig, old, new schema.Index) (diff indexDiff, requiresRecreation bool, err error) {
	if deps.seenIndexesByName == nil {
		deps.seenIndexesByName = make(map[string]bool)
	} else if deps.seenIndexesByName[new.GetName()] {
		// Prevent infinite recursion
		return indexDiff{}, false, fmt.Errorf("loop detected between indexes that starts with %q. %v", new.GetName(), deps.seenIndexesByName)
	}
	deps.seenIndexesByName[new.GetName()] = true

	updatedOld := old

	if _, isOnNewTable := deps.addedTablesByName[new.OwningTable.GetName()]; isOnNewTable {
		// If the table is new, then it must be re-created (this occurs if the base table has been
		// re-created). In other words, an index must be re-created if the owning table is re-created
		return indexDiff{}, true, nil
	}

	if old.ParentIdx == nil {
		// If the old index didn't belong to a partitioned index (and the new index does), we can resolve the parent
		// index name diff if the index now belongs to a partitioned index by attaching the index.
		// We can't switch an index partition from one parent to another; in that instance, we must
		// re-create the index
		updatedOld.ParentIdx = new.ParentIdx
	}

	if old.ParentIdx != nil && new.ParentIdx != nil && (*old.ParentIdx == *new.ParentIdx) {
		// This is a bad way of recreating the child index when the parent is recreated. Ideally, the diff function
		// should be able to handle dependency hierarchies, where if a parent is recreated, the child is recreated.
		// This is hack around because that functionality is not yet implemented
		oldParentIndex, ok := deps.oldSchemaIndexesByName[new.ParentIdx.GetName()]
		if !ok {
			return indexDiff{}, false, fmt.Errorf("could not find parent index %s", new.ParentIdx.GetName())
		}
		newParentIndex, ok := deps.newSchemaIndexesByName[new.ParentIdx.GetName()]
		if !ok {
			return indexDiff{}, false, fmt.Errorf("could not find parent index %s", new.ParentIdx.GetName())
		}

		if _, parentRecreated, err := buildIndexDiff(deps, oldParentIndex, newParentIndex); err != nil {
			return indexDiff{}, false, fmt.Errorf("diffing parent index: %w", err)
		} else if parentRecreated {
			// Re-create an index if it's parent is re-created
			return indexDiff{}, true, nil
		}
	}

	isOnPartitionedTable, err := isOnPartitionedTable(deps.newSchemaTablesByName, new)
	if err != nil {
		return indexDiff{}, false, fmt.Errorf("checking if index is on partitioned table: %w", err)
	}

	if !isOnPartitionedTable {
		if old.Constraint == nil && new.Constraint != nil {
			// Attach the constraint using the existing index. This cannot be done if the index is on a partitioned table.
			// In the case of an index being on a partitioned table, it must be re-created
			updatedOld.Constraint = new.Constraint
		}
		if old.Constraint != nil && new.Constraint != nil && old.Constraint.IsLocal && !new.Constraint.IsLocal {
			// Similar to above. The constraint can just be attached.
			updatedOld.Constraint.IsLocal = new.Constraint.IsLocal
		}
	}

	if isOnPartitionedTable && old.IsInvalid && !new.IsInvalid {
		// If the index is a partitioned index, it can be made valid automatically by attaching the index partitions
		// We don't need to re-create it.
		updatedOld.IsInvalid = new.IsInvalid
	}

	recreateIndex := !cmp.Equal(updatedOld, new)
	return indexDiff{
		oldAndNew: oldAndNew[schema.Index]{
			old: old, new: new,
		},
	}, recreateIndex, nil
}

type schemaSQLGenerator struct{}

func (schemaSQLGenerator) Alter(diff schemaDiff) ([]Statement, error) {
	tablesInNewSchemaByName := buildSchemaObjByNameMap(diff.new.Tables)
	deletedTablesByName := buildSchemaObjByNameMap(diff.tableDiffs.deletes)
	addedTablesByName := buildSchemaObjByNameMap(diff.tableDiffs.adds)

	namedSchemaStatements, err := diff.namedSchemaDiffs.resolveToSQLGroupedByEffect(&namedSchemaSQLGenerator{})
	if err != nil {
		return nil, fmt.Errorf("resolving named schema sql statements: %w", err)
	}

	partialGraph := newPartialSQLGraph()

	tablePartialGraph, err := generatePartialGraph(legacyToNewSqlVertexGenerator[schema.Table, tableDiff](&tableSQLVertexGenerator{
		deletedTablesByName:     deletedTablesByName,
		tablesInNewSchemaByName: tablesInNewSchemaByName,
		tableDiffsByName:        buildDiffByNameMap[schema.Table, tableDiff](diff.tableDiffs.alters),
	}), diff.tableDiffs)
	if err != nil {
		return nil, fmt.Errorf("resolving table diff: %w", err)
	}
	partialGraph = concatPartialGraphs(partialGraph, tablePartialGraph)

	extensionStatements, err := diff.extensionDiffs.resolveToSQLGroupedByEffect(&extensionSQLGenerator{})
	if err != nil {
		return nil, fmt.Errorf("resolving extension diff: %w", err)
	}

	enumStatements, err := diff.enumDiffs.resolveToSQLGroupedByEffect(&enumSQLGenerator{})
	if err != nil {
		return nil, fmt.Errorf("resolving enum diff: %w", err)
	}

	attachPartitionGenerator := newAttachPartitionSQLVertexGenerator(diff.new.Indexes, diff.tableDiffs.adds)
	attachPartitionsPartialGraph, err := generatePartialGraph(legacyToNewSqlVertexGenerator[schema.Table, tableDiff](attachPartitionGenerator), diff.tableDiffs)
	if err != nil {
		return nil, fmt.Errorf("resolving attach partition diff: %w", err)
	}
	partialGraph = concatPartialGraphs(partialGraph, attachPartitionsPartialGraph)

	renameConflictingIndexesGenerator := newRenameConflictingIndexSQLVertexGenerator(buildSchemaObjByNameMap(diff.old.Indexes))
	renameConflictingIndexesPartialGraph, err := generatePartialGraph(legacyToNewSqlVertexGenerator[schema.Index, indexDiff](renameConflictingIndexesGenerator), diff.indexDiffs)
	if err != nil {
		return nil, fmt.Errorf("resolving renaming conflicting indexes diff: %w", err)
	}
	partialGraph = concatPartialGraphs(partialGraph, renameConflictingIndexesPartialGraph)

	indexGenerator := legacyToNewSqlVertexGenerator[schema.Index, indexDiff](&indexSQLVertexGenerator{
		deletedTablesByName:      deletedTablesByName,
		addedTablesByName:        addedTablesByName,
		tablesInNewSchemaByName:  tablesInNewSchemaByName,
		indexesInNewSchemaByName: buildSchemaObjByNameMap(diff.new.Indexes),

		renameSQLVertexGenerator:          renameConflictingIndexesGenerator,
		attachPartitionSQLVertexGenerator: attachPartitionGenerator,
	})
	indexesPartialGraph, err := generatePartialGraph(indexGenerator, diff.indexDiffs)
	if err != nil {
		return nil, fmt.Errorf("resolving index diff: %w", err)
	}
	partialGraph = concatPartialGraphs(partialGraph, indexesPartialGraph)

	foreignKeyGenerator := newForeignKeyConstraintSQLVertexGenerator(diff.oldAndNew, diff.tableDiffs)
	fkConsPartialGraph, err := generatePartialGraph(foreignKeyGenerator, diff.foreignKeyConstraintDiffs)
	if err != nil {
		return nil, fmt.Errorf("resolving foreign key constraint diff: %w", err)
	}
	partialGraph = concatPartialGraphs(partialGraph, fkConsPartialGraph)

	sequenceGenerator := legacyToNewSqlVertexGenerator[schema.Sequence, sequenceDiff](&sequenceSQLVertexGenerator{
		deletedTablesByName: deletedTablesByName,
		tableDiffsByName:    buildDiffByNameMap[schema.Table, tableDiff](diff.tableDiffs.alters),
	})
	sequencesPartialGraph, err := generatePartialGraph(sequenceGenerator, diff.sequenceDiffs)
	if err != nil {
		return nil, fmt.Errorf("resolving sequence diff: %w", err)
	}
	partialGraph = concatPartialGraphs(partialGraph, sequencesPartialGraph)

	sequenceOwnershipGenerator := legacyToNewSqlVertexGenerator[schema.Sequence, sequenceDiff](&sequenceOwnershipSQLVertexGenerator{})
	sequenceOwnershipsPartialGraph, err := generatePartialGraph(sequenceOwnershipGenerator, diff.sequenceDiffs)
	if err != nil {
		return nil, fmt.Errorf("resolving sequence ownership diff: %w", err)
	}
	partialGraph = concatPartialGraphs(partialGraph, sequenceOwnershipsPartialGraph)

	functionsInNewSchemaByName := buildSchemaObjByNameMap(diff.new.Functions)
	functionGenerator := legacyToNewSqlVertexGenerator[schema.Function, functionDiff](&functionSQLVertexGenerator{
		functionsInNewSchemaByName: functionsInNewSchemaByName,
	})
	functionsPartialGraph, err := generatePartialGraph(functionGenerator, diff.functionDiffs)
	if err != nil {
		return nil, fmt.Errorf("resolving function diff: %w", err)
	}
	partialGraph = concatPartialGraphs(partialGraph, functionsPartialGraph)

	triggerGenerator := legacyToNewSqlVertexGenerator[schema.Trigger, triggerDiff](&triggerSQLVertexGenerator{
		functionsInNewSchemaByName: functionsInNewSchemaByName,
	})
	triggersPartialGraph, err := generatePartialGraph(triggerGenerator, diff.triggerDiffs)
	if err != nil {
		return nil, fmt.Errorf("resolving trigger diff: %w", err)
	}
	partialGraph = concatPartialGraphs(partialGraph, triggersPartialGraph)
	sqlGraph, err := graphFromPartials(partialGraph)
	if err != nil {
		return nil, fmt.Errorf("converting to graph: %w", err)
	}

	graphStatements, err := sqlGraph.toOrderedStatements()
	if err != nil {
		return nil, fmt.Errorf("getting ordered statements: %w", err)
	}

	// We migrate schemas and extensions first and disable them last since their dependencies may span across
	// all other entities in the database.
	var statements []Statement
	statements = append(statements, namedSchemaStatements.Adds...)
	statements = append(statements, namedSchemaStatements.Alters...)
	statements = append(statements, extensionStatements.Adds...)
	statements = append(statements, extensionStatements.Alters...)
	statements = append(statements, enumStatements.Adds...)
	statements = append(statements, enumStatements.Alters...)
	statements = append(statements, graphStatements...)
	statements = append(statements, enumStatements.Deletes...)
	statements = append(statements, extensionStatements.Deletes...)
	statements = append(statements, namedSchemaStatements.Deletes...)
	return statements, nil
}

func buildIndexesByTableNameMap(indexes []schema.Index) map[string][]schema.Index {
	var output = make(map[string][]schema.Index)
	for _, idx := range indexes {
		output[idx.OwningTable.GetName()] = append(output[idx.OwningTable.GetName()], idx)
	}
	return output
}

// buildChildrenByPartitionedIndexNameMap builds a map of indexes by their parent index name. This map will include
// all descendents, not just direct descendents. For example, if foobar idx  has 5 children and each of those children
// has 5 children, the slice for foobar index wil contain 30 indexes (the 5 direct children and the 25 "grandchildren")
func buildChildrenByPartitionedIndexNameMap(indexes []schema.Index) map[string][]schema.Index {
	// Build a map of children by their direct parent index
	var childrenByDirectParentIdxName = make(map[string][]schema.Index)
	for _, idx := range indexes {
		parentIdxName := ""
		if idx.ParentIdx != nil {
			parentIdxName = idx.ParentIdx.GetName()
		}
		childrenByDirectParentIdxName[parentIdxName] = append(childrenByDirectParentIdxName[parentIdxName], idx)
	}

	// Use this map to build a map of root index by child index name
	var rootsByChildIdxName = make(map[string]schema.Index)
	// We need to calculate this level by level. We will start with the roots.
	var currentNodes = childrenByDirectParentIdxName[""]
	for len(currentNodes) > 0 {
		var idx = currentNodes[0]
		currentNodes = currentNodes[1:]

		rootIdx := idx
		if idx.ParentIdx != nil {
			rootIdx = rootsByChildIdxName[idx.ParentIdx.GetName()]
		}
		rootsByChildIdxName[idx.GetName()] = rootIdx

		// We can now calculate the next level of children for this node
		for _, child := range childrenByDirectParentIdxName[idx.GetName()] {
			currentNodes = append(currentNodes, child)
		}
	}

	// Build a map of all children by the root index name using the result of the previous step
	var allChildrenByRootIndexName = make(map[string][]schema.Index)
	for _, idx := range indexes {
		if idx.ParentIdx == nil {
			// Skip nodes without parents
			continue
		}
		rootIdx := rootsByChildIdxName[idx.GetName()]
		allChildrenByRootIndexName[rootIdx.GetName()] = append(allChildrenByRootIndexName[rootIdx.GetName()], idx)
	}

	return allChildrenByRootIndexName
}

func buildSchemaObjByNameMap[S schema.Object](s []S) map[string]S {
	return buildMap(s, func(s S) string {
		return s.GetName()
	})
}

func buildDiffByNameMap[S schema.Object, D diff[S]](d []D) map[string]D {
	return buildMap(d, func(d D) string {
		return d.GetNew().GetName()
	})
}

func buildMap[K comparable, V any](v []V, getKey func(V) K) map[K]V {
	output := make(map[K]V)
	for _, obj := range v {
		output[getKey(obj)] = obj
	}
	return output
}

type tableSQLVertexGenerator struct {
	deletedTablesByName     map[string]schema.Table
	tablesInNewSchemaByName map[string]schema.Table
	tableDiffsByName        map[string]tableDiff
}

func (t *tableSQLVertexGenerator) Add(table schema.Table) ([]Statement, error) {
	if table.IsPartition() {
		if table.IsPartitioned() {
			return nil, fmt.Errorf("partitioned partitions: %w", ErrNotImplemented)
		}
		if len(table.CheckConstraints) > 0 {
			return nil, fmt.Errorf("check constraints on partitions: %w", ErrNotImplemented)
		}
		if len(table.Policies) > 0 {
			return nil, fmt.Errorf("policies on partitions: %w", ErrNotImplemented)
		}
		// We attach the partitions separately. So the partition must have all the same check constraints
		// as the original table
		table.CheckConstraints = append(table.CheckConstraints, t.tablesInNewSchemaByName[table.ParentTable.GetName()].CheckConstraints...)
	}

	var stmts []Statement

	var columnDefs []string
	for _, column := range table.Columns {
		columnDef, err := buildColumnDefinition(column)
		if err != nil {
			return nil, fmt.Errorf("building column definition: %w", err)
		}
		columnDefs = append(columnDefs, "\t"+columnDef)
	}
	createTableSb := strings.Builder{}
	createTableSb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n%s\n)",
		table.GetFQEscapedName(),
		strings.Join(columnDefs, ",\n"),
	))
	if table.IsPartitioned() {
		createTableSb.WriteString(fmt.Sprintf(" PARTITION BY %s", table.PartitionKeyDef))
	}
	stmts = append(stmts, Statement{
		DDL:         createTableSb.String(),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	})

	csg := checkConstraintSQLVertexGenerator{
		tableName:  table.SchemaQualifiedName,
		isNewTable: true,
	}
	for _, checkCon := range table.CheckConstraints {
		addConStmts, err := csg.Add(checkCon)
		if err != nil {
			return nil, fmt.Errorf("generating add check constraint statements for check constraint %s: %w", checkCon.Name, err)
		}
		// Remove hazards from statements since the table is brand new
		stmts = append(stmts, stripMigrationHazards(addConStmts...)...)
	}

	if table.ReplicaIdentity != schema.ReplicaIdentityDefault {
		// We don't need to set the replica identity if it's the default
		alterReplicaIdentityStmt, err := alterReplicaIdentityStatement(table.SchemaQualifiedName, table.ReplicaIdentity)
		if err != nil {
			return nil, fmt.Errorf("building replica identity statement: %w", err)
		}
		// Remove hazards from statements since the table is brand new
		alterReplicaIdentityStmt.Hazards = nil
		stmts = append(stmts, alterReplicaIdentityStmt)
	}

	policyGenerator, err := newPolicySQLVertexGenerator(nil, table)
	if err != nil {
		return nil, fmt.Errorf("creating policy sql vertex generator: %w", err)
	}
	for _, policy := range table.Policies {
		addPolicyPartialGraph, err := policyGenerator.Add(policy)
		if err != nil {
			return nil, fmt.Errorf("generating add policy statements for policy %s: %w", policy.EscapedName, err)
		}
		// Remove hazards from statements since the table is brand new
		stmts = append(stmts, stripMigrationHazards(addPolicyPartialGraph.statements()...)...)
	}

	if table.RLSEnabled {
		stmts = append(stmts, stripMigrationHazards(enableRLSForTable(table))...)
	}
	if table.RLSForced {
		stmts = append(stmts, stripMigrationHazards(forceRLSForTable(table))...)
	}

	return stmts, nil
}

func (t *tableSQLVertexGenerator) Delete(table schema.Table) ([]Statement, error) {
	if table.IsPartition() {
		// Don't support dropping partitions without dropping the base table. This would be easy to implement, but we
		// would need to add tests for it.
		//
		// The base table might be recreated, so check if its deleted rather than just checking if it does not exist in
		// the new schema
		if _, baseTableDropped := t.deletedTablesByName[table.ParentTable.GetName()]; !baseTableDropped {
			return nil, fmt.Errorf("deleting partitions without dropping parent table: %w", ErrNotImplemented
		}
		// It will be dropped when the parent table is dropped
		return nil, nil
	}
	return []Statement{
		{
			DDL:         fmt.Sprintf("DROP TABLE %s", table.GetFQEscapedName()),
			Timeout:     statementTimeoutTableDrop,
			LockTimeout: lockTimeoutDefault,
			Hazards: []MigrationHazard{{
				Type:    MigrationHazardTypeDeletesData,
				Message: "Deletes all rows in the table (and the table itself)",
			}},
		},
	}, nil
}
func (t *tableSQLVertexGenerator) GetDeleteDependencies(table schema.Table) ([]dependency, error) {
	var deps []dependency
	if table.ParentTable != nil {
		deps = append(deps,
			mustRun(t.GetSQLVertexId(table, diffTypeDelete)).after(buildTableVertexId(*table.ParentTable, diffTypeDelete, subgraphMarkerStart)),
		)
	}
	return deps, nil
}

func (t *tableSQLVertexGenerator) GetSQLVertexId(table schema.Table, diffType diffType) sqlVertexId {
	return buildTableVertexId(table.SchemaQualifiedName, diffType, subgraphMarkerStart)
}

func (t *tableSQLVertexGenerator) Alter(diff tableDiff) ([]Statement, error) {
	if diff.old.IsPartition() != diff.new.IsPartition() {
		return nil, fmt.Errorf("changing a partition to no longer be a partition (or vice versa): %w", ErrNotImplemented)
	}

	var stmts []Statement
	// Only handle disabling RLS if it was previously enabled.
	// We want to disable RLS before we do any other operations on the table, e.g., delete policies, to avoid creating an
	// outage while RLS is being disabled
	if !diff.new.RLSEnabled && diff.old.RLSEnabled {
		stmts = append(stmts, disableRLSForTable(diff.new))
	}
	if !diff.new.RLSForced && diff.old.RLSForced {
		stmts = append(stmts, unforceRLSForTable(diff.new))
	}

	if diff.new.IsPartition() {
		alterPartitionStmts, err := t.alterPartition(diff)
		if err != nil {
			return nil, fmt.Errorf("altering partition: %w", err)
		}
		stmts = append(stmts, alterPartitionStmts...)
	} else {
		alterBaseTableStmts, err := t.alterBaseTable(diff)
		if err != nil {
			return nil, fmt.Errorf("altering base table: %w", err)
		}
		stmts = append(stmts, alterBaseTableStmts...)
	}

	if diff.old.ReplicaIdentity != diff.new.ReplicaIdentity {
		alterReplicaIdentityStmt, err := alterReplicaIdentityStatement(diff.new.SchemaQualifiedName, diff.new.ReplicaIdentity)
		if err != nil {
			return nil, fmt.Errorf("building replica identity statement: %w", err)
		}
		stmts = append(stmts, alterReplicaIdentityStmt)
	}

	// We want to enable RLS after we do any other operations on the table, i.e., create policies, to avoid creating an
	// outtage while RLS is being enabled
	if diff.new.RLSEnabled && !diff.old.RLSEnabled {
		stmts = append(stmts, enableRLSForTable(diff.new))
	}
	if diff.new.RLSForced && !diff.old.RLSForced {
		stmts = append(stmts, forceRLSForTable(diff.new))
	}

	return stmts, nil
}

func (t *tableSQLVertexGenerator) alterBaseTable(diff tableDiff) ([]Statement, error) {
	if diff.old.PartitionKeyDef != diff.new.PartitionKeyDef {
		return nil, fmt.Errorf("changing partition key def: %w", ErrNotImplemented)
	}

	var tempCCs []schema.CheckConstraint
	for _, colDiff := range getDangerousNotNullAlters(diff.columnsDiff.alters, diff.new.CheckConstraints, diff.old.CheckConstraints) {
		tempCC, err := buildTempNotNullConstraint(colDiff)
		if err != nil {
			return nil, fmt.Errorf("building temp check constraint: %w", err)
		}
		diff.checkConstraintDiff.adds = append(diff.checkConstraintDiff.adds, tempCC)
		tempCCs = append(tempCCs, tempCC)
	}

	partialGraph := newPartialSQLGraph()

	columnGenerator := newColumnSQLVertexGenerator(diff.new.SchemaQualifiedName)
	columnsPartialGraph, err := generatePartialGraph(columnGenerator, diff.columnsDiff)
	if err != nil {
		return nil, fmt.Errorf("resolving index diff: %w", err)
	}
	partialGraph = concatPartialGraphs(partialGraph, columnsPartialGraph)

	checkConGenerator := legacyToNewSqlVertexGenerator[schema.CheckConstraint, checkConstraintDiff](&checkConstraintSQLVertexGenerator{
		tableName:              diff.new.SchemaQualifiedName,
		newSchemaColumnsByName: buildSchemaObjByNameMap(diff.new.Columns),
		oldSchemaColumnsByName: buildSchemaObjByNameMap(diff.old.Columns),
		addedColumnsByName:     buildSchemaObjByNameMap(diff.columnsDiff.adds),
		deletedColumnsByName:   buildSchemaObjByNameMap(diff.columnsDiff.deletes),
		isNewTable:             false,
	})
	checkConsPartialGraph, err := generatePartialGraph(checkConGenerator, diff.checkConstraintDiff)
	if err != nil {
		return nil, fmt.Errorf("resolving check constraints sql: %w", err)
	}
	partialGraph = concatPartialGraphs(partialGraph, checkConsPartialGraph)

	var dropTempCCs []Statement
	for _, tempCC := range tempCCs {
		dropTempCCsPartialGraph, err := checkConGenerator.Delete(tempCC)
		if err != nil {
			return nil, fmt.Errorf("deleting temp check constraint: %w", err)
		}
		dropTempCCs = append(dropTempCCs, dropTempCCsPartialGraph.statements()...)
	}

	var nilableOldTable *schema.Table
	if !cmp.Equal(diff.old, schema.Table{}) {
		nilableOldTable = &diff.old
	}
	policyGenerator, err := newPolicySQLVertexGenerator(nilableOldTable, diff.new)
	if err != nil {
		return nil, fmt.Errorf("creating policy sql vertex generator: %w", err)
	}
	policiesPartialGraph, err := generatePartialGraph(policyGenerator, diff.policiesDiff)
	if err != nil {
		return nil, fmt.Errorf("resolving policy sql: %w", err)
	}
	partialGraph = concatPartialGraphs(partialGraph, policiesPartialGraph)

	// Drop the temporary check constraints after everything else has run.
	dropTempCCsVertex := sqlVertex{
		id:         buildUUIDVertexId(),
		statements: dropTempCCs,
	}
	dropTempCCsDeps := mustRunVertex(dropTempCCsVertex).afterAllVertices(partialGraph.vertices...)
	partialGraph.addVertex(dropTempCCsVertex)
	partialGraph.addDependency(dropTempCCsDeps...)

	// TODO: Return partial graph instead of ordered statements with an organizational node tailing it
	graph, err := graphFromPartials(partialGraph)
	if err != nil {
		return nil, fmt.Errorf("converting to graph: %w", err)
	}
	graphStmts, err := graph.toOrderedStatements()
	if err != nil {
		return nil, fmt.Errorf("getting ordered statements from columnGraphs: %w", err)
	}

	return graphStmts, nil
}

func (t *tableSQLVertexGenerator) alterPartition(diff tableDiff) ([]Statement, error) {
	if diff.old.ForValues != diff.new.ForValues {
		return nil, fmt.Errorf("altering partition FOR VALUES: %w", ErrNotImplemented)
	}
	if !diff.checkConstraintDiff.isEmpty() {
		return nil, fmt.Errorf("check constraints on partitions: %w", ErrNotImplemented)
	}
	if !diff.policiesDiff.isEmpty() {
		// Policy diffing on individual partitions cannot be supported until where a SQL statement is generated is
		// _independent_ of how it is ordered.
		return nil, fmt.Errorf("policies on partitions: %w", ErrNotImplemented)
	}

	var alteredParentColumnsByName map[string]columnDiff
	if parentDiff, ok := t.tableDiffsByName[diff.new.ParentTable.GetName()]; ok {
		alteredParentColumnsByName = buildDiffByNameMap[schema.Column, columnDiff](parentDiff.columnsDiff.alters)
	}

	var stmts []Statement
	// ColumnsDiff should only have nullability changes. Partitioned tables
	// aren't concerned about old/new columns added
	for _, colDiff := range diff.columnsDiff.alters {
		if colDiff.old.IsNullable == colDiff.new.IsNullable {
			continue
		}

		if parentCol, ok := alteredParentColumnsByName[colDiff.new.Name]; ok {
			if colDiff.new.IsNullable == parentCol.new.IsNullable && parentCol.new.IsNullable != parentCol.old.IsNullable {
				// If the parent column's new nullability matches the child column, and the parent column's nullability
				// is being altered, then we don't need to alter the child column's nullability because the DDL required
				// to alter the parent's nullability will alter the child's nullability as well.
				continue
			}
		}

		alterColumnPrefix := fmt.Sprintf("%s ALTER COLUMN %s", alterTablePrefix(diff.new.SchemaQualifiedName), schema.EscapeIdentifier(colDiff.new.Name))
		if colDiff.new.IsNullable {
			stmts = append(stmts, Statement{
				DDL:         fmt.Sprintf("%s DROP NOT NULL", alterColumnPrefix),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
			})
		} else {
			stmts = append(stmts, Statement{
				DDL:         fmt.Sprintf("%s SET NOT NULL", alterColumnPrefix),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
				Hazards: []MigrationHazard{
					{
						Type: MigrationHazardTypeAcquiresAccessExclusiveLock,
						Message: "Marking a column as not null requires a full table scan, which will lock out " +
							"writes on the partition",
					},
				},
			})
		}
	}

	return stmts, nil
}

func alterReplicaIdentityStatement(table schema.SchemaQualifiedName, identity schema.ReplicaIdentity) (Statement, error) {
	alterType, err := replicaIdentityAlterType(identity)
	if err != nil {
		return Statement{}, fmt.Errorf("getting replica identity alter type: %w", err)
	}
	return Statement{
		DDL:         fmt.Sprintf("%s REPLICA IDENTITY %s", alterTablePrefix(table), alterType),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards: []MigrationHazard{{
			Type:    MigrationHazardTypeCorrectness,
			Message: "Changing replica identity may change the behavior of processes dependent on logical replication",
		}},
	}, nil
}

func replicaIdentityAlterType(identity schema.ReplicaIdentity) (string, error) {
	switch identity {
	case schema.ReplicaIdentityDefault:
		return "DEFAULT", nil
	case schema.ReplicaIdentityFull:
		return "FULL", nil
	case schema.ReplicaIdentityNothing:
		return "NOTHING", nil
		// We currently won't support index replica identity. If we want to add support, we should either:
		// option 1) Have the index sql generator generate the alter statement when the replica identity changes to index
		// option 2) Have a dedicates SQL generator for the alter replica identity statement
	}
	return "", fmt.Errorf("unknown/unsupported replica identity %s: %w", identity, ErrNotImplemented)
}

func buildTableVertexId(name schema.SchemaQualifiedName, diffType diffType, marker nestedGraphMarker) schemaObjNestedGraphSqlVertexId {
	// TODO - consider a cleaner way to serialize
	return buildSchemaObjVertexId("table", name.GetFQEscapedName(), diffType)
}

func (t *tableSQLVertexGenerator) GetAddAlterDependencies(new schema.Table, _ schema.Table) ([]dependency, error) {
	// TODO - get rid of this
	return t.getAddAlterDeps(new), nil
}

func (t *tableSQLVertexGenerator) getAddAlterDeps(table schema.Table) []dependency {
	deps := []dependency{
		mustRun(buildTableVertexId(table.SchemaQualifiedName, diffTypeAddAlter, subgraphMarkerStart)).after(buildTableVertexId(table.SchemaQualifiedName, diffTypeDelete, subgraphMarkerEnd)),
	}

	if table.ParentTable != nil {
		deps = append(deps,
			mustRun(buildTableVertexId(table.SchemaQualifiedName, diffTypeAddAlter, subgraphMarkerStart)).after(buildTableVertexId(*table.ParentTable, diffTypeAddAlter, subgraphMarkerEnd)),
		)
	}
	return deps
}

func getDangerousNotNullAlters(alteredCols []columnDiff, newSchemaCCs []schema.CheckConstraint, oldSchemaCCs []schema.CheckConstraint) []columnDiff {
	var ccs []schema.CheckConstraint
	ccs = append(ccs, newSchemaCCs...)
	ccs = append(ccs, oldSchemaCCs...)

	// A dangerous not null alter is an alter that changes a column from nullable to not null, but does not have a
	// valid NOT NULL check constraint backing it
	safeColsByName := make(map[string]bool)
	for _, cc := range ccs {
		if isValidNotNullCC(cc) {
			safeColsByName[cc.KeyColumns[0]] = true
		}
	}

	var dangerousNotNullAlters []columnDiff
	for _, colDiff := range alteredCols {
		if colDiff.old.IsNullable && !colDiff.new.IsNullable && !safeColsByName[colDiff.new.Name] {
			dangerousNotNullAlters = append(dangerousNotNullAlters, colDiff)
		}
	}

	return dangerousNotNullAlters
}

var (
	// isValidNotNullCCRegex is a regex that matches a valid NOT NULL check constraint. It's covers the simplest case,
	// so we might have to improve it in the future.
	// Notably, a false positive is much worse than a false negative because a false negative will result in an unnecessary
	// check constraint being built, but a false positive will result in a column being changed to `NOT NULL` without
	// a backing check constraint or a warning to the user. We should be very conservative with this regex.
	isNotNullCCRegex = regexp.MustCompile(`^\(*((".*")|(\S*)) IS NOT NULL\)*$`)
)

func isValidNotNullCC(cc schema.CheckConstraint) bool {
	if len(cc.KeyColumns) != 1 {
		return false
	}
	if !cc.IsValid {
		return false
	}
	return isNotNullCCRegex.MatchString(cc.Expression)
}

func buildTempNotNullConstraint(colDiff columnDiff) (schema.CheckConstraint, error) {
	uuid, err := pgidentifier.RandomUUID()
	if err != nil {
		return schema.CheckConstraint{}, fmt.Errorf("generating uuid: %w", err)
	}
	return schema.CheckConstraint{
		Name:               fmt.Sprintf("%snn_%s", tmpObjNamePrefix, uuid),
		KeyColumns:         []string{colDiff.new.Name},
		Expression:         fmt.Sprintf("%s IS NOT NULL", schema.EscapeIdentifier(colDiff.new.Name)),
		IsValid:            true,
		IsInheritable:      true,
		DependsOnFunctions: nil,
	}, nil
}

type columnSQLVertexGenerator struct {
	tableName schema.SchemaQualifiedName
}

func newColumnSQLVertexGenerator(tableName schema.SchemaQualifiedName) sqlVertexGenerator[schema.Column, columnDiff] {
	return &columnSQLVertexGenerator{tableName: tableName}
}

func (csg *columnSQLVertexGenerator) Add(column schema.Column) (*partialSQLGraph, error) {
	partialGraph := newPartialSQLGraph()

	columnDef, err := buildColumnDefinition(column)
	if err != nil {
		return partialGraph, fmt.Errorf("building column definition: %w", err)
	}

	partialGraph.addVertex(sqlVertex{
		id: buildColumnVertexId(csg.tableName, column.Name, diffTypeAddAlter),
		statements: []Statement{{
			DDL:         fmt.Sprintf("%s ADD COLUMN %s", alterTablePrefix(csg.tableName), columnDef),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		}},
		priority: sqlPrioritySooner,
	})
	partialGraph.addDependency(csg.getAddAlterDependencies(column)...)

	return partialGraph, nil
}

func (csg *columnSQLVertexGenerator) Delete(column schema.Column) (*partialSQLGraph, error) {
	partialGraph := newPartialSQLGraph()

	partialGraph.addVertex(sqlVertex{
		id: buildColumnVertexId(csg.tableName, column.Name, diffTypeDelete),
		statements: []Statement{{
			DDL:         fmt.Sprintf("%s DROP COLUMN %s", alterTablePrefix(csg.tableName), schema.EscapeIdentifier(column.Name)),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
			Hazards: []MigrationHazard{
				{
					Type:    MigrationHazardTypeDeletesData,
					Message: "Deletes all values in the column",
				},
			},
		}},
		priority: sqlPriorityLater,
	})

	return partialGraph, nil
}

func (csg *columnSQLVertexGenerator) Alter(diff columnDiff) (*partialSQLGraph, error) {
	partialGraph := newPartialSQLGraph()

	if diff.oldOrdering != diff.newOrdering {
		return nil, fmt.Errorf("old=%d; new=%d: %w", diff.oldOrdering, diff.newOrdering, ErrColumnOrderingChanged)
	}
	oldColumn, newColumn := diff.old, diff.new
	var stmts []Statement
	alterColumnPrefix := fmt.Sprintf("%s ALTER COLUMN %s", alterTablePrefix(csg.tableName), schema.EscapeIdentifier(newColumn.Name))

	// Adding a "NOT NULL" constraint must come before updating a column to be an identity column, otherwise
	// the add statement will fail because a column must be non-nullable to become an identity column.
	if oldColumn.IsNullable != newColumn.IsNullable && !newColumn.IsNullable {
		stmts = append(stmts, Statement{
			DDL:         fmt.Sprintf("%s SET NOT NULL", alterColumnPrefix),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		})
	}

	updateIdentityStmts, err := csg.buildUpdateIdentityStatements(oldColumn, newColumn)
	if err != nil {
		return nil, fmt.Errorf("building update identity statements: %w", err)
	}
	stmts = append(stmts, updateIdentityStmts...)

	// Removing a "NOT NULL" constraint must come after updating a column to no longer be an identity column, otherwise
	// the "DROP NOT NULL" statement will fail because the column will still be an identity column.
	if oldColumn.IsNullable != newColumn.IsNullable && newColumn.IsNullable {
		stmts = append(stmts, Statement{
			DDL:         fmt.Sprintf("%s DROP NOT NULL", alterColumnPrefix),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		})
	}

	if len(oldColumn.Default) > 0 && len(newColumn.Default) == 0 {
		// Drop the default before type conversion. This will allow type conversions
		// between incompatible types if the previous column has a default and the new column is dropping its default
		stmts = append(stmts, Statement{
			DDL:         fmt.Sprintf("%s DROP DEFAULT", alterColumnPrefix),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		})
	}

	if !strings.EqualFold(oldColumn.Type, newColumn.Type) ||
		!strings.EqualFold(oldColumn.Collation.GetFQEscapedName(), newColumn.Collation.GetFQEscapedName()) {
		stmts = append(stmts,
			[]Statement{
				csg.generateTypeTransformationStatement(
					diff.new,
					oldColumn.Type,
					newColumn.Type,
					newColumn.Collation,
				),
				// When "SET TYPE" is used to alter a column, that column's statistics are removed, which could
				// affect query plans. In order to mitigate the effect on queries, re-generate the statistics for the
				// column before continuing with the migration.
				{
					DDL:         fmt.Sprintf("ANALYZE %s (%s)", csg.tableName.GetFQEscapedName(), schema.EscapeIdentifier(newColumn.Name)),
					Timeout:     statementTimeoutAnalyzeColumn,
					LockTimeout: lockTimeoutDefault,
					Hazards: []MigrationHazard{
						{
							Type: MigrationHazardTypeImpactsDatabasePerformance,
							Message: "Running analyze will read rows from the table, putting increased load " +
								"on the database and consuming database resources. It won't prevent reads/writes to " +
								"the table, but it could affect performance when executing queries.",
						},
					},
				},
			}...)
	}

	if oldColumn.Default != newColumn.Default && len(newColumn.Default) > 0 {
		// Set the default after the type conversion. This will allow type conversions
		// between incompatible types if the previous column has no default and the new column has a default
		stmts = append(stmts, Statement{
			DDL:         fmt.Sprintf("%s SET DEFAULT %s", alterColumnPrefix, newColumn.Default),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		})
	}

	// todo: bplunkett - build column sql vertex?
	partialGraph.addVertex(sqlVertex{
		id:         buildColumnVertexId(csg.tableName, newColumn.Name, diffTypeAddAlter),
		statements: stmts,
		priority:   sqlPrioritySooner,
	})
	partialGraph.addDependency(csg.getAddAlterDependencies(newColumn)...)

	return partialGraph, nil
}

func (csg *columnSQLVertexGenerator) generateTypeTransformationStatement(
	col schema.Column,
	oldType string,
	newType string,
	newTypeCollation schema.SchemaQualifiedName,
) Statement {
	if strings.EqualFold(oldType, "bigint") &&
		strings.EqualFold(newType, "timestamp without time zone") {
		return Statement{
			DDL: fmt.Sprintf("%s SET DATA TYPE %s using to_timestamp(%s / 1000)",
				csg.alterColumnPrefix(col),
				newType,
				schema.EscapeIdentifier(col.Name),
			),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
			Hazards: []MigrationHazard{{
				Type: MigrationHazardTypeAcquiresAccessExclusiveLock,
				Message: "This will completely lock the table while the data is being " +
					"re-written for a duration of time that scales with the size of your data. " +
					"The values previously stored as BIGINT will be translated into a " +
					"TIMESTAMP value via the PostgreSQL to_timestamp() function. This " +
					"translation will assume that the values stored in BIGINT represent a " +
					"millisecond epoch value.",
			}},
		}
	}

	collationModifier := ""
	if !newTypeCollation.IsEmpty() {
		collationModifier = fmt.Sprintf("COLLATE %s ", newTypeCollation.GetFQEscapedName())
	}

	return Statement{
		DDL: fmt.Sprintf("%s SET DATA TYPE %s %susing %s::%s",
			csg.alterColumnPrefix(col),
			newType,
			collationModifier,
			schema.EscapeIdentifier(col.Name),
			newType,
		),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards: []MigrationHazard{{
			Type: MigrationHazardTypeAcquiresAccessExclusiveLock,
			Message: "This will completely lock the table while the data is being re-written. " +
				"The duration of this conversion depends on if the type conversion is trivial " +
				"or not. A non-trivial conversion will require a table rewrite. A trivial " +
				"conversion is one where the binary values are coercible and the column " +
				"contents are not changing.",
		}},
	}
}

func (csg *columnSQLVertexGenerator) buildUpdateIdentityStatements(old, new schema.Column) ([]Statement, error) {
	if cmp.Equal(old.Identity, new.Identity) {
		return nil, nil
	}

	// Drop the old identity
	if new.Identity == nil {
		// ALTER [ COLUMN ] column_name DROP IDENTITY [ IF EXISTS ]
		return []Statement{{
			DDL:         fmt.Sprintf("%s DROP IDENTITY", csg.alterColumnPrefix(old)),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		}}, nil
	}

	// Add the new identity
	if old.Identity == nil {
		def, err := buildColumnIdentityDefinition(*new.Identity)
		if err != nil {
			return nil, fmt.Errorf("building column identity definition: %w", err)
		}
		// ALTER [ COLUMN ] column_name ADD GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( sequence_options ) ]
		return []Statement{{
			DDL:         fmt.Sprintf("%s ADD %s", csg.alterColumnPrefix(new), def),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		}}, nil
	}

	// Alter the existing identity
	var modifications []string
	if old.Identity.Type != new.Identity.Type {
		typeModifier, err := columnIdentityTypeToModifier(new.Identity.Type)
		if err != nil {
			return nil, fmt.Errorf("column identity type modifier: %w", err)
		}
		modifications = append(modifications, fmt.Sprintf("\tSET GENERATED %s", typeModifier))
	}
	if old.Identity.Increment != new.Identity.Increment {
		modifications = append(modifications, fmt.Sprintf("\tSET INCREMENT BY %d", new.Identity.Increment))
	}
	if old.Identity.MinValue != new.Identity.MinValue {
		modifications = append(modifications, fmt.Sprintf("\tSET MINVALUE %d", new.Identity.MinValue))
	}
	if old.Identity.MaxValue != new.Identity.MaxValue {
		modifications = append(modifications, fmt.Sprintf("\tSET MAXVALUE %d", new.Identity.MaxValue))
	}
	if old.Identity.StartValue != new.Identity.StartValue {
		modifications = append(modifications, fmt.Sprintf("\tSET START %d", new.Identity.StartValue))
	}
	if old.Identity.CacheSize != new.Identity.CacheSize {
		modifications = append(modifications, fmt.Sprintf("\tSET CACHE %d", new.Identity.CacheSize))
	}
	if old.Identity.Cycle != new.Identity.Cycle {
		cycleModifier := ""
		if !new.Identity.Cycle {
			cycleModifier = "NO "
		}
		modifications = append(modifications, fmt.Sprintf("\tSET %sCYCLE", cycleModifier))
	}
	// ALTER [ COLUMN ] column_name { SET GENERATED { ALWAYS | BY DEFAULT } | SET sequence_option | RESTART [ [ WITH ] restart ] } [...]
	return []Statement{{
		DDL:         fmt.Sprintf("%s\n%s", csg.alterColumnPrefix(new), strings.Join(modifications, "\n")),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (csg *columnSQLVertexGenerator) alterColumnPrefix(col schema.Column) string {
	return fmt.Sprintf("%s ALTER COLUMN %s", alterTablePrefix(csg.tableName), schema.EscapeIdentifier(col.Name))
}

func buildColumnVertexId(tableName schema.SchemaQualifiedName, columnName string, diffType diffType) schemaObjNestedGraphSqlVertexId {
	return buildSchemaObjVertexId("column", fmt.Sprintf("%s:%s", tableName.GetFQEscapedName(), columnName), diffType)
}

func (csg *columnSQLVertexGenerator) getAddAlterDependencies(col schema.Column) []dependency {
	return []dependency{
		mustRun(buildColumnVertexId(csg.tableName, col.Name, diffTypeDelete)).before(buildColumnVertexId(csg.tableName, col.Name, diffTypeAddAlter)),
	}
}

type renameConflictingIndexSQLVertexGenerator struct {
	// indexesInOldSchemaByName is a map of index name to the index in the old schema
	// It is used to identify if an index has been re-created
	oldSchemaIndexesByName map[string]schema.Index

	indexRenamesByOldName map[string]schema.SchemaQualifiedName

	sqlVertexGenerator[schema.Index, indexDiff]
}

func newRenameConflictingIndexSQLVertexGenerator(oldSchemaIndexesByName map[string]schema.Index) *renameConflictingIndexSQLVertexGenerator {
	rsg := &renameConflictingIndexSQLVertexGenerator{
		oldSchemaIndexesByName: oldSchemaIndexesByName,
		indexRenamesByOldName:  make(map[string]schema.SchemaQualifiedName),
	}
	generator := legacyToNewSqlVertexGenerator[schema.Index, indexDiff](rsg)
	rsg.sqlVertexGenerator = generator
	return rsg
}

func (rsg *renameConflictingIndexSQLVertexGenerator) Add(index schema.Index) ([]Statement, error) {
	if oldIndex, indexIsBeingRecreated := rsg.oldSchemaIndexesByName[index.GetName()]; !indexIsBeingRecreated {
		return nil, nil
	} else if oldIndex.IsPk() && index.IsPk() {
		// Don't bother renaming if both are primary keys, since the new index will need to be created after the old
		// index because we can't have two primary keys at the same time.
		//
		// To make changing primary keys index-gap free (mostly online), we could build the underlying new primary key index,
		// drop the old primary constraint (and index), and then add the primary key constraint using the new index.
		// This would require us to split primary key constraint SQL generation from index SQL generation
		return nil, nil
	}

	newName, err := rsg.generateNonConflictingName(index)
	if err != nil {
		return nil, fmt.Errorf("generating non-conflicting name: %w", err)
	}

	newFQName := schema.SchemaQualifiedName{
		SchemaName:  index.OwningTable.SchemaName,
		EscapedName: schema.EscapeIdentifier(newName),
	}
	rsg.indexRenamesByOldName[index.GetName()] = newFQName

	return []Statement{{
		DDL:         fmt.Sprintf("ALTER INDEX %s RENAME TO %s", index.GetSchemaQualifiedName().GetFQEscapedName(), newFQName.EscapedName),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (rsg *renameConflictingIndexSQLVertexGenerator) generateNonConflictingName(index schema.Index) (string, error) {
	uuid, err := pgidentifier.RandomUUID()
	if err != nil {
		return "", fmt.Errorf("generating RandomUUID: %w", err)
	}

	prefix := fmt.Sprintf("%sidx_", tmpObjNamePrefix)
	suffix := fmt.Sprintf("_%s", uuid)
	prefixAndSuffixSize := len(prefix) + len(suffix)

	idxNameTruncationIdx := len(index.Name)
	if len(index.Name) > maxPostgresIdentifierSize-prefixAndSuffixSize {
		idxNameTruncationIdx = maxPostgresIdentifierSize - prefixAndSuffixSize
	}

	return fmt.Sprintf("%s%s%s", prefix, index.Name[:idxNameTruncationIdx], suffix), nil
}

// rename gets the rename for the index if it eixsts, otherwise it returns an empty stringa nd false
func (rsg *renameConflictingIndexSQLVertexGenerator) rename(index schema.Index) (schema.SchemaQualifiedName, bool) {
	rename, ok := rsg.indexRenamesByOldName[index.GetName()]
	if !ok {
		return schema.SchemaQualifiedName{}, false
	}
	return rename, true
}

func (rsg *renameConflictingIndexSQLVertexGenerator) Delete(_ schema.Index) ([]Statement, error) {
	return nil, nil
}

func (rsg *renameConflictingIndexSQLVertexGenerator) Alter(_ indexDiff) ([]Statement, error) {
	return nil, nil
}

func (*renameConflictingIndexSQLVertexGenerator) GetSQLVertexId(index schema.Index, diffType diffType) schemaObjNestedGraphSqlVertexId {
	return buildRenameConflictingIndexVertexId(index.GetSchemaQualifiedName(), diffType)
}

func (rsg *renameConflictingIndexSQLVertexGenerator) GetAddAlterDependencies(_, _ schema.Index) ([]dependency, error) {
	return nil, nil
}

func (rsg *renameConflictingIndexSQLVertexGenerator) GetDeleteDependencies(_ schema.Index) ([]dependency, error) {
	return nil, nil
}

func buildRenameConflictingIndexVertexId(indexName schema.SchemaQualifiedName, diffType diffType) schemaObjNestedGraphSqlVertexId {
	return buildSchemaObjVertexId("indexrename", indexName.GetName(), diffType)
}

type indexSQLVertexGenerator struct {
	// deletedTablesByName is a map of table name to the deleted tables (and partitions)
	deletedTablesByName map[string]schema.Table
	// addedTablesByName is a map of table name to the new tables (and partitions)
	// This is used to identify if hazards are necessary
	addedTablesByName map[string]schema.Table
	// tablesInNewSchemaByName is a map of table name to tables (and partitions) in the new schema.
	// These tables are not necessarily new. This is used to identify if the table is partitioned
	tablesInNewSchemaByName map[string]schema.Table
	// indexesInNewSchemaByName is a map of index name to the index
	// This is used to identify the parent index is a primary key
	indexesInNewSchemaByName map[string]schema.Index

	// renameSQLVertexGenerator is used to find renames
	renameSQLVertexGenerator *renameConflictingIndexSQLVertexGenerator
	// attachPartitionSQLVertexGenerator is used to find if a partition will be attached after an index builds
	attachPartitionSQLVertexGenerator *attachPartitionSQLVertexGenerator
}

func (isg *indexSQLVertexGenerator) Add(index schema.Index) ([]Statement, error) {
	stmts, err := isg.addIdxStmtsWithHazards(index)
	if err != nil {
		return stmts, err
	}

	if _, isNewTable := isg.addedTablesByName[index.OwningTable.GetName()]; isNewTable {
		stmts = stripMigrationHazards(stmts...)
	}
	return stmts, nil
}

func (isg *indexSQLVertexGenerator) addIdxStmtsWithHazards(index schema.Index) ([]Statement, error) {
	if index.IsInvalid {
		return nil, fmt.Errorf("can't create an invalid index: %w", ErrNotImplemented)
	}

	var stmts []Statement
	var createIdxStmtHazards []MigrationHazard

	createIdxStmt := string(index.GetIndexDefStmt)
	createIdxStmtTimeout := statementTimeoutDefault
	if isOnPartitionedTable, err := isg.isOnPartitionedTable(index); err != nil {
		return nil, err
	} else if isOnPartitionedTable {
		if index.Constraint != nil {
			// If it's associated with a constraint, the index will be created implicitly through the constraint
			// If we attempt to create the index and the primary key, it will throw an error about the relation already existing.
			// If the table is the base table of a partitioned table, the constraint should "ONLY" be added to the base
			//table. We can then concurrently build all of the partitioned indexes and attach them.
			// Without "ONLY", all the partitioned indexes will be automatically built
			return []Statement{{
				DDL:         fmt.Sprintf("ALTER TABLE ONLY %s ADD CONSTRAINT %s %s", index.OwningTable.GetFQEscapedName(), index.Constraint.EscapedConstraintName, index.Constraint.ConstraintDef),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
			}}, nil
		}
	} else if !isOnPartitionedTable {
		// Only indexes on non-partitioned tables can be created concurrently
		concurrentCreateIdxStmt, err := index.GetIndexDefStmt.ToCreateIndexConcurrently()
		if err != nil {
			return nil, fmt.Errorf("modifying index def statement to concurrently: %w", err)
		}
		createIdxStmt = concurrentCreateIdxStmt
		createIdxStmtHazards = append(createIdxStmtHazards, MigrationHazard{
			Type: MigrationHazardTypeIndexBuild,
			Message: "This might affect database performance. " +
				"Concurrent index builds require a non-trivial amount of CPU, potentially affecting database performance. " +
				"They also can take a while but do not lock out writes.",
		})
		createIdxStmtTimeout = statementTimeoutConcurrentIndexBuild
	}

	stmts = append(stmts, Statement{
		DDL:         createIdxStmt,
		Timeout:     createIdxStmtTimeout,
		LockTimeout: lockTimeoutDefault,
		Hazards:     createIdxStmtHazards,
	})

	if index.Constraint != nil {
		addConstraintStmt, err := isg.addIndexConstraint(index)
		if err != nil {
			return nil, fmt.Errorf("generating add constraint statement: %w", err)
		}
		stmts = append(stmts, addConstraintStmt)
	}

	if index.ParentIdx != nil && isg.attachPartitionSQLVertexGenerator.isPartitionAlreadyAttachedBeforeIndexBuilds(index.OwningTable) {
		// Only attach the index if the index is built after the table is partitioned. If the partition
		// hasn't already been attached, the index/constraint will be automatically attached when the table partition is
		// attached
		stmts = append(stmts, buildAttachIndex(index))
	}

	return stmts, nil
}

func (isg *indexSQLVertexGenerator) Delete(index schema.Index) ([]Statement, error) {
	_, tableWasDeleted := isg.deletedTablesByName[index.OwningTable.GetName()]
	// An index will be dropped if its owning table is dropped.
	if tableWasDeleted {
		return nil, nil
	}

	if index.ParentIdx != nil {
		if index.Constraint != nil && index.Constraint.IsLocal {
			// This creates a weird circular dependency that Postgres doesn't have any easy way of out.
			// You can't drop the parent index without dropping the local constraint. But if you try dropping the local
			// constraint, it will try to drop the partition of the index without dropping the shared index, which errors
			// because an individual index partition cannot be dropped. Only the whole index can be dropped
			// A workaround could be implemented with "CASCADE" if this ends up blocking users
			return nil, fmt.Errorf("dropping an index partition that backs a local constraint is not supported: %w", ErrNotImplemented)
		}
		// A partition of an index will be dropped when the parent index is dropped
		return nil, nil
	}

	// An index used by a primary key constraint/unique constraint cannot be dropped concurrently
	if index.Constraint != nil {
		// The index has been potentially renamed, which causes the constraint to be renamed. Use the updated name
		escapedConstraintName := index.Constraint.EscapedConstraintName
		if rename, hasRename := isg.renameSQLVertexGenerator.rename(index); hasRename {
			escapedConstraintName = rename.EscapedName
		}

		// Dropping the constraint will automatically drop the index. There is no way to drop
		// the constraint without dropping the index
		return []Statement{
			{
				DDL: dropConstraintDDL(index.OwningTable, escapedConstraintName),

				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
				Hazards: []MigrationHazard{
					migrationHazardIndexDroppedAcquiresLock,
					migrationHazardIndexDroppedQueryPerf,
				},
			},
		}, nil
	}

	var dropIndexStmtHazards []MigrationHazard
	concurrentlyModifier := "CONCURRENTLY "
	dropIndexStmtTimeout := statementTimeoutConcurrentIndexDrop
	if isOnPartitionedTable, err := isg.isOnPartitionedTable(index); err != nil {
		return nil, err
	} else if isOnPartitionedTable {
		// Currently, postgres has no good way of dropping an index partition concurrently
		concurrentlyModifier = ""
		dropIndexStmtTimeout = statementTimeoutDefault
		// Technically, CONCURRENTLY also locks the table, but it waits for an "opportunity" to lock
		// We will omit the locking hazard of concurrent drops for now
		dropIndexStmtHazards = append(dropIndexStmtHazards, migrationHazardIndexDroppedAcquiresLock)
	}

	// The index has been potentially renamed. Use the updated name
	indexName := index.GetSchemaQualifiedName()
	if rename, hasRename := isg.renameSQLVertexGenerator.rename(index); hasRename {
		indexName = rename
	}

	return []Statement{{
		DDL:         fmt.Sprintf("DROP INDEX %s%s", concurrentlyModifier, indexName.GetFQEscapedName()),
		Timeout:     dropIndexStmtTimeout,
		LockTimeout: lockTimeoutDefault,
		Hazards:     append(dropIndexStmtHazards, migrationHazardIndexDroppedQueryPerf),
	}}, nil
}

func (isg *indexSQLVertexGenerator) Alter(diff indexDiff) ([]Statement, error) {
	var stmts []Statement

	if isOnPartitionedTable, err := isg.isOnPartitionedTable(diff.new); err != nil {
		return nil, err
	} else if isOnPartitionedTable && diff.old.IsInvalid && !diff.new.IsInvalid {
		// If the index is a partitioned index, it can be made valid automatically by attaching the index partitions
		diff.old.IsInvalid = diff.new.IsInvalid
	}

	if diff.old.Constraint == nil && diff.new.Constraint != nil {
		addConstraintStmt, err := isg.addIndexConstraint(diff.new)
		if err != nil {
			return nil, fmt.Errorf("generating add constraint statement: %w", err)
		}
		stmts = append(stmts, addConstraintStmt)
		diff.old.Constraint = diff.new.Constraint
	}

	if diff.old.ParentIdx == nil && diff.new.ParentIdx != nil {
		stmts = append(stmts, buildAttachIndex(diff.new))
		diff.old.ParentIdx = diff.new.ParentIdx
	}

	if !cmp.Equal(diff.old, diff.new) {
		return nil, fmt.Errorf("index diff could not be resolved %s", cmp.Diff(diff.old, diff.new))
	}

	return stmts, nil
}

func (isg *indexSQLVertexGenerator) isOnPartitionedTable(index schema.Index) (bool, error) {
	return isOnPartitionedTable(isg.tablesInNewSchemaByName, index)
}

// Returns true if the table the index belongs too is partitioned. If the table is a partition of a
// partitioned table, this will always return false
func isOnPartitionedTable(tablesInNewSchemaByName map[string]schema.Table, index schema.Index) (bool, error) {
	if owningTable, ok := tablesInNewSchemaByName[index.OwningTable.GetName()]; !ok {
		return false, fmt.Errorf("could not find table in new schema with name %s", index.OwningTable.GetName())
	} else {
		return owningTable.IsPartitioned(), nil
	}
}

func (isg *indexSQLVertexGenerator) addIndexConstraint(index schema.Index) (Statement, error) {
	sqlConstraintType, err := constraintTypeAsSQL(index.Constraint.Type)
	if err != nil {
		return Statement{}, fmt.Errorf("getting constraint type as SQL: %w", err)
	}
	return Statement{
		DDL: fmt.Sprintf("%s %s USING INDEX %s",
			addConstraintPrefix(index.OwningTable, index.Constraint.EscapedConstraintName),
			sqlConstraintType,
			index.GetSchemaQualifiedName().EscapedName),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}, nil
}

func constraintTypeAsSQL(constraintType schema.IndexConstraintType) (string, error) {
	switch constraintType {
	case "p":
		return "PRIMARY KEY", nil
	case "u":
		return "UNIQUE", nil
	default:
		return "", fmt.Errorf("unknown/unsupported index constraint type: %s", constraintType)
	}
}

func buildAttachIndex(index schema.Index) Statement {
	return Statement{
		DDL:         fmt.Sprintf("ALTER INDEX %s ATTACH PARTITION %s", index.ParentIdx.GetFQEscapedName(), index.GetSchemaQualifiedName().GetFQEscapedName()),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}
}

func (*indexSQLVertexGenerator) GetSQLVertexId(index schema.Index, diffType diffType) schemaObjNestedGraphSqlVertexId {
	return buildIndexVertexId(index.GetSchemaQualifiedName(), diffType)
}

func buildIndexVertexId(name schema.SchemaQualifiedName, diffType diffType) schemaObjNestedGraphSqlVertexId {
	return buildSchemaObjVertexId("index", name.GetFQEscapedName(), diffType)
}

func (isg *indexSQLVertexGenerator) GetAddAlterDependencies(index, _ schema.Index) ([]dependency, error) {
	dependencies := []dependency{
		mustRun(isg.GetSQLVertexId(index, diffTypeAddAlter)).after(buildTableVertexId(index.OwningTable, diffTypeAddAlter, subgraphMarkerStart)),
		// To allow for online changes to indexes, rename the older version of the index (if it exists) before the new version is added
		mustRun(isg.GetSQLVertexId(index, diffTypeAddAlter)).after(buildRenameConflictingIndexVertexId(index.GetSchemaQualifiedName(), diffTypeAddAlter)),
	}

	if index.ParentIdx != nil {
		// Partitions of indexes must be created after the parent index is created
		dependencies = append(dependencies,
			mustRun(isg.GetSQLVertexId(index, diffTypeAddAlter)).after(buildIndexVertexId(*index.ParentIdx, diffTypeAddAlter)))
	}

	return dependencies, nil
}

func (isg *indexSQLVertexGenerator) GetDeleteDependencies(index schema.Index) ([]dependency, error) {
	dependencies := []dependency{
		mustRun(isg.GetSQLVertexId(index, diffTypeDelete)).after(buildTableVertexId(index.OwningTable, diffTypeDelete, subgraphMarkerStart)),
		// Drop the index after it has been potentially renamed
		mustRun(isg.GetSQLVertexId(index, diffTypeDelete)).after(buildRenameConflictingIndexVertexId(index.GetSchemaQualifiedName(), diffTypeAddAlter)),
	}

	if index.ParentIdx != nil {
		// Since dropping the parent index will cause the partition of the index to drop, the parent drop should come
		// before
		dependencies = append(dependencies,
			mustRun(isg.GetSQLVertexId(index, diffTypeDelete)).after(buildIndexVertexId(*index.ParentIdx, diffTypeDelete)))
	}
	dependencies = append(dependencies, isg.addDepsOnTableAddAlterIfNecessary(index)...)

	return dependencies, nil
}

func (isg *indexSQLVertexGenerator) addDepsOnTableAddAlterIfNecessary(index schema.Index) []dependency {
	// This could be cleaner if start sorting columns separately in the graph
	parentTable, ok := isg.tablesInNewSchemaByName[index.OwningTable.GetName()]
	if !ok {
		// If the parent table is deleted, we don't need to worry about making the index statement come
		// before any alters
		return nil
	}

	// These dependencies will force the index deletion statement to come before the table AddAlter
	addAlterColumnDeps := []dependency{
		mustRun(isg.GetSQLVertexId(index, diffTypeDelete)).before(buildTableVertexId(index.OwningTable, diffTypeAddAlter, subgraphMarkerStart)),
	}
	if parentTable.ParentTable != nil {
		// If the table is partitioned, columns modifications occur on the base table not the children. Thus, we
		// need the dependency to also be on the parent table add/alter statements
		addAlterColumnDeps = append(
			addAlterColumnDeps,
			mustRun(isg.GetSQLVertexId(index, diffTypeDelete)).before(buildTableVertexId(*parentTable.ParentTable, diffTypeAddAlter, subgraphMarkerStart)),
		)
	}

	// If the parent table still exists and the index is a primary key, we should drop the PK index before
	// any statements associated with altering the table run. This is important for changing the nullability of
	// columns
	if index.IsPk() {
		return addAlterColumnDeps
	}

	parentTableColumnsByName := buildSchemaObjByNameMap(parentTable.Columns)
	for _, idxColumn := range index.Columns {
		// We need to force the index drop to come before the statements to drop columns. Otherwise, the columns
		// drops will force the index to drop non-concurrently
		if _, columnStillPresent := parentTableColumnsByName[idxColumn]; !columnStillPresent {
			return addAlterColumnDeps
		}
	}

	// Otherwise, we can drop the index whenever we want.
	return nil
}

type checkConstraintSQLVertexGenerator struct {
	tableName              schema.SchemaQualifiedName
	newSchemaColumnsByName map[string]schema.Column
	oldSchemaColumnsByName map[string]schema.Column
	addedColumnsByName     map[string]schema.Column
	deletedColumnsByName   map[string]schema.Column
	isNewTable             bool
}

func (csg *checkConstraintSQLVertexGenerator) Add(con schema.CheckConstraint) ([]Statement, error) {
	// UDF's in check constraints are a bad idea. Check constraints are not re-validated
	// if the UDF changes, so it's not really a safe practice. We won't support it for now
	if len(con.DependsOnFunctions) > 0 {
		return nil, fmt.Errorf("check constraints that depend on UDFs: %w", ErrNotImplemented)
	}

	var stmts []Statement
	if !con.IsValid || csg.isNewTable {
		stmts = append(stmts, csg.createCheckConstraintStatement(con))
	} else {
		// If the check constraint is not on a new table and is marked as valid, we should:
		// 1. Build the constraint as invalid
		// 2. Validate the constraint
		con.IsValid = false
		stmts = append(stmts, csg.createCheckConstraintStatement(con))
		stmts = append(stmts, validateConstraintStatement(csg.tableName, schema.EscapeIdentifier(con.Name)))
	}

	return stmts, nil
}

func (csg *checkConstraintSQLVertexGenerator) createCheckConstraintStatement(con schema.CheckConstraint) Statement {
	var hazards []MigrationHazard
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("%s CHECK(%s)",
		addConstraintPrefix(csg.tableName, schema.EscapeIdentifier(con.Name)), con.Expression))
	if !con.IsInheritable {
		sb.WriteString(" NO INHERIT")
	}

	if !con.IsValid {
		sb.WriteString(" NOT VALID")
	} else {
		hazards = append(hazards, MigrationHazard{
			Type: MigrationHazardTypeAcquiresAccessExclusiveLock,
			Message: "This will lock reads and writes to the owning table while the constraint is being added. " +
				"Instead, consider adding the constraint as NOT VALID and validating it later.",
		})
	}

	return Statement{
		DDL:         sb.String(),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     hazards,
	}
}

func (csg *checkConstraintSQLVertexGenerator) Delete(con schema.CheckConstraint) ([]Statement, error) {
	// We won't support deleting check constraints depending on UDF's to align with not supporting adding check
	// constraints that depend on UDF's
	if len(con.DependsOnFunctions) > 0 {
		return nil, fmt.Errorf("check constraints that depend on UDFs: %w", ErrNotImplemented)
	}

	return []Statement{{
		DDL:         dropConstraintDDL(csg.tableName, schema.EscapeIdentifier(con.Name)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (csg *checkConstraintSQLVertexGenerator) Alter(diff checkConstraintDiff) ([]Statement, error) {
	oldCopy := diff.old

	var stmts []Statement
	if !diff.old.IsValid && diff.new.IsValid {
		stmts = append(stmts, validateConstraintStatement(csg.tableName, schema.EscapeIdentifier(diff.new.Name)))
		oldCopy.IsValid = diff.new.IsValid
	}

	// Normalize the key columns, since order does not matter.
	sort.Strings(oldCopy.KeyColumns)
	newCopy := diff.new
	sort.Strings(newCopy.KeyColumns)
	if !cmp.Equal(oldCopy, newCopy) {
		// Technically, we could support altering expression, but I don't see the use case for it. it would require more test
		// cases than force re-adding it, and I'm not convinced it unlocks any functionality
		return nil, fmt.Errorf("altering check constraint to resolve the following diff %s: %w", cmp.Diff(oldCopy, diff.new), ErrNotImplemented)
	} else if len(diff.old.DependsOnFunctions) > 0 || len(diff.new.DependsOnFunctions) > 0 {
		return nil, fmt.Errorf("check constraints that depend on UDFs: %w", ErrNotImplemented)
	}

	return stmts, nil
}

func (*checkConstraintSQLVertexGenerator) GetSQLVertexId(con schema.CheckConstraint, diffType diffType) schemaObjNestedGraphSqlVertexId {
	return buildSchemaObjVertexId("checkconstraint", con.Name, diffType)
}

func (csg *checkConstraintSQLVertexGenerator) GetAddAlterDependencies(con, _ schema.CheckConstraint) ([]dependency, error) {
	deps := []dependency{
		mustRun(csg.GetSQLVertexId(con, diffTypeDelete)).before(csg.GetSQLVertexId(con, diffTypeAddAlter)),
	}

	targetColumns, err := getTargetColumns(con.KeyColumns, csg.newSchemaColumnsByName)
	if err != nil {
		return nil, fmt.Errorf("getting target columns: %w", err)
	}

	isOnValidNotNullPreExistingColumn := false
	if len(targetColumns) == 1 {
		targetColumn := targetColumns[0]
		if _, ok := csg.addedColumnsByName[targetColumn.Name]; !ok && isValidNotNullCC(con) {
			isOnValidNotNullPreExistingColumn = true
		}
	}

	if isOnValidNotNullPreExistingColumn {
		// If the NOT NULL check constraint is on a pre-existing column, then we should ensure it is added before
		// the column alter.
		deps = append(deps, mustRun(csg.GetSQLVertexId(con, diffTypeAddAlter)).before(buildColumnVertexId(csg.tableName, targetColumns[0].Name, diffTypeAddAlter)))
	} else {
		for _, tc := range targetColumns {
			deps = append(deps, mustRun(csg.GetSQLVertexId(con, diffTypeAddAlter)).after(buildColumnVertexId(csg.tableName, tc.Name, diffTypeAddAlter)))
		}
	}
	return deps, nil
}

func (csg *checkConstraintSQLVertexGenerator) GetDeleteDependencies(con schema.CheckConstraint) ([]dependency, error) {
	var deps []dependency

	targetColumns, err := getTargetColumns(con.KeyColumns, csg.oldSchemaColumnsByName)
	if err != nil {
		return nil, fmt.Errorf("getting target columns: %w", err)
	}

	// If it's a not-null check constraint, we can drop the check constraint whenever convenient, i.e., after
	// the column has been altered because `NOT NULL` does not depend on the type of the column. It is important we
	// delete the NOT NULL check constraint AFTER the column is altered because we want to ensure any `SET NULL` alters
	// are backed with a check constraint.
	//
	// For all other check constraints, they can rely on the type of the column. Thus, we should drop these
	// check constraint before any columns are altered because the new type might not be compatible with the old
	// check constraint.
	if isValidNotNullCC(con) {
		tc := targetColumns[0]
		if _, ok := csg.deletedColumnsByName[tc.Name]; ok {
			// If the column is being deleted, we should drop the not null check constraint before the column is deleted.
			deps = append(deps, mustRun(csg.GetSQLVertexId(con, diffTypeDelete)).before(buildColumnVertexId(csg.tableName, tc.Name, diffTypeDelete)))
		} else {
			// Otherwise, we should drop the not null check constraint after the column is altered. This dependency
			// doesn't need to be explicitly, since our topological sort prioritizes adds/alters over deletes. Nevertheless,
			// we'll add it for clarity and to ensure that an error is returned if the delete is not placed after the alter.
			deps = append(deps, mustRun(csg.GetSQLVertexId(con, diffTypeDelete)).after(buildColumnVertexId(csg.tableName, tc.Name, diffTypeAddAlter)))
		}
	} else {
		for _, tc := range targetColumns {
			deps = append(deps, mustRun(csg.GetSQLVertexId(con, diffTypeDelete)).before(buildColumnVertexId(csg.tableName, tc.Name, diffTypeAddAlter)))
			// This is a weird quirk of our graph system, where if a -> b and b -> c and b does-not-exist, b will be
			// implicitly created s.t. a -> b -> c (https://github.com/stripe/pg-schema-diff/issues/84)
			//
			// In this case, "a" is the deletion of the check constraint, "b" is the deletion of
			// the column, and "c" is the alter/addition of the column. We do not want this behavior. We only want
			// a -> b -> c iff the column is being deleted.
			if _, ok := csg.deletedColumnsByName[tc.Name]; ok {
				deps = append(deps, mustRun(csg.GetSQLVertexId(con, diffTypeDelete)).before(buildColumnVertexId(csg.tableName, tc.Name, diffTypeDelete)))
			}
		}
	}

	return deps, nil
}

func getTargetColumns(targetColumnNames []string, columnsByName map[string]schema.Column) ([]schema.Column, error) {
	var targetColumns []schema.Column
	for _, name := range targetColumnNames {
		targetColumn, ok := columnsByName[name]
		if !ok {
			return nil, fmt.Errorf("could not find column with name %s", name)
		}
		targetColumns = append(targetColumns, targetColumn)
	}
	return targetColumns, nil
}

type attachPartitionSQLVertexGenerator struct {
	indexesInNewSchemaByTableName map[string][]schema.Index
	addedTablesByName             map[string]schema.Table

	// isPartitionAttachedAfterIdxBuildsByTableName is a map of table name to whether or not the table partition will be
	// attached after its indexes are built. This is useful for determining when indexes need to be attached
	isPartitionAttachedAfterIdxBuildsByTableName map[string]bool

	sqlVertexGenerator[schema.Table, tableDiff]
}

func newAttachPartitionSQLVertexGenerator(newSchemaIndexes []schema.Index, addedTables []schema.Table) *attachPartitionSQLVertexGenerator {
	asg := &attachPartitionSQLVertexGenerator{
		indexesInNewSchemaByTableName: buildIndexesByTableNameMap(newSchemaIndexes),
		addedTablesByName:             buildSchemaObjByNameMap(addedTables),

		isPartitionAttachedAfterIdxBuildsByTableName: make(map[string]bool),
	}
	sqlVertexGenerator := legacyToNewSqlVertexGenerator[schema.Table, tableDiff](asg)
	asg.sqlVertexGenerator = sqlVertexGenerator
	return asg
}

func (*attachPartitionSQLVertexGenerator) Add(table schema.Table) ([]Statement, error) {
	if table.ParentTable == nil {
		return nil, nil
	}

	return []Statement{{
		DDL:         fmt.Sprintf("%s ATTACH PARTITION %s %s", alterTablePrefix(*table.ParentTable), table.SchemaQualifiedName.GetFQEscapedName(), table.ForValues),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (*attachPartitionSQLVertexGenerator) Alter(_ tableDiff) ([]Statement, error) {
	return nil, nil
}

func (*attachPartitionSQLVertexGenerator) Delete(_ schema.Table) ([]Statement, error) {
	return nil, nil
}

func (*attachPartitionSQLVertexGenerator) GetSQLVertexId(table schema.Table, diffType diffType) schemaObjNestedGraphSqlVertexId {
	return buildSchemaObjVertexId("attachpartition", table.GetName(), diffType)
}

func (a *attachPartitionSQLVertexGenerator) GetAddAlterDependencies(table, old schema.Table) ([]dependency, error) {
	if table.ParentTable == nil || !cmp.Equal(old, schema.Table{}) {
		// The table is not partitioned or the table already exists. Skip building dependencies
		return nil, nil
	}

	deps := []dependency{
		mustRun(a.GetSQLVertexId(table, diffTypeAddAlter)).after(buildTableVertexId(table.SchemaQualifiedName, diffTypeAddAlter, subgraphMarkerStart)),
	}

	if _, baseTableIsNew := a.addedTablesByName[table.ParentTable.GetName()]; baseTableIsNew {
		// If the base table is new, we should force the partition to be attached before we build any non-local indexes.
		// This allows us to create fresh schemas where the base table has a PK but none of the children tables
		// have the PK (this is useful when creating the fresh database schema for migration validation)
		// If we attach the partition after the index is built, the index will be automatically built by Postgres
		for _, idx := range a.indexesInNewSchemaByTableName[table.ParentTable.GetName()] {
			deps = append(deps, mustRun(a.GetSQLVertexId(table, diffTypeAddAlter)).before(buildIndexVertexId(idx.GetSchemaQualifiedName(), diffTypeAddAlter)))
		}
		return deps, nil
	}

	a.isPartitionAttachedAfterIdxBuildsByTableName[table.GetName()] = true
	for _, idx := range a.indexesInNewSchemaByTableName[table.GetName()] {
		deps = append(deps, mustRun(a.GetSQLVertexId(table, diffTypeAddAlter)).after(buildIndexVertexId(idx.GetSchemaQualifiedName(), diffTypeAddAlter)))
	}
	return deps, nil
}

func (a *attachPartitionSQLVertexGenerator) isPartitionAlreadyAttachedBeforeIndexBuilds(table schema.SchemaQualifiedName) bool {
	return !a.isPartitionAttachedAfterIdxBuildsByTableName[table.GetName()]
}

func (a *attachPartitionSQLVertexGenerator) GetDeleteDependencies(_ schema.Table) ([]dependency, error) {
	return nil, nil
}

func buildForeignKeyConstraintDiff(fsg sqlVertexGenerator[schema.ForeignKeyConstraint, foreignKeyConstraintDiff], addedTablesByName map[string]schema.Table, old, new schema.ForeignKeyConstraint) (foreignKeyConstraintDiff, bool, error) {
	if _, isOnNewTable := addedTablesByName[new.OwningTable.GetName()]; isOnNewTable {
		// If the owning table is new, then it must be re-created (this occurs if the base table has been
		// re-created). In other words, a foreign key constraint must be re-created if the owning table or referenced
		// table is re-created
		return foreignKeyConstraintDiff{}, true, nil
	}
	if _, isReferencingNewTable := addedTablesByName[new.ForeignTable.GetName()]; isReferencingNewTable {
		// Same as above, but for the referenced table
		return foreignKeyConstraintDiff{}, true, nil
	}
	fkDiff := foreignKeyConstraintDiff{
		oldAndNew[schema.ForeignKeyConstraint]{
			old: old,
			new: new,
		},
	}
	if _, err := fsg.Alter(fkDiff); err != nil {
		if errors.Is(err, ErrNotImplemented) {
			// The foreign key must be re-created if the diff cannot be reconciled via alter statements
			return foreignKeyConstraintDiff{}, true, nil
		}
		return foreignKeyConstraintDiff{}, false, fmt.Errorf("generating foreign key alter statements: %w", err)
	}

	return fkDiff, false, nil
}

type foreignKeyConstraintSQLVertexGenerator struct {
	// newSchemaTablesByName is a map of table name to tables in the new schema.
	newSchemaTablesByName map[string]schema.Table
	// addedTablesByName is a map of table name to the added tables
	// This is used to identify if hazards are necessary
	addedTablesByName map[string]schema.Table
	// indexInOldSchemaByTableName is a map of index name to the index in the old schema
	// This is used to force the foreign key constraint to be dropped before the index it depends on is dropped
	indexInOldSchemaByTableName map[string][]schema.Index
	// childrenInOldSchemaByPartitionedIndexName gives all child indexes (across all levels) for a given
	// partitioned index in the old schema.
	childrenInOldSchemaByPartitionedIndexName map[string][]schema.Index
	// indexesInNewSchemaByTableName is a map of index name to the index
	// Same as above but for adds and after
	indexesInNewSchemaByTableName map[string][]schema.Index
	// childrenInNewSchemaByPartitionedIndexName gives all child indexes (across all levels) for a given
	// partitioned index in the new schema.
	childrenInNewSchemaByPartitionedIndexName map[string][]schema.Index
}

func newForeignKeyConstraintSQLVertexGenerator(oldAndNewSchema oldAndNew[schema.Schema], tableDiffs listDiff[schema.Table, tableDiff]) sqlVertexGenerator[schema.ForeignKeyConstraint, foreignKeyConstraintDiff] {
	return legacyToNewSqlVertexGenerator[schema.ForeignKeyConstraint, foreignKeyConstraintDiff](&foreignKeyConstraintSQLVertexGenerator{
		newSchemaTablesByName:                     buildSchemaObjByNameMap(oldAndNewSchema.new.Tables),
		addedTablesByName:                         buildSchemaObjByNameMap(tableDiffs.adds),
		indexInOldSchemaByTableName:               buildIndexesByTableNameMap(oldAndNewSchema.old.Indexes),
		childrenInOldSchemaByPartitionedIndexName: buildChildrenByPartitionedIndexNameMap(oldAndNewSchema.old.Indexes),
		indexesInNewSchemaByTableName:             buildIndexesByTableNameMap(oldAndNewSchema.new.Indexes),
		childrenInNewSchemaByPartitionedIndexName: buildChildrenByPartitionedIndexNameMap(oldAndNewSchema.new.Indexes),
	})
}

func (f *foreignKeyConstraintSQLVertexGenerator) Add(con schema.ForeignKeyConstraint) ([]Statement, error) {
	if con.IsValid {
		table, ok := f.newSchemaTablesByName[con.OwningTable.GetName()]
		if !ok {
			return nil, fmt.Errorf("could not find table with name %s", con.OwningTable.GetName())
		}
		if !table.IsPartitioned() {
			// Postgres does not support adding invalid foreign keys to partitioned tables.
			return f.addAsInvalidThenValidateStatements(con), nil
		}
	}

	var hazards []MigrationHazard
	_, isOnNewTable := f.addedTablesByName[con.OwningTable.GetName()]
	_, isReferencedTableNew := f.addedTablesByName[con.ForeignTable.GetName()]
	if con.IsValid && (!isOnNewTable || !isReferencedTableNew) {
		hazards = append(hazards, MigrationHazard{
			Type: MigrationHazardTypeAcquiresShareRowExclusiveLock,
			Message: "This will lock writes to the owning table and referenced table while the constraint is being added. " +
				"Instead, consider adding the constraint as NOT VALID and validating it later.",
		})
	}
	return []Statement{{
		DDL:         fmt.Sprintf("%s %s", addConstraintPrefix(con.OwningTable, con.EscapedName), con.ConstraintDef),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     hazards,
	}}, nil
}

func (f *foreignKeyConstraintSQLVertexGenerator) addAsInvalidThenValidateStatements(con schema.ForeignKeyConstraint) []Statement {
	// If adding a valid constraint, we will first add the constraint as not valid then validate it in order to
	// circumvent requiring a SHARE_ROW_EXCLUSIVE lock on the tables.
	return []Statement{
		{
			DDL:         fmt.Sprintf("%s %s NOT VALID", addConstraintPrefix(con.OwningTable, con.EscapedName), con.ConstraintDef),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		},
		validateConstraintStatement(con.OwningTable, con.EscapedName),
	}
}

func (f *foreignKeyConstraintSQLVertexGenerator) Delete(con schema.ForeignKeyConstraint) ([]Statement, error) {
	// Always generate a drop statement even if the owning table is being deleted. This simplifies the logic a bit because
	// if the owning table has a circular FK dependency with another table being dropped, we will need to explicitly drop
	// one of the FK's first
	return []Statement{{
		DDL:         dropConstraintDDL(con.OwningTable, con.EscapedName),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (f *foreignKeyConstraintSQLVertexGenerator) Alter(diff foreignKeyConstraintDiff) ([]Statement, error) {
	var stmts []Statement
	if !diff.old.IsValid && diff.new.IsValid {
		diff.old.IsValid = diff.new.IsValid
		// We're not keeping track of the other FK attributes, so it's easiest to ensure all other properties are equal
		// by just modifying the old constraint diff to exclude "NOT VALID", which should make the diffs equal if no
		// other properties have changed
		if !strings.HasSuffix(diff.old.ConstraintDef, " NOT VALID") {
			return nil, fmt.Errorf("expected the old constraint def to be suffixed with NOT VALID: %q", diff.old.ConstraintDef)
		}
		diff.old.ConstraintDef = strings.TrimSuffix(diff.old.ConstraintDef, " NOT VALID")
		stmts = append(stmts, validateConstraintStatement(diff.new.OwningTable, diff.new.EscapedName))
	}
	if !cmp.Equal(diff.old, diff.new) {
		return nil, fmt.Errorf("altering foreign key constraint to resolve the following diff %s: %w", cmp.Diff(diff.old, diff.new), ErrNotImplemented)
	}

	return stmts, nil
}

func (*foreignKeyConstraintSQLVertexGenerator) GetSQLVertexId(con schema.ForeignKeyConstraint, diffType diffType) schemaObjNestedGraphSqlVertexId {
	return buildSchemaObjVertexId("fkconstraint", con.GetName(), diffType)
}

func (f *foreignKeyConstraintSQLVertexGenerator) GetAddAlterDependencies(con, _ schema.ForeignKeyConstraint) ([]dependency, error) {
	deps := []dependency{
		mustRun(f.GetSQLVertexId(con, diffTypeAddAlter)).after(f.GetSQLVertexId(con, diffTypeDelete)),
		mustRun(f.GetSQLVertexId(con, diffTypeAddAlter)).after(buildTableVertexId(con.OwningTable, diffTypeAddAlter, subgraphMarkerStart)),
		mustRun(f.GetSQLVertexId(con, diffTypeAddAlter)).after(buildTableVertexId(con.ForeignTable, diffTypeAddAlter, subgraphMarkerStart)),
	}
	// This is the slightly lazy way of ensuring the foreign key constraint is added after the requisite index is
	// built and marked as valid.
	// We __could__ do this just for the index the fk depends on, but that's slightly more wiring than we need right now
	// because of partitioned indexes, which are only valid when all child indexes have been built
	for _, i := range f.indexesInNewSchemaByTableName[con.ForeignTable.GetName()] {
		deps = append(deps, mustRun(f.GetSQLVertexId(con, diffTypeAddAlter)).after(buildIndexVertexId(i.GetSchemaQualifiedName(), diffTypeAddAlter)))
		// Build a dependency on any child index if the index is partitioned
		for _, c := range f.childrenInNewSchemaByPartitionedIndexName[i.GetName()] {
			deps = append(deps, mustRun(f.GetSQLVertexId(con, diffTypeAddAlter)).after(buildIndexVertexId(c.GetSchemaQualifiedName(), diffTypeAddAlter)))
		}
	}

	return deps, nil
}

func (f *foreignKeyConstraintSQLVertexGenerator) GetDeleteDependencies(con schema.ForeignKeyConstraint) ([]dependency, error) {
	deps := []dependency{
		mustRun(f.GetSQLVertexId(con, diffTypeDelete)).before(buildTableVertexId(con.OwningTable, diffTypeDelete, subgraphMarkerStart)),
		mustRun(f.GetSQLVertexId(con, diffTypeDelete)).before(buildTableVertexId(con.ForeignTable, diffTypeDelete, subgraphMarkerStart)),
	}
	// This is the slightly lazy way of ensuring the foreign key constraint is deleted before the index it depends on is deleted
	// We __could__ do this just for the index the fk depends on, but that's slightly more wiring than we need right now
	// because of partitioned indexes, which are only valid when all child indexes have been built
	for _, i := range f.indexInOldSchemaByTableName[con.ForeignTable.GetName()] {
		deps = append(deps, mustRun(f.GetSQLVertexId(con, diffTypeDelete)).before(buildIndexVertexId(i.GetSchemaQualifiedName(), diffTypeDelete)))
		// Build a dependency on any child index if the index is partitioned
		for _, c := range f.childrenInOldSchemaByPartitionedIndexName[i.GetName()] {
			deps = append(deps, mustRun(f.GetSQLVertexId(con, diffTypeDelete)).before(buildIndexVertexId(c.GetSchemaQualifiedName(), diffTypeDelete)))
		}
	}
	return deps, nil
}

type sequenceSQLVertexGenerator struct {
	deletedTablesByName map[string]schema.Table
	tableDiffsByName    map[string]tableDiff
}

func (s *sequenceSQLVertexGenerator) Add(seq schema.Sequence) ([]Statement, error) {
	return []Statement{
		s.buildAddAlterSequenceStatement(seq, false),
	}, nil
}

func (s *sequenceSQLVertexGenerator) Delete(seq schema.Sequence) ([]Statement, error) {
	hazards := []MigrationHazard{{
		Type:    MigrationHazardTypeDeletesData,
		Message: "By deleting a sequence, its value will be permanently lost",
	}}
	if seq.Owner != nil && (s.isDeletedWithOwningTable(seq) || s.isDeletedWithColumns(seq)) {
		return nil, nil
	} else if seq.Owner == nil {
		hazards = append(hazards, migrationHazardSequenceCannotTrackDependencies)
	}
	return []Statement{{
		DDL:         fmt.Sprintf("DROP SEQUENCE %s", seq.GetFQEscapedName()),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     hazards,
	}}, nil
}

func (s *sequenceSQLVertexGenerator) Alter(diff sequenceDiff) ([]Statement, error) {
	var stmts []Statement
	// Ownership changes handled by the sequenceOwnershipSQLVertexGenerator
	diff.old.Owner = diff.new.Owner

	// Explicitly list all the diffs supported by the alter statement, rather than just using !cmp.Equal, so we don't
	// risk introducing a bug if we add new fields to schema.Sequence
	if diff.old.Type != diff.new.Type ||
		diff.old.Increment != diff.new.Increment ||
		diff.old.MinValue != diff.new.MinValue ||
		diff.old.MaxValue != diff.new.MaxValue ||
		diff.old.StartValue != diff.new.StartValue ||
		diff.old.CacheSize != diff.new.CacheSize ||
		diff.old.Cycle != diff.new.Cycle {
		stmts = append(stmts, s.buildAddAlterSequenceStatement(diff.new, true))

		// Diffs handled by alter statement
		diff.old.Type = diff.new.Type
		diff.old.Increment = diff.new.Increment
		diff.old.MinValue = diff.new.MinValue
		diff.old.MaxValue = diff.new.MaxValue
		diff.old.StartValue = diff.new.StartValue
		diff.old.CacheSize = diff.new.CacheSize
		diff.old.Cycle = diff.new.Cycle
	}

	if !cmp.Equal(diff.old, diff.new) {
		return nil, fmt.Errorf("altering sequence to resolve the following diff %s: %w", cmp.Diff(diff.old, diff.new), ErrNotImplemented)
	}

	return stmts, nil
}

func (s *sequenceSQLVertexGenerator) buildAddAlterSequenceStatement(seq schema.Sequence, isAlter bool) Statement {
	sb := strings.Builder{}
	action := "CREATE"
	if isAlter {
		action = "ALTER"
	}
	sb.WriteString(fmt.Sprintf("%s SEQUENCE %s\n", action, seq.GetFQEscapedName()))
	sb.WriteString(fmt.Sprintf("\tAS %s\n", seq.Type))
	sb.WriteString(fmt.Sprintf("\tINCREMENT BY %d\n", seq.Increment))
	sb.WriteString(fmt.Sprintf("\tMINVALUE %d MAXVALUE %d\n", seq.MinValue, seq.MaxValue))
	cycleModifier := ""
	if !seq.Cycle {
		cycleModifier = "NO "
	}
	sb.WriteString(fmt.Sprintf("\tSTART WITH %d CACHE %d %sCYCLE\n", seq.StartValue, seq.CacheSize, cycleModifier))

	var hazards []MigrationHazard
	if !isAlter && seq.Owner == nil {
		hazards = append(hazards, migrationHazardSequenceCannotTrackDependencies)
	}

	return Statement{
		DDL:         sb.String(),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     hazards,
	}
}

func (s *sequenceSQLVertexGenerator) GetSQLVertexId(seq schema.Sequence, diffType diffType) schemaObjNestedGraphSqlVertexId {
	return buildSequenceVertexId(seq.SchemaQualifiedName, diffType)
}

func buildSequenceVertexId(name schema.SchemaQualifiedName, diffType diffType) schemaObjNestedGraphSqlVertexId {
	return buildSchemaObjVertexId("sequence", name.GetFQEscapedName(), diffType)
}

func (s *sequenceSQLVertexGenerator) GetAddAlterDependencies(new schema.Sequence, _ schema.Sequence) ([]dependency, error) {
	deps := []dependency{
		mustRun(s.GetSQLVertexId(new, diffTypeAddAlter)).after(s.GetSQLVertexId(new, diffTypeDelete)),
	}
	if new.Owner != nil {
		// Sequences should be added/altered before the table they are owned by
		deps = append(deps, mustRun(s.GetSQLVertexId(new, diffTypeAddAlter)).before(buildTableVertexId(new.Owner.TableName, diffTypeAddAlter, subgraphMarkerStart)))
	}
	return deps, nil
}

func (s *sequenceSQLVertexGenerator) GetDeleteDependencies(seq schema.Sequence) ([]dependency, error) {
	var deps []dependency
	// This is an unfortunate hackaround. It would make sense to also have a dependency on the owner column, such that
	// the sequence can only be considered deleted after the owning column is deleted. However, we currently don't separate
	// column deletes from table add/alters. We can't build this dependency without possibly creating a circular dependency:
	// the sequence add/alter must occur before the new owner column add/alter, but the sequence delete must occur after the
	// old owner column delete (equivalent to add/alter) and the sequence add/alter. We can get away with this because
	// we, so far, no columns are ever "re-created". If we ever do support that, we'll need to revisit this.
	if seq.Owner != nil {
		deps = append(deps, mustRun(s.GetSQLVertexId(seq, diffTypeDelete)).after(buildTableVertexId(seq.Owner.TableName, diffTypeDelete, subgraphMarkerStart)))
	}
	return deps, nil
}

func (s *sequenceSQLVertexGenerator) isDeletedWithOwningTable(seq schema.Sequence) bool {
	if _, ok := s.deletedTablesByName[seq.Owner.TableName.GetName()]; ok {
		// If the sequence is owned by a table that is also being deleted, we don't need to drop the sequence.
		return true
	}
	return false
}

func (s *sequenceSQLVertexGenerator) isDeletedWithColumns(seq schema.Sequence) bool {
	for _, dc := range s.tableDiffsByName[seq.Owner.TableName.GetName()].columnsDiff.deletes {
		if dc.Name == seq.Owner.ColumnName {
			// If the sequence is owned by a column that is also being deleted, we don't need to drop the sequence.
			return true
		}
	}
	return false
}

type sequenceOwnershipSQLVertexGenerator struct{}

func (s sequenceOwnershipSQLVertexGenerator) Add(seq schema.Sequence) ([]Statement, error) {
	if seq.Owner == nil {
		// If a new sequence has no owner, we don't need to alter it. The default is no owner
		return nil, nil
	}
	return []Statement{s.buildAlterOwnershipStmt(seq)}, nil
}

func (s sequenceOwnershipSQLVertexGenerator) Delete(_ schema.Sequence) ([]Statement, error) {
	return nil, nil
}

func (s sequenceOwnershipSQLVertexGenerator) Alter(diff sequenceDiff) ([]Statement, error) {
	if cmp.Equal(diff.new.Owner, diff.old.Owner) {
		return nil, nil
	}
	return []Statement{s.buildAlterOwnershipStmt(diff.new)}, nil
}

func (s sequenceOwnershipSQLVertexGenerator) buildAlterOwnershipStmt(new schema.Sequence) Statement {
	newOwner := "NONE"
	if new.Owner != nil {
		newOwner = schema.FQEscapedColumnName(new.Owner.TableName, new.Owner.ColumnName)
	}

	return Statement{
		DDL:         fmt.Sprintf("ALTER SEQUENCE %s OWNED BY %s", new.GetFQEscapedName(), newOwner),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}
}

func (s sequenceOwnershipSQLVertexGenerator) GetSQLVertexId(seq schema.Sequence, diffType diffType) schemaObjNestedGraphSqlVertexId {
	return buildSchemaObjVertexId("sequence_ownership", seq.SchemaQualifiedName.GetFQEscapedName(), diffType)
}

func (s sequenceOwnershipSQLVertexGenerator) GetAddAlterDependencies(new schema.Sequence, old schema.Sequence) ([]dependency, error) {
	if cmp.Equal(old.Owner, new.Owner) {
		return nil, nil
	}

	deps := []dependency{
		// Always change ownership after the sequence has been added/altered
		mustRun(s.GetSQLVertexId(new, diffTypeAddAlter)).after(buildSequenceVertexId(new.SchemaQualifiedName, diffTypeAddAlter)),
	}

	if old.Owner != nil {
		// Always update ownership before the old owner has been deleted
		deps = append(deps, mustRun(s.GetSQLVertexId(new, diffTypeAddAlter)).before(buildTableVertexId(old.Owner.TableName, diffTypeDelete, subgraphMarkerStart)))
	}

	if new.Owner != nil {
		// Always update ownership after the new owner has been created
		deps = append(deps, mustRun(s.GetSQLVertexId(new, diffTypeAddAlter)).after(buildTableVertexId(new.Owner.TableName, diffTypeAddAlter, subgraphMarkerStart)))
	}

	return deps, nil
}

func (s sequenceOwnershipSQLVertexGenerator) GetDeleteDependencies(_ schema.Sequence) ([]dependency, error) {
	return nil, nil
}

type extensionSQLGenerator struct{}

func (e *extensionSQLGenerator) Add(extension schema.Extension) ([]Statement, error) {
	s := fmt.Sprintf(
		"CREATE EXTENSION %s WITH SCHEMA %s",
		extension.EscapedName,
		schema.EscapeIdentifier(extension.SchemaName),
	)

	if len(extension.Version) != 0 {
		s += fmt.Sprintf(" VERSION %s", schema.EscapeIdentifier(extension.Version))
	}

	return []Statement{{
		DDL:         s,
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     nil,
	}}, nil
}

func (e *extensionSQLGenerator) Delete(extension schema.Extension) ([]Statement, error) {
	return []Statement{{
		DDL:         fmt.Sprintf("DROP EXTENSION %s", extension.EscapedName),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     []MigrationHazard{migrationHazardExtensionDroppedCannotTrackDependencies},
	}}, nil
}

func (e *extensionSQLGenerator) Alter(diff extensionDiff) ([]Statement, error) {
	var statements []Statement
	if diff.new.Version != diff.old.Version {
		if len(diff.new.Version) == 0 {
			// This is an implicit upgrade to the latest extension version.
			statements = append(statements, Statement{
				DDL:         fmt.Sprintf("ALTER EXTENSION %s UPDATE", diff.new.EscapedName),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
				Hazards:     []MigrationHazard{migrationHazardExtensionAlteredVersionUpgraded},
			})
		} else {
			// We optimistically assume an update path from the old to new version exists. When we
			// validate the plan later, any issues will be caught and an error will be thrown.
			statements = append(statements, Statement{
				DDL: fmt.Sprintf(
					"ALTER EXTENSION %s UPDATE TO %s",
					diff.new.EscapedName,
					schema.EscapeIdentifier(diff.new.Version),
				),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
				Hazards:     []MigrationHazard{migrationHazardExtensionAlteredVersionUpgraded},
			})
		}
	}
	return statements, nil
}

type functionSQLVertexGenerator struct {
	// functionsInNewSchemaByName is a map of function new to functions in the new schema.
	// These functions are not necessarily new
	functionsInNewSchemaByName map[string]schema.Function
}

func (f *functionSQLVertexGenerator) Add(function schema.Function) ([]Statement, error) {
	var hazards []MigrationHazard
	if !canFunctionDependenciesBeTracked(function) {
		hazards = append(hazards, migrationHazardAddAlterFunctionCannotTrackDependencies)
	}
	return []Statement{{
		DDL:         function.FunctionDef,
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     hazards,
	}}, nil
}

func (f *functionSQLVertexGenerator) Delete(function schema.Function) ([]Statement, error) {
	var hazards []MigrationHazard
	if !canFunctionDependenciesBeTracked(function) {
		hazards = append(hazards, MigrationHazard{
			Type: MigrationHazardTypeHasUntrackableDependencies,
			Message: "Dependencies, i.e. other functions used in the function body, of non-sql functions cannot be " +
				"tracked. As a result, we cannot guarantee that function dependencies are ordered properly relative to " +
				"this statement. For drops, this means you need to ensure that all functions this function depends on " +
				"are dropped after this statement.",
		})
	}
	return []Statement{{
		DDL:         fmt.Sprintf("DROP FUNCTION %s", function.GetFQEscapedName()),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     hazards,
	}}, nil
}

func (f *functionSQLVertexGenerator) Alter(diff functionDiff) ([]Statement, error) {
	// We are assuming the function has been normalized, i.e., we don't have to worry DependsOnFunctions ordering
	// causing a false positive diff detected.
	if cmp.Equal(diff.old, diff.new) {
		return nil, nil
	}

	var hazards []MigrationHazard
	if !canFunctionDependenciesBeTracked(diff.new) {
		hazards = append(hazards, migrationHazardAddAlterFunctionCannotTrackDependencies)
	}
	return []Statement{{
		DDL:         diff.new.FunctionDef,
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     hazards,
	}}, nil
}

func canFunctionDependenciesBeTracked(function schema.Function) bool {
	return function.Language == "sql"
}

func (f *functionSQLVertexGenerator) GetSQLVertexId(function schema.Function, diffType diffType) schemaObjNestedGraphSqlVertexId {
	return buildFunctionVertexId(function.SchemaQualifiedName, diffType)
}

func buildFunctionVertexId(name schema.SchemaQualifiedName, diffType diffType) schemaObjNestedGraphSqlVertexId {
	return buildSchemaObjVertexId("function", name.GetFQEscapedName(), diffType)
}

func (f *functionSQLVertexGenerator) GetAddAlterDependencies(newFunction, oldFunction schema.Function) ([]dependency, error) {
	// Since functions can just be `CREATE OR REPLACE`, there will never be a case where a function is
	// added and dropped in the same migration. Thus, we don't need a dependency on the delete vertex of a function
	// because there won't be one if it is being added/altered
	var deps []dependency
	for _, depFunction := range newFunction.DependsOnFunctions {
		deps = append(deps, mustRun(f.GetSQLVertexId(newFunction, diffTypeAddAlter)).after(buildFunctionVertexId(depFunction, diffTypeAddAlter)))
	}

	if !cmp.Equal(oldFunction, schema.Function{}) {
		// If the function is being altered:
		// If the old version of the function calls other functions that are being deleted come, those deletions
		// must come after the function is altered, so it is no longer dependent on those dropped functions
		for _, depFunction := range oldFunction.DependsOnFunctions {
			deps = append(deps, mustRun(f.GetSQLVertexId(newFunction, diffTypeAddAlter)).before(buildFunctionVertexId(depFunction, diffTypeDelete)))
		}
	}

	return deps, nil
}

func (f *functionSQLVertexGenerator) GetDeleteDependencies(function schema.Function) ([]dependency, error) {
	var deps []dependency
	for _, depFunction := range function.DependsOnFunctions {
		deps = append(deps, mustRun(f.GetSQLVertexId(function, diffTypeDelete)).before(buildFunctionVertexId(depFunction, diffTypeDelete)))
	}
	return deps, nil
}

type triggerSQLVertexGenerator struct {
	// functionsInNewSchemaByName is a map of function new to functions in the new schema.
	// These functions are not necessarily new
	functionsInNewSchemaByName map[string]schema.Function
}

func (t *triggerSQLVertexGenerator) Add(trigger schema.Trigger) ([]Statement, error) {
	return []Statement{{
		DDL:         string(trigger.GetTriggerDefStmt),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (t *triggerSQLVertexGenerator) Delete(trigger schema.Trigger) ([]Statement, error) {
	return []Statement{{
		DDL:         fmt.Sprintf("DROP TRIGGER %s ON %s", trigger.EscapedName, trigger.OwningTable.GetFQEscapedName()),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (t *triggerSQLVertexGenerator) Alter(diff triggerDiff) ([]Statement, error) {
	if cmp.Equal(diff.old, diff.new) {
		return nil, nil
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

func (t *triggerSQLVertexGenerator) GetSQLVertexId(trigger schema.Trigger, diffType diffType) schemaObjNestedGraphSqlVertexId {
	return buildSchemaObjVertexId("trigger", trigger.GetName(), diffType)
}

func (t *triggerSQLVertexGenerator) GetAddAlterDependencies(newTrigger, oldTrigger schema.Trigger) ([]dependency, error) {
	// Since a trigger can just be `CREATE OR REPLACE`, there will never be a case where a trigger is
	// added and dropped in the same migration. Thus, we don't need a dependency on the delete node of a function
	// because there won't be one if it is being added/altered
	deps := []dependency{
		mustRun(t.GetSQLVertexId(newTrigger, diffTypeAddAlter)).after(buildFunctionVertexId(newTrigger.Function, diffTypeAddAlter)),
		mustRun(t.GetSQLVertexId(newTrigger, diffTypeAddAlter)).after(buildTableVertexId(newTrigger.OwningTable, diffTypeAddAlter, subgraphMarkerStart)),
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
		mustRun(t.GetSQLVertexId(trigger, diffTypeDelete)).before(buildTableVertexId(trigger.OwningTable, diffTypeDelete, subgraphMarkerStart)),
	}, nil
}

func stripMigrationHazards(stmts ...Statement) []Statement {
	var noHazardsStmts []Statement
	for _, stmt := range stmts {
		stmt.Hazards = nil
		noHazardsStmts = append(noHazardsStmts, stmt)
	}
	return noHazardsStmts
}

func addConstraintPrefix(table schema.SchemaQualifiedName, escapedConstraintName string) string {
	return fmt.Sprintf("%s ADD CONSTRAINT %s", alterTablePrefix(table), escapedConstraintName)
}

func dropConstraintDDL(table schema.SchemaQualifiedName, escapedConstraintName string) string {
	return fmt.Sprintf("%s DROP CONSTRAINT %s", alterTablePrefix(table), escapedConstraintName)
}

func validateConstraintStatement(owningTable schema.SchemaQualifiedName, escapedConstraintName string) Statement {
	return Statement{
		DDL:         fmt.Sprintf("%s VALIDATE CONSTRAINT %s", alterTablePrefix(owningTable), escapedConstraintName),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}
}

func alterTablePrefix(table schema.SchemaQualifiedName) string {
	return fmt.Sprintf("ALTER TABLE %s", table.GetFQEscapedName())
}

func buildColumnDefinition(column schema.Column) (string, error) {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("%s %s", schema.EscapeIdentifier(column.Name), column.Type))
	if column.IsCollated() {
		sb.WriteString(fmt.Sprintf(" COLLATE %s", column.Collation.GetFQEscapedName()))
	}
	if !column.IsNullable {
		sb.WriteString(" NOT NULL")
	}
	if len(column.Default) > 0 {
		sb.WriteString(fmt.Sprintf(" DEFAULT %s", column.Default))
	}
	if column.Identity != nil {
		identityDef, err := buildColumnIdentityDefinition(*column.Identity)
		if err != nil {
			return "", fmt.Errorf("building column identity definition: %w", err)
		}
		sb.WriteString(" " + identityDef)
	}
	return sb.String(), nil
}

func buildColumnIdentityDefinition(identity schema.ColumnIdentity) (string, error) {
	typeModifier, err := columnIdentityTypeToModifier(identity.Type)
	if err != nil {
		return "", fmt.Errorf("column identity type modifier: %w", err)
	}

	cycleModifier := ""
	if !identity.Cycle {
		cycleModifier = "NO "
	}

	return fmt.Sprintf("GENERATED %s AS IDENTITY (INCREMENT BY %d MINVALUE %d MAXVALUE %d START WITH %d CACHE %d %sCYCLE)", typeModifier, identity.Increment, identity.MinValue, identity.MaxValue, identity.StartValue, identity.CacheSize, cycleModifier), nil
}

func columnIdentityTypeToModifier(val schema.ColumnIdentityType) (string, error) {
	switch val {
	case schema.ColumnIdentityTypeAlways:
		return "ALWAYS", nil
	case schema.ColumnIdentityTypeByDefault:
		return "BY DEFAULT", nil
	default:
		return "", fmt.Errorf("unknown identity type %q", val)
	}
}
