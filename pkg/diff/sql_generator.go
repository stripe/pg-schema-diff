package diff

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
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

	extensionDiff struct {
		oldAndNew[schema.Extension]
	}
)

type schemaDiff struct {
	oldAndNew[schema.Schema]
	extensionDiffs            listDiff[schema.Extension, extensionDiff]
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

	foreignKeyConstraintDiffs, err := diffLists(old.ForeignKeyConstraints, new.ForeignKeyConstraints, func(old, new schema.ForeignKeyConstraint, _, _ int) (foreignKeyConstraintDiff, bool, error) {
		if _, isOnNewTable := addedTablesByName[new.OwningTableUnescapedName]; isOnNewTable {
			// If the owning table is new, then it must be re-created (this occurs if the base table has been
			// re-created). In other words, a foreign key constraint must be re-created if the owning table or referenced
			// table is re-created
			return foreignKeyConstraintDiff{}, true, nil
		} else if _, isReferencingNewTable := addedTablesByName[new.ForeignTableUnescapedName]; isReferencingNewTable {
			// Same as above, but for the referenced table
			return foreignKeyConstraintDiff{}, true, nil
		}

		extractColumnName := func(constraint string) (string, error) {
			// TODO: doesn't handle the case of multiple columns for fk
			re := regexp.MustCompile(`FOREIGN KEY \("?([^"]+)"?\)`)
			matches := re.FindStringSubmatch(constraint)
		
			if len(matches) > 1 {
				return matches[1], nil
			} else {
				return "", fmt.Errorf("invalid constraint definition")
			}	
		}

		// Check if we're casting from bigint to timestamp without timezone for some column
		if old.ConstraintDef == new.ConstraintDef {
			// For now, only consider the case where column name remains the same
			for _, tableDiff := range tableDiffs.alters {
				for _, columnDiff := range tableDiff.columnsDiff.alters {
					columns := columnDiff.oldAndNew;
					if columns.old.Name != columns.new.Name {
						// For now, only consider the case where column name remains the same
						// TODO: handle the case where column is renamed
						continue;
					}

					columnName, err := extractColumnName(old.ConstraintDef)
					if err != nil {
						return foreignKeyConstraintDiff{}, false, err
					}

					isBigintToTimestampWithFk := columns.old.Type == "bigint" && columns.new.Type == "timestamp without time zone" && columns.old.Name == columnName
					if isBigintToTimestampWithFk {
						return foreignKeyConstraintDiff{}, true, nil
					}
				}
			}
		}

		// Set the new clone to be equal to the old for all fields that can actually be altered
		newClone := new
		if !old.IsValid && new.IsValid {
			// We only support alter from NOT VALID to VALID and no other alterations.
			// Instead of checking that each individual property is equal (that's a lot of parsing), we will just
			// assert that the constraint definitions are equal if we append "NOT VALID" to the new constraint def
			newClone.IsValid = old.IsValid
			newClone.ConstraintDef = fmt.Sprintf("%s NOT VALID", newClone.ConstraintDef)
		}
		if !cmp.Equal(old, newClone) {
			return foreignKeyConstraintDiff{}, true, nil
		}

		return foreignKeyConstraintDiff{
			oldAndNew[schema.ForeignKeyConstraint]{
				old: old,
				new: new,
			},
		}, false, nil
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
			if _, isOnNewTable := addedTablesByName[new.Owner.TableUnescapedName]; isOnNewTable {
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
		if _, isOnNewTable := addedTablesByName[new.OwningTableUnescapedName]; isOnNewTable {
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
		extensionDiffs:            extensionDiffs,
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

	if oldTable.ParentTableName != newTable.ParentTableName {
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
		return tableDiff{}, false, fmt.Errorf("diffing lists: %w", err)
	}

	return tableDiff{
		oldAndNew: oldAndNew[schema.Table]{
			old: oldTable,
			new: newTable,
		},
		columnsDiff:         columnsDiff,
		checkConstraintDiff: checkConsDiff,
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
	} else if deps.seenIndexesByName[new.Name] {
		// Prevent infinite recursion
		return indexDiff{}, false, fmt.Errorf("loop detected between indexes that starts with %q. %v", new.Name, deps.seenIndexesByName)
	}
	deps.seenIndexesByName[new.Name] = true

	updatedOld := old

	if _, isOnNewTable := deps.addedTablesByName[new.TableName]; isOnNewTable {
		// If the table is new, then it must be re-created (this occurs if the base table has been
		// re-created). In other words, an index must be re-created if the owning table is re-created
		return indexDiff{}, true, nil
	}

	if len(old.ParentIdxName) == 0 {
		// If the old index didn't belong to a partitioned index (and the new index does), we can resolve the parent
		// index name diff if the index now belongs to a partitioned index by attaching the index.
		// We can't switch an index partition from one parent to another; in that instance, we must
		// re-create the index
		updatedOld.ParentIdxName = new.ParentIdxName
	}

	if old.IsPartitionOfIndex() && new.IsPartitionOfIndex() && old.ParentIdxName == new.ParentIdxName {
		// This is a bad way of recreating the child index when the parent is recreated. Ideally, the diff function
		// should be able to handle dependency hierarchies, where if a parent is recreated, the child is recreated.
		// This is hack around because that functionality is not yet implemented
		oldParentIndex, ok := deps.oldSchemaIndexesByName[new.ParentIdxName]
		if !ok {
			return indexDiff{}, false, fmt.Errorf("could not find parent index %s", new.ParentIdxName)
		}
		newParentIndex, ok := deps.newSchemaIndexesByName[new.ParentIdxName]
		if !ok {
			return indexDiff{}, false, fmt.Errorf("could not find parent index %s", new.ParentIdxName)
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

	indexesOldSchemaByTableName := make(map[string][]schema.Index)
	for _, idx := range diff.old.Indexes {
		indexesOldSchemaByTableName[idx.TableName] = append(indexesOldSchemaByTableName[idx.TableName], idx)
	}
	indexesInNewSchemaByTableName := make(map[string][]schema.Index)
	for _, idx := range diff.new.Indexes {
		indexesInNewSchemaByTableName[idx.TableName] = append(indexesInNewSchemaByTableName[idx.TableName], idx)
	}

	tableGraphs, err := diff.tableDiffs.resolveToSQLGraph(&tableSQLVertexGenerator{
		deletedTablesByName:     deletedTablesByName,
		tablesInNewSchemaByName: tablesInNewSchemaByName,
	})
	if err != nil {
		return nil, fmt.Errorf("resolving table sql graphs: %w", err)
	}

	extensionStatements, err := diff.extensionDiffs.resolveToSQLGroupedByEffect(&extensionSQLGenerator{})
	if err != nil {
		return nil, fmt.Errorf("resolving extension sql graphs: %w", err)
	}

	attachPartitionSQLVertexGenerator := newAttachPartitionSQLVertexGenerator(indexesInNewSchemaByTableName, addedTablesByName)
	attachPartitionGraphs, err := diff.tableDiffs.resolveToSQLGraph(attachPartitionSQLVertexGenerator)
	if err != nil {
		return nil, fmt.Errorf("resolving attach partition sql graphs: %w", err)
	}

	renameConflictingIndexSQLVertexGenerator := newRenameConflictingIndexSQLVertexGenerator(buildSchemaObjByNameMap(diff.old.Indexes))
	renameConflictingIndexGraphs, err := diff.indexDiffs.resolveToSQLGraph(renameConflictingIndexSQLVertexGenerator)
	if err != nil {
		return nil, fmt.Errorf("resolving renaming conflicting indexes: %w", err)
	}

	indexGraphs, err := diff.indexDiffs.resolveToSQLGraph(&indexSQLVertexGenerator{
		deletedTablesByName:      deletedTablesByName,
		addedTablesByName:        addedTablesByName,
		tablesInNewSchemaByName:  tablesInNewSchemaByName,
		indexesInNewSchemaByName: buildSchemaObjByNameMap(diff.new.Indexes),

		renameSQLVertexGenerator:          renameConflictingIndexSQLVertexGenerator,
		attachPartitionSQLVertexGenerator: attachPartitionSQLVertexGenerator,
	})
	if err != nil {
		return nil, fmt.Errorf("resolving index sql graphs: %w", err)
	}

	fkConsGraphs, err := diff.foreignKeyConstraintDiffs.resolveToSQLGraph(&foreignKeyConstraintSQLVertexGenerator{
		deletedTablesByName:           deletedTablesByName,
		addedTablesByName:             addedTablesByName,
		indexInOldSchemaByTableName:   indexesInNewSchemaByTableName,
		indexesInNewSchemaByTableName: indexesInNewSchemaByTableName,
	})
	if err != nil {
		return nil, fmt.Errorf("resolving foreign key constraint sql graphs: %w", err)
	}

	sequenceGraphs, err := diff.sequenceDiffs.resolveToSQLGraph(&sequenceSQLVertexGenerator{
		deletedTablesByName: deletedTablesByName,
		tableDiffsByName:    buildDiffByNameMap[schema.Table, tableDiff](diff.tableDiffs.alters),
	})
	if err != nil {
		return nil, fmt.Errorf("resolving sequence sql graphs: %w", err)
	}
	sequenceOwnershipGraphs, err := diff.sequenceDiffs.resolveToSQLGraph(&sequenceOwnershipSQLVertexGenerator{})
	if err != nil {
		return nil, fmt.Errorf("resolving sequence ownership sql graphs: %w", err)
	}

	functionsInNewSchemaByName := buildSchemaObjByNameMap(diff.new.Functions)
	functionGraphs, err := diff.functionDiffs.resolveToSQLGraph(&functionSQLVertexGenerator{
		functionsInNewSchemaByName: functionsInNewSchemaByName,
	})
	if err != nil {
		return nil, fmt.Errorf("resolving function sql graphs: %w", err)
	}

	triggerGraphs, err := diff.triggerDiffs.resolveToSQLGraph(&triggerSQLVertexGenerator{
		functionsInNewSchemaByName: functionsInNewSchemaByName,
	})
	if err != nil {
		return nil, fmt.Errorf("resolving trigger sql graphs: %w", err)
	}

	if err := tableGraphs.union(attachPartitionGraphs); err != nil {
		return nil, fmt.Errorf("unioning table and attach partition graphs: %w", err)
	}
	if err := tableGraphs.union(indexGraphs); err != nil {
		return nil, fmt.Errorf("unioning table and index graphs: %w", err)
	}
	if err := tableGraphs.union(renameConflictingIndexGraphs); err != nil {
		return nil, fmt.Errorf("unioning table and rename conflicting index graphs: %w", err)
	}
	if err := tableGraphs.union(fkConsGraphs); err != nil {
		return nil, fmt.Errorf("unioning table and foreign key constraint graphs: %w", err)
	}
	if err := tableGraphs.union(sequenceGraphs); err != nil {
		return nil, fmt.Errorf("unioning table and sequence graphs: %w", err)
	}
	if err := tableGraphs.union(sequenceOwnershipGraphs); err != nil {
		return nil, fmt.Errorf("unioning table and sequence ownership graphs: %w", err)
	}
	if err := tableGraphs.union(functionGraphs); err != nil {
		return nil, fmt.Errorf("unioning table and function graphs: %w", err)
	}
	if err := tableGraphs.union(triggerGraphs); err != nil {
		return nil, fmt.Errorf("unioning table and trigger graphs: %w", err)
	}

	graphStatements, err := tableGraphs.toOrderedStatements()
	if err != nil {
		return nil, fmt.Errorf("getting ordered statements from tableGraph: %w", err)
	}

	// We enable extensions first and disable them last since their dependencies may span across
	// all other entities in the database.
	var statements []Statement
	statements = append(statements, extensionStatements.Adds...)
	statements = append(statements, extensionStatements.Alters...)
	statements = append(statements, graphStatements...)
	statements = append(statements, extensionStatements.Deletes...)
	return statements, nil
}

func buildSchemaObjByNameMap[S schema.Object](s []S) map[string]S {
	return buildObjByNameMap(s, func(s S) string {
		return s.GetName()
	})
}

func buildDiffByNameMap[S schema.Object, D diff[S]](d []D) map[string]D {
	return buildObjByNameMap(d, func(d D) string {
		return d.GetNew().GetName()
	})
}

func buildObjByNameMap[V any](v []V, getName func(V) string) map[string]V {
	output := make(map[string]V)
	for _, obj := range v {
		output[getName(obj)] = obj
	}
	return output
}

type tableSQLVertexGenerator struct {
	deletedTablesByName     map[string]schema.Table
	tablesInNewSchemaByName map[string]schema.Table
}

var _ sqlVertexGenerator[schema.Table, tableDiff] = &tableSQLVertexGenerator{}

func (t *tableSQLVertexGenerator) Add(table schema.Table) ([]Statement, error) {
	if table.IsPartition() {
		if table.IsPartitioned() {
			return nil, fmt.Errorf("partitioned partitions: %w", ErrNotImplemented)
		}
		if len(table.CheckConstraints) > 0 {
			return nil, fmt.Errorf("check constraints on partitions: %w", ErrNotImplemented)
		}
		// We attach the partitions separately. So the partition must have all the same check constraints
		// as the original table
		table.CheckConstraints = append(table.CheckConstraints, t.tablesInNewSchemaByName[table.ParentTableName].CheckConstraints...)
	}

	var stmts []Statement

	var columnDefs []string
	for _, column := range table.Columns {
		columnDefs = append(columnDefs, "\t"+buildColumnDefinition(column))
	}
	createTableSb := strings.Builder{}
	createTableSb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n%s\n)",
		schema.EscapeIdentifier(table.Name),
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

	csg := checkConstraintSQLGenerator{tableName: table.Name, isNewTable: true}
	for _, checkCon := range table.CheckConstraints {
		addConStmts, err := csg.Add(checkCon)
		if err != nil {
			return nil, fmt.Errorf("generating add check constraint statements for check constraint %s: %w", checkCon.Name, err)
		}
		// Remove hazards from statements since the table is brand new
		stmts = append(stmts, stripMigrationHazards(addConStmts)...)
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
		if _, baseTableDropped := t.deletedTablesByName[table.ParentTableName]; !baseTableDropped {
			return nil, fmt.Errorf("deleting partitions without dropping parent table: %w", ErrNotImplemented)
		}
		// It will be dropped when the parent table is dropped
		return nil, nil
	}
	return []Statement{
		{
			DDL:         fmt.Sprintf("DROP TABLE %s", schema.EscapeIdentifier(table.Name)),
			Timeout:     statementTimeoutTableDrop,
			LockTimeout: lockTimeoutDefault,
			Hazards: []MigrationHazard{{
				Type:    MigrationHazardTypeDeletesData,
				Message: "Deletes all rows in the table (and the table itself)",
			}},
		},
	}, nil
}

func (t *tableSQLVertexGenerator) Alter(diff tableDiff) ([]Statement, error) {
	if diff.old.IsPartition() != diff.new.IsPartition() {
		return nil, fmt.Errorf("changing a partition to no longer be a partition (or vice versa): %w", ErrNotImplemented)
	} else if diff.new.IsPartition() {
		return t.alterPartition(diff)
	}

	if diff.old.PartitionKeyDef != diff.new.PartitionKeyDef {
		return nil, fmt.Errorf("changing partition key def: %w", ErrNotImplemented)
	}

	columnSQLGenerator := columnSQLGenerator{tableName: diff.new.Name}
	columnGeneratedSQL, err := diff.columnsDiff.resolveToSQLGroupedByEffect(&columnSQLGenerator)
	if err != nil {
		return nil, fmt.Errorf("resolving index diff: %w", err)
	}

	checkConSQLGenerator := checkConstraintSQLGenerator{tableName: diff.new.Name, isNewTable: false}
	checkConGeneratedSQL, err := diff.checkConstraintDiff.resolveToSQLGroupedByEffect(&checkConSQLGenerator)
	if err != nil {
		return nil, fmt.Errorf("resolving check constraints diff: %w", err)
	}

	var stmts []Statement
	stmts = append(stmts, checkConGeneratedSQL.Deletes...)
	stmts = append(stmts, columnGeneratedSQL.Deletes...)
	stmts = append(stmts, columnGeneratedSQL.Adds...)
	stmts = append(stmts, columnGeneratedSQL.Alters...)
	stmts = append(stmts, checkConGeneratedSQL.Adds...)
	stmts = append(stmts, checkConGeneratedSQL.Alters...)
	return stmts, nil
}

func (t *tableSQLVertexGenerator) alterPartition(diff tableDiff) ([]Statement, error) {
	if diff.old.ForValues != diff.new.ForValues {
		return nil, fmt.Errorf("altering partition FOR VALUES: %w", ErrNotImplemented)
	}
	if !diff.checkConstraintDiff.isEmpty() {
		return nil, fmt.Errorf("check constraints on partitions: %w", ErrNotImplemented)
	}

	var stmts []Statement
	// ColumnsDiff should only have nullability changes. Partitioned tables
	// aren't concerned about old/new columns added
	for _, colDiff := range diff.columnsDiff.alters {
		if colDiff.old.IsNullable == colDiff.new.IsNullable {
			continue
		}
		alterColumnPrefix := fmt.Sprintf("%s ALTER COLUMN %s", publicTableAlterPrefix(diff.new.Name), schema.EscapeIdentifier(colDiff.new.Name))
		if colDiff.new.IsNullable {
			stmts = append(stmts, Statement{
				DDL:         fmt.Sprintf("%s DROP NOT NULL", alterColumnPrefix),
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
		} else {
			stmts = append(stmts, Statement{
				DDL:         fmt.Sprintf("%s SET NOT NULL", alterColumnPrefix),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
			})
		}
	}

	return stmts, nil
}

func (t *tableSQLVertexGenerator) GetSQLVertexId(table schema.Table) string {
	return buildTableVertexId(table.Name)
}

func (t *tableSQLVertexGenerator) GetAddAlterDependencies(table, _ schema.Table) []dependency {
	deps := []dependency{
		mustRun(t.GetSQLVertexId(table), diffTypeAddAlter).after(t.GetSQLVertexId(table), diffTypeDelete),
	}

	if table.IsPartition() {
		deps = append(deps,
			mustRun(t.GetSQLVertexId(table), diffTypeAddAlter).after(buildTableVertexId(table.ParentTableName), diffTypeAddAlter),
		)
	}
	return deps
}

func (t *tableSQLVertexGenerator) GetDeleteDependencies(table schema.Table) []dependency {
	var deps []dependency
	if table.IsPartition() {
		deps = append(deps,
			mustRun(t.GetSQLVertexId(table), diffTypeDelete).after(buildTableVertexId(table.ParentTableName), diffTypeDelete),
		)
	}
	return deps
}

type columnSQLGenerator struct {
	tableName string
}

func (csg *columnSQLGenerator) Add(column schema.Column) ([]Statement, error) {
	return []Statement{{
		DDL:         fmt.Sprintf("%s ADD COLUMN %s", publicTableAlterPrefix(csg.tableName), buildColumnDefinition(column)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (csg *columnSQLGenerator) Delete(column schema.Column) ([]Statement, error) {
	return []Statement{{
		DDL:         fmt.Sprintf("%s DROP COLUMN %s", publicTableAlterPrefix(csg.tableName), schema.EscapeIdentifier(column.Name)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards: []MigrationHazard{
			{
				Type:    MigrationHazardTypeDeletesData,
				Message: "Deletes all values in the column",
			},
		},
	}}, nil
}

func (csg *columnSQLGenerator) Alter(diff columnDiff) ([]Statement, error) {
	if diff.oldOrdering != diff.newOrdering {
		return nil, fmt.Errorf("old=%d; new=%d: %w", diff.oldOrdering, diff.newOrdering, ErrColumnOrderingChanged)
	}
	oldColumn, newColumn := diff.old, diff.new
	var stmts []Statement
	alterColumnPrefix := fmt.Sprintf("%s ALTER COLUMN %s", publicTableAlterPrefix(csg.tableName), schema.EscapeIdentifier(newColumn.Name))

	if oldColumn.IsNullable != newColumn.IsNullable {
		if newColumn.IsNullable {
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
						Type:    MigrationHazardTypeAcquiresAccessExclusiveLock,
						Message: "Marking a column as not null requires a full table scan, which will lock out writes",
					},
				},
			})
		}
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
					alterColumnPrefix,
					schema.EscapeIdentifier(newColumn.Name),
					oldColumn.Type,
					newColumn.Type,
					newColumn.Collation,
				),
				// When "SET TYPE" is used to alter a column, that column's statistics are removed, which could
				// affect query plans. In order to mitigate the effect on queries, re-generate the statistics for the
				// column before continuing with the migration.
				{
					DDL:         fmt.Sprintf("ANALYZE %s (%s)", schema.EscapeIdentifier(csg.tableName), schema.EscapeIdentifier(newColumn.Name)),
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

	return stmts, nil
}

func (csg *columnSQLGenerator) generateTypeTransformationStatement(
	prefix string,
	name string,
	oldType string,
	newType string,
	newTypeCollation schema.SchemaQualifiedName,
) Statement {
	if strings.EqualFold(oldType, "bigint") &&
		strings.EqualFold(newType, "timestamp without time zone") {
		return Statement{
			DDL: fmt.Sprintf("%s SET DATA TYPE %s using to_timestamp(%s / 1000)",
				prefix,
				newType,
				name,
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
			prefix,
			newType,
			collationModifier,
			name,
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

type renameConflictingIndexSQLVertexGenerator struct {
	// indexesInOldSchemaByName is a map of index name to the index in the old schema
	// It is used to identify if an index has been re-created
	oldSchemaIndexesByName map[string]schema.Index

	indexRenamesByOldName map[string]string
}

func newRenameConflictingIndexSQLVertexGenerator(oldSchemaIndexesByName map[string]schema.Index) *renameConflictingIndexSQLVertexGenerator {
	return &renameConflictingIndexSQLVertexGenerator{
		oldSchemaIndexesByName: oldSchemaIndexesByName,
		indexRenamesByOldName:  make(map[string]string),
	}
}

func (rsg *renameConflictingIndexSQLVertexGenerator) Add(index schema.Index) ([]Statement, error) {
	if oldIndex, indexIsBeingRecreated := rsg.oldSchemaIndexesByName[index.Name]; !indexIsBeingRecreated {
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

	rsg.indexRenamesByOldName[index.Name] = newName

	return []Statement{{
		DDL:         fmt.Sprintf("ALTER INDEX %s RENAME TO %s", schema.EscapeIdentifier(index.Name), schema.EscapeIdentifier(newName)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (rsg *renameConflictingIndexSQLVertexGenerator) generateNonConflictingName(index schema.Index) (string, error) {
	uuid, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("generating UUID: %w", err)
	}

	newNameSuffix := fmt.Sprintf("_%s", uuid.String())
	idxNameTruncationIdx := len(index.Name)
	if len(index.Name) > maxPostgresIdentifierSize-len(newNameSuffix) {
		idxNameTruncationIdx = maxPostgresIdentifierSize - len(newNameSuffix)
	}

	return index.Name[:idxNameTruncationIdx] + newNameSuffix, nil
}

// rename gets the rename for the index if it eixsts, otherwise it returns an empty stringa nd false
func (rsg *renameConflictingIndexSQLVertexGenerator) rename(index string) (string, bool) {
	rename, ok := rsg.indexRenamesByOldName[index]
	if !ok {
		return "", false
	}
	return rename, true
}

func (rsg *renameConflictingIndexSQLVertexGenerator) Delete(_ schema.Index) ([]Statement, error) {
	return nil, nil
}

func (rsg *renameConflictingIndexSQLVertexGenerator) Alter(_ indexDiff) ([]Statement, error) {
	return nil, nil
}

func (*renameConflictingIndexSQLVertexGenerator) GetSQLVertexId(index schema.Index) string {
	return buildRenameConflictingIndexVertexId(index.Name)
}

func (rsg *renameConflictingIndexSQLVertexGenerator) GetAddAlterDependencies(_, _ schema.Index) []dependency {
	return nil
}

func (rsg *renameConflictingIndexSQLVertexGenerator) GetDeleteDependencies(_ schema.Index) []dependency {
	return nil
}

func buildRenameConflictingIndexVertexId(indexName string) string {
	return buildVertexId("indexrename", indexName)
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

	if _, isNewTable := isg.addedTablesByName[index.TableName]; isNewTable {
		stmts = stripMigrationHazards(stmts)
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
		if index.IsPk() {
			// If it's a primary key on a partitioned table, the index will be created implicitly through the constraint
			// If we attempt to create the index and the primary key, it will throw an error about the relation already existing
			owningTableName := schema.SchemaQualifiedName{
				// Assumes public
				SchemaName:  "public",
				EscapedName: schema.EscapeIdentifier(index.TableName),
			}
			// If the table is the base table of a partitioned table, the constraint should "ONLY" be added to the base
			//table. We can then concurrently build all of the partitioned indexes and attach them.
			// Without "ONLY", all the partitioned indexes will be automatically built
			return []Statement{{
				DDL:         fmt.Sprintf("ALTER TABLE ONLY %s ADD CONSTRAINT %s %s", owningTableName.GetFQEscapedName(), index.Constraint.EscapedConstraintName, index.Constraint.ConstraintDef),
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

	if index.IsPk() {
		addConstraintStmt, err := isg.addIndexConstraint(index)
		if err != nil {
			return nil, fmt.Errorf("generating add constraint statement: %w", err)
		}
		stmts = append(stmts, addConstraintStmt)
	}

	if index.IsPartitionOfIndex() && isg.attachPartitionSQLVertexGenerator.isPartitionAlreadyAttachedBeforeIndexBuilds(index.TableName) {
		// Only attach the index if the index is built after the table is partitioned. If the partition
		// hasn't already been attached, the index/constraint will be automatically attached when the table partition is
		// attached
		stmts = append(stmts, buildAttachIndex(index))
	}

	return stmts, nil
}

func (isg *indexSQLVertexGenerator) Delete(index schema.Index) ([]Statement, error) {
	_, tableWasDeleted := isg.deletedTablesByName[index.TableName]
	// An index will be dropped if its owning table is dropped.
	if tableWasDeleted {
		return nil, nil
	}

	if index.IsPartitionOfIndex() {
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
		if rename, hasRename := isg.renameSQLVertexGenerator.rename(index.Name); hasRename {
			escapedConstraintName = schema.EscapeIdentifier(rename)
		}

		// Dropping the constraint will automatically drop the index. There is no way to drop
		// the constraint without dropping the index
		return []Statement{
			{
				DDL: dropConstraintDDL(schema.SchemaQualifiedName{
					// Assumes public
					SchemaName:  "public",
					EscapedName: schema.EscapeIdentifier(index.TableName),
				}, escapedConstraintName),

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
	indexName := index.Name
	if rename, hasRename := isg.renameSQLVertexGenerator.rename(index.Name); hasRename {
		indexName = rename
	}

	return []Statement{{
		DDL:         fmt.Sprintf("DROP INDEX %s%s", concurrentlyModifier, schema.EscapeIdentifier(indexName)),
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

	if len(diff.old.ParentIdxName) == 0 && len(diff.new.ParentIdxName) > 0 {
		stmts = append(stmts, buildAttachIndex(diff.new))
		diff.old.ParentIdxName = diff.new.ParentIdxName
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
	if owningTable, ok := tablesInNewSchemaByName[index.TableName]; !ok {
		return false, fmt.Errorf("could not find table in new schema with name %s", index.TableName)
	} else {
		return owningTable.IsPartitioned(), nil
	}
}

func (isg *indexSQLVertexGenerator) addIndexConstraint(index schema.Index) (Statement, error) {
	owningTableName := schema.SchemaQualifiedName{
		// Assumes public
		SchemaName:  "public",
		EscapedName: schema.EscapeIdentifier(index.TableName),
	}

	return Statement{
		DDL: fmt.Sprintf("%s %s USING INDEX %s",
			addConstraintPrefix(owningTableName, index.Constraint.EscapedConstraintName),
			index.Constraint.Type,
			schema.EscapeIdentifier(index.Name)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}, nil
}

func buildAttachIndex(index schema.Index) Statement {
	return Statement{
		DDL:         fmt.Sprintf("ALTER INDEX %s ATTACH PARTITION %s", schema.EscapeIdentifier(index.ParentIdxName), schema.EscapeIdentifier(index.Name)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}
}

func (*indexSQLVertexGenerator) GetSQLVertexId(index schema.Index) string {
	return buildIndexVertexId(index.Name)
}

func (isg *indexSQLVertexGenerator) GetAddAlterDependencies(index, _ schema.Index) []dependency {
	dependencies := []dependency{
		mustRun(isg.GetSQLVertexId(index), diffTypeAddAlter).after(buildTableVertexId(index.TableName), diffTypeAddAlter),
		// To allow for online changes to indexes, rename the older version of the index (if it exists) before the new version is added
		mustRun(isg.GetSQLVertexId(index), diffTypeAddAlter).after(buildRenameConflictingIndexVertexId(index.Name), diffTypeAddAlter),
	}

	if index.IsPartitionOfIndex() {
		// Partitions of indexes must be created after the parent index is created
		dependencies = append(dependencies,
			mustRun(isg.GetSQLVertexId(index), diffTypeAddAlter).after(buildIndexVertexId(index.ParentIdxName), diffTypeAddAlter))
	}

	return dependencies
}

func (isg *indexSQLVertexGenerator) GetDeleteDependencies(index schema.Index) []dependency {
	dependencies := []dependency{
		mustRun(isg.GetSQLVertexId(index), diffTypeDelete).after(buildTableVertexId(index.TableName), diffTypeDelete),
		// Drop the index after it has been potentially renamed
		mustRun(isg.GetSQLVertexId(index), diffTypeDelete).after(buildRenameConflictingIndexVertexId(index.Name), diffTypeAddAlter),
	}

	if index.IsPartitionOfIndex() {
		// Since dropping the parent index will cause the partition of the index to drop, the parent drop should come
		// before
		dependencies = append(dependencies,
			mustRun(isg.GetSQLVertexId(index), diffTypeDelete).after(buildIndexVertexId(index.ParentIdxName), diffTypeDelete))
	}
	dependencies = append(dependencies, isg.addDepsOnTableAddAlterIfNecessary(index)...)

	return dependencies
}

func (isg *indexSQLVertexGenerator) addDepsOnTableAddAlterIfNecessary(index schema.Index) []dependency {
	// This could be cleaner if start sorting columns separately in the graph
	parentTable, ok := isg.tablesInNewSchemaByName[index.TableName]
	if !ok {
		// If the parent table is deleted, we don't need to worry about making the index statement come
		// before any alters
		return nil
	}

	// These dependencies will force the index deletion statement to come before the table AddAlter
	addAlterColumnDeps := []dependency{
		mustRun(isg.GetSQLVertexId(index), diffTypeDelete).before(buildTableVertexId(index.TableName), diffTypeAddAlter),
	}
	if len(parentTable.ParentTableName) > 0 {
		// If the table is partitioned, columns modifications occur on the base table not the children. Thus, we
		// need the dependency to also be on the parent table add/alter statements
		addAlterColumnDeps = append(
			addAlterColumnDeps,
			mustRun(isg.GetSQLVertexId(index), diffTypeDelete).before(buildTableVertexId(parentTable.ParentTableName), diffTypeAddAlter),
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

	return nil
}

type checkConstraintSQLGenerator struct {
	tableName  string
	isNewTable bool
}

func (csg *checkConstraintSQLGenerator) Add(con schema.CheckConstraint) ([]Statement, error) {
	var hazards []MigrationHazard

	// UDF's in check constraints are a bad idea. Check constraints are not re-validated
	// if the UDF changes, so it's not really a safe practice. We won't support it for now
	if len(con.DependsOnFunctions) > 0 {
		return nil, fmt.Errorf("check constraints that depend on UDFs: %w", ErrNotImplemented)
	}

	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("%s CHECK(%s)",
		addConstraintPrefix(schema.SchemaQualifiedName{
			// Assumes public
			SchemaName:  "public",
			EscapedName: schema.EscapeIdentifier(csg.tableName),
		}, schema.EscapeIdentifier(con.Name)), con.Expression))
	if !con.IsInheritable {
		sb.WriteString(" NO INHERIT")
	}

	if !con.IsValid {
		sb.WriteString(" NOT VALID")
	} else if !csg.isNewTable {
		hazards = append(hazards, MigrationHazard{
			Type: MigrationHazardTypeAcquiresAccessExclusiveLock,
			Message: "This will lock reads and writes to the owning table while the constraint is being added. " +
				"Instead, consider adding the constraint as NOT VALID and validating it later.",
		})
	}

	return []Statement{{
		DDL:         sb.String(),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     hazards,
	}}, nil
}

func (csg *checkConstraintSQLGenerator) Delete(con schema.CheckConstraint) ([]Statement, error) {
	// We won't support deleting check constraints depending on UDF's to align with not supporting adding check
	// constraints that depend on UDF's
	if len(con.DependsOnFunctions) > 0 {
		return nil, fmt.Errorf("check constraints that depend on UDFs: %w", ErrNotImplemented)
	}

	return []Statement{{
		DDL: dropConstraintDDL(schema.SchemaQualifiedName{
			// Assumes public
			SchemaName:  "public",
			EscapedName: schema.EscapeIdentifier(csg.tableName),
		}, schema.EscapeIdentifier(con.Name)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (csg *checkConstraintSQLGenerator) Alter(diff checkConstraintDiff) ([]Statement, error) {
	if cmp.Equal(diff.old, diff.new) {
		return nil, nil
	}

	oldCopy := diff.old
	oldCopy.IsValid = diff.new.IsValid
	if !cmp.Equal(oldCopy, diff.new) {
		// Technically, we could support altering expression, but I don't see the use case for it. it would require more test
		// cases than force re-adding it, and I'm not convinced it unlocks any functionality
		return nil, fmt.Errorf("altering check constraint to resolve the following diff %s: %w", cmp.Diff(oldCopy, diff.new), ErrNotImplemented)
	} else if diff.old.IsValid && !diff.new.IsValid {
		return nil, fmt.Errorf("check constraint can't go from invalid to valid")
	} else if len(diff.old.DependsOnFunctions) > 0 || len(diff.new.DependsOnFunctions) > 0 {
		return nil, fmt.Errorf("check constraints that depend on UDFs: %w", ErrNotImplemented)
	}

	return []Statement{validateConstraintStatement(schema.SchemaQualifiedName{
		// Assumes public
		SchemaName:  "public",
		EscapedName: schema.EscapeIdentifier(csg.tableName),
	}, schema.EscapeIdentifier(diff.new.Name))}, nil
}

type attachPartitionSQLVertexGenerator struct {
	indexesInNewSchemaByTableName map[string][]schema.Index
	addedTablesByName             map[string]schema.Table

	// isPartitionAttachedAfterIdxBuildsByTableName is a map of table name to whether or not the table partition will be
	// attached after its indexes are built. This is useful for determining when indexes need to be attached
	isPartitionAttachedAfterIdxBuildsByTableName map[string]bool
}

func newAttachPartitionSQLVertexGenerator(indexesInNewSchemaByTableName map[string][]schema.Index, addedTablesByName map[string]schema.Table) *attachPartitionSQLVertexGenerator {
	return &attachPartitionSQLVertexGenerator{
		indexesInNewSchemaByTableName: indexesInNewSchemaByTableName,
		addedTablesByName:             addedTablesByName,

		isPartitionAttachedAfterIdxBuildsByTableName: make(map[string]bool),
	}
}

func (*attachPartitionSQLVertexGenerator) Add(table schema.Table) ([]Statement, error) {
	if !table.IsPartition() {
		return nil, nil
	}
	return []Statement{buildAttachPartitionStatement(table)}, nil
}

func (*attachPartitionSQLVertexGenerator) Alter(_ tableDiff) ([]Statement, error) {
	return nil, nil
}

func buildAttachPartitionStatement(table schema.Table) Statement {
	return Statement{
		DDL:         fmt.Sprintf("%s ATTACH PARTITION %s %s", publicTableAlterPrefix(table.ParentTableName), schema.EscapeIdentifier(table.Name), table.ForValues),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}
}

func (*attachPartitionSQLVertexGenerator) Delete(_ schema.Table) ([]Statement, error) {
	return nil, nil
}

func (*attachPartitionSQLVertexGenerator) GetSQLVertexId(table schema.Table) string {
	return fmt.Sprintf("attachpartition_%s", table.Name)
}

func (a *attachPartitionSQLVertexGenerator) GetAddAlterDependencies(table, old schema.Table) []dependency {
	if !cmp.Equal(old, schema.Table{}) {
		// The table already exists. Skip building dependencies
		return nil
	}

	deps := []dependency{
		mustRun(a.GetSQLVertexId(table), diffTypeAddAlter).after(buildTableVertexId(table.Name), diffTypeAddAlter),
	}

	if _, baseTableIsNew := a.addedTablesByName[table.ParentTableName]; baseTableIsNew {
		// If the base table is new, we should force the partition to be attached before we build any non-local indexes.
		// This allows us to create fresh schemas where the base table has a PK but none of the children tables
		// have the PK (this is useful when creating the fresh database schema for migration validation)
		// If we attach the partition after the index is built, the index will be automatically built by Postgres
		for _, idx := range a.indexesInNewSchemaByTableName[table.ParentTableName] {
			deps = append(deps, mustRun(a.GetSQLVertexId(table), diffTypeAddAlter).before(buildIndexVertexId(idx.Name), diffTypeAddAlter))
		}
		return deps
	}

	a.isPartitionAttachedAfterIdxBuildsByTableName[table.Name] = true
	for _, idx := range a.indexesInNewSchemaByTableName[table.Name] {
		deps = append(deps, mustRun(a.GetSQLVertexId(table), diffTypeAddAlter).after(buildIndexVertexId(idx.Name), diffTypeAddAlter))
	}
	return deps
}

func (a *attachPartitionSQLVertexGenerator) isPartitionAlreadyAttachedBeforeIndexBuilds(partitionName string) bool {
	return !a.isPartitionAttachedAfterIdxBuildsByTableName[partitionName]
}

func (a *attachPartitionSQLVertexGenerator) GetDeleteDependencies(_ schema.Table) []dependency {
	return nil
}

type foreignKeyConstraintSQLVertexGenerator struct {
	// deletedTablesByName is a map of table name to tables (and partitions) that are deleted
	// This is used to identify if the owning table is being dropped (meaning
	// the foreign key can be implicitly dropped)
	deletedTablesByName map[string]schema.Table
	// addedTablesByName is a map of table name to the added tables
	// This is used to identify if hazards are necessary
	addedTablesByName map[string]schema.Table
	// indexInOldSchemaByTableName is a map of index name to the index in the old schema
	// This is used to force the foreign key constraint to be dropped before the index it depends on is dropped
	indexInOldSchemaByTableName map[string][]schema.Index
	// indexesInNewSchemaByTableName is a map of index name to the index
	// Same as above but for adds and after
	indexesInNewSchemaByTableName map[string][]schema.Index
}

func (f *foreignKeyConstraintSQLVertexGenerator) Add(con schema.ForeignKeyConstraint) ([]Statement, error) {
	var hazards []MigrationHazard
	_, isOnNewTable := f.addedTablesByName[con.OwningTableUnescapedName]
	_, isReferencedTableNew := f.addedTablesByName[con.ForeignTableUnescapedName]
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

func (*foreignKeyConstraintSQLVertexGenerator) GetSQLVertexId(con schema.ForeignKeyConstraint) string {
	return buildVertexId("fkconstraint", con.GetName())
}

func (f *foreignKeyConstraintSQLVertexGenerator) GetAddAlterDependencies(con, _ schema.ForeignKeyConstraint) []dependency {
	deps := []dependency{
		mustRun(f.GetSQLVertexId(con), diffTypeAddAlter).after(f.GetSQLVertexId(con), diffTypeDelete),
		mustRun(f.GetSQLVertexId(con), diffTypeAddAlter).after(buildTableVertexId(con.OwningTableUnescapedName), diffTypeAddAlter),
		mustRun(f.GetSQLVertexId(con), diffTypeAddAlter).after(buildTableVertexId(con.ForeignTableUnescapedName), diffTypeAddAlter),
	}
	// This is the slightly lazy way of ensuring the foreign key constraint is added after the requisite index is
	// built and marked as valid.
	// We __could__ do this just for the index the fk depends on, but that's slightly more wiring than we need right now
	// because of partitioned indexes, which are only valid when all child indexes have been built
	for _, i := range f.indexesInNewSchemaByTableName[con.ForeignTableUnescapedName] {
		deps = append(deps, mustRun(f.GetSQLVertexId(con), diffTypeAddAlter).after(buildIndexVertexId(i.Name), diffTypeAddAlter))
	}

	return deps
}

func (f *foreignKeyConstraintSQLVertexGenerator) GetDeleteDependencies(con schema.ForeignKeyConstraint) []dependency {
	deps := []dependency{
		mustRun(f.GetSQLVertexId(con), diffTypeDelete).before(buildTableVertexId(con.OwningTableUnescapedName), diffTypeDelete),
		mustRun(f.GetSQLVertexId(con), diffTypeDelete).before(buildTableVertexId(con.ForeignTableUnescapedName), diffTypeDelete),
	}
	// This is the slightly lazy way of ensuring the foreign key constraint is deleted before the index it depends on is deleted
	// We __could__ do this just for the index the fk depends on, but that's slightly more wiring than we need right now
	// because of partitioned indexes, which are only valid when all child indexes have been built
	for _, i := range f.indexInOldSchemaByTableName[con.ForeignTableUnescapedName] {
		deps = append(deps, mustRun(f.GetSQLVertexId(con), diffTypeDelete).before(buildIndexVertexId(i.Name), diffTypeDelete))
	}
	return deps
}

type sequenceSQLVertexGenerator struct {
	deletedTablesByName map[string]schema.Table
	tableDiffsByName    map[string]tableDiff
}

func (s *sequenceSQLVertexGenerator) Add(seq schema.Sequence) ([]Statement, error) {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("CREATE SEQUENCE %s\n", seq.GetFQEscapedName()))
	sb.WriteString(fmt.Sprintf("\tAS %s\n", seq.Type))
	sb.WriteString(fmt.Sprintf("\tINCREMENT BY %d\n", seq.Increment))
	sb.WriteString(fmt.Sprintf("\tMINVALUE %d MAXVALUE %d\n", seq.MinValue, seq.MaxValue))
	cycleModifier := ""
	if !seq.Cycle {
		cycleModifier = "NO "
	}
	sb.WriteString(fmt.Sprintf("\tSTART WITH %d CACHE %d %sCYCLE\n", seq.StartValue, seq.CacheSize, cycleModifier))

	var hazards []MigrationHazard
	if seq.Owner == nil {
		hazards = append(hazards, migrationHazardSequenceCannotTrackDependencies)
	}

	return []Statement{{
		DDL:         sb.String(),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     hazards,
	}}, nil
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
	// Ownership changes handled by the sequenceOwnershipSQLVertexGenerator
	diff.old.Owner = diff.new.Owner
	if !cmp.Equal(diff.old, diff.new) {
		// Technically, we could support altering expression, but I don't see the use case for it. it would require more test
		// cases than force re-adding it, and I'm not convinced it unlocks any functionality
		return nil, fmt.Errorf("altering sequence to resolve the following diff %s: %w", cmp.Diff(diff.old, diff.new), ErrNotImplemented)
	}

	return nil, nil
}

func (s *sequenceSQLVertexGenerator) GetSQLVertexId(seq schema.Sequence) string {
	return buildSequenceVertexId(seq.SchemaQualifiedName)
}

func (s *sequenceSQLVertexGenerator) GetAddAlterDependencies(new schema.Sequence, _ schema.Sequence) []dependency {
	deps := []dependency{
		mustRun(s.GetSQLVertexId(new), diffTypeAddAlter).after(s.GetSQLVertexId(new), diffTypeDelete),
	}
	if new.Owner != nil {
		// Sequences should be added/altered before the table they are owned by
		deps = append(deps, mustRun(s.GetSQLVertexId(new), diffTypeAddAlter).before(buildTableVertexId(new.Owner.TableUnescapedName), diffTypeAddAlter))
	}
	return deps
}

func (s *sequenceSQLVertexGenerator) GetDeleteDependencies(seq schema.Sequence) []dependency {
	var deps []dependency
	// This is an unfortunate hackaround. It would make sense to also have a dependency on the owner column, such that
	// the sequence can only be considered deleted after the owning column is deleted. However, we currently don't separate
	// column deletes from table add/alters. We can't build this dependency without possibly creating a circular dependency:
	// the sequence add/alter must occur before the new owner column add/alter, but the sequence delete must occur after the
	// old owner column delete (equivalent to add/alter) and the sequence add/alter. We can get away with this because
	// we, so far, no columns are ever "re-created". If we ever do support that, we'll need to revisit this.
	if seq.Owner != nil {
		deps = append(deps, mustRun(s.GetSQLVertexId(seq), diffTypeDelete).after(buildTableVertexId(seq.Owner.TableUnescapedName), diffTypeDelete))
	}
	return deps
}

func (s *sequenceSQLVertexGenerator) isDeletedWithOwningTable(seq schema.Sequence) bool {
	if _, ok := s.deletedTablesByName[seq.Owner.TableUnescapedName]; ok {
		// If the sequence is owned by a table that is also being deleted, we don't need to drop the sequence.
		return true
	}
	return false
}

func (s *sequenceSQLVertexGenerator) isDeletedWithColumns(seq schema.Sequence) bool {
	for _, dc := range s.tableDiffsByName[seq.Owner.TableUnescapedName].columnsDiff.deletes {
		if dc.Name == seq.Owner.ColumnName {
			// If the sequence is owned by a column that is also being deleted, we don't need to drop the sequence.
			return true
		}
	}
	return false
}

func buildSequenceVertexId(name schema.SchemaQualifiedName) string {
	return buildVertexId("sequence", name.GetFQEscapedName())
}

type sequenceOwnershipSQLVertexGenerator struct{}

func (s sequenceOwnershipSQLVertexGenerator) Add(seq schema.Sequence) ([]Statement, error) {
	if seq.Owner == nil {
		// If a new sequence has no owner, we don't need to alter it. The default is no owner
		return nil, nil
	}
	return []Statement{s.buildAlterOwnershipStmt(seq, nil)}, nil
}

func (s sequenceOwnershipSQLVertexGenerator) Delete(_ schema.Sequence) ([]Statement, error) {
	return nil, nil
}

func (s sequenceOwnershipSQLVertexGenerator) Alter(diff sequenceDiff) ([]Statement, error) {
	if cmp.Equal(diff.new.Owner, diff.old.Owner) {
		return nil, nil
	}
	return []Statement{s.buildAlterOwnershipStmt(diff.new, &diff.old)}, nil
}

func (s sequenceOwnershipSQLVertexGenerator) buildAlterOwnershipStmt(new schema.Sequence, old *schema.Sequence) Statement {
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

func (s sequenceOwnershipSQLVertexGenerator) GetSQLVertexId(seq schema.Sequence) string {
	return fmt.Sprintf("%s-ownership", buildSequenceVertexId(seq.SchemaQualifiedName))
}

func (s sequenceOwnershipSQLVertexGenerator) GetAddAlterDependencies(new schema.Sequence, old schema.Sequence) []dependency {
	if cmp.Equal(old.Owner, new.Owner) {
		return nil
	}

	deps := []dependency{
		// Always change ownership after the sequence has been added/altered
		mustRun(s.GetSQLVertexId(new), diffTypeAddAlter).after(buildSequenceVertexId(new.SchemaQualifiedName), diffTypeAddAlter),
	}

	if old.Owner != nil {
		// Always update ownership before the old owner has been deleted
		deps = append(deps, mustRun(s.GetSQLVertexId(new), diffTypeAddAlter).before(buildTableVertexId(old.Owner.TableUnescapedName), diffTypeDelete))
	}

	if new.Owner != nil {
		// Always update ownership after the new owner has been created
		deps = append(deps, mustRun(s.GetSQLVertexId(new), diffTypeAddAlter).after(buildTableVertexId(new.Owner.TableUnescapedName), diffTypeAddAlter))
	}

	return deps
}

func (s sequenceOwnershipSQLVertexGenerator) GetDeleteDependencies(_ schema.Sequence) []dependency {
	return nil
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

func (f *functionSQLVertexGenerator) GetSQLVertexId(function schema.Function) string {
	return buildFunctionVertexId(function.SchemaQualifiedName)
}

func (f *functionSQLVertexGenerator) GetAddAlterDependencies(newFunction, oldFunction schema.Function) []dependency {
	// Since functions can just be `CREATE OR REPLACE`, there will never be a case where a function is
	// added and dropped in the same migration. Thus, we don't need a dependency on the delete vertex of a function
	// because there won't be one if it is being added/altered
	var deps []dependency
	for _, depFunction := range newFunction.DependsOnFunctions {
		deps = append(deps, mustRun(f.GetSQLVertexId(newFunction), diffTypeAddAlter).after(buildFunctionVertexId(depFunction), diffTypeAddAlter))
	}

	if !cmp.Equal(oldFunction, schema.Function{}) {
		// If the function is being altered:
		// If the old version of the function calls other functions that are being deleted come, those deletions
		// must come after the function is altered, so it is no longer dependent on those dropped functions
		for _, depFunction := range oldFunction.DependsOnFunctions {
			deps = append(deps, mustRun(f.GetSQLVertexId(newFunction), diffTypeAddAlter).before(buildFunctionVertexId(depFunction), diffTypeDelete))
		}
	}

	return deps
}

func (f *functionSQLVertexGenerator) GetDeleteDependencies(function schema.Function) []dependency {
	var deps []dependency
	for _, depFunction := range function.DependsOnFunctions {
		deps = append(deps, mustRun(f.GetSQLVertexId(function), diffTypeDelete).before(buildFunctionVertexId(depFunction), diffTypeDelete))
	}
	return deps
}

func buildFunctionVertexId(name schema.SchemaQualifiedName) string {
	return buildVertexId("function", name.GetFQEscapedName())
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

func (t *triggerSQLVertexGenerator) GetSQLVertexId(trigger schema.Trigger) string {
	return buildVertexId("trigger", trigger.GetName())
}

func (t *triggerSQLVertexGenerator) GetAddAlterDependencies(newTrigger, oldTrigger schema.Trigger) []dependency {
	// Since a trigger can just be `CREATE OR REPLACE`, there will never be a case where a trigger is
	// added and dropped in the same migration. Thus, we don't need a dependency on the delete node of a function
	// because there won't be one if it is being added/altered
	deps := []dependency{
		mustRun(t.GetSQLVertexId(newTrigger), diffTypeAddAlter).after(buildFunctionVertexId(newTrigger.Function), diffTypeAddAlter),
		mustRun(t.GetSQLVertexId(newTrigger), diffTypeAddAlter).after(buildTableVertexId(newTrigger.OwningTableUnescapedName), diffTypeAddAlter),
	}

	if !cmp.Equal(oldTrigger, schema.Trigger{}) {
		// If the trigger is being altered:
		// If the old version of the trigger called a function being deleted, the function deletion must come after the
		// trigger is altered, so the trigger no longer has a dependency on the function
		deps = append(deps,
			mustRun(t.GetSQLVertexId(newTrigger), diffTypeAddAlter).before(buildFunctionVertexId(oldTrigger.Function), diffTypeDelete),
		)
	}

	return deps
}

func (t *triggerSQLVertexGenerator) GetDeleteDependencies(trigger schema.Trigger) []dependency {
	return []dependency{
		mustRun(t.GetSQLVertexId(trigger), diffTypeDelete).before(buildFunctionVertexId(trigger.Function), diffTypeDelete),
		mustRun(t.GetSQLVertexId(trigger), diffTypeDelete).before(buildTableVertexId(trigger.OwningTableUnescapedName), diffTypeDelete),
	}
}

func buildVertexId(objType string, id string) string {
	return fmt.Sprintf("%s_%s", objType, id)
}

func stripMigrationHazards(stmts []Statement) []Statement {
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

func publicTableAlterPrefix(tableName string) string {
	return alterTablePrefix(schema.SchemaQualifiedName{
		// Assumes public
		SchemaName:  "public",
		EscapedName: schema.EscapeIdentifier(tableName),
	})
}

func alterTablePrefix(table schema.SchemaQualifiedName) string {
	return fmt.Sprintf("ALTER TABLE %s", table.GetFQEscapedName())
}

func buildColumnDefinition(column schema.Column) string {
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
	return sb.String()
}
