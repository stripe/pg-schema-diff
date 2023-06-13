package diff

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

const (
	maxPostgresIdentifierSize = 63

	statementTimeoutDefault = 3 * time.Second
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
)

type oldAndNew[S schema.Object] struct {
	old S
	new S
}

func (o oldAndNew[S]) GetNew() S {
	return o.old
}

func (o oldAndNew[S]) GetOld() S {
	return o.new
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

	functionDiff struct {
		oldAndNew[schema.Function]
	}

	triggerDiff struct {
		oldAndNew[schema.Trigger]
	}
)

type schemaDiff struct {
	oldAndNew[schema.Schema]
	tableDiffs    listDiff[schema.Table, tableDiff]
	indexDiffs    listDiff[schema.Index, indexDiff]
	functionDiffs listDiff[schema.Function, functionDiff]
	triggerDiffs  listDiff[schema.Trigger, triggerDiff]
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
	tableDiffs, err := diffLists(old.Tables, new.Tables, buildTableDiff)
	if err != nil {
		return schemaDiff{}, false, fmt.Errorf("diffing tables: %w", err)
	}

	newSchemaTablesByName := buildSchemaObjMap(new.Tables)
	addedTablesByName := buildSchemaObjMap(tableDiffs.adds)
	indexesDiff, err := diffLists(old.Indexes, new.Indexes, func(old, new schema.Index, oldIndex, newIndex int) (indexDiff, bool, error) {
		return buildIndexDiff(newSchemaTablesByName, addedTablesByName, old, new, oldIndex, newIndex)
	})
	if err != nil {
		return schemaDiff{}, false, fmt.Errorf("diffing indexes: %w", err)
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
		tableDiffs:    tableDiffs,
		indexDiffs:    indexesDiff,
		functionDiffs: functionDiffs,
		triggerDiffs:  triggerDiffs,
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

// buildIndexDiff builds the index diff
func buildIndexDiff(newSchemaTablesByName map[string]schema.Table, addedTablesByName map[string]schema.Table, old, new schema.Index, _, _ int) (diff indexDiff, requiresRecreation bool, err error) {
	updatedOld := old

	if _, isOnNewTable := addedTablesByName[new.TableName]; isOnNewTable {
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

	if !new.IsPartitionOfIndex() && !old.IsPk && new.IsPk {
		// If the old index is not part of a primary key and the new index is part of a primary key,
		// the constraint name diff is resolvable by adding the index to the primary key.
		// Partitioned indexes are the exception; for partitioned indexes that are
		// primary keys, the indexes are created with the constraint on the base table and cannot
		// be attached to the base index
		// In the future, we can change this behavior to ONLY create the constraint on the base table
		// and follow a similar paradigm to adding indexes
		updatedOld.ConstraintName = new.ConstraintName
		updatedOld.IsPk = new.IsPk
	}

	if isOnPartitionedTable, err := isOnPartitionedTable(newSchemaTablesByName, new); err != nil {
		return indexDiff{}, false, err
	} else if isOnPartitionedTable && old.IsInvalid && !new.IsInvalid {
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
	tablesInNewSchemaByName := buildSchemaObjMap(diff.new.Tables)
	deletedTablesByName := buildSchemaObjMap(diff.tableDiffs.deletes)

	tableSQLVertexGenerator := tableSQLVertexGenerator{
		deletedTablesByName:     deletedTablesByName,
		tablesInNewSchemaByName: tablesInNewSchemaByName,
	}
	tableGraphs, err := diff.tableDiffs.resolveToSQLGraph(&tableSQLVertexGenerator)
	if err != nil {
		return nil, fmt.Errorf("resolving table sql graphs: %w", err)
	}

	indexesInNewSchemaByTableName := make(map[string][]schema.Index)
	for _, idx := range diff.new.Indexes {
		indexesInNewSchemaByTableName[idx.TableName] = append(indexesInNewSchemaByTableName[idx.TableName], idx)
	}
	attachPartitionSQLVertexGenerator := attachPartitionSQLVertexGenerator{
		indexesInNewSchemaByTableName: indexesInNewSchemaByTableName,
	}
	attachPartitionGraphs, err := diff.tableDiffs.resolveToSQLGraph(&attachPartitionSQLVertexGenerator)
	if err != nil {
		return nil, fmt.Errorf("resolving attach partition sql graphs: %w", err)
	}

	renameConflictingIndexSQLVertexGenerator := newRenameConflictingIndexSQLVertexGenerator(buildSchemaObjMap(diff.old.Indexes))
	renameConflictingIndexGraphs, err := diff.indexDiffs.resolveToSQLGraph(&renameConflictingIndexSQLVertexGenerator)
	if err != nil {
		return nil, fmt.Errorf("resolving renaming conflicting indexes: %w", err)
	}

	indexSQLVertexGenerator := indexSQLVertexGenerator{
		deletedTablesByName:      deletedTablesByName,
		addedTablesByName:        buildSchemaObjMap(diff.tableDiffs.adds),
		tablesInNewSchemaByName:  tablesInNewSchemaByName,
		indexesInNewSchemaByName: buildSchemaObjMap(diff.new.Indexes),
		indexRenamesByOldName:    renameConflictingIndexSQLVertexGenerator.getRenames(),
	}
	indexGraphs, err := diff.indexDiffs.resolveToSQLGraph(&indexSQLVertexGenerator)
	if err != nil {
		return nil, fmt.Errorf("resolving index sql graphs: %w", err)
	}

	functionsInNewSchemaByName := buildSchemaObjMap(diff.new.Functions)

	functionSQLVertexGenerator := functionSQLVertexGenerator{
		functionsInNewSchemaByName: functionsInNewSchemaByName,
	}
	functionGraphs, err := diff.functionDiffs.resolveToSQLGraph(&functionSQLVertexGenerator)
	if err != nil {
		return nil, fmt.Errorf("resolving function sql graphs: %w", err)
	}

	triggerSQLVertexGenerator := triggerSQLVertexGenerator{
		functionsInNewSchemaByName: functionsInNewSchemaByName,
	}
	triggerGraphs, err := diff.triggerDiffs.resolveToSQLGraph(&triggerSQLVertexGenerator)
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
	if err := tableGraphs.union(functionGraphs); err != nil {
		return nil, fmt.Errorf("unioning table and function graphs: %w", err)
	}
	if err := tableGraphs.union(triggerGraphs); err != nil {
		return nil, fmt.Errorf("unioning table and trigger graphs: %w", err)
	}

	return tableGraphs.toOrderedStatements()
}

func buildSchemaObjMap[S schema.Object](s []S) map[string]S {
	output := make(map[string]S)
	for _, obj := range s {
		output[obj.GetName()] = obj
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
		createTableSb.WriteString(fmt.Sprintf("PARTITION BY %s", table.PartitionKeyDef))
	}
	stmts = append(stmts, Statement{
		DDL:     createTableSb.String(),
		Timeout: statementTimeoutDefault,
	})

	csg := checkConstraintSQLGenerator{tableName: table.Name}
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
			DDL:     fmt.Sprintf("DROP TABLE %s", schema.EscapeIdentifier(table.Name)),
			Timeout: statementTimeoutTableDrop,
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

	checkConSQLGenerator := checkConstraintSQLGenerator{tableName: diff.new.Name}
	checkConGeneratedSQL, err := diff.checkConstraintDiff.resolveToSQLGroupedByEffect(&checkConSQLGenerator)
	if err != nil {
		return nil, fmt.Errorf("Resolving check constraints diff: %w", err)
	}

	var stmts []Statement
	stmts = append(stmts, checkConGeneratedSQL.Deletes...)
	stmts = append(stmts, columnGeneratedSQL.Deletes...)
	stmts = append(stmts, columnGeneratedSQL.Adds...)
	stmts = append(stmts, checkConGeneratedSQL.Adds...)
	stmts = append(stmts, columnGeneratedSQL.Alters...)
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
		alterColumnPrefix := fmt.Sprintf("%s ALTER COLUMN %s", alterTablePrefix(diff.new.Name), schema.EscapeIdentifier(colDiff.new.Name))
		if colDiff.new.IsNullable {
			stmts = append(stmts, Statement{
				DDL:     fmt.Sprintf("%s DROP NOT NULL", alterColumnPrefix),
				Timeout: statementTimeoutDefault,
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
				DDL:     fmt.Sprintf("%s SET NOT NULL", alterColumnPrefix),
				Timeout: statementTimeoutDefault,
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
		DDL:     fmt.Sprintf("%s ADD COLUMN %s", alterTablePrefix(csg.tableName), buildColumnDefinition(column)),
		Timeout: statementTimeoutDefault,
	}}, nil
}

func (csg *columnSQLGenerator) Delete(column schema.Column) ([]Statement, error) {
	return []Statement{{
		DDL:     fmt.Sprintf("%s DROP COLUMN %s", alterTablePrefix(csg.tableName), schema.EscapeIdentifier(column.Name)),
		Timeout: statementTimeoutDefault,
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
	alterColumnPrefix := fmt.Sprintf("%s ALTER COLUMN %s", alterTablePrefix(csg.tableName), schema.EscapeIdentifier(newColumn.Name))

	if oldColumn.IsNullable != newColumn.IsNullable {
		if newColumn.IsNullable {
			stmts = append(stmts, Statement{
				DDL:     fmt.Sprintf("%s DROP NOT NULL", alterColumnPrefix),
				Timeout: statementTimeoutDefault,
			})
		} else {
			stmts = append(stmts, Statement{
				DDL:     fmt.Sprintf("%s SET NOT NULL", alterColumnPrefix),
				Timeout: statementTimeoutDefault,
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
			DDL:     fmt.Sprintf("%s DROP DEFAULT", alterColumnPrefix),
			Timeout: statementTimeoutDefault,
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
					DDL:     fmt.Sprintf("ANALYZE %s (%s)", schema.EscapeIdentifier(csg.tableName), schema.EscapeIdentifier(newColumn.Name)),
					Timeout: statementTimeoutAnalyzeColumn,
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
			DDL:     fmt.Sprintf("%s SET DEFAULT %s", alterColumnPrefix, newColumn.Default),
			Timeout: statementTimeoutDefault,
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
			Timeout: statementTimeoutDefault,
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
		Timeout: statementTimeoutDefault,
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

func newRenameConflictingIndexSQLVertexGenerator(oldSchemaIndexesByName map[string]schema.Index) renameConflictingIndexSQLVertexGenerator {
	return renameConflictingIndexSQLVertexGenerator{
		oldSchemaIndexesByName: oldSchemaIndexesByName,
		indexRenamesByOldName:  make(map[string]string),
	}
}

func (rsg *renameConflictingIndexSQLVertexGenerator) Add(index schema.Index) ([]Statement, error) {
	if oldIndex, indexIsBeingRecreated := rsg.oldSchemaIndexesByName[index.Name]; !indexIsBeingRecreated {
		return nil, nil
	} else if oldIndex.IsPk && index.IsPk {
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
		DDL:     fmt.Sprintf("ALTER INDEX %s RENAME TO %s", schema.EscapeIdentifier(index.Name), schema.EscapeIdentifier(newName)),
		Timeout: statementTimeoutDefault,
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

func (rsg *renameConflictingIndexSQLVertexGenerator) getRenames() map[string]string {
	return rsg.indexRenamesByOldName
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
	// indexRenamesByOldName is a map of any renames performed by the conflicting index sql vertex generator
	indexRenamesByOldName map[string]string
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

	if index.IsPk {
		if index.IsPartitionOfIndex() {
			if parentIdx, ok := isg.indexesInNewSchemaByName[index.ParentIdxName]; !ok {
				return nil, fmt.Errorf("could not find parent index %s", index.ParentIdxName)
			} else if parentIdx.IsPk {
				// All indexes associated with parent primary keys are automatically created by their parent
				return nil, nil
			}
		}

		if isOnPartitionedTable, err := isg.isOnPartitionedTable(index); err != nil {
			return nil, err
		} else if isOnPartitionedTable {
			// A partitioned table can't have a constraint added to it with "USING INDEX", so just make the index
			// automatically through the constraint. This currently blocks and is a dangerous operation
			// If users want to be able to switch primary keys concurrently, support can be added in the future,
			// with a similar strategy to adding indexes to a partitioned table
			return []Statement{
				{
					DDL: fmt.Sprintf("%s ADD CONSTRAINT %s PRIMARY KEY (%s)",
						alterTablePrefix(index.TableName),
						schema.EscapeIdentifier(index.Name),
						strings.Join(formattedNamesForSQL(index.Columns), ", "),
					),
					Timeout: statementTimeoutDefault,
					Hazards: []MigrationHazard{
						{
							Type:    MigrationHazardTypeAcquiresShareLock,
							Message: "This will lock writes to the table while the index build occurs.",
						},
						{
							Type: MigrationHazardTypeIndexBuild,
							Message: "This is non-concurrent because adding PK's concurrently hasn't been" +
								"implemented yet. It WILL lock out writes. Index builds require a non-trivial " +
								"amount of CPU as well, which might affect database performance",
						},
					},
				},
			}, nil
		}
	}

	var stmts []Statement
	var createIdxStmtHazards []MigrationHazard

	createIdxStmt := string(index.GetIndexDefStmt)
	createIdxStmtTimeout := statementTimeoutDefault
	if isOnPartitionedTable, err := isg.isOnPartitionedTable(index); err != nil {
		return nil, err
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
		DDL:     createIdxStmt,
		Timeout: createIdxStmtTimeout,
		Hazards: createIdxStmtHazards,
	})

	_, isNewTable := isg.addedTablesByName[index.TableName]
	if index.IsPartitionOfIndex() && !isNewTable {
		// Exclude if the partition is new because the index will be attached when the partition is attached
		stmts = append(stmts, buildAttachIndex(index))
	}

	if index.IsPk {
		stmts = append(stmts, isg.addPkConstraintUsingIdx(index))
	} else if len(index.ConstraintName) > 0 {
		return nil, fmt.Errorf("constraints not supported for non-primary key indexes: %w", ErrNotImplemented)
	}
	return stmts, nil
}

func (isg *indexSQLVertexGenerator) Delete(index schema.Index) ([]Statement, error) {
	_, tableWasDeleted := isg.deletedTablesByName[index.TableName]
	// An index will be dropped if its owning table is dropped.
	// Similarly, a partition of an index will be dropped when the parent index is dropped
	if tableWasDeleted || index.IsPartitionOfIndex() {
		return nil, nil
	}

	// An index used by a primary key constraint/unique constraint cannot be dropped concurrently
	if len(index.ConstraintName) > 0 {
		// The index has been potentially renamed, which causes the constraint to be renamed. Use the updated name
		constraintName := index.ConstraintName
		if rename, hasRename := isg.indexRenamesByOldName[index.Name]; hasRename {
			constraintName = rename
		}

		// Dropping the constraint will automatically drop the index. There is no way to drop
		// the constraint without dropping the index
		return []Statement{
			{
				DDL:     dropConstraintDDL(index.TableName, constraintName),
				Timeout: statementTimeoutDefault,
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
	if rename, hasRename := isg.indexRenamesByOldName[index.Name]; hasRename {
		indexName = rename
	}

	return []Statement{{
		DDL:     fmt.Sprintf("DROP INDEX %s%s", concurrentlyModifier, schema.EscapeIdentifier(indexName)),
		Timeout: dropIndexStmtTimeout,
		Hazards: append(dropIndexStmtHazards, migrationHazardIndexDroppedQueryPerf),
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

	if !diff.new.IsPartitionOfIndex() && !diff.old.IsPk && diff.new.IsPk {
		stmts = append(stmts, isg.addPkConstraintUsingIdx(diff.new))
		diff.old.IsPk = diff.new.IsPk
		diff.old.ConstraintName = diff.new.ConstraintName
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

func (isg *indexSQLVertexGenerator) addPkConstraintUsingIdx(index schema.Index) Statement {
	return Statement{
		DDL:     fmt.Sprintf("%s ADD CONSTRAINT %s PRIMARY KEY USING INDEX %s", alterTablePrefix(index.TableName), schema.EscapeIdentifier(index.ConstraintName), schema.EscapeIdentifier(index.Name)),
		Timeout: statementTimeoutDefault,
	}
}

func buildAttachIndex(index schema.Index) Statement {
	return Statement{
		DDL:     fmt.Sprintf("ALTER INDEX %s ATTACH PARTITION %s", schema.EscapeIdentifier(index.ParentIdxName), schema.EscapeIdentifier(index.Name)),
		Timeout: statementTimeoutDefault,
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
	if index.IsPk {
		return addAlterColumnDeps
	}

	parentTableColumnsByName := buildSchemaObjMap(parentTable.Columns)
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
	tableName string
}

func (csg *checkConstraintSQLGenerator) Add(con schema.CheckConstraint) ([]Statement, error) {
	// UDF's in check constraints are a bad idea. Check constraints are not re-validated
	// if the UDF changes, so it's not really a safe practice. We won't support it for now
	if len(con.DependsOnFunctions) > 0 {
		return nil, fmt.Errorf("check constraints that depend on UDFs: %w", ErrNotImplemented)
	}

	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("%s ADD CONSTRAINT %s CHECK(%s)", alterTablePrefix(csg.tableName), schema.EscapeIdentifier(con.Name), con.Expression))
	if !con.IsInheritable {
		sb.WriteString(" NO INHERIT")
	}

	if !con.IsValid {
		sb.WriteString(" NOT VALID")
	}

	return []Statement{{
		DDL:     sb.String(),
		Timeout: statementTimeoutDefault,
	}}, nil
}

func (csg *checkConstraintSQLGenerator) Delete(con schema.CheckConstraint) ([]Statement, error) {
	// We won't support deleting check constraints depending on UDF's to align with not supporting adding check
	// constraints that depend on UDF's
	if len(con.DependsOnFunctions) > 0 {
		return nil, fmt.Errorf("check constraints that depend on UDFs: %w", ErrNotImplemented)
	}

	return []Statement{{
		DDL:     dropConstraintDDL(csg.tableName, con.Name),
		Timeout: statementTimeoutDefault,
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
		// cases than forceReadding it, and I'm not convinced it unlocks any functionality
		return nil, fmt.Errorf("altering check constraint to resolve the following diff %s: %w", cmp.Diff(oldCopy, diff.new), ErrNotImplemented)
	} else if diff.old.IsValid && !diff.new.IsValid {
		return nil, fmt.Errorf("check constraint can't go from invalid to valid")
	} else if len(diff.old.DependsOnFunctions) > 0 || len(diff.new.DependsOnFunctions) > 0 {
		return nil, fmt.Errorf("check constraints that depend on UDFs: %w", ErrNotImplemented)
	}

	return []Statement{{
		DDL:     fmt.Sprintf("%s VALIDATE CONSTRAINT %s", alterTablePrefix(csg.tableName), schema.EscapeIdentifier(diff.old.Name)),
		Timeout: statementTimeoutDefault,
	}}, nil
}

type attachPartitionSQLVertexGenerator struct {
	indexesInNewSchemaByTableName map[string][]schema.Index
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
		DDL:     fmt.Sprintf("%s ATTACH PARTITION %s %s", alterTablePrefix(table.ParentTableName), schema.EscapeIdentifier(table.Name), table.ForValues),
		Timeout: statementTimeoutDefault,
	}
}

func (*attachPartitionSQLVertexGenerator) Delete(_ schema.Table) ([]Statement, error) {
	return nil, nil
}

func (*attachPartitionSQLVertexGenerator) GetSQLVertexId(table schema.Table) string {
	return fmt.Sprintf("attachpartition_%s", table.Name)
}

func (a *attachPartitionSQLVertexGenerator) GetAddAlterDependencies(table, _ schema.Table) []dependency {
	deps := []dependency{
		mustRun(a.GetSQLVertexId(table), diffTypeAddAlter).after(buildTableVertexId(table.Name), diffTypeAddAlter),
	}

	for _, idx := range a.indexesInNewSchemaByTableName[table.Name] {
		deps = append(deps, mustRun(a.GetSQLVertexId(table), diffTypeAddAlter).after(buildIndexVertexId(idx.Name), diffTypeAddAlter))
	}
	return deps
}

func (a *attachPartitionSQLVertexGenerator) GetDeleteDependencies(_ schema.Table) []dependency {
	return nil
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
		DDL:     function.FunctionDef,
		Timeout: statementTimeoutDefault,
		Hazards: hazards,
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
		DDL:     fmt.Sprintf("DROP FUNCTION %s", function.GetFQEscapedName()),
		Timeout: statementTimeoutDefault,
		Hazards: hazards,
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
		DDL:     diff.new.FunctionDef,
		Timeout: statementTimeoutDefault,
		Hazards: hazards,
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
		DDL:     string(trigger.GetTriggerDefStmt),
		Timeout: statementTimeoutDefault,
	}}, nil
}

func (t *triggerSQLVertexGenerator) Delete(trigger schema.Trigger) ([]Statement, error) {
	return []Statement{{
		DDL:     fmt.Sprintf("DROP TRIGGER %s ON %s", trigger.EscapedName, trigger.OwningTable.GetFQEscapedName()),
		Timeout: statementTimeoutDefault,
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
		DDL:     createOrReplaceStmt,
		Timeout: statementTimeoutDefault,
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

func dropConstraintDDL(tableName, constraintName string) string {
	return fmt.Sprintf("%s DROP CONSTRAINT %s", alterTablePrefix(tableName), schema.EscapeIdentifier(constraintName))
}

func alterTablePrefix(tableName string) string {
	return fmt.Sprintf("ALTER TABLE %s", schema.EscapeIdentifier(tableName))
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

func formattedNamesForSQL(names []string) []string {
	var formattedNames []string
	for _, name := range names {
		formattedNames = append(formattedNames, schema.EscapeIdentifier(name))
	}
	return formattedNames
}
