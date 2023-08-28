// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.18.0
// source: queries.sql

package queries

import (
	"context"
)

const getCheckConstraints = `-- name: GetCheckConstraints :many
SELECT
    pg_constraint.oid,
    pg_constraint.conname::TEXT AS constraint_name,
    pg_class.relname::TEXT AS table_name,
    pg_constraint.convalidated AS is_valid,
    pg_constraint.connoinherit AS is_not_inheritable,
    pg_catalog.pg_get_expr(
        pg_constraint.conbin, pg_constraint.conrelid
    ) AS constraint_expression
FROM pg_catalog.pg_constraint
INNER JOIN pg_catalog.pg_class ON pg_constraint.conrelid = pg_class.oid
WHERE
    pg_class.relnamespace
    = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public')
    AND pg_constraint.contype = 'c'
    AND pg_constraint.conislocal
`

type GetCheckConstraintsRow struct {
	Oid                  interface{}
	ConstraintName       string
	TableName            string
	IsValid              bool
	IsNotInheritable     bool
	ConstraintExpression string
}

func (q *Queries) GetCheckConstraints(ctx context.Context) ([]GetCheckConstraintsRow, error) {
	rows, err := q.db.QueryContext(ctx, getCheckConstraints)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetCheckConstraintsRow
	for rows.Next() {
		var i GetCheckConstraintsRow
		if err := rows.Scan(
			&i.Oid,
			&i.ConstraintName,
			&i.TableName,
			&i.IsValid,
			&i.IsNotInheritable,
			&i.ConstraintExpression,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getColumnsForIndex = `-- name: GetColumnsForIndex :many
SELECT a.attname::TEXT AS column_name
FROM pg_catalog.pg_attribute AS a
WHERE
    a.attrelid = $1
    AND a.attnum > 0
ORDER BY a.attnum
`

func (q *Queries) GetColumnsForIndex(ctx context.Context, attrelid interface{}) ([]string, error) {
	rows, err := q.db.QueryContext(ctx, getColumnsForIndex, attrelid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []string
	for rows.Next() {
		var column_name string
		if err := rows.Scan(&column_name); err != nil {
			return nil, err
		}
		items = append(items, column_name)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getColumnsForTable = `-- name: GetColumnsForTable :many
SELECT
    a.attname::TEXT AS column_name,
    COALESCE(coll.collname, '')::TEXT AS collation_name,
    COALESCE(collation_namespace.nspname, '')::TEXT AS collation_schema_name,
    COALESCE(
        pg_catalog.pg_get_expr(d.adbin, d.adrelid), ''
    )::TEXT AS default_value,
    a.attnotnull AS is_not_null,
    a.attlen AS column_size,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type
FROM pg_catalog.pg_attribute AS a
LEFT JOIN
    pg_catalog.pg_attrdef AS d
    ON (d.adrelid = a.attrelid AND d.adnum = a.attnum)
LEFT JOIN pg_catalog.pg_collation AS coll ON coll.oid = a.attcollation
LEFT JOIN
    pg_catalog.pg_namespace AS collation_namespace
    ON collation_namespace.oid = coll.collnamespace
WHERE
    a.attrelid = $1
    AND a.attnum > 0
    AND NOT a.attisdropped
ORDER BY a.attnum
`

type GetColumnsForTableRow struct {
	ColumnName          string
	CollationName       string
	CollationSchemaName string
	DefaultValue        string
	IsNotNull           bool
	ColumnSize          int16
	ColumnType          string
}

func (q *Queries) GetColumnsForTable(ctx context.Context, attrelid interface{}) ([]GetColumnsForTableRow, error) {
	rows, err := q.db.QueryContext(ctx, getColumnsForTable, attrelid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetColumnsForTableRow
	for rows.Next() {
		var i GetColumnsForTableRow
		if err := rows.Scan(
			&i.ColumnName,
			&i.CollationName,
			&i.CollationSchemaName,
			&i.DefaultValue,
			&i.IsNotNull,
			&i.ColumnSize,
			&i.ColumnType,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getDependsOnFunctions = `-- name: GetDependsOnFunctions :many
SELECT
    pg_proc.proname::TEXT AS func_name,
    proc_namespace.nspname::TEXT AS func_schema_name,
    pg_catalog.pg_get_function_identity_arguments(
        pg_proc.oid
    ) AS func_identity_arguments
FROM pg_catalog.pg_depend AS depend
INNER JOIN pg_catalog.pg_proc AS pg_proc
    ON
        depend.refclassid = 'pg_proc'::REGCLASS
        AND depend.refobjid = pg_proc.oid
INNER JOIN
    pg_catalog.pg_namespace AS proc_namespace
    ON pg_proc.pronamespace = proc_namespace.oid
WHERE
    depend.classid = $1::REGCLASS
    AND depend.objid = $2
    AND depend.deptype = 'n'
`

type GetDependsOnFunctionsParams struct {
	SystemCatalog interface{}
	ObjectID      interface{}
}

type GetDependsOnFunctionsRow struct {
	FuncName              string
	FuncSchemaName        string
	FuncIdentityArguments string
}

func (q *Queries) GetDependsOnFunctions(ctx context.Context, arg GetDependsOnFunctionsParams) ([]GetDependsOnFunctionsRow, error) {
	rows, err := q.db.QueryContext(ctx, getDependsOnFunctions, arg.SystemCatalog, arg.ObjectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetDependsOnFunctionsRow
	for rows.Next() {
		var i GetDependsOnFunctionsRow
		if err := rows.Scan(&i.FuncName, &i.FuncSchemaName, &i.FuncIdentityArguments); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getExtensions = `-- name: GetExtensions :many
SELECT
    ext.oid,
    ext.extname::TEXT AS extension_name,
    ext.extversion AS extension_version,
    extension_namespace.nspname::TEXT AS schema_name
FROM pg_catalog.pg_namespace AS extension_namespace
INNER JOIN
    pg_catalog.pg_extension AS ext
    ON ext.extnamespace = extension_namespace.oid
WHERE extension_namespace.nspname = 'public'
`

type GetExtensionsRow struct {
	Oid              interface{}
	ExtensionName    string
	ExtensionVersion string
	SchemaName       string
}

func (q *Queries) GetExtensions(ctx context.Context) ([]GetExtensionsRow, error) {
	rows, err := q.db.QueryContext(ctx, getExtensions)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetExtensionsRow
	for rows.Next() {
		var i GetExtensionsRow
		if err := rows.Scan(
			&i.Oid,
			&i.ExtensionName,
			&i.ExtensionVersion,
			&i.SchemaName,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getForeignKeyConstraints = `-- name: GetForeignKeyConstraints :many
SELECT
    pg_constraint.conname::TEXT AS constraint_name,
    constraint_c.relname::TEXT AS owning_table_name,
    constraint_namespace.nspname::TEXT AS owning_table_schema_name,
    foreign_table_c.relname::TEXT AS foreign_table_name,
    foreign_table_namespace.nspname::TEXT AS foreign_table_schema_name,
    pg_constraint.convalidated AS is_valid,
    pg_catalog.pg_get_constraintdef(pg_constraint.oid) AS constraint_def
FROM pg_catalog.pg_constraint
INNER JOIN
    pg_catalog.pg_class AS constraint_c
    ON pg_constraint.conrelid = constraint_c.oid
INNER JOIN pg_catalog.pg_namespace AS constraint_namespace
    ON pg_constraint.connamespace = constraint_namespace.oid
INNER JOIN
    pg_catalog.pg_class AS foreign_table_c
    ON pg_constraint.confrelid = foreign_table_c.oid
INNER JOIN pg_catalog.pg_namespace AS foreign_table_namespace
    ON
        foreign_table_c.relnamespace = foreign_table_namespace.oid
WHERE
    constraint_namespace.nspname = 'public'
    AND pg_constraint.contype = 'f'
    AND pg_constraint.conislocal
`

type GetForeignKeyConstraintsRow struct {
	ConstraintName         string
	OwningTableName        string
	OwningTableSchemaName  string
	ForeignTableName       string
	ForeignTableSchemaName string
	IsValid                bool
	ConstraintDef          string
}

func (q *Queries) GetForeignKeyConstraints(ctx context.Context) ([]GetForeignKeyConstraintsRow, error) {
	rows, err := q.db.QueryContext(ctx, getForeignKeyConstraints)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetForeignKeyConstraintsRow
	for rows.Next() {
		var i GetForeignKeyConstraintsRow
		if err := rows.Scan(
			&i.ConstraintName,
			&i.OwningTableName,
			&i.OwningTableSchemaName,
			&i.ForeignTableName,
			&i.ForeignTableSchemaName,
			&i.IsValid,
			&i.ConstraintDef,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getFunctions = `-- name: GetFunctions :many
SELECT
    pg_proc.oid,
    pg_proc.proname::TEXT AS func_name,
    proc_namespace.nspname::TEXT AS func_schema_name,
    proc_lang.lanname::TEXT AS func_lang,
    pg_catalog.pg_get_function_identity_arguments(
        pg_proc.oid
    ) AS func_identity_arguments,
    pg_catalog.pg_get_functiondef(pg_proc.oid) AS func_def
FROM pg_catalog.pg_proc
INNER JOIN
    pg_catalog.pg_namespace AS proc_namespace
    ON pg_proc.pronamespace = proc_namespace.oid
INNER JOIN
    pg_catalog.pg_language AS proc_lang
    ON proc_lang.oid = pg_proc.prolang
WHERE
    proc_namespace.nspname = 'public'
    AND pg_proc.prokind = 'f'
    -- Exclude functions belonging to extensions
    AND NOT EXISTS (
        SELECT depend.objid
        FROM pg_catalog.pg_depend AS depend
        WHERE
            depend.classid = 'pg_proc'::REGCLASS
            AND depend.objid = pg_proc.oid
            AND depend.deptype = 'e'
    )
`

type GetFunctionsRow struct {
	Oid                   interface{}
	FuncName              string
	FuncSchemaName        string
	FuncLang              string
	FuncIdentityArguments string
	FuncDef               string
}

func (q *Queries) GetFunctions(ctx context.Context) ([]GetFunctionsRow, error) {
	rows, err := q.db.QueryContext(ctx, getFunctions)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetFunctionsRow
	for rows.Next() {
		var i GetFunctionsRow
		if err := rows.Scan(
			&i.Oid,
			&i.FuncName,
			&i.FuncSchemaName,
			&i.FuncLang,
			&i.FuncIdentityArguments,
			&i.FuncDef,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getIndexes = `-- name: GetIndexes :many
SELECT
    c.oid AS oid,
    c.relname::TEXT AS index_name,
    table_c.relname::TEXT AS table_name,
    pg_catalog.pg_get_indexdef(c.oid)::TEXT AS def_stmt,
    COALESCE(con.conname, '')::TEXT AS constraint_name,
    i.indisvalid AS index_is_valid,
    i.indisprimary AS index_is_pk,
    i.indisunique AS index_is_unique,
    COALESCE(parent_c.relname, '')::TEXT AS parent_index_name,
    COALESCE(parent_namespace.nspname, '')::TEXT AS parent_index_schema_name
FROM pg_catalog.pg_class AS c
INNER JOIN pg_catalog.pg_index AS i ON (i.indexrelid = c.oid)
INNER JOIN pg_catalog.pg_class AS table_c ON (table_c.oid = i.indrelid)
LEFT JOIN
    pg_catalog.pg_constraint AS con
    ON (con.conindid = c.oid AND con.contype IN ('p', 'u', NULL))
LEFT JOIN
    pg_catalog.pg_inherits AS idx_inherits
    ON (c.oid = idx_inherits.inhrelid)
LEFT JOIN
    pg_catalog.pg_class AS parent_c
    ON (idx_inherits.inhparent = parent_c.oid)
LEFT JOIN
    pg_catalog.pg_namespace AS parent_namespace
    ON parent_c.relnamespace = parent_namespace.oid
WHERE
    c.relnamespace
    = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public')
    AND (c.relkind = 'i' OR c.relkind = 'I')
`

type GetIndexesRow struct {
	Oid                   interface{}
	IndexName             string
	TableName             string
	DefStmt               string
	ConstraintName        string
	IndexIsValid          bool
	IndexIsPk             bool
	IndexIsUnique         bool
	ParentIndexName       string
	ParentIndexSchemaName string
}

func (q *Queries) GetIndexes(ctx context.Context) ([]GetIndexesRow, error) {
	rows, err := q.db.QueryContext(ctx, getIndexes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetIndexesRow
	for rows.Next() {
		var i GetIndexesRow
		if err := rows.Scan(
			&i.Oid,
			&i.IndexName,
			&i.TableName,
			&i.DefStmt,
			&i.ConstraintName,
			&i.IndexIsValid,
			&i.IndexIsPk,
			&i.IndexIsUnique,
			&i.ParentIndexName,
			&i.ParentIndexSchemaName,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getSequences = `-- name: GetSequences :many
SELECT
    seq_c.relname::TEXT AS sequence_name,
    seq_ns.nspname::TEXT AS sequence_schema_name,
    COALESCE(owner_attr.attname, '')::TEXT AS owner_column_name,
    COALESCE(owner_ns.nspname, '')::TEXT AS owner_schema_name,
    COALESCE(owner_c.relname, '')::TEXT AS owner_table_name,
    pg_seq.seqstart AS start_value,
    pg_seq.seqincrement AS increment_value,
    pg_seq.seqmax AS max_value,
    pg_seq.seqmin AS min_value,
    pg_seq.seqcache AS cache_size,
    pg_seq.seqcycle AS is_cycle,
    FORMAT_TYPE(pg_seq.seqtypid, NULL) AS data_type
FROM pg_catalog.pg_sequence AS pg_seq
INNER JOIN pg_catalog.pg_class AS seq_c ON pg_seq.seqrelid = seq_c.oid
INNER JOIN pg_catalog.pg_namespace AS seq_ns ON seq_c.relnamespace = seq_ns.oid
LEFT JOIN pg_catalog.pg_depend AS depend
    ON
        depend.classid = 'pg_class'::REGCLASS
        AND pg_seq.seqrelid = depend.objid
        AND depend.refclassid = 'pg_class'::REGCLASS
        AND depend.deptype = 'a'
LEFT JOIN pg_catalog.pg_attribute AS owner_attr
    ON
        depend.refobjid = owner_attr.attrelid
        AND depend.refobjsubid = owner_attr.attnum
LEFT JOIN pg_catalog.pg_class AS owner_c ON depend.refobjid = owner_c.oid
LEFT JOIN
    pg_catalog.pg_namespace AS owner_ns
    ON owner_c.relnamespace = owner_ns.oid
WHERE seq_ns.nspname = 'public'
AND NOT EXISTS (
    SELECT ext_depend.objid
    FROM pg_catalog.pg_depend AS ext_depend
    WHERE
        ext_depend.classid = 'pg_class'::REGCLASS
        AND ext_depend.objid = pg_seq.seqrelid
        AND ext_depend.deptype = 'e'
)
`

type GetSequencesRow struct {
	SequenceName       string
	SequenceSchemaName string
	OwnerColumnName    string
	OwnerSchemaName    string
	OwnerTableName     string
	StartValue         int64
	IncrementValue     int64
	MaxValue           int64
	MinValue           int64
	CacheSize          int64
	IsCycle            bool
	DataType           string
}

// It doesn't belong to an extension
func (q *Queries) GetSequences(ctx context.Context) ([]GetSequencesRow, error) {
	rows, err := q.db.QueryContext(ctx, getSequences)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetSequencesRow
	for rows.Next() {
		var i GetSequencesRow
		if err := rows.Scan(
			&i.SequenceName,
			&i.SequenceSchemaName,
			&i.OwnerColumnName,
			&i.OwnerSchemaName,
			&i.OwnerTableName,
			&i.StartValue,
			&i.IncrementValue,
			&i.MaxValue,
			&i.MinValue,
			&i.CacheSize,
			&i.IsCycle,
			&i.DataType,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getTables = `-- name: GetTables :many
SELECT
    c.oid AS oid,
    c.relname::TEXT AS table_name,
    COALESCE(parent_c.relname, '')::TEXT AS parent_table_name,
    COALESCE(parent_namespace.nspname, '')::TEXT AS parent_table_schema_name,
    (CASE
        WHEN c.relkind = 'p' THEN pg_catalog.pg_get_partkeydef(c.oid)
        ELSE ''
    END)::TEXT
    AS partition_key_def,
    (CASE
        WHEN c.relispartition THEN pg_catalog.pg_get_expr(c.relpartbound, c.oid)
        ELSE ''
    END)::TEXT AS partition_for_values
FROM pg_catalog.pg_class AS c
LEFT JOIN
    pg_catalog.pg_inherits AS table_inherits
    ON table_inherits.inhrelid = c.oid
LEFT JOIN
    pg_catalog.pg_class AS parent_c
    ON table_inherits.inhparent = parent_c.oid
LEFT JOIN
    pg_catalog.pg_namespace AS parent_namespace
    ON parent_c.relnamespace = parent_namespace.oid
WHERE
    c.relnamespace
    = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public')
    AND (c.relkind = 'r' OR c.relkind = 'p')
`

type GetTablesRow struct {
	Oid                   interface{}
	TableName             string
	ParentTableName       string
	ParentTableSchemaName string
	PartitionKeyDef       string
	PartitionForValues    string
}

func (q *Queries) GetTables(ctx context.Context) ([]GetTablesRow, error) {
	rows, err := q.db.QueryContext(ctx, getTables)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetTablesRow
	for rows.Next() {
		var i GetTablesRow
		if err := rows.Scan(
			&i.Oid,
			&i.TableName,
			&i.ParentTableName,
			&i.ParentTableSchemaName,
			&i.PartitionKeyDef,
			&i.PartitionForValues,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getTriggers = `-- name: GetTriggers :many
SELECT
    trig.tgname::TEXT AS trigger_name,
    owning_c.relname::TEXT AS owning_table_name,
    owning_c_namespace.nspname::TEXT AS owning_table_schema_name,
    pg_proc.proname::TEXT AS func_name,
    proc_namespace.nspname::TEXT AS func_schema_name,
    pg_catalog.pg_get_function_identity_arguments(
        pg_proc.oid
    ) AS func_identity_arguments,
    pg_catalog.pg_get_triggerdef(trig.oid) AS trigger_def
FROM pg_catalog.pg_trigger AS trig
INNER JOIN pg_catalog.pg_class AS owning_c ON trig.tgrelid = owning_c.oid
INNER JOIN
    pg_catalog.pg_namespace AS owning_c_namespace
    ON owning_c.relnamespace = owning_c_namespace.oid
INNER JOIN pg_catalog.pg_proc AS pg_proc ON trig.tgfoid = pg_proc.oid
INNER JOIN
    pg_catalog.pg_namespace AS proc_namespace
    ON pg_proc.pronamespace = proc_namespace.oid
WHERE
    proc_namespace.nspname = 'public'
    AND owning_c_namespace.nspname = 'public'
    AND trig.tgparentid = 0
    AND NOT trig.tgisinternal
`

type GetTriggersRow struct {
	TriggerName           string
	OwningTableName       string
	OwningTableSchemaName string
	FuncName              string
	FuncSchemaName        string
	FuncIdentityArguments string
	TriggerDef            string
}

func (q *Queries) GetTriggers(ctx context.Context) ([]GetTriggersRow, error) {
	rows, err := q.db.QueryContext(ctx, getTriggers)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetTriggersRow
	for rows.Next() {
		var i GetTriggersRow
		if err := rows.Scan(
			&i.TriggerName,
			&i.OwningTableName,
			&i.OwningTableSchemaName,
			&i.FuncName,
			&i.FuncSchemaName,
			&i.FuncIdentityArguments,
			&i.TriggerDef,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getUniqueConstraints = `-- name: GetUniqueConstraints :many
SELECT
    pg_constraint.conname::TEXT AS constraint_name,
    constraint_c.relname::TEXT AS table_name,
    constraint_namespace.nspname::TEXT AS table_schema_name,
    index_c.relname AS index_name,
    pg_constraint.conparentid,
    parent_constraint_c.relname AS parent_constraint_name,
    parent_constraint_c.relname AS parent_constraint_schema_name
FROM pg_catalog.pg_constraint
INNER JOIN
    pg_catalog.pg_class AS constraint_c
    ON pg_constraint.conrelid = constraint_c.oid
INNER JOIN pg_catalog.pg_namespace AS constraint_namespace
    ON pg_constraint.connamespace = constraint_namespace.oid
INNER JOIN
    pg_catalog.pg_class AS index_c
    ON pg_constraint.conindid = index_c.oid
LEFT JOIN
    pg_catalog.pg_constraint AS parent_constraint
    ON pg_constraint.conparentid = parent_constraint.oid
LEFT JOIN
    pg_catalog.pg_class AS parent_constraint_c
    ON parent_constraint.conrelid = parent_constraint_c.oid
LEFT JOIN
    pg_catalog.pg_class AS parent_constraint_namespace
    ON parent_constraint.connamespace = parent_constraint_namespace.oid
WHERE
    constraint_namespace.nspname = 'public'
    AND pg_constraint.contype = 'u'
`

type GetUniqueConstraintsRow struct {
	ConstraintName             string
	TableName                  string
	TableSchemaName            string
	IndexName                  interface{}
	Conparentid                interface{}
	ParentConstraintName       interface{}
	ParentConstraintSchemaName interface{}
}

func (q *Queries) GetUniqueConstraints(ctx context.Context) ([]GetUniqueConstraintsRow, error) {
	rows, err := q.db.QueryContext(ctx, getUniqueConstraints)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetUniqueConstraintsRow
	for rows.Next() {
		var i GetUniqueConstraintsRow
		if err := rows.Scan(
			&i.ConstraintName,
			&i.TableName,
			&i.TableSchemaName,
			&i.IndexName,
			&i.Conparentid,
			&i.ParentConstraintName,
			&i.ParentConstraintSchemaName,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
