-- name: GetSchemas :many
SELECT nspname::TEXT AS schema_name
FROM pg_catalog.pg_namespace
WHERE
    nspname NOT IN ('pg_catalog', 'information_schema')
    AND nspname !~ '^pg_toast'
    AND nspname !~ '^pg_temp'
    -- Exclude schemas owned by extensions
    AND NOT EXISTS (
        SELECT depend.objid
        FROM pg_catalog.pg_depend AS depend
        WHERE
            depend.classid = 'pg_namespace'::REGCLASS
            AND depend.objid = pg_namespace.oid
            AND depend.deptype = 'e'
    );

-- name: GetTables :many
SELECT
    c.oid,
    c.relname::TEXT AS table_name,
    table_namespace.nspname::TEXT AS table_schema_name,
    c.relreplident::TEXT AS replica_identity,
    c.relrowsecurity AS rls_enabled,
    c.relforcerowsecurity AS rls_forced,
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
INNER JOIN
    pg_catalog.pg_namespace AS table_namespace
    ON c.relnamespace = table_namespace.oid
LEFT JOIN
    pg_catalog.pg_inherits AS table_inherits
    ON c.oid = table_inherits.inhrelid
LEFT JOIN
    pg_catalog.pg_class AS parent_c
    ON table_inherits.inhparent = parent_c.oid
LEFT JOIN
    pg_catalog.pg_namespace AS parent_namespace
    ON parent_c.relnamespace = parent_namespace.oid
WHERE
    table_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND table_namespace.nspname !~ '^pg_toast'
    AND table_namespace.nspname !~ '^pg_temp'
    AND (c.relkind = 'r' OR c.relkind = 'p');

-- name: GetColumnsForTable :many
WITH identity_col_seq AS (
    SELECT
        depend.refobjid AS owner_relid,
        depend.refobjsubid AS owner_attnum,
        pg_seq.seqstart,
        pg_seq.seqincrement,
        pg_seq.seqmax,
        pg_seq.seqmin,
        pg_seq.seqcache,
        pg_seq.seqcycle
    FROM pg_catalog.pg_sequence AS pg_seq
    INNER JOIN pg_catalog.pg_depend AS depend
        ON
            depend.classid = 'pg_class'::REGCLASS
            AND pg_seq.seqrelid = depend.objid
            AND depend.refclassid = 'pg_class'::REGCLASS
            AND depend.deptype = 'i'
    INNER JOIN pg_catalog.pg_attribute AS owner_attr
        ON
            depend.refobjid = owner_attr.attrelid
            AND depend.refobjsubid = owner_attr.attnum
    WHERE owner_attr.attidentity != ''
)

SELECT
    a.attname::TEXT AS column_name,
    COALESCE(coll.collname, '')::TEXT AS collation_name,
    COALESCE(collation_namespace.nspname, '')::TEXT AS collation_schema_name,
    COALESCE(
        pg_catalog.pg_get_expr(d.adbin, d.adrelid), ''
    )::TEXT AS default_value,
    a.attnotnull AS is_not_null,
    a.attlen AS column_size,
    a.attidentity::TEXT AS identity_type,
    identity_col_seq.seqstart AS start_value,
    identity_col_seq.seqincrement AS increment_value,
    identity_col_seq.seqmax AS max_value,
    identity_col_seq.seqmin AS min_value,
    identity_col_seq.seqcache AS cache_size,
    identity_col_seq.seqcycle AS is_cycle,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type
FROM pg_catalog.pg_attribute AS a
LEFT JOIN
    pg_catalog.pg_attrdef AS d
    ON (a.attrelid = d.adrelid AND a.attnum = d.adnum)
LEFT JOIN pg_catalog.pg_collation AS coll ON a.attcollation = coll.oid
LEFT JOIN
    pg_catalog.pg_namespace AS collation_namespace
    ON coll.collnamespace = collation_namespace.oid
LEFT JOIN
    identity_col_seq
    ON
        a.attrelid = identity_col_seq.owner_relid
        AND a.attnum = identity_col_seq.owner_attnum
WHERE
    a.attrelid = $1
    AND a.attnum > 0
    AND NOT a.attisdropped
ORDER BY a.attnum;

-- name: GetIndexes :many
SELECT
    c.oid,
    c.relname::TEXT AS index_name,
    table_c.relname::TEXT AS table_name,
    table_namespace.nspname::TEXT AS table_schema_name,
    pg_catalog.pg_get_indexdef(c.oid)::TEXT AS def_stmt,
    COALESCE(con.conname, '')::TEXT AS constraint_name,
    COALESCE(con.contype, '')::TEXT AS constraint_type,
    COALESCE(
        pg_catalog.pg_get_constraintdef(con.oid), ''
    )::TEXT AS constraint_def,
    i.indisvalid AS index_is_valid,
    i.indisprimary AS index_is_pk,
    i.indisunique AS index_is_unique,
    COALESCE(parent_c.relname, '')::TEXT AS parent_index_name,
    COALESCE(parent_namespace.nspname, '')::TEXT AS parent_index_schema_name,
    (
        SELECT
            ARRAY_AGG(
                att.attname
                ORDER BY indkey_ord.ord
            )
        FROM UNNEST(i.indkey) WITH ORDINALITY AS indkey_ord (attnum, ord)
        INNER JOIN
            pg_catalog.pg_attribute AS att
            ON att.attrelid = table_c.oid AND indkey_ord.attnum = att.attnum
    )::TEXT [] AS column_names,
    COALESCE(con.conislocal, false) AS constraint_is_local
FROM pg_catalog.pg_class AS c
INNER JOIN pg_catalog.pg_index AS i ON (c.oid = i.indexrelid)
INNER JOIN pg_catalog.pg_class AS table_c ON (i.indrelid = table_c.oid)
INNER JOIN pg_catalog.pg_namespace AS table_namespace
    ON table_c.relnamespace = table_namespace.oid
LEFT JOIN
    pg_catalog.pg_constraint AS con
    ON (c.oid = con.conindid AND con.contype IN ('p', 'u', null))
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
    table_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND table_namespace.nspname !~ '^pg_toast'
    AND table_namespace.nspname !~ '^pg_temp'
    AND (c.relkind = 'i' OR c.relkind = 'I');

-- name: GetCheckConstraints :many
SELECT
    pg_constraint.oid,
    pg_constraint.conname::TEXT AS constraint_name,
    (
        SELECT ARRAY_AGG(a.attname)
        FROM UNNEST(pg_constraint.conkey) AS conkey
        INNER JOIN pg_catalog.pg_attribute AS a ON conkey = a.attnum
        WHERE
            a.attrelid = pg_constraint.conrelid
            AND a.attnum = ANY(pg_constraint.conkey)
            AND NOT a.attisdropped
    )::TEXT [] AS column_names,
    pg_class.relname::TEXT AS table_name,
    table_namespace.nspname::TEXT AS table_schema_name,
    pg_constraint.convalidated AS is_valid,
    pg_constraint.connoinherit AS is_not_inheritable,
    pg_catalog.pg_get_expr(
        pg_constraint.conbin, pg_constraint.conrelid
    ) AS constraint_expression
FROM pg_catalog.pg_constraint
INNER JOIN pg_catalog.pg_class ON pg_constraint.conrelid = pg_class.oid
INNER JOIN
    pg_catalog.pg_namespace AS table_namespace
    ON pg_class.relnamespace = table_namespace.oid
WHERE
    table_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND table_namespace.nspname !~ '^pg_toast'
    AND table_namespace.nspname !~ '^pg_temp'
    AND pg_constraint.contype = 'c'
    AND pg_constraint.conislocal;

-- name: GetForeignKeyConstraints :many
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
    constraint_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND constraint_namespace.nspname !~ '^pg_toast'
    AND constraint_namespace.nspname !~ '^pg_temp'
    AND pg_constraint.contype = 'f'
    AND pg_constraint.conislocal;

-- name: GetProcs :many
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
    ON pg_proc.prolang = proc_lang.oid
WHERE
    proc_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND proc_namespace.nspname !~ '^pg_toast'
    AND proc_namespace.nspname !~ '^pg_temp'
    AND pg_proc.prokind = $1
    -- Exclude functions belonging to extensions
    AND NOT EXISTS (
        SELECT depend.objid
        FROM pg_catalog.pg_depend AS depend
        WHERE
            depend.classid = 'pg_proc'::REGCLASS
            AND depend.objid = pg_proc.oid
            AND depend.deptype = 'e'
    );

-- name: GetDependsOnFunctions :many
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
    depend.classid = sqlc.arg(system_catalog)::REGCLASS
    AND depend.objid = sqlc.arg(object_id)
    AND depend.deptype = 'n';

-- name: GetTriggers :many
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
    owning_c_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND owning_c_namespace.nspname !~ '^pg_toast'
    AND owning_c_namespace.nspname !~ '^pg_temp'
    AND trig.tgparentid = 0
    AND NOT trig.tgisinternal;

-- name: GetSequences :many
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
    FORMAT_TYPE(pg_seq.seqtypid, null) AS data_type
FROM pg_catalog.pg_sequence AS pg_seq
INNER JOIN pg_catalog.pg_class AS seq_c ON pg_seq.seqrelid = seq_c.oid
INNER JOIN pg_catalog.pg_namespace AS seq_ns ON seq_c.relnamespace = seq_ns.oid
LEFT JOIN pg_catalog.pg_depend AS depend
    ON
        depend.classid = 'pg_class'::REGCLASS
        AND pg_seq.seqrelid = depend.objid
        AND depend.refclassid = 'pg_class'::REGCLASS
        AND depend.deptype IN ('a', 'i')
LEFT JOIN pg_catalog.pg_attribute AS owner_attr
    ON
        depend.refobjid = owner_attr.attrelid
        AND depend.refobjsubid = owner_attr.attnum
LEFT JOIN pg_catalog.pg_class AS owner_c ON depend.refobjid = owner_c.oid
LEFT JOIN
    pg_catalog.pg_namespace AS owner_ns
    ON owner_c.relnamespace = owner_ns.oid
WHERE
    seq_ns.nspname NOT IN ('pg_catalog', 'information_schema')
    AND seq_ns.nspname !~ '^pg_toast'
    AND seq_ns.nspname !~ '^pg_temp'
    -- Exclude sequences owned by identity columns.
    --  These manifest as internal dependency on the column
    AND (depend.deptype IS NULL OR depend.deptype != 'i')
    -- Exclude sequences belonging to extensions
    AND NOT EXISTS (
        SELECT ext_depend.objid
        FROM pg_catalog.pg_depend AS ext_depend
        WHERE
            ext_depend.classid = 'pg_class'::REGCLASS
            AND ext_depend.objid = pg_seq.seqrelid
            AND ext_depend.deptype = 'e'
    );

-- name: GetExtensions :many
SELECT
    ext.oid,
    ext.extname::TEXT AS extension_name,
    ext.extversion AS extension_version,
    extension_namespace.nspname::TEXT AS schema_name
FROM pg_catalog.pg_namespace AS extension_namespace
INNER JOIN
    pg_catalog.pg_extension AS ext
    ON extension_namespace.oid = ext.extnamespace
WHERE
    extension_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND extension_namespace.nspname !~ '^pg_toast'
    AND extension_namespace.nspname !~ '^pg_temp';


-- name: GetEnums :many
SELECT
    pg_type.typname::TEXT AS enum_name,
    type_namespace.nspname::TEXT AS enum_schema_name,
    (
        SELECT
            ARRAY_AGG(
                pg_enum.enumlabel
                ORDER BY pg_enum.enumsortorder
            )
        FROM pg_catalog.pg_enum
        WHERE pg_enum.enumtypid = pg_type.oid
    )::TEXT [] AS enum_labels
FROM pg_catalog.pg_type AS pg_type
INNER JOIN
    pg_catalog.pg_namespace AS type_namespace
    ON pg_type.typnamespace = type_namespace.oid
WHERE
    pg_type.typtype = 'e'
    AND type_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND type_namespace.nspname !~ '^pg_toast'
    AND type_namespace.nspname !~ '^pg_temp'
    -- Exclude enums belonging to extensions
    AND NOT EXISTS (
        SELECT ext_depend.objid
        FROM pg_catalog.pg_depend AS ext_depend
        WHERE
            ext_depend.classid = 'pg_class'::REGCLASS
            AND ext_depend.objid = pg_type.oid
            AND ext_depend.deptype = 'e'
    );


-- name: GetPolicies :many
WITH roles AS (
    SELECT
        oid,
        rolname
    FROM pg_catalog.pg_roles
    UNION
    (
        SELECT
            0 AS ois,
            'PUBLIC' AS role_name
    )
)

SELECT
    pol.polname::TEXT AS policy_name,
    table_c.relname::TEXT AS owning_table_name,
    table_namespace.nspname::TEXT AS owning_table_schema_name,
    pol.polpermissive AS is_permissive,
    (
        SELECT ARRAY_AGG(roles.rolname)
        FROM roles
        WHERE roles.oid = ANY(pol.polroles)
    )::TEXT [] AS applies_to,
    pol.polcmd::TEXT AS cmd,
    COALESCE(pg_catalog.pg_get_expr(
        pol.polwithcheck, pol.polrelid
    ), '')::TEXT AS check_expression,
    COALESCE(
        pg_catalog.pg_get_expr(pol.polqual, pol.polrelid), ''
    )::TEXT AS using_expression,
    (
        SELECT ARRAY_AGG(a.attname)
        FROM pg_catalog.pg_attribute AS a
        INNER JOIN pg_catalog.pg_depend AS d ON a.attnum = d.refobjsubid
        WHERE
            d.objid = pol.oid
            AND d.refobjid = table_c.oid
            AND d.refclassid = 'pg_class'::REGCLASS
            AND a.attrelid = table_c.oid
            AND NOT a.attisdropped
    )::TEXT [] AS column_names
FROM pg_catalog.pg_policy AS pol
INNER JOIN pg_catalog.pg_class AS table_c ON pol.polrelid = table_c.oid
INNER JOIN
    pg_catalog.pg_namespace AS table_namespace
    ON table_c.relnamespace = table_namespace.oid
WHERE
    table_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND table_namespace.nspname !~ '^pg_toast'
    AND table_namespace.nspname !~ '^pg_temp';
