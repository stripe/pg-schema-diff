-- name: GetTables :many
SELECT c.oid                                        AS oid,
       c.relname::TEXT                              AS table_name,
       COALESCE(parent_c.relname, '')::TEXT         AS parent_table_name,
       COALESCE(parent_namespace.nspname, '')::TEXT AS parent_table_schema_name,
       (CASE
            WHEN c.relkind = 'p' THEN pg_catalog.pg_get_partkeydef(c.oid)
            ELSE ''
           END)::text
                                                    AS partition_key_def,
       (CASE
            WHEN c.relispartition THEN pg_catalog.pg_get_expr(c.relpartbound, c.oid)
            ELSE ''
           END)::text                               AS partition_for_values
FROM pg_catalog.pg_class c
         LEFT JOIN pg_catalog.pg_inherits inherits ON inherits.inhrelid = c.oid
         LEFT JOIN pg_catalog.pg_class parent_c ON inherits.inhparent = parent_c.oid
         LEFT JOIN pg_catalog.pg_namespace as parent_namespace ON parent_c.relnamespace = parent_namespace.oid
WHERE c.relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public')
  AND (c.relkind = 'r' OR c.relkind = 'p');

-- name: GetColumnsForTable :many
SELECT a.attname::TEXT                                                AS column_name,
       pg_catalog.format_type(a.atttypid, a.atttypmod)                AS column_type,
       COALESCE(coll.collname, '')::TEXT                              AS collation_name,
       COALESCE(collation_namespace.nspname, '')::TEXT                AS collation_schema_name,
       COALESCE(pg_catalog.pg_get_expr(d.adbin, d.adrelid), '')::TEXT AS default_value,
       a.attnotnull                                                   AS is_not_null,
       a.attlen                                                       AS column_size
FROM pg_catalog.pg_attribute a
         LEFT JOIN pg_catalog.pg_attrdef d ON (d.adrelid = a.attrelid AND d.adnum = a.attnum)
         LEFT JOIN pg_catalog.pg_collation coll ON coll.oid = a.attcollation
         LEFT JOIN pg_catalog.pg_namespace collation_namespace ON collation_namespace.oid = coll.collnamespace
WHERE a.attrelid = $1
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;

-- name: GetIndexes :many
SELECT c.oid                                        AS oid,
       c.relname::TEXT                              as index_name,
       table_c.relname::TEXT                        as table_name,
       pg_catalog.pg_get_indexdef(c.oid)::TEXT      as def_stmt,
       COALESCE(con.conname, '')::TEXT              as constraint_name,
       i.indisvalid                                 as index_is_valid,
       i.indisprimary                               as index_is_pk,
       i.indisunique                                AS index_is_unique,
       COALESCE(parent_c.relname, '')::TEXT         as parent_index_name,
       COALESCE(parent_namespace.nspname, '')::TEXT as parent_index_schema_name
FROM pg_catalog.pg_class c
         INNER JOIN pg_catalog.pg_index i ON (i.indexrelid = c.oid)
         INNER JOIN pg_catalog.pg_class table_c ON (table_c.oid = i.indrelid)
         LEFT JOIN pg_catalog.pg_constraint con ON (con.conindid = c.oid)
         LEFT JOIN pg_catalog.pg_inherits inherits ON (c.oid = inherits.inhrelid)
         LEFT JOIN pg_catalog.pg_class parent_c ON (inherits.inhparent = parent_c.oid)
         LEFT JOIN pg_catalog.pg_namespace as parent_namespace ON parent_c.relnamespace = parent_namespace.oid
WHERE c.relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public')
  AND (c.relkind = 'i' OR c.relkind = 'I');

-- name: GetColumnsForIndex :many
SELECT a.attname::TEXT AS column_name
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = $1
  AND a.attnum > 0
ORDER BY a.attnum;

-- name: GetCheckConstraints :many
SELECT pg_constraint.oid,
       conname::TEXT                            as name,
       pg_class.relname::TEXT                   as table_name,
       pg_catalog.pg_get_expr(conbin, conrelid) as expression,
       convalidated                             as is_valid,
       connoinherit                             as is_not_inheritable
FROM pg_catalog.pg_constraint
         JOIN pg_catalog.pg_class ON pg_constraint.conrelid = pg_class.oid
WHERE pg_class.relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public')
  AND contype = 'c'
  AND pg_constraint.conislocal;

-- name: GetFunctions :many
SELECT proc.oid,
       proname::TEXT                                           as func_name,
       pg_catalog.pg_get_function_identity_arguments(proc.oid) as func_identity_arguments,
       proc_namespace.nspname::TEXT                            as func_schema_name,
       pg_catalog.pg_get_functiondef(proc.oid)                 as func_def,
       proc_lang.lanname::TEXT                                 as func_lang
FROM pg_catalog.pg_proc proc
         JOIN pg_catalog.pg_namespace proc_namespace ON proc.pronamespace = proc_namespace.oid
         JOIN pg_catalog.pg_language proc_lang ON proc_lang.oid = proc.prolang
WHERE proc_namespace.nspname = 'public'
  AND proc.prokind = 'f'
  -- Exclude functions belonging to extensions
  AND NOT EXISTS(
        SELECT depend.objid
        FROM pg_catalog.pg_depend depend
        WHERE depend.classid = 'pg_proc'::regclass
          AND depend.objid = proc.oid
          AND depend.deptype = 'e'
    );

-- name: GetDependsOnFunctions :many
SELECT proc.proname::TEXT                                      as func_name,
       pg_catalog.pg_get_function_identity_arguments(proc.oid) as func_identity_arguments,
       proc_namespace.nspname::TEXT                            as func_schema_name
FROM pg_catalog.pg_depend depend
         JOIN pg_catalog.pg_proc proc
              ON depend.refclassid = 'pg_proc'::regclass
                  AND depend.refobjid = proc.oid
         JOIN pg_catalog.pg_namespace proc_namespace ON proc.pronamespace = proc_namespace.oid
WHERE depend.classid = sqlc.arg(system_catalog)::regclass
  AND depend.objid = sqlc.arg(object_id)
  AND depend.deptype = 'n';

-- name: GetTriggers :many
SELECT trig.tgname::TEXT                                       as trigger_name,
       owning_c.relname::TEXT                                  as owning_table_name,
       owning_c_namespace.nspname::TEXT                        as owning_table_schema_name,
       proc.proname::TEXT                                      as func_name,
       pg_catalog.pg_get_function_identity_arguments(proc.oid) as func_identity_arguments,
       proc_namespace.nspname::TEXT                            as func_schema_name,
       pg_catalog.pg_get_triggerdef(trig.oid)                  as trigger_def
FROM pg_catalog.pg_trigger trig
         JOIN pg_catalog.pg_class owning_c ON trig.tgrelid = owning_c.oid
         JOIN pg_catalog.pg_namespace owning_c_namespace ON owning_c.relnamespace = owning_c_namespace.oid
         JOIN pg_catalog.pg_proc proc ON trig.tgfoid = proc.oid
         JOIN pg_catalog.pg_namespace proc_namespace ON proc.pronamespace = proc_namespace.oid
WHERE proc_namespace.nspname = 'public'
  AND owning_c_namespace.nspname = 'public'
  AND trig.tgparentid = 0
  AND NOT trig.tgisinternal;

-- name: GetSequences :many
SELECT seq_c.relname::TEXT                    as sequence_name,
       seq_ns.nspname::TEXT                   as sequence_schema_name,
       COALESCE(owner_attr.attname, '')::TEXT as owner_column_name,
       COALESCE(owner_ns.nspname, '')::TEXt   as owner_schema_name,
       COALESCE(owner_c.relname, '')::TEXT    as owner_table_name,
       format_type(seq.seqtypid, NULL)        as type,
       seq.seqstart                           as start_value,
       seq.seqincrement                       as increment,
       seq.seqmax                             as max_value,
       seq.seqmin                             as min_value,
       seq.seqcache                           as cache_size,
       seq.seqcycle                           as cycle
FROM pg_catalog.pg_sequence seq
         JOIN pg_catalog.pg_class seq_c ON seq.seqrelid = seq_c.oid
         JOIN pg_catalog.pg_namespace seq_ns ON seq_c.relnamespace = seq_ns.oid
         LEFT JOIN pg_catalog.pg_depend depend
                   ON depend.classid = 'pg_class'::regclass
                       AND seq.seqrelid = depend.objid
                       AND depend.refclassid = 'pg_class'::regclass
                       AND depend.deptype = 'a'
         LEFT JOIN pg_catalog.pg_attribute owner_attr
                   ON depend.refobjid = owner_attr.attrelid AND depend.refobjsubid = owner_attr.attnum
         LEFT JOIN pg_catalog.pg_class owner_c ON depend.refobjid = owner_c.oid
         LEFT JOIN pg_catalog.pg_namespace owner_ns ON owner_c.relnamespace = owner_ns.oid
WHERE seq_ns.nspname = 'public'
  -- It doesn't belong to an extension
  AND NOT EXISTS(
        SELECT objid
        FROM pg_catalog.pg_depend ext_depend
        WHERE ext_depend.classid = 'pg_class'::regclass
          AND ext_depend.objid = seq.seqrelid
          AND ext_depend.deptype = 'e'
    );
