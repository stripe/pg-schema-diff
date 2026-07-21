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

-- name: GetCatalogSchemas :many
SELECT
    namespace.oid::BIGINT AS schema_oid,
    namespace.nspname::TEXT AS schema_name,
    namespace.nspowner::BIGINT AS owner_oid,
    owner.rolname::TEXT AS owner_name,
    COALESCE(pg_catalog.obj_description(
        namespace.oid, 'pg_namespace'
    ), '')::TEXT AS schema_comment
FROM pg_catalog.pg_namespace AS namespace
INNER JOIN pg_catalog.pg_roles AS owner ON namespace.nspowner = owner.oid
WHERE
    namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND namespace.nspname !~ '^pg_toast'
    AND namespace.nspname !~ '^pg_temp'
ORDER BY namespace.nspname, namespace.oid;

-- name: GetCatalogRelations :many
SELECT
    relation.oid::BIGINT AS relation_oid,
    relation.relnamespace::BIGINT AS schema_oid,
    relation_namespace.nspname::TEXT AS schema_name,
    relation.relname::TEXT AS relation_name,
    COALESCE(pg_catalog.obj_description(
        relation.oid, 'pg_class'
    ), '')::TEXT AS relation_comment,
    relation.relowner::BIGINT AS owner_oid,
    owner.rolname::TEXT AS owner_name,
    relation.relkind::TEXT AS relation_kind,
    relation.relpersistence::TEXT AS persistence,
    relation.relispartition AS is_partition,
    relation.reltype::BIGINT AS row_type_oid,
    COALESCE(row_type.typnamespace, 0)::BIGINT AS row_type_schema_oid,
    COALESCE(row_type_namespace.nspname, '')::TEXT AS row_type_schema_name,
    COALESCE(row_type.typname, '')::TEXT AS row_type_name,
    COALESCE(row_type.typarray, 0)::BIGINT AS array_type_oid,
    COALESCE(array_type.typnamespace, 0)::BIGINT AS array_type_schema_oid,
    COALESCE(array_type_namespace.nspname, '')::TEXT AS array_type_schema_name,
    COALESCE(array_type.typname, '')::TEXT AS array_type_name,
    COALESCE(toast_relation.oid, 0)::BIGINT AS toast_relation_oid,
    COALESCE(toast_namespace.oid, 0)::BIGINT AS toast_schema_oid,
    COALESCE(toast_namespace.nspname, '')::TEXT AS toast_schema_name,
    COALESCE(toast_relation.relname, '')::TEXT AS toast_relation_name,
    COALESCE(index_data.indrelid, 0)::BIGINT AS indexed_relation_oid,
    COALESCE(index_constraint.oid, 0)::BIGINT AS index_constraint_oid,
    COALESCE(index_data.indisclustered, false) AS index_is_clustered,
    COALESCE(index_data.indisreplident, false) AS index_is_replica_identity,
    COALESCE(index_data.indisprimary, false) AS index_is_primary,
    COALESCE(index_data.indisunique, false) AS index_is_unique,
    COALESCE(index_data.indisexclusion, false) AS index_is_exclusion,
    COALESCE(index_data.indisvalid, false) AS index_is_valid,
    COALESCE(index_data.indisready, false) AS index_is_ready,
    COALESCE(index_data.indislive, false) AS index_is_live,
    COALESCE(extension.oid, 0)::BIGINT AS extension_oid,
    COALESCE(extension.extname, '')::TEXT AS extension_name
FROM pg_catalog.pg_class AS relation
INNER JOIN pg_catalog.pg_namespace AS relation_namespace
    ON relation.relnamespace = relation_namespace.oid
INNER JOIN pg_catalog.pg_roles AS owner ON relation.relowner = owner.oid
LEFT JOIN pg_catalog.pg_type AS row_type ON relation.reltype = row_type.oid
LEFT JOIN pg_catalog.pg_namespace AS row_type_namespace
    ON row_type.typnamespace = row_type_namespace.oid
LEFT JOIN pg_catalog.pg_type AS array_type ON row_type.typarray = array_type.oid
LEFT JOIN pg_catalog.pg_namespace AS array_type_namespace
    ON array_type.typnamespace = array_type_namespace.oid
LEFT JOIN pg_catalog.pg_class AS toast_relation
    ON relation.reltoastrelid = toast_relation.oid
LEFT JOIN pg_catalog.pg_namespace AS toast_namespace
    ON toast_relation.relnamespace = toast_namespace.oid
LEFT JOIN pg_catalog.pg_index AS index_data
    ON relation.oid = index_data.indexrelid
LEFT JOIN pg_catalog.pg_constraint AS index_constraint
    ON relation.oid = index_constraint.conindid
LEFT JOIN pg_catalog.pg_depend AS extension_dependency
    ON
        extension_dependency.classid = 'pg_class'::REGCLASS
        AND extension_dependency.objid = relation.oid
        AND extension_dependency.objsubid = 0
        AND extension_dependency.deptype = 'e'
LEFT JOIN pg_catalog.pg_extension AS extension
    ON extension_dependency.refclassid = 'pg_extension'::REGCLASS
    AND extension_dependency.refobjid = extension.oid
WHERE
    relation_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND relation_namespace.nspname !~ '^pg_toast'
    AND relation_namespace.nspname !~ '^pg_temp'
    AND relation.relkind IN ('r', 'p', 'f', 'v', 'm', 'S', 'i', 'I', 'c')
ORDER BY
    relation_namespace.nspname,
    relation.relname,
    relation.relkind,
    relation.oid;

-- name: GetCatalogConstraints :many
SELECT
    constraint_data.oid::BIGINT AS constraint_oid,
    constraint_data.conname::TEXT AS constraint_name,
    constraint_data.contype::TEXT AS constraint_type,
    constraint_data.conrelid::BIGINT AS relation_oid,
    relation.relnamespace::BIGINT AS schema_oid,
    relation_namespace.nspname::TEXT AS schema_name,
    constraint_data.conindid::BIGINT AS index_oid,
    constraint_data.conparentid::BIGINT AS parent_constraint_oid,
    constraint_data.confrelid::BIGINT AS referenced_relation_oid,
    COALESCE(constraint_data.conkey, ARRAY[]::SMALLINT[])::SMALLINT[]
        AS key_column_numbers,
    constraint_data.condeferrable AS is_deferrable,
    constraint_data.condeferred AS is_deferred,
    constraint_data.convalidated AS is_validated,
    constraint_data.conislocal AS is_local,
    constraint_data.coninhcount AS inheritance_count,
    constraint_data.connoinherit AS is_no_inherit,
    COALESCE(
        CASE
            WHEN constraint_data.contype = 'c'
                THEN pg_catalog.pg_get_expr(
                    constraint_data.conbin, constraint_data.conrelid
                )
            ELSE ''
        END,
        ''
    )::TEXT AS check_expression,
    COALESCE(pg_catalog.obj_description(
        constraint_data.oid, 'pg_constraint'
    ), '')::TEXT AS constraint_comment,
    COALESCE(extension.oid, 0)::BIGINT AS extension_oid,
    COALESCE(extension.extname, '')::TEXT AS extension_name
FROM pg_catalog.pg_constraint AS constraint_data
INNER JOIN pg_catalog.pg_class AS relation
    ON constraint_data.conrelid = relation.oid
INNER JOIN pg_catalog.pg_namespace AS relation_namespace
    ON relation.relnamespace = relation_namespace.oid
LEFT JOIN pg_catalog.pg_depend AS extension_dependency
    ON
        extension_dependency.classid = 'pg_constraint'::REGCLASS
        AND extension_dependency.objid = constraint_data.oid
        AND extension_dependency.objsubid = 0
        AND extension_dependency.deptype = 'e'
LEFT JOIN pg_catalog.pg_extension AS extension
    ON extension_dependency.refclassid = 'pg_extension'::REGCLASS
    AND extension_dependency.refobjid = extension.oid
WHERE
    relation_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND relation_namespace.nspname !~ '^pg_toast'
    AND relation_namespace.nspname !~ '^pg_temp'
    AND relation.relkind IN ('r', 'p', 'f', 'v', 'm', 'S', 'i', 'I', 'c')
ORDER BY
    relation_namespace.nspname,
    constraint_data.conname,
    constraint_data.conrelid,
    constraint_data.oid;

-- name: GetCatalogInheritanceEdges :many
SELECT
    inheritance.inhrelid::BIGINT AS child_relation_oid,
    inheritance.inhparent::BIGINT AS parent_relation_oid,
    inheritance.inhseqno AS sequence_number
FROM pg_catalog.pg_inherits AS inheritance
INNER JOIN pg_catalog.pg_class AS child_relation
    ON inheritance.inhrelid = child_relation.oid
INNER JOIN pg_catalog.pg_namespace AS child_namespace
    ON child_relation.relnamespace = child_namespace.oid
INNER JOIN pg_catalog.pg_class AS parent_relation
    ON inheritance.inhparent = parent_relation.oid
INNER JOIN pg_catalog.pg_namespace AS parent_namespace
    ON parent_relation.relnamespace = parent_namespace.oid
WHERE
    child_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND child_namespace.nspname !~ '^pg_toast'
    AND child_namespace.nspname !~ '^pg_temp'
    AND parent_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND parent_namespace.nspname !~ '^pg_toast'
    AND parent_namespace.nspname !~ '^pg_temp'
    AND child_relation.relkind IN ('r', 'p', 'f', 'v', 'm', 'S', 'i', 'I', 'c')
    AND parent_relation.relkind IN ('r', 'p', 'f', 'v', 'm', 'S', 'i', 'I', 'c')
ORDER BY inheritance.inhrelid, inheritance.inhseqno, inheritance.inhparent;

-- name: GetCatalogTables :many
SELECT
    relation.oid::BIGINT AS relation_oid,
    relation.relrowsecurity AS rls_enabled,
    relation.relforcerowsecurity AS rls_forced,
    relation.relreplident::TEXT AS replica_identity,
    COALESCE(replica_index.indexrelid, 0)::BIGINT
        AS replica_identity_index_oid,
    COALESCE(clustered_index.indexrelid, 0)::BIGINT AS clustered_index_oid,
    COALESCE(relation.reloptions, ARRAY[]::TEXT[])::TEXT[] AS options,
    relation.reltablespace::BIGINT AS tablespace_oid,
    COALESCE(tablespace.spcname, '')::TEXT AS tablespace_name,
    relation.relam::BIGINT AS access_method_oid,
    COALESCE(access_method.amname, '')::TEXT AS access_method_name
FROM pg_catalog.pg_class AS relation
INNER JOIN pg_catalog.pg_namespace AS relation_namespace
    ON relation.relnamespace = relation_namespace.oid
LEFT JOIN pg_catalog.pg_index AS replica_index
    ON relation.oid = replica_index.indrelid
    AND replica_index.indisreplident
LEFT JOIN pg_catalog.pg_index AS clustered_index
    ON relation.oid = clustered_index.indrelid
    AND clustered_index.indisclustered
LEFT JOIN pg_catalog.pg_tablespace AS tablespace
    ON relation.reltablespace = tablespace.oid
LEFT JOIN pg_catalog.pg_am AS access_method ON relation.relam = access_method.oid
WHERE
    relation_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND relation_namespace.nspname !~ '^pg_toast'
    AND relation_namespace.nspname !~ '^pg_temp'
    AND relation.relkind IN ('r', 'p', 'f')
ORDER BY relation.oid;

-- name: GetCatalogColumns :many
SELECT
    attribute.attrelid::BIGINT AS relation_oid,
    attribute.attnum AS column_number,
    CASE
        WHEN attribute.attisdropped THEN ''
        ELSE attribute.attname
    END::TEXT AS column_name,
    attribute.attisdropped AS is_dropped,
    attribute.atttypid::BIGINT AS type_oid,
    COALESCE(type_namespace.oid, 0)::BIGINT AS type_schema_oid,
    COALESCE(type_namespace.nspname, '')::TEXT AS type_schema_name,
    COALESCE(column_type.typname, '')::TEXT AS type_name,
    attribute.atttypmod AS type_modifier,
    CASE
        WHEN attribute.attisdropped THEN ''
        ELSE pg_catalog.format_type(attribute.atttypid, attribute.atttypmod)
    END::TEXT AS formatted_type,
    attribute.attcollation::BIGINT AS collation_oid,
    COALESCE(collation_namespace.oid, 0)::BIGINT AS collation_schema_oid,
    COALESCE(collation_namespace.nspname, '')::TEXT AS collation_schema_name,
    COALESCE(collation_data.collname, '')::TEXT AS collation_name,
    attribute.attnotnull AS is_not_null,
    attribute.attidentity::TEXT AS identity_mode,
    attribute.attgenerated::TEXT AS generated_mode,
    COALESCE(
        CASE
            WHEN attribute.attgenerated = ''
                THEN pg_catalog.pg_get_expr(default_value.adbin, default_value.adrelid)
            ELSE ''
        END,
        ''
    )::TEXT AS default_expression,
    COALESCE(
        CASE
            WHEN attribute.attgenerated != ''
                THEN pg_catalog.pg_get_expr(default_value.adbin, default_value.adrelid)
            ELSE ''
        END,
        ''
    )::TEXT AS generated_expression,
    attribute.atthasmissing AS has_missing_value,
    COALESCE(
        pg_catalog.encode(
            pg_catalog.array_send(attribute.attmissingval), 'hex'
        ),
        ''
    )::TEXT AS missing_value_binary,
    attribute.attstorage::TEXT AS storage_mode,
    attribute.attcompression::TEXT AS compression_mode,
    COALESCE(attribute.attoptions, ARRAY[]::TEXT[])::TEXT[] AS options,
    COALESCE(attribute.attstattarget, -1) AS statistics_target,
    attribute.attislocal AS is_local,
    attribute.attinhcount AS inheritance_count,
    COALESCE(pg_catalog.col_description(
        attribute.attrelid, attribute.attnum
    ), '')::TEXT AS column_comment
FROM pg_catalog.pg_attribute AS attribute
INNER JOIN pg_catalog.pg_class AS relation
    ON attribute.attrelid = relation.oid
INNER JOIN pg_catalog.pg_namespace AS relation_namespace
    ON relation.relnamespace = relation_namespace.oid
LEFT JOIN pg_catalog.pg_type AS column_type
    ON attribute.atttypid = column_type.oid
LEFT JOIN pg_catalog.pg_namespace AS type_namespace
    ON column_type.typnamespace = type_namespace.oid
LEFT JOIN pg_catalog.pg_collation AS collation_data
    ON attribute.attcollation = collation_data.oid
LEFT JOIN pg_catalog.pg_namespace AS collation_namespace
    ON collation_data.collnamespace = collation_namespace.oid
LEFT JOIN pg_catalog.pg_attrdef AS default_value
    ON attribute.attrelid = default_value.adrelid
    AND attribute.attnum = default_value.adnum
WHERE
    relation_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND relation_namespace.nspname !~ '^pg_toast'
    AND relation_namespace.nspname !~ '^pg_temp'
    AND relation.relkind IN ('r', 'p', 'f')
    AND attribute.attnum > 0
ORDER BY attribute.attrelid, attribute.attnum;

-- name: GetCatalogTriggers :many
SELECT
    trigger_data.oid::BIGINT AS trigger_oid,
    trigger_data.tgrelid::BIGINT AS relation_oid,
    trigger_data.tgname::TEXT AS trigger_name,
    trigger_data.tgfoid::BIGINT AS function_oid,
    trigger_data.tgtype AS trigger_type,
    trigger_data.tgenabled::TEXT AS enabled_mode,
    trigger_data.tgisinternal AS is_internal,
    trigger_data.tgparentid::BIGINT AS parent_trigger_oid,
    trigger_data.tgconstraint::BIGINT AS constraint_oid,
    pg_catalog.pg_get_triggerdef(trigger_data.oid, false)::TEXT
        AS trigger_definition,
    COALESCE(pg_catalog.obj_description(
        trigger_data.oid, 'pg_trigger'
    ), '')::TEXT AS trigger_comment
FROM pg_catalog.pg_trigger AS trigger_data
INNER JOIN pg_catalog.pg_class AS relation
    ON trigger_data.tgrelid = relation.oid
INNER JOIN pg_catalog.pg_namespace AS relation_namespace
    ON relation.relnamespace = relation_namespace.oid
WHERE
    relation_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND relation_namespace.nspname !~ '^pg_toast'
    AND relation_namespace.nspname !~ '^pg_temp'
    AND relation.relkind IN ('r', 'p', 'f')
ORDER BY trigger_data.tgrelid, trigger_data.tgname, trigger_data.oid;

-- name: GetCatalogRules :many
SELECT
    rule.oid::BIGINT AS rule_oid,
    rule.ev_class::BIGINT AS relation_oid,
    rule.rulename::TEXT AS rule_name,
    rule.ev_type::TEXT AS event_type,
    rule.ev_enabled::TEXT AS enabled_mode,
    rule.is_instead,
    pg_catalog.pg_get_ruledef(rule.oid, false)::TEXT AS rule_definition,
    COALESCE(pg_catalog.obj_description(
        rule.oid, 'pg_rewrite'
    ), '')::TEXT AS rule_comment
FROM pg_catalog.pg_rewrite AS rule
INNER JOIN pg_catalog.pg_class AS relation ON rule.ev_class = relation.oid
INNER JOIN pg_catalog.pg_namespace AS relation_namespace
    ON relation.relnamespace = relation_namespace.oid
WHERE
    relation_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND relation_namespace.nspname !~ '^pg_toast'
    AND relation_namespace.nspname !~ '^pg_temp'
    AND relation.relkind IN ('r', 'p', 'f')
ORDER BY rule.ev_class, rule.rulename, rule.oid;

-- name: GetCatalogPolicies :many
SELECT
    policy.oid::BIGINT AS policy_oid,
    policy.polrelid::BIGINT AS relation_oid,
    policy.polname::TEXT AS policy_name,
    policy.polcmd::TEXT AS command,
    policy.polpermissive AS is_permissive,
    ARRAY(
        SELECT role_oid::BIGINT
        FROM UNNEST(policy.polroles) AS policy_role (role_oid)
        ORDER BY role_oid
    )::BIGINT[] AS role_oids,
    ARRAY(
        SELECT CASE
            WHEN policy_role.role_oid = 0 THEN 'PUBLIC'
            ELSE role.rolname
        END::TEXT
        FROM UNNEST(policy.polroles) AS policy_role (role_oid)
        LEFT JOIN pg_catalog.pg_roles AS role ON policy_role.role_oid = role.oid
        ORDER BY policy_role.role_oid
    )::TEXT[] AS role_names,
    COALESCE(pg_catalog.pg_get_expr(
        policy.polqual, policy.polrelid
    ), '')::TEXT AS using_expression,
    COALESCE(pg_catalog.pg_get_expr(
        policy.polwithcheck, policy.polrelid
    ), '')::TEXT AS check_expression,
    COALESCE(pg_catalog.obj_description(
        policy.oid, 'pg_policy'
    ), '')::TEXT AS policy_comment
FROM pg_catalog.pg_policy AS policy
INNER JOIN pg_catalog.pg_class AS relation ON policy.polrelid = relation.oid
INNER JOIN pg_catalog.pg_namespace AS relation_namespace
    ON relation.relnamespace = relation_namespace.oid
WHERE
    relation_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND relation_namespace.nspname !~ '^pg_toast'
    AND relation_namespace.nspname !~ '^pg_temp'
    AND relation.relkind IN ('r', 'p', 'f')
ORDER BY policy.polrelid, policy.polname, policy.oid;

-- name: GetCatalogOwnedSequences :many
SELECT
    dependency.objid::BIGINT AS sequence_oid,
    dependency.refobjid::BIGINT AS relation_oid,
    dependency.refobjsubid AS column_number,
    owner_attribute.attname::TEXT AS column_name,
    dependency.deptype::TEXT AS dependency_type
FROM pg_catalog.pg_depend AS dependency
INNER JOIN pg_catalog.pg_class AS sequence
    ON dependency.classid = 'pg_class'::REGCLASS
    AND dependency.objid = sequence.oid
    AND sequence.relkind = 'S'
INNER JOIN pg_catalog.pg_namespace AS sequence_namespace
    ON sequence.relnamespace = sequence_namespace.oid
INNER JOIN pg_catalog.pg_class AS owner_relation
    ON dependency.refclassid = 'pg_class'::REGCLASS
    AND dependency.refobjid = owner_relation.oid
INNER JOIN pg_catalog.pg_namespace AS owner_namespace
    ON owner_relation.relnamespace = owner_namespace.oid
INNER JOIN pg_catalog.pg_attribute AS owner_attribute
    ON dependency.refobjid = owner_attribute.attrelid
    AND dependency.refobjsubid = owner_attribute.attnum
WHERE
    dependency.deptype IN ('a', 'i')
    AND dependency.objsubid = 0
    AND dependency.refobjsubid > 0
    AND sequence_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND sequence_namespace.nspname !~ '^pg_toast'
    AND sequence_namespace.nspname !~ '^pg_temp'
    AND owner_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND owner_namespace.nspname !~ '^pg_toast'
    AND owner_namespace.nspname !~ '^pg_temp'
    AND owner_relation.relkind IN ('r', 'p', 'f')
ORDER BY dependency.refobjid, dependency.refobjsubid, dependency.objid;

-- name: GetCatalogExtendedStatistics :many
SELECT
    statistic.oid::BIGINT AS statistic_oid,
    statistic.stxnamespace::BIGINT AS schema_oid,
    statistic_namespace.nspname::TEXT AS schema_name,
    statistic.stxname::TEXT AS statistic_name,
    statistic.stxowner::BIGINT AS owner_oid,
    owner.rolname::TEXT AS owner_name,
    statistic.stxrelid::BIGINT AS relation_oid,
    statistic.stxkeys::SMALLINT[] AS column_numbers,
    COALESCE(
        pg_catalog.pg_get_statisticsobjdef_expressions(statistic.oid),
        ARRAY[]::TEXT[]
    )::TEXT[] AS expressions,
    ARRAY(
        SELECT kind::TEXT
        FROM UNNEST(statistic.stxkind) AS kind
        ORDER BY kind
    )::TEXT[] AS kinds,
    COALESCE(pg_catalog.obj_description(
        statistic.oid, 'pg_statistic_ext'
    ), '')::TEXT AS statistic_comment
FROM pg_catalog.pg_statistic_ext AS statistic
INNER JOIN pg_catalog.pg_namespace AS statistic_namespace
    ON statistic.stxnamespace = statistic_namespace.oid
INNER JOIN pg_catalog.pg_roles AS owner ON statistic.stxowner = owner.oid
INNER JOIN pg_catalog.pg_class AS relation
    ON statistic.stxrelid = relation.oid
INNER JOIN pg_catalog.pg_namespace AS relation_namespace
    ON relation.relnamespace = relation_namespace.oid
WHERE
    statistic_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND statistic_namespace.nspname !~ '^pg_toast'
    AND statistic_namespace.nspname !~ '^pg_temp'
    AND relation_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND relation_namespace.nspname !~ '^pg_toast'
    AND relation_namespace.nspname !~ '^pg_temp'
    AND relation.relkind IN ('r', 'p', 'f')
ORDER BY statistic_namespace.nspname, statistic.stxname, statistic.oid;

-- name: GetCatalogPartitionAttachments :many
SELECT
    child.oid::BIGINT AS relation_oid,
    inheritance.inhparent::BIGINT AS parent_relation_oid,
    pg_catalog.pg_get_expr(child.relpartbound, child.oid)::TEXT
        AS bound_expression
FROM pg_catalog.pg_class AS child
INNER JOIN pg_catalog.pg_namespace AS child_namespace
    ON child.relnamespace = child_namespace.oid
INNER JOIN pg_catalog.pg_inherits AS inheritance
    ON child.oid = inheritance.inhrelid
INNER JOIN pg_catalog.pg_class AS parent
    ON inheritance.inhparent = parent.oid
INNER JOIN pg_catalog.pg_namespace AS parent_namespace
    ON parent.relnamespace = parent_namespace.oid
WHERE
    child.relispartition
    AND child_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND child_namespace.nspname !~ '^pg_toast'
    AND child_namespace.nspname !~ '^pg_temp'
    AND parent_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND parent_namespace.nspname !~ '^pg_toast'
    AND parent_namespace.nspname !~ '^pg_temp'
    AND child.relkind IN ('r', 'p', 'f')
    AND parent.relkind IN ('r', 'p', 'f')
ORDER BY child.oid, inheritance.inhseqno, inheritance.inhparent;

-- name: GetCatalogSecurityLabels :many
SELECT
    security_label.objoid::BIGINT AS relation_oid,
    security_label.objsubid AS column_number,
    security_label.provider::TEXT AS provider,
    security_label.label::TEXT AS label
FROM pg_catalog.pg_seclabel AS security_label
INNER JOIN pg_catalog.pg_class AS relation
    ON security_label.classoid = 'pg_class'::REGCLASS
    AND security_label.objoid = relation.oid
INNER JOIN pg_catalog.pg_namespace AS relation_namespace
    ON relation.relnamespace = relation_namespace.oid
WHERE
    relation_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND relation_namespace.nspname !~ '^pg_toast'
    AND relation_namespace.nspname !~ '^pg_temp'
    AND relation.relkind IN ('r', 'p', 'f')
ORDER BY
    security_label.objoid,
    security_label.objsubid,
    security_label.provider,
    security_label.label;

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
    AND (c.relkind = 'r' OR c.relkind = 'p')
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT depend.objid
        FROM pg_catalog.pg_depend AS depend
        WHERE
            depend.classid = 'pg_class'::REGCLASS
            AND depend.objid = c.oid
            AND depend.deptype = 'e'
    );

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
    a.attnotnull AS is_not_null,
    a.atthasmissing AS has_missing_val_optimization,
    a.attidentity::TEXT AS identity_type,
    identity_col_seq.seqstart AS start_value,
    identity_col_seq.seqincrement AS increment_value,
    identity_col_seq.seqmax AS max_value,
    identity_col_seq.seqmin AS min_value,
    identity_col_seq.seqcache AS cache_size,
    identity_col_seq.seqcycle AS is_cycle,
    COALESCE(coll.collname, '')::TEXT AS collation_name,
    COALESCE(collation_namespace.nspname, '')::TEXT AS collation_schema_name,
    COALESCE(
        CASE
            WHEN a.attgenerated = 's' THEN ''
            ELSE pg_catalog.pg_get_expr(d.adbin, d.adrelid)
        END, ''
    )::TEXT AS default_value,
    COALESCE(
        CASE
            WHEN a.attgenerated = 's'
                THEN pg_catalog.pg_get_expr(d.adbin, d.adrelid)
            ELSE ''
        END, ''
    )::TEXT AS generation_expression,
    (a.attgenerated = 's') AS is_generated,
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
    table_c.relkind::TEXT AS owning_table_relkind,
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
    AND (c.relkind = 'i' OR c.relkind = 'I')
    -- Exclude indexes of tables's  extensions
    AND NOT EXISTS (
        SELECT depend.objid
        FROM pg_catalog.pg_depend AS depend
        WHERE
            depend.classid = 'pg_class'::REGCLASS
            AND depend.objid = table_c.oid
            AND depend.deptype = 'e'
    );

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
    pg_catalog.pg_get_triggerdef(trig.oid) AS trigger_def,
    trig.tgconstraint != 0 AS is_constraint
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
    AND (depend.deptype IS null OR depend.deptype != 'i')
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
    (SELECT
        ARRAY_AGG(
            pg_enum.enumlabel
            ORDER BY pg_enum.enumsortorder
        )
    FROM pg_catalog.pg_enum
    WHERE pg_enum.enumtypid = pg_type.oid)::TEXT [] AS enum_labels
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
            0 AS is,
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

-- name: GetViews :many
SELECT
    n.nspname::TEXT AS schema_name,
    c.relname::TEXT AS view_name,
    c.reloptions::TEXT [] AS rel_options,
    (SELECT
        ARRAY_AGG(DISTINCT JSONB_BUILD_OBJECT(
            'schema', dep_ns.nspname,
            'name', dep_c.relname,
            'columns', (
                SELECT
                    ARRAY_AGG(
                        a.attname::TEXT
                        ORDER BY a.attnum
                    )
                FROM pg_catalog.pg_attribute AS a
                WHERE
                    a.attrelid = dep_c.oid
                    AND a.attnum > 0
                    AND NOT a.attisdropped
                    AND a.attnum IN (
                        -- Get only columns that the view depends on
                        SELECT DISTINCT d3.refobjsubid
                        FROM pg_catalog.pg_depend AS d3
                        WHERE
                            d3.refobjid = dep_c.oid
                            AND d3.refobjsubid > 0
                            AND d3.classid = 'pg_rewrite'::REGCLASS
                            AND EXISTS (
                                SELECT 1
                                FROM pg_catalog.pg_rewrite AS rw
                                WHERE
                                    rw.oid = d3.objid
                                    AND rw.ev_class = c.oid
                            )
                    )
            )
        ))
    FROM pg_catalog.pg_depend AS d
    INNER JOIN pg_catalog.pg_rewrite AS r ON d.objid = r.oid
    INNER JOIN pg_catalog.pg_depend AS d2 ON r.oid = d2.objid
    INNER JOIN
        pg_catalog.pg_class AS dep_c
        ON d2.refobjid = dep_c.oid AND dep_c.relkind IN ('r', 'p')
    INNER JOIN
        pg_catalog.pg_namespace AS dep_ns
        ON dep_c.relnamespace = dep_ns.oid
    -- Cast to text so table dependencies can be scanned as string arrays.
    WHERE d.refobjid = c.oid)::TEXT [] AS table_dependencies,
    PG_GET_VIEWDEF(c.oid, true) AS view_definition
FROM pg_catalog.pg_class AS c
INNER JOIN pg_catalog.pg_namespace AS n ON c.relnamespace = n.oid
WHERE
    c.relkind = 'v'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND n.nspname !~ '^pg_toast'
    AND n.nspname !~ '^pg_temp'
    AND NOT EXISTS (
        SELECT depend.objid
        FROM pg_catalog.pg_depend AS depend
        WHERE
            depend.classid = 'pg_class'::REGCLASS
            AND depend.objid = c.oid
            AND depend.deptype = 'e'
    );

-- name: GetMaterializedViews :many
SELECT
    n.nspname::TEXT AS schema_name,
    c.relname::TEXT AS view_name,
    c.reloptions::TEXT [] AS rel_options,
    COALESCE(ts.spcname, '')::TEXT AS tablespace_name,
    (SELECT
        ARRAY_AGG(DISTINCT JSONB_BUILD_OBJECT(
            'schema', dep_ns.nspname,
            'name', dep_c.relname,
            'columns', (
                SELECT
                    ARRAY_AGG(
                        a.attname::TEXT
                        ORDER BY a.attnum
                    )
                FROM pg_catalog.pg_attribute AS a
                WHERE
                    a.attrelid = dep_c.oid
                    AND a.attnum > 0
                    AND NOT a.attisdropped
                    AND a.attnum IN (
                        -- Get only columns that the materialized view depends
                        -- on.
                        SELECT DISTINCT d3.refobjsubid
                        FROM pg_catalog.pg_depend AS d3
                        WHERE
                            d3.refobjid = dep_c.oid
                            AND d3.refobjsubid > 0
                            AND d3.classid = 'pg_rewrite'::REGCLASS
                            AND EXISTS (
                                SELECT 1
                                FROM pg_catalog.pg_rewrite AS rw
                                WHERE
                                    rw.oid = d3.objid
                                    AND rw.ev_class = c.oid
                            )
                    )
            )
        ))
    FROM pg_catalog.pg_depend AS d
    INNER JOIN pg_catalog.pg_rewrite AS r ON d.objid = r.oid
    INNER JOIN pg_catalog.pg_depend AS d2 ON r.oid = d2.objid
    INNER JOIN
        pg_catalog.pg_class AS dep_c
        ON d2.refobjid = dep_c.oid AND dep_c.relkind IN ('r', 'p')
    INNER JOIN
        pg_catalog.pg_namespace AS dep_ns
        ON dep_c.relnamespace = dep_ns.oid
    -- Cast to text so table dependencies can be scanned as string arrays.
    WHERE d.refobjid = c.oid)::TEXT [] AS table_dependencies,
    PG_GET_VIEWDEF(c.oid, true) AS view_definition
FROM pg_catalog.pg_class AS c
INNER JOIN pg_catalog.pg_namespace AS n ON c.relnamespace = n.oid
LEFT JOIN pg_catalog.pg_tablespace AS ts ON c.reltablespace = ts.oid
WHERE
    c.relkind = 'm'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND n.nspname !~ '^pg_toast'
    AND n.nspname !~ '^pg_temp'
    AND NOT EXISTS (
        SELECT depend.objid
        FROM pg_catalog.pg_depend AS depend
        WHERE
            depend.classid = 'pg_class'::REGCLASS
            AND depend.objid = c.oid
            AND depend.deptype = 'e'
    );

-- name: GetTablePrivileges :many
WITH parsed_acl AS (
    SELECT
        c.oid AS table_oid,
        c.relname AS table_name,
        n.nspname AS table_schema_name,
        c.relowner AS owner_oid,
        (ACLEXPLODE(c.relacl)).grantee AS grantee_oid,
        (ACLEXPLODE(c.relacl)).privilege_type AS privilege_type,
        (ACLEXPLODE(c.relacl)).is_grantable AS is_grantable
    FROM pg_catalog.pg_class AS c
    INNER JOIN pg_catalog.pg_namespace AS n ON c.relnamespace = n.oid
    WHERE
        n.nspname NOT IN ('pg_catalog', 'information_schema')
        AND n.nspname !~ '^pg_toast'
        AND n.nspname !~ '^pg_temp'
        AND (c.relkind = 'r' OR c.relkind = 'p')
        AND c.relacl IS NOT null
        -- Exclude tables owned by extensions
        AND NOT EXISTS (
            SELECT depend.objid
            FROM pg_catalog.pg_depend AS depend
            WHERE
                depend.classid = 'pg_class'::REGCLASS
                AND depend.objid = c.oid
                AND depend.deptype = 'e'
        )
)

SELECT
    pa.table_name::TEXT,
    pa.table_schema_name::TEXT,
    COALESCE(grantee_role.rolname, '')::TEXT AS grantee,
    pa.privilege_type::TEXT AS privilege,
    pa.is_grantable
FROM parsed_acl AS pa
LEFT JOIN pg_catalog.pg_roles AS grantee_role
    ON pa.grantee_oid = grantee_role.oid
-- Exclude privileges granted to the table owner (these are implicit)
WHERE pa.grantee_oid != pa.owner_oid OR pa.grantee_oid = 0
ORDER BY pa.table_schema_name, pa.table_name, grantee, pa.privilege_type;
