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
    AND relation.relkind IN ('r', 'p', 'f', 'v', 'm')
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

-- name: GetCatalogACLGrants :many
WITH raw_acl AS (
    SELECT
        'schema'::TEXT AS object_class,
        'pg_namespace'::REGCLASS::OID AS class_id,
        namespace.oid AS object_id,
        0::INTEGER AS sub_object_id,
        namespace.nspowner AS owner_oid,
        acl.grantor_oid,
        acl.grantee_oid,
        acl.privilege,
        acl.is_grantable
    FROM pg_catalog.pg_namespace AS namespace
    CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
        namespace.nspacl,
        pg_catalog.acldefault('n'::"char", namespace.nspowner)
    )) AS acl (grantor_oid, grantee_oid, privilege, is_grantable)
    WHERE
        namespace.nspname NOT IN ('pg_catalog', 'information_schema')
        AND namespace.nspname !~ '^pg_toast'
        AND namespace.nspname !~ '^pg_temp'

    UNION ALL

    SELECT
        'table'::TEXT,
        'pg_class'::REGCLASS::OID,
        relation.oid,
        0::INTEGER,
        relation.relowner,
        acl.grantor_oid,
        acl.grantee_oid,
        acl.privilege,
        acl.is_grantable
    FROM pg_catalog.pg_class AS relation
    INNER JOIN pg_catalog.pg_namespace AS relation_namespace
        ON relation.relnamespace = relation_namespace.oid
    CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
        relation.relacl,
        pg_catalog.acldefault('r'::"char", relation.relowner)
    )) AS acl (grantor_oid, grantee_oid, privilege, is_grantable)
    WHERE
        relation.relkind IN ('r', 'p')
        AND relation_namespace.nspname NOT IN (
            'pg_catalog', 'information_schema'
        )
        AND relation_namespace.nspname !~ '^pg_toast'
        AND relation_namespace.nspname !~ '^pg_temp'

    UNION ALL

    SELECT
        'table'::TEXT,
        'pg_class'::REGCLASS::OID,
        relation.oid,
        attribute.attnum::INTEGER,
        relation.relowner,
        acl.grantor_oid,
        acl.grantee_oid,
        acl.privilege,
        acl.is_grantable
    FROM pg_catalog.pg_attribute AS attribute
    INNER JOIN pg_catalog.pg_class AS relation
        ON attribute.attrelid = relation.oid
    INNER JOIN pg_catalog.pg_namespace AS relation_namespace
        ON relation.relnamespace = relation_namespace.oid
    CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl)
        AS acl (grantor_oid, grantee_oid, privilege, is_grantable)
    WHERE
        relation.relkind IN ('r', 'p')
        AND attribute.attnum > 0
        AND NOT attribute.attisdropped
        AND attribute.attacl IS NOT NULL
        AND relation_namespace.nspname NOT IN (
            'pg_catalog', 'information_schema'
        )
        AND relation_namespace.nspname !~ '^pg_toast'
        AND relation_namespace.nspname !~ '^pg_temp'

    UNION ALL

    SELECT
        'sequence'::TEXT,
        'pg_class'::REGCLASS::OID,
        sequence_relation.oid,
        0::INTEGER,
        sequence_relation.relowner,
        acl.grantor_oid,
        acl.grantee_oid,
        acl.privilege,
        acl.is_grantable
    FROM pg_catalog.pg_class AS sequence_relation
    INNER JOIN pg_catalog.pg_namespace AS sequence_namespace
        ON sequence_relation.relnamespace = sequence_namespace.oid
    CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
        sequence_relation.relacl,
        pg_catalog.acldefault('S'::"char", sequence_relation.relowner)
    )) AS acl (grantor_oid, grantee_oid, privilege, is_grantable)
    WHERE
        sequence_relation.relkind = 'S'
        AND sequence_namespace.nspname NOT IN (
            'pg_catalog', 'information_schema'
        )
        AND sequence_namespace.nspname !~ '^pg_toast'
        AND sequence_namespace.nspname !~ '^pg_temp'

    UNION ALL

    SELECT
        'routine'::TEXT,
        'pg_proc'::REGCLASS::OID,
        routine.oid,
        0::INTEGER,
        routine.proowner,
        acl.grantor_oid,
        acl.grantee_oid,
        acl.privilege,
        acl.is_grantable
    FROM pg_catalog.pg_proc AS routine
    INNER JOIN pg_catalog.pg_namespace AS routine_namespace
        ON routine.pronamespace = routine_namespace.oid
    CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
        routine.proacl,
        pg_catalog.acldefault('f'::"char", routine.proowner)
    )) AS acl (grantor_oid, grantee_oid, privilege, is_grantable)
    WHERE
        routine_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
        AND routine_namespace.nspname !~ '^pg_toast'
        AND routine_namespace.nspname !~ '^pg_temp'

    UNION ALL

    SELECT
        'type'::TEXT,
        'pg_type'::REGCLASS::OID,
        type_data.oid,
        0::INTEGER,
        type_data.typowner,
        acl.grantor_oid,
        acl.grantee_oid,
        acl.privilege,
        acl.is_grantable
    FROM pg_catalog.pg_type AS type_data
    INNER JOIN pg_catalog.pg_namespace AS type_namespace
        ON type_data.typnamespace = type_namespace.oid
    LEFT JOIN pg_catalog.pg_class AS composite_relation
        ON type_data.typrelid = composite_relation.oid
    CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
        type_data.typacl,
        pg_catalog.acldefault('T'::"char", type_data.typowner)
    )) AS acl (grantor_oid, grantee_oid, privilege, is_grantable)
    WHERE
        type_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
        AND type_namespace.nspname !~ '^pg_toast'
        AND type_namespace.nspname !~ '^pg_temp'
        AND type_data.typtype IN ('b', 'c', 'd', 'e', 'r', 'm')
        AND NOT (type_data.typcategory = 'A' AND type_data.typelem != 0)
        AND (type_data.typtype != 'c' OR composite_relation.relkind = 'c')
)
SELECT
    raw_acl.object_class::TEXT AS object_class,
    raw_acl.class_id::BIGINT AS class_id,
    raw_acl.object_id::BIGINT AS object_id,
    raw_acl.sub_object_id,
    object_data.object_type::TEXT AS object_type,
    COALESCE(object_data.object_schema_name, '')::TEXT AS object_schema_name,
    COALESCE(object_data.object_name, '')::TEXT AS object_name,
    object_data.object_identity::TEXT AS object_identity,
    raw_acl.owner_oid::BIGINT AS owner_oid,
    COALESCE(owner_role.rolname, '')::TEXT AS owner_name,
    raw_acl.grantor_oid::BIGINT AS grantor_oid,
    COALESCE(grantor_role.rolname, '')::TEXT AS grantor_name,
    raw_acl.grantee_oid::BIGINT AS grantee_oid,
    CASE
        WHEN raw_acl.grantee_oid = 0 THEN 'PUBLIC'
        ELSE COALESCE(grantee_role.rolname, '')
    END::TEXT AS grantee_name,
    (raw_acl.grantee_oid = 0)::BOOLEAN AS grantee_is_public,
    raw_acl.privilege::TEXT AS privilege,
    raw_acl.is_grantable::BOOLEAN AS is_grantable
FROM raw_acl
CROSS JOIN LATERAL pg_catalog.pg_identify_object(
    raw_acl.class_id,
    raw_acl.object_id,
    raw_acl.sub_object_id
) AS object_data(
    object_type, object_schema_name, object_name, object_identity
)
LEFT JOIN pg_catalog.pg_roles AS owner_role
    ON raw_acl.owner_oid = owner_role.oid
LEFT JOIN pg_catalog.pg_roles AS grantor_role
    ON raw_acl.grantor_oid = grantor_role.oid
LEFT JOIN pg_catalog.pg_roles AS grantee_role
    ON raw_acl.grantee_oid = grantee_role.oid
ORDER BY
    raw_acl.object_class,
    raw_acl.class_id,
    raw_acl.object_id,
    raw_acl.sub_object_id,
    raw_acl.privilege,
    raw_acl.grantor_oid,
    raw_acl.grantee_oid;

-- name: GetCatalogDefaultACLs :many
SELECT
    default_acl.oid::BIGINT AS default_acl_oid,
    default_acl.defaclrole::BIGINT AS owner_oid,
    COALESCE(owner_role.rolname, '')::TEXT AS owner_name,
    default_acl.defaclnamespace::BIGINT AS schema_oid,
    COALESCE(namespace.nspname, '')::TEXT AS schema_name,
    (default_acl.defaclnamespace = 0)::BOOLEAN AS is_global,
    CASE default_acl.defaclobjtype
        WHEN 'r' THEN 'table'
        WHEN 'S' THEN 'sequence'
        WHEN 'f' THEN 'routine'
        WHEN 'T' THEN 'type'
        WHEN 'n' THEN 'schema'
    END::TEXT AS object_type,
    acl.grantor_oid::BIGINT AS grantor_oid,
    COALESCE(grantor_role.rolname, '')::TEXT AS grantor_name,
    acl.grantee_oid::BIGINT AS grantee_oid,
    CASE
        WHEN acl.grantee_oid = 0 THEN 'PUBLIC'
        ELSE COALESCE(grantee_role.rolname, '')
    END::TEXT AS grantee_name,
    (acl.grantee_oid = 0)::BOOLEAN AS grantee_is_public,
    acl.privilege::TEXT AS privilege,
    acl.is_grantable::BOOLEAN AS is_grantable
FROM pg_catalog.pg_default_acl AS default_acl
LEFT JOIN pg_catalog.pg_namespace AS namespace
    ON default_acl.defaclnamespace = namespace.oid
CROSS JOIN LATERAL pg_catalog.aclexplode(default_acl.defaclacl)
    AS acl (grantor_oid, grantee_oid, privilege, is_grantable)
LEFT JOIN pg_catalog.pg_roles AS owner_role
    ON default_acl.defaclrole = owner_role.oid
LEFT JOIN pg_catalog.pg_roles AS grantor_role
    ON acl.grantor_oid = grantor_role.oid
LEFT JOIN pg_catalog.pg_roles AS grantee_role
    ON acl.grantee_oid = grantee_role.oid
WHERE
    default_acl.defaclobjtype IN ('r', 'S', 'f', 'T', 'n')
    AND (
        default_acl.defaclnamespace = 0
        OR (
            namespace.nspname NOT IN ('pg_catalog', 'information_schema')
            AND namespace.nspname !~ '^pg_toast'
            AND namespace.nspname !~ '^pg_temp'
        )
    )
ORDER BY
    default_acl.defaclrole,
    default_acl.defaclnamespace,
    default_acl.defaclobjtype,
    privilege,
    grantor_oid,
    grantee_oid,
    default_acl.oid;

-- name: GetCatalogRoles :many
SELECT
    role.oid::BIGINT AS role_oid,
    role.rolname::TEXT AS role_name,
    COALESCE(role.rolsuper, false)::BOOLEAN AS is_superuser,
    COALESCE(role.rolinherit, false)::BOOLEAN AS inherits_privileges,
    COALESCE(role.rolcreaterole, false)::BOOLEAN AS can_create_roles,
    COALESCE(role.rolcanlogin, false)::BOOLEAN AS can_login,
    COALESCE(role.rolreplication, false)::BOOLEAN AS can_replicate,
    COALESCE(role.rolbypassrls, false)::BOOLEAN AS bypasses_rls
FROM pg_catalog.pg_roles AS role
ORDER BY role.oid, role.rolname;

-- name: GetCatalogRoleMemberships :many
SELECT
    membership.roleid::BIGINT AS role_oid,
    COALESCE(granted_role.rolname, '')::TEXT AS role_name,
    membership.member::BIGINT AS member_oid,
    COALESCE(member_role.rolname, '')::TEXT AS member_name,
    membership.grantor::BIGINT AS grantor_oid,
    COALESCE(grantor_role.rolname, '')::TEXT AS grantor_name,
    membership.admin_option AS admin_option,
    COALESCE(
        (pg_catalog.to_jsonb(membership)->>'inherit_option')::BOOLEAN,
        true
    )::BOOLEAN AS inherit_option,
    COALESCE(
        (pg_catalog.to_jsonb(membership)->>'set_option')::BOOLEAN,
        true
    )::BOOLEAN AS set_option
FROM pg_catalog.pg_auth_members AS membership
LEFT JOIN pg_catalog.pg_roles AS granted_role
    ON membership.roleid = granted_role.oid
LEFT JOIN pg_catalog.pg_roles AS member_role
    ON membership.member = member_role.oid
LEFT JOIN pg_catalog.pg_roles AS grantor_role
    ON membership.grantor = grantor_role.oid
ORDER BY
    membership.roleid,
    membership.member,
    membership.grantor,
    membership.admin_option;

-- name: GetCatalogDependencies :many
WITH raw_dependencies AS (
    SELECT
        false AS is_shared,
        dependency.classid AS class_id,
        dependency.objid AS object_id,
        dependency.objsubid AS sub_object_id,
        dependency.refclassid AS referenced_class_id,
        dependency.refobjid AS referenced_object_id,
        dependency.refobjsubid AS referenced_sub_object_id,
        dependency.deptype AS dependency_type
    FROM pg_catalog.pg_depend AS dependency

    UNION ALL

    SELECT
        true AS is_shared,
        dependency.classid AS class_id,
        dependency.objid AS object_id,
        0 AS sub_object_id,
        dependency.refclassid AS referenced_class_id,
        dependency.refobjid AS referenced_object_id,
        0 AS referenced_sub_object_id,
        dependency.deptype AS dependency_type
    FROM pg_catalog.pg_shdepend AS dependency
    WHERE dependency.dbid = (
        SELECT database.oid
        FROM pg_catalog.pg_database AS database
        WHERE database.datname = pg_catalog.current_database()
    )
), identified_dependencies AS (
    SELECT
        dependency.*,
        object_data.object_type,
        COALESCE(object_data.object_schema_name, '') AS object_schema_name,
        COALESCE(object_data.object_name, '') AS object_name,
        object_data.object_identity,
        referenced_object_data.object_type AS referenced_object_type,
        COALESCE(referenced_object_data.object_schema_name, '')
            AS referenced_object_schema_name,
        COALESCE(referenced_object_data.object_name, '')
            AS referenced_object_name,
        referenced_object_data.object_identity AS referenced_object_identity
    FROM raw_dependencies AS dependency
    CROSS JOIN LATERAL pg_catalog.pg_identify_object(
        dependency.class_id,
        dependency.object_id,
        dependency.sub_object_id
    ) AS object_data(
        object_type, object_schema_name, object_name, object_identity
    )
    CROSS JOIN LATERAL pg_catalog.pg_identify_object(
        dependency.referenced_class_id,
        dependency.referenced_object_id,
        dependency.referenced_sub_object_id
    ) AS referenced_object_data(
        object_type, object_schema_name, object_name, object_identity
    )
)
SELECT
    dependency.is_shared,
    dependency.class_id::BIGINT AS class_id,
    dependency.object_id::BIGINT AS object_id,
    dependency.sub_object_id,
    dependency.object_type::TEXT AS object_type,
    dependency.object_schema_name::TEXT AS object_schema_name,
    dependency.object_name::TEXT AS object_name,
    dependency.object_identity::TEXT AS object_identity,
    dependency.referenced_class_id::BIGINT AS referenced_class_id,
    dependency.referenced_object_id::BIGINT AS referenced_object_id,
    dependency.referenced_sub_object_id,
    dependency.referenced_object_type::TEXT AS referenced_object_type,
    dependency.referenced_object_schema_name::TEXT
        AS referenced_object_schema_name,
    dependency.referenced_object_name::TEXT AS referenced_object_name,
    dependency.referenced_object_identity::TEXT AS referenced_object_identity,
    dependency.dependency_type::TEXT AS dependency_type
FROM identified_dependencies AS dependency
WHERE
    (
        dependency.object_schema_name != ''
        AND dependency.object_schema_name NOT IN (
            'pg_catalog', 'information_schema'
        )
        AND dependency.object_schema_name !~ '^pg_toast'
        AND dependency.object_schema_name !~ '^pg_temp'
    )
    OR (
        dependency.referenced_object_schema_name != ''
        AND dependency.referenced_object_schema_name NOT IN (
            'pg_catalog', 'information_schema'
        )
        AND dependency.referenced_object_schema_name !~ '^pg_toast'
        AND dependency.referenced_object_schema_name !~ '^pg_temp'
    )
    OR (
        dependency.class_id = 'pg_namespace'::REGCLASS
        AND EXISTS (
            SELECT 1
            FROM pg_catalog.pg_namespace AS namespace
            WHERE
                namespace.oid = dependency.object_id
                AND namespace.nspname NOT IN (
                    'pg_catalog', 'information_schema'
                )
                AND namespace.nspname !~ '^pg_toast'
                AND namespace.nspname !~ '^pg_temp'
        )
    )
    OR (
        dependency.referenced_class_id = 'pg_namespace'::REGCLASS
        AND EXISTS (
            SELECT 1
            FROM pg_catalog.pg_namespace AS namespace
            WHERE
                namespace.oid = dependency.referenced_object_id
                AND namespace.nspname NOT IN (
                    'pg_catalog', 'information_schema'
                )
                AND namespace.nspname !~ '^pg_toast'
                AND namespace.nspname !~ '^pg_temp'
        )
    )
    OR dependency.class_id IN (
        'pg_extension'::REGCLASS,
        'pg_event_trigger'::REGCLASS,
        'pg_publication'::REGCLASS,
        'pg_publication_rel'::REGCLASS,
        'pg_publication_namespace'::REGCLASS
    )
    OR dependency.referenced_class_id IN (
        'pg_extension'::REGCLASS,
        'pg_event_trigger'::REGCLASS,
        'pg_publication'::REGCLASS,
        'pg_publication_rel'::REGCLASS,
        'pg_publication_namespace'::REGCLASS
    )
ORDER BY
    dependency.is_shared,
    dependency.class_id,
    dependency.object_id,
    dependency.sub_object_id,
    dependency.referenced_class_id,
    dependency.referenced_object_id,
    dependency.referenced_sub_object_id,
    dependency.dependency_type;

-- name: GetCatalogForeignKeys :many
SELECT
    constraint_data.oid::BIGINT AS constraint_oid,
    constraint_data.connamespace::BIGINT AS schema_oid,
    constraint_namespace.nspname::TEXT AS schema_name,
    constraint_data.conname::TEXT AS constraint_name,
    constraint_data.conrelid::BIGINT AS owning_relation_oid,
    owning_namespace.nspname::TEXT AS owning_schema_name,
    owning_relation.relname::TEXT AS owning_relation_name,
    constraint_data.confrelid::BIGINT AS referenced_relation_oid,
    referenced_namespace.nspname::TEXT AS referenced_schema_name,
    referenced_relation.relname::TEXT AS referenced_relation_name,
    constraint_data.conkey::SMALLINT[] AS owning_column_numbers,
    ARRAY(
        SELECT attribute.attname::TEXT
        FROM UNNEST(constraint_data.conkey) WITH ORDINALITY
            AS key_column (column_number, position)
        INNER JOIN pg_catalog.pg_attribute AS attribute
            ON attribute.attrelid = constraint_data.conrelid
            AND attribute.attnum = key_column.column_number
        ORDER BY key_column.position
    )::TEXT[] AS owning_column_names,
    constraint_data.confkey::SMALLINT[] AS referenced_column_numbers,
    ARRAY(
        SELECT attribute.attname::TEXT
        FROM UNNEST(constraint_data.confkey) WITH ORDINALITY
            AS key_column (column_number, position)
        INNER JOIN pg_catalog.pg_attribute AS attribute
            ON attribute.attrelid = constraint_data.confrelid
            AND attribute.attnum = key_column.column_number
        ORDER BY key_column.position
    )::TEXT[] AS referenced_column_names,
    constraint_data.confmatchtype::TEXT AS match_type,
    constraint_data.confupdtype::TEXT AS update_action,
    constraint_data.confdeltype::TEXT AS delete_action,
    constraint_data.condeferrable AS is_deferrable,
    constraint_data.condeferred AS is_deferred,
    constraint_data.convalidated AS is_validated,
    constraint_data.conparentid::BIGINT AS parent_constraint_oid,
    pg_catalog.pg_get_constraintdef(
        constraint_data.oid, false
    )::TEXT AS constraint_definition
FROM pg_catalog.pg_constraint AS constraint_data
INNER JOIN pg_catalog.pg_namespace AS constraint_namespace
    ON constraint_data.connamespace = constraint_namespace.oid
INNER JOIN pg_catalog.pg_class AS owning_relation
    ON constraint_data.conrelid = owning_relation.oid
INNER JOIN pg_catalog.pg_namespace AS owning_namespace
    ON owning_relation.relnamespace = owning_namespace.oid
INNER JOIN pg_catalog.pg_class AS referenced_relation
    ON constraint_data.confrelid = referenced_relation.oid
INNER JOIN pg_catalog.pg_namespace AS referenced_namespace
    ON referenced_relation.relnamespace = referenced_namespace.oid
WHERE
    constraint_data.contype = 'f'
    AND owning_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND owning_namespace.nspname !~ '^pg_toast'
    AND owning_namespace.nspname !~ '^pg_temp'
ORDER BY
    constraint_namespace.nspname,
    constraint_data.conname,
    constraint_data.conrelid,
    constraint_data.oid;

-- name: GetCatalogViews :many
SELECT
    relation.oid::BIGINT AS relation_oid,
    relation_namespace.oid::BIGINT AS schema_oid,
    relation_namespace.nspname::TEXT AS schema_name,
    relation.relname::TEXT AS view_name,
    relation.relkind::TEXT AS relation_kind,
    relation.relispopulated AS is_populated,
    COALESCE(relation.reloptions, ARRAY[]::TEXT[])::TEXT[] AS options,
    pg_catalog.pg_get_viewdef(relation.oid, true)::TEXT AS view_definition
FROM pg_catalog.pg_class AS relation
INNER JOIN pg_catalog.pg_namespace AS relation_namespace
    ON relation.relnamespace = relation_namespace.oid
WHERE
    relation.relkind IN ('v', 'm')
    AND relation_namespace.nspname NOT IN (
        'pg_catalog', 'information_schema'
    )
    AND relation_namespace.nspname !~ '^pg_toast'
    AND relation_namespace.nspname !~ '^pg_temp'
ORDER BY
    relation_namespace.nspname,
    relation.relname,
    relation.relkind,
    relation.oid;

-- name: GetCatalogRoutines :many
SELECT
    routine.oid::BIGINT AS routine_oid,
    routine.pronamespace::BIGINT AS schema_oid,
    routine_namespace.nspname::TEXT AS schema_name,
    routine.proname::TEXT AS routine_name,
    routine.proowner::BIGINT AS owner_oid,
    owner.rolname::TEXT AS owner_name,
    routine.prokind::TEXT AS routine_kind,
    routine.prolang::BIGINT AS language_oid,
    language.lanname::TEXT AS language_name,
    pg_catalog.pg_get_function_identity_arguments(
        routine.oid
    )::TEXT AS identity_arguments,
    pg_catalog.pg_get_function_arguments(routine.oid)::TEXT AS arguments,
    COALESCE(pg_catalog.pg_get_function_result(routine.oid), '')::TEXT AS result,
    ARRAY(
        SELECT argument_type::BIGINT
        FROM UNNEST(routine.proargtypes) WITH ORDINALITY
            AS argument (argument_type, position)
        ORDER BY argument.position
    )::BIGINT[] AS input_argument_type_oids,
    ARRAY(
        SELECT argument_type::BIGINT
        FROM UNNEST(COALESCE(routine.proallargtypes, ARRAY[]::OID[]))
            WITH ORDINALITY AS argument (argument_type, position)
        ORDER BY argument.position
    )::BIGINT[] AS all_argument_type_oids,
    COALESCE(routine.proargmodes, ARRAY[]::"char"[])::TEXT[]
        AS argument_modes,
    COALESCE(routine.proargnames, ARRAY[]::TEXT[])::TEXT[]
        AS argument_names,
    routine.prorettype::BIGINT AS result_type_oid,
    routine.proretset AS returns_set,
    routine.prosrc::TEXT AS source,
    COALESCE(routine.probin, '')::TEXT AS binary,
    (routine.prosqlbody IS NOT NULL)::BOOLEAN AS has_sql_body,
    COALESCE(routine.prosqlbody::TEXT, '')::TEXT AS sql_body,
    COALESCE(routine.proconfig, ARRAY[]::TEXT[])::TEXT[] AS configuration,
    pg_catalog.pg_get_functiondef(routine.oid)::TEXT AS definition
FROM pg_catalog.pg_proc AS routine
INNER JOIN pg_catalog.pg_namespace AS routine_namespace
    ON routine.pronamespace = routine_namespace.oid
INNER JOIN pg_catalog.pg_roles AS owner ON routine.proowner = owner.oid
INNER JOIN pg_catalog.pg_language AS language ON routine.prolang = language.oid
WHERE
    routine_namespace.nspname !~ '^pg_toast'
    AND routine_namespace.nspname !~ '^pg_temp'
    AND routine.prokind != 'a'
ORDER BY
    routine_namespace.nspname,
    routine.proname,
    pg_catalog.pg_get_function_identity_arguments(routine.oid),
    routine.oid;

-- name: GetCatalogTypes :many
SELECT
    type_data.oid::BIGINT AS type_oid,
    type_data.typnamespace::BIGINT AS schema_oid,
    type_namespace.nspname::TEXT AS schema_name,
    type_data.typname::TEXT AS type_name,
    type_data.typowner::BIGINT AS owner_oid,
    owner.rolname::TEXT AS owner_name,
    CASE
        WHEN type_data.typcategory = 'A' AND type_data.typelem != 0 THEN 'array'
        WHEN type_data.typtype = 'd' THEN 'domain'
        WHEN type_data.typtype = 'e' THEN 'enum'
        WHEN type_data.typtype = 'r' THEN 'range'
        WHEN type_data.typtype = 'm' THEN 'multirange'
        WHEN type_data.typtype = 'c' AND relation.relkind = 'c'
            THEN 'composite'
        WHEN type_data.typtype = 'c' THEN 'row'
        WHEN type_data.typtype = 'p' THEN 'pseudo'
        ELSE 'base'
    END::TEXT AS type_kind,
    type_data.typtype::TEXT AS raw_type_kind,
    type_data.typcategory::TEXT AS category,
    type_data.typispreferred AS is_preferred,
    type_data.typisdefined AS is_defined,
    type_data.typlen AS internal_length,
    type_data.typbyval AS is_passed_by_value,
    type_data.typdelim::TEXT AS delimiter,
    type_data.typalign::TEXT AS alignment,
    type_data.typstorage::TEXT AS storage,
    type_data.typrelid::BIGINT AS relation_oid,
    type_data.typelem::BIGINT AS element_type_oid,
    type_data.typarray::BIGINT AS array_type_oid,
    type_data.typbasetype::BIGINT AS base_type_oid,
    type_data.typtypmod AS type_modifier,
    type_data.typndims AS dimensions,
    type_data.typcollation::BIGINT AS collation_oid,
    type_data.typnotnull AS is_not_null,
    COALESCE(type_data.typdefault, '')::TEXT AS default_value,
    COALESCE(extension.oid, 0)::BIGINT AS extension_oid,
    COALESCE(extension.extname, '')::TEXT AS extension_name
FROM pg_catalog.pg_type AS type_data
INNER JOIN pg_catalog.pg_namespace AS type_namespace
    ON type_data.typnamespace = type_namespace.oid
INNER JOIN pg_catalog.pg_roles AS owner ON type_data.typowner = owner.oid
LEFT JOIN pg_catalog.pg_class AS relation ON type_data.typrelid = relation.oid
LEFT JOIN pg_catalog.pg_depend AS extension_dependency
    ON extension_dependency.classid = 'pg_type'::REGCLASS
    AND extension_dependency.objid = type_data.oid
    AND extension_dependency.objsubid = 0
    AND extension_dependency.deptype = 'e'
LEFT JOIN pg_catalog.pg_extension AS extension
    ON extension_dependency.refclassid = 'pg_extension'::REGCLASS
    AND extension_dependency.refobjid = extension.oid
WHERE
    type_namespace.nspname !~ '^pg_toast'
    AND type_namespace.nspname !~ '^pg_temp'
ORDER BY type_namespace.nspname, type_data.typname, type_data.oid;

-- name: GetCatalogEnumLabels :many
SELECT
    enum_label.enumtypid::BIGINT AS type_oid,
    enum_label.oid::BIGINT AS label_oid,
    enum_label.enumsortorder::DOUBLE PRECISION AS sort_order,
    enum_label.enumlabel::TEXT AS label
FROM pg_catalog.pg_enum AS enum_label
INNER JOIN pg_catalog.pg_type AS type_data
    ON enum_label.enumtypid = type_data.oid
INNER JOIN pg_catalog.pg_namespace AS type_namespace
    ON type_data.typnamespace = type_namespace.oid
WHERE
    type_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND type_namespace.nspname !~ '^pg_toast'
    AND type_namespace.nspname !~ '^pg_temp'
ORDER BY enum_label.enumtypid, enum_label.enumsortorder, enum_label.oid;

-- name: GetCatalogCompositeAttributes :many
SELECT
    type_data.oid::BIGINT AS type_oid,
    type_data.typrelid::BIGINT AS relation_oid,
    attribute.attnum AS attribute_number,
    attribute.attname::TEXT AS attribute_name,
    attribute.atttypid::BIGINT AS attribute_type_oid,
    attribute.atttypmod AS type_modifier,
    attribute.attcollation::BIGINT AS collation_oid
FROM pg_catalog.pg_type AS type_data
INNER JOIN pg_catalog.pg_namespace AS type_namespace
    ON type_data.typnamespace = type_namespace.oid
INNER JOIN pg_catalog.pg_class AS relation
    ON type_data.typrelid = relation.oid
    AND relation.relkind = 'c'
INNER JOIN pg_catalog.pg_attribute AS attribute
    ON relation.oid = attribute.attrelid
WHERE
    type_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND type_namespace.nspname !~ '^pg_toast'
    AND type_namespace.nspname !~ '^pg_temp'
    AND attribute.attnum > 0
    AND NOT attribute.attisdropped
ORDER BY type_data.oid, attribute.attnum;

-- name: GetCatalogDomainConstraints :many
SELECT
    constraint_data.oid::BIGINT AS constraint_oid,
    constraint_data.contypid::BIGINT AS type_oid,
    constraint_data.conname::TEXT AS constraint_name,
    constraint_data.condeferrable AS is_deferrable,
    constraint_data.condeferred AS is_deferred,
    constraint_data.convalidated AS is_validated,
    pg_catalog.pg_get_constraintdef(
        constraint_data.oid, false
    )::TEXT AS constraint_definition
FROM pg_catalog.pg_constraint AS constraint_data
INNER JOIN pg_catalog.pg_type AS type_data
    ON constraint_data.contypid = type_data.oid
INNER JOIN pg_catalog.pg_namespace AS type_namespace
    ON type_data.typnamespace = type_namespace.oid
WHERE
    constraint_data.contype = 'c'
    AND type_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND type_namespace.nspname !~ '^pg_toast'
    AND type_namespace.nspname !~ '^pg_temp'
ORDER BY constraint_data.contypid, constraint_data.conname, constraint_data.oid;

-- name: GetCatalogRanges :many
SELECT
    range_data.rngtypid::BIGINT AS range_type_oid,
    range_data.rngsubtype::BIGINT AS subtype_oid,
    range_data.rngmultitypid::BIGINT AS multirange_type_oid,
    range_data.rngcollation::BIGINT AS collation_oid,
    range_data.rngsubopc::BIGINT AS operator_class_oid,
    operator_class_namespace.nspname::TEXT AS operator_class_schema_name,
    operator_class.opcname::TEXT AS operator_class_name,
    range_data.rngcanonical::BIGINT AS canonical_function_oid,
    range_data.rngsubdiff::BIGINT AS subtype_diff_function_oid
FROM pg_catalog.pg_range AS range_data
INNER JOIN pg_catalog.pg_type AS range_type
    ON range_data.rngtypid = range_type.oid
INNER JOIN pg_catalog.pg_namespace AS type_namespace
    ON range_type.typnamespace = type_namespace.oid
INNER JOIN pg_catalog.pg_opclass AS operator_class
    ON range_data.rngsubopc = operator_class.oid
INNER JOIN pg_catalog.pg_namespace AS operator_class_namespace
    ON operator_class.opcnamespace = operator_class_namespace.oid
WHERE
    type_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND type_namespace.nspname !~ '^pg_toast'
    AND type_namespace.nspname !~ '^pg_temp'
ORDER BY range_data.rngtypid;

-- name: GetCatalogTypeSupportFunctions :many
WITH user_type AS (
    SELECT type_data.*
    FROM pg_catalog.pg_type AS type_data
    INNER JOIN pg_catalog.pg_namespace AS type_namespace
        ON type_data.typnamespace = type_namespace.oid
    WHERE
        type_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
        AND type_namespace.nspname !~ '^pg_toast'
        AND type_namespace.nspname !~ '^pg_temp'
), support_function AS (
    SELECT oid AS type_oid, 'input'::TEXT AS support_role,
        typinput AS function_oid
    FROM user_type WHERE typinput != 0

    UNION ALL
    SELECT oid, 'output'::TEXT, typoutput FROM user_type WHERE typoutput != 0

    UNION ALL
    SELECT oid, 'receive'::TEXT, typreceive
    FROM user_type WHERE typreceive != 0

    UNION ALL
    SELECT oid, 'send'::TEXT, typsend FROM user_type WHERE typsend != 0

    UNION ALL
    SELECT oid, 'typmod_input'::TEXT, typmodin
    FROM user_type WHERE typmodin != 0

    UNION ALL
    SELECT oid, 'typmod_output'::TEXT, typmodout
    FROM user_type WHERE typmodout != 0

    UNION ALL
    SELECT oid, 'analyze'::TEXT, typanalyze
    FROM user_type WHERE typanalyze != 0

    UNION ALL
    SELECT oid, 'subscript'::TEXT, typsubscript
    FROM user_type WHERE typsubscript != 0

    UNION ALL

    SELECT
        range_data.rngtypid AS type_oid,
        'range_canonical'::TEXT,
        range_data.rngcanonical
    FROM pg_catalog.pg_range AS range_data
    INNER JOIN user_type ON range_data.rngtypid = user_type.oid
    WHERE range_data.rngcanonical != 0

    UNION ALL

    SELECT
        range_data.rngtypid,
        'range_subtype_diff'::TEXT,
        range_data.rngsubdiff
    FROM pg_catalog.pg_range AS range_data
    INNER JOIN user_type ON range_data.rngtypid = user_type.oid
    WHERE range_data.rngsubdiff != 0
)
SELECT
    support_function.type_oid::BIGINT AS type_oid,
    support_function.support_role::TEXT AS support_role,
    support_function.function_oid::BIGINT AS function_oid,
    function_namespace.nspname::TEXT AS function_schema_name,
    function_data.proname::TEXT AS function_name,
    pg_catalog.pg_get_function_identity_arguments(
        function_data.oid
    )::TEXT AS function_identity_arguments
FROM support_function
INNER JOIN pg_catalog.pg_proc AS function_data
    ON support_function.function_oid = function_data.oid
INNER JOIN pg_catalog.pg_namespace AS function_namespace
    ON function_data.pronamespace = function_namespace.oid
ORDER BY support_function.type_oid, support_function.support_role;

-- name: GetCatalogCollations :many
SELECT
    collation_data.oid::BIGINT AS collation_oid,
    collation_data.collnamespace::BIGINT AS schema_oid,
    collation_namespace.nspname::TEXT AS schema_name,
    collation_data.collname::TEXT AS collation_name,
    collation_data.collowner::BIGINT AS owner_oid,
    owner.rolname::TEXT AS owner_name,
    collation_data.collprovider::TEXT AS provider,
    collation_data.collisdeterministic AS is_deterministic,
    collation_data.collencoding AS encoding,
    COALESCE(collation_data.collcollate, '')::TEXT AS collate,
    COALESCE(collation_data.collctype, '')::TEXT AS ctype,
    COALESCE(collation_data.colllocale, '')::TEXT AS locale,
    COALESCE(collation_data.collicurules, '')::TEXT AS icu_rules,
    COALESCE(collation_data.collversion, '')::TEXT AS version
FROM pg_catalog.pg_collation AS collation_data
INNER JOIN pg_catalog.pg_namespace AS collation_namespace
    ON collation_data.collnamespace = collation_namespace.oid
INNER JOIN pg_catalog.pg_roles AS owner ON collation_data.collowner = owner.oid
WHERE
    collation_namespace.nspname !~ '^pg_toast'
    AND collation_namespace.nspname !~ '^pg_temp'
ORDER BY
    collation_namespace.nspname,
    collation_data.collname,
    collation_data.oid;

-- name: GetCatalogOperators :many
SELECT
    operator.oid::BIGINT AS operator_oid,
    operator.oprnamespace::BIGINT AS schema_oid,
    operator_namespace.nspname::TEXT AS schema_name,
    operator.oprname::TEXT AS operator_name,
    operator.oprowner::BIGINT AS owner_oid,
    owner.rolname::TEXT AS owner_name,
    operator.oprkind::TEXT AS operator_kind,
    operator.oprcanmerge AS can_merge,
    operator.oprcanhash AS can_hash,
    operator.oprleft::BIGINT AS left_type_oid,
    operator.oprright::BIGINT AS right_type_oid,
    operator.oprresult::BIGINT AS result_type_oid,
    operator.oprcode::BIGINT AS function_oid,
    operator.oprrest::BIGINT AS restriction_function_oid,
    operator.oprjoin::BIGINT AS join_function_oid,
    operator.oprcom::BIGINT AS commutator_operator_oid,
    operator.oprnegate::BIGINT AS negator_operator_oid,
    pg_catalog.format_type(operator.oprleft, NULL)::TEXT AS left_type,
    pg_catalog.format_type(operator.oprright, NULL)::TEXT AS right_type,
    pg_catalog.format_type(operator.oprresult, NULL)::TEXT AS result_type
FROM pg_catalog.pg_operator AS operator
INNER JOIN pg_catalog.pg_namespace AS operator_namespace
    ON operator.oprnamespace = operator_namespace.oid
INNER JOIN pg_catalog.pg_roles AS owner ON operator.oprowner = owner.oid
WHERE
    operator_namespace.nspname !~ '^pg_toast'
    AND operator_namespace.nspname !~ '^pg_temp'
ORDER BY
    operator_namespace.nspname,
    operator.oprname,
    operator.oprleft,
    operator.oprright,
    operator.oid;

-- name: GetCatalogSequences :many
SELECT
    sequence_relation.oid::BIGINT AS sequence_oid,
    sequence_relation.relnamespace::BIGINT AS schema_oid,
    sequence_namespace.nspname::TEXT AS schema_name,
    sequence_relation.relname::TEXT AS sequence_name,
    sequence_relation.relowner::BIGINT AS owner_oid,
    owner.rolname::TEXT AS owner_name,
    sequence_relation.relpersistence::TEXT AS persistence,
    sequence_data.seqtypid::BIGINT AS data_type_oid,
    sequence_data.seqstart AS start_value,
    sequence_data.seqincrement AS increment_value,
    sequence_data.seqmax AS max_value,
    sequence_data.seqmin AS min_value,
    sequence_data.seqcache AS cache_size,
    sequence_data.seqcycle AS is_cycle,
    COALESCE(extension.oid, 0)::BIGINT AS extension_oid,
    COALESCE(extension.extname, '')::TEXT AS extension_name
FROM pg_catalog.pg_sequence AS sequence_data
INNER JOIN pg_catalog.pg_class AS sequence_relation
    ON sequence_data.seqrelid = sequence_relation.oid
INNER JOIN pg_catalog.pg_namespace AS sequence_namespace
    ON sequence_relation.relnamespace = sequence_namespace.oid
INNER JOIN pg_catalog.pg_roles AS owner
    ON sequence_relation.relowner = owner.oid
LEFT JOIN pg_catalog.pg_depend AS extension_dependency
    ON extension_dependency.classid = 'pg_class'::REGCLASS
    AND extension_dependency.objid = sequence_relation.oid
    AND extension_dependency.objsubid = 0
    AND extension_dependency.deptype = 'e'
LEFT JOIN pg_catalog.pg_extension AS extension
    ON extension_dependency.refclassid = 'pg_extension'::REGCLASS
    AND extension_dependency.refobjid = extension.oid
WHERE
    sequence_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
    AND sequence_namespace.nspname !~ '^pg_toast'
    AND sequence_namespace.nspname !~ '^pg_temp'
ORDER BY
    sequence_namespace.nspname,
    sequence_relation.relname,
    sequence_relation.oid;

-- name: GetCatalogExtensions :many
SELECT
    extension.oid::BIGINT AS extension_oid,
    extension.extname::TEXT AS extension_name,
    extension.extowner::BIGINT AS owner_oid,
    owner.rolname::TEXT AS owner_name,
    extension.extnamespace::BIGINT AS schema_oid,
    extension_namespace.nspname::TEXT AS schema_name,
    extension.extrelocatable AS is_relocatable,
    extension.extversion::TEXT AS version,
    ARRAY(
        SELECT config_relation_oid::BIGINT
        FROM UNNEST(COALESCE(extension.extconfig, ARRAY[]::OID[]))
            WITH ORDINALITY AS config (config_relation_oid, position)
        ORDER BY config.position
    )::BIGINT[] AS configuration_relation_oids,
    COALESCE(extension.extcondition, ARRAY[]::TEXT[])::TEXT[]
        AS configuration_conditions
FROM pg_catalog.pg_extension AS extension
INNER JOIN pg_catalog.pg_roles AS owner ON extension.extowner = owner.oid
INNER JOIN pg_catalog.pg_namespace AS extension_namespace
    ON extension.extnamespace = extension_namespace.oid
ORDER BY extension.extname, extension.oid;

-- name: GetCatalogExtensionMembers :many
SELECT
    extension.oid::BIGINT AS extension_oid,
    extension.extname::TEXT AS extension_name,
    dependency.classid::BIGINT AS class_id,
    dependency.objid::BIGINT AS object_id,
    dependency.objsubid AS sub_object_id,
    object_data.object_type::TEXT AS object_type,
    COALESCE(object_data.object_schema_name, '')::TEXT AS object_schema_name,
    COALESCE(object_data.object_name, '')::TEXT AS object_name,
    object_data.object_identity::TEXT AS object_identity
FROM pg_catalog.pg_depend AS dependency
INNER JOIN pg_catalog.pg_extension AS extension
    ON dependency.refclassid = 'pg_extension'::REGCLASS
    AND dependency.refobjid = extension.oid
CROSS JOIN LATERAL pg_catalog.pg_identify_object(
    dependency.classid, dependency.objid, dependency.objsubid
) AS object_data(
    object_type, object_schema_name, object_name, object_identity
)
WHERE dependency.deptype = 'e'
ORDER BY
    extension.extname,
    dependency.classid,
    dependency.objid,
    dependency.objsubid;

-- name: GetCatalogEventTriggers :many
SELECT
    event_trigger.oid::BIGINT AS event_trigger_oid,
    event_trigger.evtname::TEXT AS event_trigger_name,
    event_trigger.evtowner::BIGINT AS owner_oid,
    owner.rolname::TEXT AS owner_name,
    event_trigger.evtevent::TEXT AS event,
    event_trigger.evtfoid::BIGINT AS function_oid,
    event_trigger.evtenabled::TEXT AS enabled_mode,
    COALESCE(event_trigger.evttags, ARRAY[]::TEXT[])::TEXT[] AS tags
FROM pg_catalog.pg_event_trigger AS event_trigger
INNER JOIN pg_catalog.pg_roles AS owner ON event_trigger.evtowner = owner.oid
ORDER BY event_trigger.evtname, event_trigger.oid;

-- name: GetCatalogPublications :many
SELECT
    publication.oid::BIGINT AS publication_oid,
    publication.pubname::TEXT AS publication_name,
    publication.pubowner::BIGINT AS owner_oid,
    owner.rolname::TEXT AS owner_name,
    publication.puballtables AS publishes_all_tables,
    publication.pubinsert AS publishes_inserts,
    publication.pubupdate AS publishes_updates,
    publication.pubdelete AS publishes_deletes,
    publication.pubtruncate AS publishes_truncates,
    publication.pubviaroot AS publishes_via_partition_root
FROM pg_catalog.pg_publication AS publication
INNER JOIN pg_catalog.pg_roles AS owner ON publication.pubowner = owner.oid
ORDER BY publication.pubname, publication.oid;

-- name: GetCatalogPublicationRelations :many
SELECT
    publication_relation.oid::BIGINT AS membership_oid,
    publication_relation.prpubid::BIGINT AS publication_oid,
    publication.pubname::TEXT AS publication_name,
    publication_relation.prrelid::BIGINT AS relation_oid,
    relation_namespace.nspname::TEXT AS relation_schema_name,
    relation.relname::TEXT AS relation_name,
    COALESCE(publication_relation.prattrs, ARRAY[]::SMALLINT[])::SMALLINT[]
        AS column_numbers,
    ARRAY(
        SELECT attribute.attname::TEXT
        FROM UNNEST(COALESCE(
            publication_relation.prattrs, ARRAY[]::SMALLINT[]
        )) WITH ORDINALITY AS published_column (column_number, position)
        INNER JOIN pg_catalog.pg_attribute AS attribute
            ON attribute.attrelid = publication_relation.prrelid
            AND attribute.attnum = published_column.column_number
        ORDER BY published_column.position
    )::TEXT[] AS column_names,
    COALESCE(pg_catalog.pg_get_expr(
        publication_relation.prqual, publication_relation.prrelid
    ), '')::TEXT AS row_filter
FROM pg_catalog.pg_publication_rel AS publication_relation
INNER JOIN pg_catalog.pg_publication AS publication
    ON publication_relation.prpubid = publication.oid
INNER JOIN pg_catalog.pg_class AS relation
    ON publication_relation.prrelid = relation.oid
INNER JOIN pg_catalog.pg_namespace AS relation_namespace
    ON relation.relnamespace = relation_namespace.oid
ORDER BY
    publication.pubname,
    relation_namespace.nspname,
    relation.relname,
    publication_relation.oid;

-- name: GetCatalogPublicationSchemas :many
SELECT
    publication_namespace.oid::BIGINT AS membership_oid,
    publication_namespace.pnpubid::BIGINT AS publication_oid,
    publication.pubname::TEXT AS publication_name,
    publication_namespace.pnnspid::BIGINT AS schema_oid,
    published_namespace.nspname::TEXT AS schema_name
FROM pg_catalog.pg_publication_namespace AS publication_namespace
INNER JOIN pg_catalog.pg_publication AS publication
    ON publication_namespace.pnpubid = publication.oid
INNER JOIN pg_catalog.pg_namespace AS published_namespace
    ON publication_namespace.pnnspid = published_namespace.oid
ORDER BY
    publication.pubname,
    published_namespace.nspname,
    publication_namespace.oid;

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
