package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/testdb"
)

func TestCatalogInventoryExpectedTableMove(t *testing.T) {
	inventory := CatalogInventory{
		Relations: []CatalogRelation{
			{
				OID: 10, SchemaOID: 1, SchemaName: "inventory", Name: "table_name",
				Kind: RelKindOrdinaryTable, RowTypeOID: 20, ArrayTypeOID: 21,
				ToastRelation: &CatalogRelationIdentity{
					OID: 90, SchemaOID: 9, SchemaName: "pg_toast", Name: "pg_toast_10",
				},
			},
			{OID: 11, SchemaOID: 1, SchemaName: "inventory", Name: "view_name", Kind: RelKindView},
		},
		Types: []CatalogType{
			{OID: 21, SchemaOID: 1, SchemaName: "inventory", Name: "_table_name", RelationOID: 10, Kind: CatalogTypeKindArray},
			{OID: 20, SchemaOID: 1, SchemaName: "inventory", Name: "table_name", RelationOID: 10, Kind: CatalogTypeKindRow},
		},
		Indexes: []CatalogIndex{
			{OID: 31, SchemaOID: 1, SchemaName: "inventory", Name: "z_idx", RelationOID: 10},
			{OID: 30, SchemaOID: 1, SchemaName: "inventory", Name: "a_idx", RelationOID: 10},
		},
		Constraints: []CatalogConstraint{
			{OID: 40, SchemaOID: 1, SchemaName: "inventory", Name: "table_name_pkey", RelationOID: 10},
		},
		Triggers: []CatalogTrigger{{OID: 50, RelationOID: 10, Name: "table_trigger"}},
		Rules:    []CatalogRule{{OID: 51, RelationOID: 10, Name: "table_rule"}},
		Policies: []CatalogPolicy{{OID: 52, RelationOID: 10, Name: "table_policy"}},
		Sequences: []CatalogSequence{
			{OID: 60, SchemaOID: 1, SchemaName: "inventory", Name: "table_name_id_seq"},
		},
		OwnedSequences: []CatalogOwnedSequence{
			{SequenceOID: 60, RelationOID: 10, ColumnNumber: 1, ColumnName: "id"},
		},
		ExtendedStatistics: []CatalogExtendedStatistic{
			{OID: 70, SchemaOID: 1, SchemaName: "inventory", Name: "table_stats", RelationOID: 10},
		},
	}

	move, err := inventory.ExpectedTableMove(10)
	require.NoError(t, err)
	assert.Equal(t, uint32(10), move.RelationOID)
	assert.Equal(t, []CatalogObjectKind{
		CatalogObjectKindArrayType,
		CatalogObjectKindConstraint,
		CatalogObjectKindIndex,
		CatalogObjectKindIndex,
		CatalogObjectKindOwnedSequence,
		CatalogObjectKindRelation,
		CatalogObjectKindRowType,
	}, catalogObjectKinds(move.CleanupSchemaObjects))
	assert.Equal(t, []CatalogObjectKind{
		CatalogObjectKindPolicy,
		CatalogObjectKindRule,
		CatalogObjectKindTrigger,
	}, catalogObjectKinds(move.AttachedObjects))
	assert.Equal(t, []CatalogObjectKind{CatalogObjectKindExtendedStatistic},
		catalogObjectKinds(move.ExplicitMoveObjects))
	assert.Equal(t, []CatalogObjectKind{CatalogObjectKindToastRelation},
		catalogObjectKinds(move.InternalObjects))
	assert.Equal(t, "pg_toast", move.InternalObjects[0].SchemaName)
	assert.NotEqual(t, "inventory", move.InternalObjects[0].SchemaName)
	assert.Equal(t, move, mustExpectedTableMove(t, inventory.Normalize(), 10),
		"normalization must not change expected identities")

	_, err = inventory.ExpectedTableMove(11)
	assert.EqualError(t, err, "relation OID 11 (inventory.view_name) is not table-like")
	_, err = inventory.ExpectedTableMove(999)
	assert.EqualError(t, err, "relation OID 999 is not present in the catalog inventory")
}

func TestCatalogInventoryNormalizeTableLocalMetadata(t *testing.T) {
	inventory := CatalogInventory{
		Tables: []CatalogTable{
			{RelationOID: 2, Options: []string{"z=1", "a=1"}},
			{RelationOID: 1},
		},
		Columns: []CatalogColumn{
			{RelationOID: 2, Number: 3},
			{RelationOID: 1, Number: 2},
			{RelationOID: 1, Number: 1},
		},
		Triggers:       []CatalogTrigger{{OID: 2, RelationOID: 2}, {OID: 1, RelationOID: 1}},
		Rules:          []CatalogRule{{OID: 2, RelationOID: 2}, {OID: 1, RelationOID: 1}},
		Policies:       []CatalogPolicy{{OID: 2, RelationOID: 2}, {OID: 1, RelationOID: 1}},
		OwnedSequences: []CatalogOwnedSequence{{SequenceOID: 2, RelationOID: 2}, {SequenceOID: 1, RelationOID: 1}},
		ExtendedStatistics: []CatalogExtendedStatistic{{
			OID: 2, SchemaName: "z",
			Kinds: []string{"m", "d"},
		}, {OID: 1, SchemaName: "a"}},
		PartitionAttachments: []CatalogPartitionAttachment{{RelationOID: 2}, {RelationOID: 1}},
		SecurityLabels:       []CatalogSecurityLabel{{RelationOID: 2}, {RelationOID: 1}},
	}

	normalized := inventory.Normalize()
	assert.Equal(t, []uint32{1, 2}, []uint32{normalized.Tables[0].RelationOID, normalized.Tables[1].RelationOID})
	assert.Equal(t, []int16{1, 2}, []int16{normalized.Columns[0].Number, normalized.Columns[1].Number},
		"physical column order must follow attnum")
	assert.Equal(t, []string{"a=1", "z=1"}, normalized.Tables[1].Options)
	assert.Equal(t, []string{"d", "m"}, normalized.ExtendedStatistics[1].Kinds)
	assert.Equal(t, normalized, normalized.Normalize())
	assert.Equal(t, []string{"z=1", "a=1"}, inventory.Tables[0].Options,
		"normalization must not mutate nested input slices")
}

func TestCatalogTableLocalInventoryDatabaseFixtures(t *testing.T) {
	factory := testdb.MustNewFactory(t)

	t.Run("table metadata and move identities", func(t *testing.T) {
		db := factory.CreateDatabase(t)
		_, err := db.ConnPool.Exec(t.Context(), `
			CREATE SCHEMA local_inventory;
			CREATE TABLE local_inventory.metadata (
				id BIGSERIAL CONSTRAINT metadata_pkey PRIMARY KEY,
				identity_id BIGINT GENERATED ALWAYS AS IDENTITY
					CONSTRAINT metadata_identity_key UNIQUE,
				base TEXT COLLATE "C" NOT NULL,
				dropped_value TEXT,
				defaulted INTEGER DEFAULT 42,
				generated_value TEXT GENERATED ALWAYS AS (upper(base)) STORED,
				constrained INTEGER CONSTRAINT metadata_positive CHECK (constrained > 0),
				parent_id BIGINT CONSTRAINT metadata_self_fk
					REFERENCES local_inventory.metadata (id)
			) WITH (fillfactor = 80);
			ALTER TABLE local_inventory.metadata DROP COLUMN dropped_value;
			COMMENT ON TABLE local_inventory.metadata IS 'table inventory comment';
			COMMENT ON COLUMN local_inventory.metadata.base IS 'column inventory comment';
			COMMENT ON SEQUENCE local_inventory.metadata_id_seq IS 'sequence inventory comment';
			COMMENT ON CONSTRAINT metadata_positive ON local_inventory.metadata
				IS 'constraint inventory comment';
			ALTER TABLE local_inventory.metadata ALTER COLUMN base SET STORAGE EXTERNAL;
			ALTER TABLE local_inventory.metadata ALTER COLUMN base SET COMPRESSION pglz;
			ALTER TABLE local_inventory.metadata ALTER COLUMN base SET STATISTICS 321;
			ALTER TABLE local_inventory.metadata ALTER COLUMN base SET (n_distinct = 0.25);

			CREATE UNIQUE INDEX metadata_replica_idx
				ON local_inventory.metadata (base);
			COMMENT ON INDEX local_inventory.metadata_replica_idx IS 'index inventory comment';
			ALTER TABLE local_inventory.metadata
				REPLICA IDENTITY USING INDEX metadata_replica_idx;
			CREATE INDEX metadata_clustered_idx
				ON local_inventory.metadata (constrained);
			CLUSTER local_inventory.metadata USING metadata_clustered_idx;

			CREATE FUNCTION local_inventory.metadata_trigger_fn() RETURNS trigger
				LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
			CREATE TRIGGER metadata_trigger BEFORE UPDATE ON local_inventory.metadata
				FOR EACH ROW EXECUTE FUNCTION local_inventory.metadata_trigger_fn();
			ALTER TABLE local_inventory.metadata DISABLE TRIGGER metadata_trigger;
			COMMENT ON TRIGGER metadata_trigger ON local_inventory.metadata
				IS 'trigger inventory comment';
			CREATE RULE metadata_rule AS ON UPDATE TO local_inventory.metadata
				DO ALSO NOTIFY metadata_changed;
			COMMENT ON RULE metadata_rule ON local_inventory.metadata
				IS 'rule inventory comment';

			ALTER TABLE local_inventory.metadata ENABLE ROW LEVEL SECURITY;
			ALTER TABLE local_inventory.metadata FORCE ROW LEVEL SECURITY;
			CREATE POLICY metadata_policy ON local_inventory.metadata
				AS RESTRICTIVE FOR UPDATE TO PUBLIC
				USING (constrained > 0) WITH CHECK (constrained < 100);
			COMMENT ON POLICY metadata_policy ON local_inventory.metadata
				IS 'policy inventory comment';

			CREATE STATISTICS local_inventory.metadata_columns_stats
				(dependencies, ndistinct) ON base, constrained
				FROM local_inventory.metadata;
			CREATE STATISTICS local_inventory.metadata_expression_stats
				ON (lower(base)), constrained FROM local_inventory.metadata;
			COMMENT ON STATISTICS local_inventory.metadata_expression_stats
				IS 'statistics inventory comment';

			INSERT INTO local_inventory.metadata
				(identity_id, base, defaulted, constrained)
				OVERRIDING SYSTEM VALUE VALUES (10, 'value', 42, 1);
			ALTER TABLE local_inventory.metadata
				ADD COLUMN fast_default INTEGER DEFAULT 7 NOT NULL;
		`)
		require.NoError(t, err)

		snapshot, err := GetSchemaSnapshot(t.Context(), db.ConnPool)
		require.NoError(t, err)
		inventory := snapshot.Inventory
		assert.Equal(t, inventory, inventory.Normalize())
		relation := catalogRelationByName(t, inventory, "local_inventory", "metadata")
		table := catalogTableByRelationOID(t, inventory, relation.OID)
		assert.True(t, table.RLSEnabled)
		assert.True(t, table.RLSForced)
		assert.Equal(t, "i", table.ReplicaIdentity)
		assert.Equal(t, catalogIndexByName(t, inventory, "local_inventory", "metadata_replica_idx").OID,
			table.ReplicaIdentityIndexOID)
		assert.Equal(t, catalogIndexByName(t, inventory, "local_inventory", "metadata_clustered_idx").OID,
			table.ClusteredIndexOID)
		assert.Equal(t, []string{"fillfactor=80"}, table.Options)
		assert.Zero(t, table.TablespaceOID)
		assert.Empty(t, table.TablespaceName)
		assert.Equal(t, "heap", table.AccessMethodName)
		assert.Equal(t, "table inventory comment", relation.Comment)

		columns := catalogColumnsByRelationOID(inventory, relation.OID)
		require.Len(t, columns, 9)
		assert.Equal(t, []int16{1, 2, 3, 4, 5, 6, 7, 8, 9}, catalogColumnNumbers(columns))
		assert.True(t, columns[3].IsDropped)
		assert.Empty(t, columns[3].Name)
		base := catalogColumnByName(t, columns, "base")
		assert.NotZero(t, base.TypeOID)
		assert.Equal(t, "pg_catalog", base.TypeSchemaName)
		assert.Equal(t, "text", base.TypeName)
		assert.Equal(t, "C", base.CollationName)
		assert.True(t, base.IsNotNull)
		assert.Equal(t, "e", base.StorageMode)
		assert.Equal(t, "p", base.CompressionMode)
		assert.Equal(t, int32(321), base.StatisticsTarget)
		assert.Equal(t, []string{"n_distinct=0.25"}, base.Options)
		assert.Equal(t, "column inventory comment", base.Comment)
		assert.Equal(t, "42", catalogColumnByName(t, columns, "defaulted").DefaultExpression)
		generated := catalogColumnByName(t, columns, "generated_value")
		assert.Equal(t, "s", generated.GeneratedMode)
		assert.Contains(t, generated.GeneratedExpression, "upper(base)")
		assert.Equal(t, "a", catalogColumnByName(t, columns, "identity_id").IdentityMode)
		fastDefault := catalogColumnByName(t, columns, "fast_default")
		assert.True(t, fastDefault.HasMissingValue)
		assert.NotEmpty(t, fastDefault.MissingValueBinary)

		primaryIndex := catalogIndexByName(t, inventory, "local_inventory", "metadata_pkey")
		assert.NotZero(t, primaryIndex.ConstraintOID)
		assert.True(t, primaryIndex.IsPrimary)
		assert.True(t, primaryIndex.IsUnique)
		assert.True(t, primaryIndex.IsValid)
		assert.True(t, primaryIndex.IsReady)
		assert.True(t, primaryIndex.IsLive)
		replicaIndex := catalogIndexByName(t, inventory, "local_inventory", "metadata_replica_idx")
		assert.Zero(t, replicaIndex.ConstraintOID)
		assert.True(t, replicaIndex.IsReplicaIdentity)
		assert.Equal(t, "index inventory comment",
			catalogRelationByName(t, inventory, "local_inventory", "metadata_replica_idx").Comment)
		assert.True(t, catalogIndexByName(t, inventory, "local_inventory", "metadata_clustered_idx").IsClustered)

		primaryConstraint := catalogConstraintByName(t, inventory, relation.OID, "metadata_pkey")
		assert.Equal(t, "p", primaryConstraint.Type)
		assert.Equal(t, []int16{1}, primaryConstraint.KeyColumnNumbers)
		assert.Equal(t, primaryIndex.OID, primaryConstraint.IndexOID)
		uniqueConstraint := catalogConstraintByName(t, inventory, relation.OID, "metadata_identity_key")
		assert.Equal(t, "u", uniqueConstraint.Type)
		checkConstraint := catalogConstraintByName(t, inventory, relation.OID, "metadata_positive")
		assert.Equal(t, "c", checkConstraint.Type)
		assert.Contains(t, checkConstraint.CheckExpression, "constrained > 0")
		assert.Equal(t, "constraint inventory comment", checkConstraint.Comment)
		selfConstraint := catalogConstraintByName(t, inventory, relation.OID, "metadata_self_fk")
		assert.Equal(t, "f", selfConstraint.Type)
		assert.Equal(t, relation.OID, selfConstraint.ReferencedRelationOID)
		assert.True(t, selfConstraint.IsValidated)

		userTrigger := catalogTriggerByName(t, inventory, relation.OID, "metadata_trigger")
		assert.False(t, userTrigger.IsInternal)
		assert.Equal(t, "D", userTrigger.EnabledMode)
		assert.Zero(t, userTrigger.ParentTriggerOID)
		assert.Contains(t, userTrigger.Definition, "metadata_trigger_fn")
		assert.Equal(t, "trigger inventory comment", userTrigger.Comment)
		internalTriggers := 0
		for _, trigger := range inventory.Triggers {
			if trigger.RelationOID == relation.OID && trigger.IsInternal {
				internalTriggers++
				assert.NotZero(t, trigger.ConstraintOID)
			}
		}
		assert.GreaterOrEqual(t, internalTriggers, 2)

		rule := catalogRuleByName(t, inventory, relation.OID, "metadata_rule")
		assert.Equal(t, "2", rule.EventType)
		assert.Equal(t, "O", rule.EnabledMode)
		assert.Contains(t, rule.Definition, "NOTIFY metadata_changed")
		assert.Equal(t, "rule inventory comment", rule.Comment)
		policy := catalogPolicyByName(t, inventory, relation.OID, "metadata_policy")
		assert.False(t, policy.IsPermissive)
		assert.Equal(t, []uint32{0}, policy.RoleOIDs)
		assert.Equal(t, []string{"PUBLIC"}, policy.RoleNames)
		assert.Equal(t, "w", policy.Command)
		assert.Contains(t, policy.UsingExpression, "constrained > 0")
		assert.Contains(t, policy.CheckExpression, "constrained < 100")
		assert.Equal(t, "policy inventory comment", policy.Comment)

		ownerships := catalogSequenceOwnerships(inventory, relation.OID)
		require.Len(t, ownerships, 2)
		assert.Equal(t, "id", ownerships[0].ColumnName)
		assert.Equal(t, CatalogSequenceOwnershipTypeSerial, ownerships[0].DependencyType)
		assert.Equal(t, "identity_id", ownerships[1].ColumnName)
		assert.Equal(t, CatalogSequenceOwnershipTypeIdentity, ownerships[1].DependencyType)
		for _, ownership := range ownerships {
			assert.Equal(t, catalogColumnByName(t, columns, ownership.ColumnName).Number,
				int16(ownership.ColumnNumber))
			assert.NotZero(t, catalogSequenceByOID(t, inventory, ownership.SequenceOID).OID)
		}
		assert.Equal(t, "sequence inventory comment",
			catalogRelationByName(t, inventory, "local_inventory", "metadata_id_seq").Comment)

		columnStats := catalogExtendedStatisticByName(t, inventory, "local_inventory", "metadata_columns_stats")
		assert.Equal(t, relation.OID, columnStats.RelationOID)
		assert.Equal(t, []int16{3, 7}, columnStats.ColumnNumbers)
		assert.Empty(t, columnStats.Expressions)
		assert.Equal(t, []string{"d", "f"}, columnStats.Kinds)
		expressionStats := catalogExtendedStatisticByName(t, inventory, "local_inventory", "metadata_expression_stats")
		assert.Equal(t, []int16{7}, expressionStats.ColumnNumbers)
		require.Len(t, expressionStats.Expressions, 1)
		assert.Contains(t, expressionStats.Expressions[0], "lower(base)")
		assert.Contains(t, expressionStats.Kinds, "e")
		assert.Equal(t, "statistics inventory comment", expressionStats.Comment)

		move, err := inventory.ExpectedTableMove(relation.OID)
		require.NoError(t, err)
		assert.Len(t, move.ExplicitMoveObjects, 2)
		assert.Contains(t, catalogObjectKinds(move.ExplicitMoveObjects), CatalogObjectKindExtendedStatistic)
		assert.Len(t, move.InternalObjects, 1)
		assert.Equal(t, "pg_toast", move.InternalObjects[0].SchemaName)
		assert.Equal(t, 2, countCatalogObjectKind(move.CleanupSchemaObjects, CatalogObjectKindOwnedSequence))
	})

	t.Run("recursive partitions and cloned triggers", func(t *testing.T) {
		db := factory.CreateDatabase(t)
		_, err := db.ConnPool.Exec(t.Context(), `
			CREATE SCHEMA partition_root;
			CREATE SCHEMA partition_branch;
			CREATE SCHEMA partition_leaf;
			CREATE TABLE partition_root.root (id INTEGER) PARTITION BY RANGE (id);
			CREATE TABLE partition_branch.branch
				PARTITION OF partition_root.root FOR VALUES FROM (0) TO (100)
				PARTITION BY RANGE (id);
			CREATE TABLE partition_leaf.leaf
				PARTITION OF partition_branch.branch FOR VALUES FROM (0) TO (50);
			CREATE FUNCTION partition_root.trigger_fn() RETURNS trigger
				LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
			CREATE TRIGGER tree_trigger BEFORE INSERT ON partition_root.root
				FOR EACH ROW EXECUTE FUNCTION partition_root.trigger_fn();
		`)
		require.NoError(t, err)

		snapshot, err := GetSchemaSnapshot(t.Context(), db.ConnPool)
		require.NoError(t, err)
		inventory := snapshot.Inventory
		root := catalogRelationByName(t, inventory, "partition_root", "root")
		branch := catalogRelationByName(t, inventory, "partition_branch", "branch")
		leaf := catalogRelationByName(t, inventory, "partition_leaf", "leaf")
		assert.Equal(t, CatalogPartitionAttachment{
			RelationOID: branch.OID, ParentRelationOID: root.OID,
			BoundExpression: "FOR VALUES FROM (0) TO (100)",
		}, catalogPartitionAttachment(t, inventory, branch.OID))
		assert.Equal(t, CatalogPartitionAttachment{
			RelationOID: leaf.OID, ParentRelationOID: branch.OID,
			BoundExpression: "FOR VALUES FROM (0) TO (50)",
		}, catalogPartitionAttachment(t, inventory, leaf.OID))
		assert.Equal(t, root.OID, catalogParentEdges(inventory, branch.OID)[0].ParentRelationOID)
		assert.Equal(t, branch.OID, catalogParentEdges(inventory, leaf.OID)[0].ParentRelationOID)

		rootTrigger := catalogTriggerByName(t, inventory, root.OID, "tree_trigger")
		branchTrigger := catalogTriggerByName(t, inventory, branch.OID, "tree_trigger")
		leafTrigger := catalogTriggerByName(t, inventory, leaf.OID, "tree_trigger")
		assert.Zero(t, rootTrigger.ParentTriggerOID)
		assert.Equal(t, rootTrigger.OID, branchTrigger.ParentTriggerOID)
		assert.Contains(t, []uint32{rootTrigger.OID, branchTrigger.OID}, leafTrigger.ParentTriggerOID)
		assert.False(t, rootTrigger.IsInternal)
		assert.False(t, branchTrigger.IsInternal)
		assert.False(t, leafTrigger.IsInternal)
	})

	t.Run("excluded schemas hash isolation and empty security labels", func(t *testing.T) {
		db := factory.CreateDatabase(t)
		_, err := db.ConnPool.Exec(t.Context(), `
			CREATE SCHEMA excluded_local;
			CREATE TABLE excluded_local.settings (
				id INTEGER,
				payload TEXT
			);
		`)
		require.NoError(t, err)

		before, err := GetSchemaSnapshot(t.Context(), db.ConnPool, WithIncludeSchemaPatterns("public"))
		require.NoError(t, err)
		relation := catalogRelationByName(t, before.Inventory, "excluded_local", "settings")
		assert.NotEmpty(t, catalogColumnsByRelationOID(before.Inventory, relation.OID))
		assert.Empty(t, before.Inventory.SecurityLabels,
			"the test PostgreSQL has no security-label provider configured")

		_, err = db.ConnPool.Exec(t.Context(), `
			COMMENT ON TABLE excluded_local.settings IS 'excluded table comment';
			COMMENT ON COLUMN excluded_local.settings.payload IS 'excluded column comment';
			ALTER TABLE excluded_local.settings SET (fillfactor = 75);
			ALTER TABLE excluded_local.settings ALTER COLUMN payload SET DEFAULT 'new';
			ALTER TABLE excluded_local.settings ENABLE ROW LEVEL SECURITY;
			CREATE POLICY excluded_policy ON excluded_local.settings USING (id > 0);
		`)
		require.NoError(t, err)
		after, err := GetSchemaSnapshot(t.Context(), db.ConnPool, WithIncludeSchemaPatterns("public"))
		require.NoError(t, err)
		assert.Equal(t, before.Schema, after.Schema)
		assert.Equal(t, before.Hash, after.Hash,
			"table-local inventory must not expand the legacy modeled-schema hash")
		assert.NotEqual(t, before.Inventory, after.Inventory)
		table := catalogTableByRelationOID(t, after.Inventory, relation.OID)
		assert.Equal(t, "excluded table comment",
			catalogRelationByName(t, after.Inventory, "excluded_local", "settings").Comment)
		assert.Equal(t, []string{"fillfactor=75"}, table.Options)
		assert.True(t, table.RLSEnabled)
		assert.Equal(t, "'new'::text",
			catalogColumnByName(t, catalogColumnsByRelationOID(after.Inventory, relation.OID), "payload").DefaultExpression)
		catalogPolicyByName(t, after.Inventory, relation.OID, "excluded_policy")
		assert.Empty(t, after.Inventory.SecurityLabels)
	})
}

func catalogObjectKinds(objects []CatalogObjectIdentity) []CatalogObjectKind {
	kinds := make([]CatalogObjectKind, 0, len(objects))
	for _, object := range objects {
		kinds = append(kinds, object.Kind)
	}
	return kinds
}

func countCatalogObjectKind(objects []CatalogObjectIdentity, kind CatalogObjectKind) int {
	count := 0
	for _, object := range objects {
		if object.Kind == kind {
			count++
		}
	}
	return count
}

func mustExpectedTableMove(t *testing.T, inventory CatalogInventory, relationOID uint32) CatalogExpectedTableMove {
	t.Helper()
	move, err := inventory.ExpectedTableMove(relationOID)
	require.NoError(t, err)
	return move
}

func catalogTableByRelationOID(t *testing.T, inventory CatalogInventory, relationOID uint32) CatalogTable {
	t.Helper()
	for _, table := range inventory.Tables {
		if table.RelationOID == relationOID {
			return table
		}
	}
	require.FailNow(t, "catalog table not found", "relation OID: %d", relationOID)
	return CatalogTable{}
}

func catalogColumnsByRelationOID(inventory CatalogInventory, relationOID uint32) []CatalogColumn {
	var columns []CatalogColumn
	for _, column := range inventory.Columns {
		if column.RelationOID == relationOID {
			columns = append(columns, column)
		}
	}
	return columns
}

func catalogColumnByName(t *testing.T, columns []CatalogColumn, name string) CatalogColumn {
	t.Helper()
	for _, column := range columns {
		if column.Name == name {
			return column
		}
	}
	require.FailNow(t, "catalog column not found", "column: %s", name)
	return CatalogColumn{}
}

func catalogColumnNumbers(columns []CatalogColumn) []int16 {
	numbers := make([]int16, 0, len(columns))
	for _, column := range columns {
		numbers = append(numbers, column.Number)
	}
	return numbers
}

func catalogTriggerByName(t *testing.T, inventory CatalogInventory, relationOID uint32, name string) CatalogTrigger {
	t.Helper()
	for _, trigger := range inventory.Triggers {
		if trigger.RelationOID == relationOID && trigger.Name == name {
			return trigger
		}
	}
	require.FailNow(t, "catalog trigger not found", "relation OID: %d, trigger: %s", relationOID, name)
	return CatalogTrigger{}
}

func catalogRuleByName(t *testing.T, inventory CatalogInventory, relationOID uint32, name string) CatalogRule {
	t.Helper()
	for _, rule := range inventory.Rules {
		if rule.RelationOID == relationOID && rule.Name == name {
			return rule
		}
	}
	require.FailNow(t, "catalog rule not found", "relation OID: %d, rule: %s", relationOID, name)
	return CatalogRule{}
}

func catalogPolicyByName(t *testing.T, inventory CatalogInventory, relationOID uint32, name string) CatalogPolicy {
	t.Helper()
	for _, policy := range inventory.Policies {
		if policy.RelationOID == relationOID && policy.Name == name {
			return policy
		}
	}
	require.FailNow(t, "catalog policy not found", "relation OID: %d, policy: %s", relationOID, name)
	return CatalogPolicy{}
}

func catalogSequenceOwnerships(inventory CatalogInventory, relationOID uint32) []CatalogOwnedSequence {
	var ownerships []CatalogOwnedSequence
	for _, ownership := range inventory.OwnedSequences {
		if ownership.RelationOID == relationOID {
			ownerships = append(ownerships, ownership)
		}
	}
	return ownerships
}

func catalogSequenceByOID(t *testing.T, inventory CatalogInventory, oid uint32) CatalogSequence {
	t.Helper()
	for _, sequence := range inventory.Sequences {
		if sequence.OID == oid {
			return sequence
		}
	}
	require.FailNow(t, "catalog sequence not found", "OID: %d", oid)
	return CatalogSequence{}
}

func catalogExtendedStatisticByName(
	t *testing.T,
	inventory CatalogInventory,
	schemaName string,
	name string,
) CatalogExtendedStatistic {
	t.Helper()
	for _, statistic := range inventory.ExtendedStatistics {
		if statistic.SchemaName == schemaName && statistic.Name == name {
			return statistic
		}
	}
	require.FailNow(t, "catalog extended statistic not found", "statistic: %s.%s", schemaName, name)
	return CatalogExtendedStatistic{}
}

func catalogPartitionAttachment(
	t *testing.T,
	inventory CatalogInventory,
	relationOID uint32,
) CatalogPartitionAttachment {
	t.Helper()
	for _, attachment := range inventory.PartitionAttachments {
		if attachment.RelationOID == relationOID {
			return attachment
		}
	}
	require.FailNow(t, "catalog partition attachment not found", "relation OID: %d", relationOID)
	return CatalogPartitionAttachment{}
}
