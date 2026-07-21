package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/testdb"
)

func TestCatalogInventoryNormalizeAndClassifyTable(t *testing.T) {
	inventory := CatalogInventory{
		Schemas: []CatalogSchema{
			{OID: 2, Name: "z"},
			{OID: 1, Name: "a"},
		},
		Relations: []CatalogRelation{
			{OID: 10, SchemaName: "public", Name: "traditional_parent", Kind: RelKindOrdinaryTable},
			{OID: 9, SchemaName: "public", Name: "plain", Kind: RelKindOrdinaryTable},
			{OID: 8, SchemaName: "public", Name: "view", Kind: RelKindView},
			{OID: 7, SchemaName: "public", Name: "multiple", Kind: RelKindOrdinaryTable},
			{OID: 6, SchemaName: "public", Name: "inherited", Kind: RelKindOrdinaryTable},
			{OID: 5, SchemaName: "public", Name: "foreign_partition", Kind: RelKindForeignTable, IsPartition: true},
			{OID: 4, SchemaName: "public", Name: "foreign", Kind: RelKindForeignTable},
			{OID: 3, SchemaName: "public", Name: "partition", Kind: RelKindOrdinaryTable, IsPartition: true},
			{OID: 2, SchemaName: "public", Name: "partitioned", Kind: RelKindPartitionedTable},
			{OID: 1, SchemaName: "public", Name: "ordinary", Kind: RelKindOrdinaryTable},
		},
		InheritanceEdges: []CatalogInheritanceEdge{
			{ChildRelationOID: 7, ParentRelationOID: 10, SequenceNumber: 2},
			{ChildRelationOID: 3, ParentRelationOID: 2, SequenceNumber: 1},
			{ChildRelationOID: 7, ParentRelationOID: 1, SequenceNumber: 1},
			{ChildRelationOID: 6, ParentRelationOID: 1, SequenceNumber: 1},
			{ChildRelationOID: 5, ParentRelationOID: 2, SequenceNumber: 1},
		},
	}

	normalized := inventory.Normalize()
	assert.Equal(t, []string{"a", "z"}, []string{normalized.Schemas[0].Name, normalized.Schemas[1].Name})
	assert.Equal(t, "foreign", normalized.Relations[0].Name)
	assert.Equal(t, uint32(3), normalized.InheritanceEdges[0].ChildRelationOID)
	assert.Equal(t, uint32(2), normalized.InheritanceEdges[0].ParentRelationOID)
	assert.Equal(t, uint32(2), inventory.Schemas[0].OID,
		"normalization must not mutate the input")
	assert.Equal(t, normalized, normalized.Normalize())

	testCases := []struct {
		relationOID uint32
		expected    CatalogTableKind
		exists      bool
	}{
		{relationOID: 1, expected: CatalogTableKindTraditionalInheritance, exists: true},
		{relationOID: 2, expected: CatalogTableKindPartitioned, exists: true},
		{relationOID: 3, expected: CatalogTableKindDeclarativePartition, exists: true},
		{relationOID: 4, expected: CatalogTableKindForeign, exists: true},
		{relationOID: 5, expected: CatalogTableKindForeignPartition, exists: true},
		{relationOID: 6, expected: CatalogTableKindTraditionalInheritance, exists: true},
		{relationOID: 7, expected: CatalogTableKindMultipleInheritance, exists: true},
		{relationOID: 8},
		{relationOID: 9, expected: CatalogTableKindOrdinary, exists: true},
		{relationOID: 10, expected: CatalogTableKindTraditionalInheritance, exists: true},
		{relationOID: 99},
	}
	for _, testCase := range testCases {
		actual, exists := inventory.ClassifyTable(testCase.relationOID)
		assert.Equal(t, testCase.exists, exists, "relation OID %d", testCase.relationOID)
		assert.Equal(t, testCase.expected, actual, "relation OID %d", testCase.relationOID)
	}
}

func TestCatalogInventoryDatabaseFixtures(t *testing.T) {
	factory := testdb.MustNewFactory(t)

	t.Run("unfiltered schemas and extension members", func(t *testing.T) {
		db := factory.CreateDatabase(t)
		_, err := db.ConnPool.Exec(t.Context(), `
			CREATE SCHEMA excluded_inventory;
			COMMENT ON SCHEMA excluded_inventory IS 'excluded inventory comment';
			CREATE EXTENSION pg_trgm;
			CREATE TABLE excluded_inventory.extension_table (payload text);
			ALTER EXTENSION pg_trgm ADD TABLE excluded_inventory.extension_table;
		`)
		require.NoError(t, err)

		snapshot, err := GetSchemaSnapshot(t.Context(), db.ConnPool, WithIncludeSchemaPatterns("public"))
		require.NoError(t, err)
		assert.Equal(t, []NamedSchema{{Name: "public"}}, snapshot.Schema.NamedSchemas)
		for _, table := range snapshot.Schema.Tables {
			assert.NotEqual(t, "excluded_inventory", table.SchemaName)
		}

		excludedSchema := catalogSchemaByName(t, snapshot.Inventory, "excluded_inventory")
		assert.NotZero(t, excludedSchema.OID)
		assert.NotZero(t, excludedSchema.OwnerOID)
		assert.Equal(t, db.ConnPool.Config().ConnConfig.User, excludedSchema.OwnerName)
		assert.Equal(t, "excluded inventory comment", excludedSchema.Comment)

		extensionTable := catalogRelationByName(t, snapshot.Inventory, "excluded_inventory", "extension_table")
		require.NotNil(t, extensionTable.Extension)
		assert.NotZero(t, extensionTable.Extension.OID)
		assert.Equal(t, "pg_trgm", extensionTable.Extension.Name)
		require.NotNil(t, extensionTable.ToastRelation)
		assert.NotZero(t, extensionTable.ToastRelation.OID)
		assert.NotZero(t, extensionTable.ToastRelation.SchemaOID)
		assert.Equal(t, "pg_toast", extensionTable.ToastRelation.SchemaName)
		assert.NotEmpty(t, extensionTable.ToastRelation.Name)
		extensionScopedSnapshot, err := GetSchemaSnapshot(t.Context(), db.ConnPool,
			WithIncludeSchemaPatterns("excluded_inventory"))
		require.NoError(t, err)
		assert.Equal(t, []NamedSchema{{Name: "excluded_inventory"}},
			extensionScopedSnapshot.Schema.NamedSchemas)
		assert.Empty(t, extensionScopedSnapshot.Schema.Tables,
			"normal modeled discovery must hide extension-member tables")
		catalogRelationByName(t, extensionScopedSnapshot.Inventory, "excluded_inventory", "extension_table")

		for _, schema := range snapshot.Inventory.Schemas {
			assert.NotContains(t, []string{"pg_catalog", "information_schema", "pg_toast"}, schema.Name)
		}
		for _, relation := range snapshot.Inventory.Relations {
			assert.NotContains(t, []string{"pg_catalog", "information_schema", "pg_toast"}, relation.SchemaName)
		}

		excludeSnapshot, err := GetSchemaSnapshot(t.Context(), db.ConnPool,
			WithExcludeSchemaPatterns("excluded_inventory"))
		require.NoError(t, err)
		assert.Equal(t, snapshot.Schema, excludeSnapshot.Schema)
		assert.Equal(t, snapshot.Hash, excludeSnapshot.Hash)
		catalogRelationByName(t, excludeSnapshot.Inventory, "excluded_inventory", "extension_table")

		_, err = db.ConnPool.Exec(t.Context(), `CREATE SEQUENCE excluded_inventory.later_sequence`)
		require.NoError(t, err)
		afterDDL, err := GetSchemaSnapshot(t.Context(), db.ConnPool, WithIncludeSchemaPatterns("public"))
		require.NoError(t, err)
		assert.Equal(t, snapshot.Hash, afterDDL.Hash,
			"inventory must not expand the existing modeled-schema hash")
		catalogSequenceByName(t, afterDDL.Inventory, "excluded_inventory", "later_sequence")
	})

	t.Run("partition and inheritance topology", func(t *testing.T) {
		db := factory.CreateDatabase(t)
		_, err := db.ConnPool.Exec(t.Context(), `
			CREATE SCHEMA tree_root;
			CREATE SCHEMA tree_middle;
			CREATE SCHEMA tree_leaf;
			CREATE TABLE tree_root.root (id integer) PARTITION BY RANGE (id);
			CREATE TABLE tree_middle.branch
				PARTITION OF tree_root.root FOR VALUES FROM (0) TO (100)
				PARTITION BY RANGE (id);
			CREATE TABLE tree_leaf.leaf
				PARTITION OF tree_middle.branch FOR VALUES FROM (0) TO (50);

			CREATE FOREIGN DATA WRAPPER inventory_fdw NO HANDLER;
			CREATE SERVER inventory_server FOREIGN DATA WRAPPER inventory_fdw;
			CREATE FOREIGN TABLE public.foreign_table (id integer) SERVER inventory_server;
			CREATE FOREIGN TABLE tree_leaf.foreign_leaf
				PARTITION OF tree_root.root FOR VALUES FROM (100) TO (200)
				SERVER inventory_server;

			CREATE TABLE public.parent_one (one_id integer);
			CREATE TABLE public.parent_two (two_id integer);
			CREATE TABLE public.plain_table (id integer);
			CREATE TABLE public.inherited_once (local_value text) INHERITS (public.parent_one);
			CREATE TABLE public.inherited_multiple (local_value text)
				INHERITS (public.parent_one, public.parent_two);
		`)
		require.NoError(t, err)

		snapshot, err := GetSchemaSnapshot(t.Context(), db.ConnPool)
		require.NoError(t, err)
		inventory := snapshot.Inventory
		root := catalogRelationByName(t, inventory, "tree_root", "root")
		branch := catalogRelationByName(t, inventory, "tree_middle", "branch")
		leaf := catalogRelationByName(t, inventory, "tree_leaf", "leaf")
		foreignTable := catalogRelationByName(t, inventory, "public", "foreign_table")
		foreignLeaf := catalogRelationByName(t, inventory, "tree_leaf", "foreign_leaf")
		parentOne := catalogRelationByName(t, inventory, "public", "parent_one")
		parentTwo := catalogRelationByName(t, inventory, "public", "parent_two")
		plainTable := catalogRelationByName(t, inventory, "public", "plain_table")
		inheritedOnce := catalogRelationByName(t, inventory, "public", "inherited_once")
		inheritedMultiple := catalogRelationByName(t, inventory, "public", "inherited_multiple")

		assertCatalogTableKind(t, inventory, root.OID, CatalogTableKindPartitioned)
		assertCatalogTableKind(t, inventory, branch.OID, CatalogTableKindDeclarativePartition)
		assert.Equal(t, RelKindPartitionedTable, branch.Kind,
			"a partitioned partition must retain both catalog facts")
		assertCatalogTableKind(t, inventory, leaf.OID, CatalogTableKindDeclarativePartition)
		assertCatalogTableKind(t, inventory, foreignTable.OID, CatalogTableKindForeign)
		assertCatalogTableKind(t, inventory, foreignLeaf.OID, CatalogTableKindForeignPartition)
		assertCatalogTableKind(t, inventory, parentOne.OID, CatalogTableKindTraditionalInheritance)
		assertCatalogTableKind(t, inventory, parentTwo.OID, CatalogTableKindTraditionalInheritance)
		assertCatalogTableKind(t, inventory, plainTable.OID, CatalogTableKindOrdinary)
		assertCatalogTableKind(t, inventory, inheritedOnce.OID, CatalogTableKindTraditionalInheritance)
		assertCatalogTableKind(t, inventory, inheritedMultiple.OID, CatalogTableKindMultipleInheritance)

		assert.Equal(t, []CatalogInheritanceEdge{{
			ChildRelationOID: branch.OID, ParentRelationOID: root.OID, SequenceNumber: 1,
		}}, catalogParentEdges(inventory, branch.OID))
		assert.Equal(t, []CatalogInheritanceEdge{{
			ChildRelationOID: leaf.OID, ParentRelationOID: branch.OID, SequenceNumber: 1,
		}}, catalogParentEdges(inventory, leaf.OID))
		assert.Equal(t, []CatalogInheritanceEdge{{
			ChildRelationOID: foreignLeaf.OID, ParentRelationOID: root.OID, SequenceNumber: 1,
		}}, catalogParentEdges(inventory, foreignLeaf.OID))
		assert.Equal(t, []CatalogInheritanceEdge{
			{ChildRelationOID: inheritedMultiple.OID, ParentRelationOID: parentOne.OID, SequenceNumber: 1},
			{ChildRelationOID: inheritedMultiple.OID, ParentRelationOID: parentTwo.OID, SequenceNumber: 2},
		}, catalogParentEdges(inventory, inheritedMultiple.OID))
	})

	t.Run("relation and type namespaces", func(t *testing.T) {
		db := factory.CreateDatabase(t)
		_, err := db.ConnPool.Exec(t.Context(), `
			CREATE SCHEMA namespace_inventory;
			COMMENT ON SCHEMA namespace_inventory IS 'namespace inventory comment';
			CREATE TABLE namespace_inventory.collision (
				id bigserial CONSTRAINT collision_pk PRIMARY KEY,
				payload text CONSTRAINT payload_nonempty CHECK (payload <> '')
			);
			CREATE INDEX collision_payload_idx ON namespace_inventory.collision (payload);
			CREATE SEQUENCE namespace_inventory._collision;
			CREATE VIEW namespace_inventory.collision_view AS
				SELECT id, payload FROM namespace_inventory.collision;
			CREATE MATERIALIZED VIEW namespace_inventory.collision_materialized AS
				SELECT id FROM namespace_inventory.collision WITH NO DATA;
			CREATE TYPE namespace_inventory.composite_identity AS (id bigint);
			CREATE TABLE namespace_inventory.partitioned_index_owner (id bigint)
				PARTITION BY RANGE (id);
			CREATE INDEX partitioned_owner_idx
				ON namespace_inventory.partitioned_index_owner (id);
		`)
		require.NoError(t, err)

		snapshot, err := GetSchemaSnapshot(t.Context(), db.ConnPool)
		require.NoError(t, err)
		inventory := snapshot.Inventory
		assert.Equal(t, inventory, inventory.Normalize())

		schemaIdentity := catalogSchemaByName(t, inventory, "namespace_inventory")
		assert.NotZero(t, schemaIdentity.OID)
		assert.NotZero(t, schemaIdentity.OwnerOID)
		assert.Equal(t, db.ConnPool.Config().ConnConfig.User, schemaIdentity.OwnerName)
		assert.Equal(t, "namespace inventory comment", schemaIdentity.Comment)

		collision := catalogRelationByName(t, inventory, "namespace_inventory", "collision")
		assert.NotZero(t, collision.OwnerOID)
		assert.Equal(t, schemaIdentity.OwnerName, collision.OwnerName)
		assert.Equal(t, RelationPersistencePermanent, collision.Persistence)
		assert.NotZero(t, collision.RowTypeOID)
		assert.NotZero(t, collision.ArrayTypeOID)
		require.NotNil(t, collision.ToastRelation)
		assert.Equal(t, "pg_toast", collision.ToastRelation.SchemaName)

		rowType := catalogTypeByOID(t, inventory, collision.RowTypeOID)
		assert.Equal(t, CatalogTypeKindRow, rowType.Kind)
		assert.Equal(t, collision.OID, rowType.RelationOID)
		assert.Equal(t, collision.SchemaOID, rowType.SchemaOID)
		assert.Equal(t, "namespace_inventory", rowType.SchemaName)
		assert.Equal(t, "collision", rowType.Name)
		arrayType := catalogTypeByOID(t, inventory, collision.ArrayTypeOID)
		assert.Equal(t, CatalogTypeKindArray, arrayType.Kind)
		assert.Equal(t, "__collision", arrayType.Name)

		sequence := catalogSequenceByName(t, inventory, "namespace_inventory", "_collision")
		assert.Equal(t, schemaIdentity.OID, sequence.SchemaOID)
		assert.NotEqual(t, sequence.Name, arrayType.Name,
			"PostgreSQL must resolve relation/type namespace collisions without hiding either identity")
		catalogSequenceByName(t, inventory, "namespace_inventory", "collision_id_seq")

		primaryIndex := catalogIndexByName(t, inventory, "namespace_inventory", "collision_pk")
		assert.Equal(t, collision.OID, primaryIndex.RelationOID)
		assert.Equal(t, RelKindIndex, primaryIndex.Kind)
		payloadIndex := catalogIndexByName(t, inventory, "namespace_inventory", "collision_payload_idx")
		assert.Equal(t, collision.OID, payloadIndex.RelationOID)
		partitionedIndex := catalogIndexByName(t, inventory, "namespace_inventory", "partitioned_owner_idx")
		assert.Equal(t, RelKindPartitionedIndex, partitionedIndex.Kind)

		primaryConstraint := catalogConstraintByName(t, inventory, collision.OID, "collision_pk")
		assert.Equal(t, "p", primaryConstraint.Type)
		assert.Equal(t, primaryIndex.OID, primaryConstraint.IndexOID)
		checkConstraint := catalogConstraintByName(t, inventory, collision.OID, "payload_nonempty")
		assert.Equal(t, "c", checkConstraint.Type)
		assert.Zero(t, checkConstraint.IndexOID)

		assert.Equal(t, RelKindView,
			catalogRelationByName(t, inventory, "namespace_inventory", "collision_view").Kind)
		assert.Equal(t, RelKindMaterializedView,
			catalogRelationByName(t, inventory, "namespace_inventory", "collision_materialized").Kind)
		assert.Equal(t, RelKindCompositeType,
			catalogRelationByName(t, inventory, "namespace_inventory", "composite_identity").Kind)
	})
}

func catalogSchemaByName(t *testing.T, inventory CatalogInventory, name string) CatalogSchema {
	t.Helper()
	for _, schema := range inventory.Schemas {
		if schema.Name == name {
			return schema
		}
	}
	require.FailNow(t, "catalog schema not found", "schema: %s", name)
	return CatalogSchema{}
}

func catalogRelationByName(t *testing.T, inventory CatalogInventory, schemaName, name string) CatalogRelation {
	t.Helper()
	for _, relation := range inventory.Relations {
		if relation.SchemaName == schemaName && relation.Name == name {
			return relation
		}
	}
	require.FailNow(t, "catalog relation not found", "relation: %s.%s", schemaName, name)
	return CatalogRelation{}
}

func catalogTypeByOID(t *testing.T, inventory CatalogInventory, oid uint32) CatalogType {
	t.Helper()
	for _, catalogType := range inventory.Types {
		if catalogType.OID == oid {
			return catalogType
		}
	}
	require.FailNow(t, "catalog type not found", "OID: %d", oid)
	return CatalogType{}
}

func catalogIndexByName(t *testing.T, inventory CatalogInventory, schemaName, name string) CatalogIndex {
	t.Helper()
	for _, index := range inventory.Indexes {
		if index.SchemaName == schemaName && index.Name == name {
			return index
		}
	}
	require.FailNow(t, "catalog index not found", "index: %s.%s", schemaName, name)
	return CatalogIndex{}
}

func catalogConstraintByName(
	t *testing.T,
	inventory CatalogInventory,
	relationOID uint32,
	name string,
) CatalogConstraint {
	t.Helper()
	for _, constraint := range inventory.Constraints {
		if constraint.RelationOID == relationOID && constraint.Name == name {
			return constraint
		}
	}
	require.FailNow(t, "catalog constraint not found", "relation OID: %d, constraint: %s", relationOID, name)
	return CatalogConstraint{}
}

func catalogSequenceByName(t *testing.T, inventory CatalogInventory, schemaName, name string) CatalogSequence {
	t.Helper()
	for _, sequence := range inventory.Sequences {
		if sequence.SchemaName == schemaName && sequence.Name == name {
			return sequence
		}
	}
	require.FailNow(t, "catalog sequence not found", "sequence: %s.%s", schemaName, name)
	return CatalogSequence{}
}

func catalogParentEdges(inventory CatalogInventory, childOID uint32) []CatalogInheritanceEdge {
	var edges []CatalogInheritanceEdge
	for _, edge := range inventory.InheritanceEdges {
		if edge.ChildRelationOID == childOID {
			edges = append(edges, edge)
		}
	}
	return edges
}

func assertCatalogTableKind(
	t *testing.T,
	inventory CatalogInventory,
	relationOID uint32,
	expected CatalogTableKind,
) {
	t.Helper()
	actual, exists := inventory.ClassifyTable(relationOID)
	require.True(t, exists)
	assert.Equal(t, expected, actual)
}
