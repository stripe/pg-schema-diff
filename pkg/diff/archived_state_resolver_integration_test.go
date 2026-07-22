package diff

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/internal/testdb"
)

func TestArchivedStateResolverPostgresFixtures(t *testing.T) {
	factory := testdb.MustNewFactory(t)

	t.Run("complete table with later same-name replacement", func(t *testing.T) {
		db := factory.CreateDatabase(t)
		_, err := db.ConnPool.Exec(t.Context(), `
			CREATE TABLE public.accounts (
				id BIGSERIAL PRIMARY KEY,
				tenant_id BIGINT NOT NULL,
				positive_value INTEGER CONSTRAINT accounts_positive CHECK (positive_value > 0),
				payload TEXT
			);
			CREATE INDEX accounts_payload_idx ON public.accounts (payload);
			CREATE STATISTICS accounts_stats ON tenant_id, positive_value FROM public.accounts;
			ALTER TABLE public.accounts ENABLE ROW LEVEL SECURITY;
			CREATE POLICY accounts_policy ON public.accounts USING (tenant_id > 0);
			CREATE FUNCTION public.accounts_trigger_function() RETURNS trigger
				LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END';
			CREATE TRIGGER accounts_trigger BEFORE UPDATE ON public.accounts
				FOR EACH ROW EXECUTE FUNCTION public.accounts_trigger_function();
			CREATE RULE accounts_rule AS ON DELETE TO public.accounts DO INSTEAD NOTHING;
		`)
		require.NoError(t, err)

		before, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
		require.NoError(t, err)
		original := mustCatalogRelationByName(t, before.Inventory, "public", "accounts")
		marker := buildPostgresArchivalMarker(t, before.Inventory, []schema.CatalogRelation{original})
		createMarkedSchemas(t, db.ConnPool, marker)
		moveMarkedMembers(t, db.ConnPool, marker)
		_, err = db.ConnPool.Exec(t.Context(), `
			CREATE TABLE public.accounts (id BIGINT PRIMARY KEY, replacement BOOLEAN NOT NULL);
		`)
		require.NoError(t, err)

		after, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
		require.NoError(t, err)
		assert.Contains(t, after.Schema.NamedSchemas, schema.NamedSchema{
			Name: marker.Members[0].CleanupTable.SchemaName,
		},
			"the fixture intentionally disables modeled cleanup exclusion")
		moved := mustCatalogRelationByOID(t, after.Inventory, original.OID)
		assert.Equal(t, marker.Members[0].CleanupTable.SchemaName, moved.SchemaName)
		replacement := mustCatalogRelationByName(t, after.Inventory, "public", "accounts")
		assert.NotEqual(t, original.OID, replacement.OID)

		resolution, err := resolveArchivedState(resolverTestPrefix, after, schema.SchemaSnapshot{})
		require.NoError(t, err)
		require.Len(t, resolution.CandidateGroups, 1)
		assert.Equal(t, archivedCandidateGroupStateCompleteCandidate, resolution.CandidateGroups[0].State)
		assert.Empty(t, resolution.CandidateGroups[0].Resume.RemainingMemberMoves)
	})

	t.Run("recursive cross-schema partition tree", func(t *testing.T) {
		db := factory.CreateDatabase(t)
		_, err := db.ConnPool.Exec(t.Context(), `
			CREATE SCHEMA partition_root;
			CREATE SCHEMA partition_branch;
			CREATE SCHEMA partition_leaf;
			CREATE TABLE partition_root.events (
				event_date DATE NOT NULL,
				tenant_id BIGINT NOT NULL,
				payload TEXT
			) PARTITION BY RANGE (event_date);
			CREATE TABLE partition_branch.events_2026
				PARTITION OF partition_root.events
				FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')
				PARTITION BY RANGE (tenant_id);
			CREATE TABLE partition_leaf.events_small
				PARTITION OF partition_branch.events_2026
				FOR VALUES FROM (0) TO (1000);
		`)
		require.NoError(t, err)

		before, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
		require.NoError(t, err)
		members := []schema.CatalogRelation{
			mustCatalogRelationByName(t, before.Inventory, "partition_root", "events"),
			mustCatalogRelationByName(t, before.Inventory, "partition_branch", "events_2026"),
			mustCatalogRelationByName(t, before.Inventory, "partition_leaf", "events_small"),
		}
		marker := buildPostgresArchivalMarker(t, before.Inventory, members)
		createMarkedSchemas(t, db.ConnPool, marker)
		moveMarkedMembers(t, db.ConnPool, marker)

		after, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
		require.NoError(t, err)
		resolution, err := resolveArchivedState(resolverTestPrefix, after, schema.SchemaSnapshot{})
		require.NoError(t, err)
		require.Len(t, resolution.CandidateGroups, 1)
		candidate := resolution.CandidateGroups[0]
		assert.Equal(t, archivedCandidateGroupStateCompleteCandidate, candidate.State)
		assert.Len(t, candidate.SchemaNames, 3)

		membersBySource := make(map[string]archivalMarkerMemberV1)
		for _, member := range marker.Members {
			membersBySource[member.SourceTable.SchemaName] = member
		}
		branch := membersBySource["partition_branch"]
		leaf := membersBySource["partition_leaf"]
		_, err = db.ConnPool.Exec(t.Context(), fmt.Sprintf(
			"ALTER TABLE %s.%s DETACH PARTITION %s.%s",
			schema.EscapeIdentifier(branch.CleanupTable.SchemaName),
			schema.EscapeIdentifier(branch.CleanupTable.Name),
			schema.EscapeIdentifier(leaf.CleanupTable.SchemaName),
			schema.EscapeIdentifier(leaf.CleanupTable.Name),
		))
		require.NoError(t, err)
		detached, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
		require.NoError(t, err)
		_, err = resolveArchivedState(resolverTestPrefix, detached, schema.SchemaSnapshot{})
		require.Error(t, err)
		assert.ErrorContains(t, err, "missing expected inheritance edge")
	})
}

func buildPostgresArchivalMarker(
	t *testing.T,
	inventory schema.CatalogInventory,
	relations []schema.CatalogRelation,
) archivalMarkerV1 {
	t.Helper()
	const timestamp = "20260721T091011123456Z"
	const nonce = "ABCDEFGH"
	marker := archivalMarkerV1{
		Version:       archivalMarkerVersion,
		GroupID:       resolverTestGroupID,
		CleanupDigest: cleanupOperationDigest("sha256:" + strings.Repeat("0", 64)),
	}
	memberIDByOID := make(map[uint32]string, len(relations))
	for _, relation := range relations {
		cleanupName, err := buildArchivalSchemaName(
			resolverTestPrefix, relation.SchemaName, relation.Name, timestamp, nonce,
		)
		require.NoError(t, err)
		move, err := inventory.ExpectedTableMove(relation.OID)
		require.NoError(t, err)
		memberID := fmt.Sprintf("member-%d", relation.OID)
		memberIDByOID[relation.OID] = memberID
		marker.Members = append(marker.Members, archivalMarkerMemberV1{
			MemberID: memberID,
			SourceTable: markerObject(
				relation.OID, archivalMarkerObjectKindTable, relation.SchemaName, relation.Name,
			),
			CleanupTable: markerObject(
				relation.OID, archivalMarkerObjectKindTable, cleanupName, relation.Name,
			),
			AutomaticallyMovedObjects: markerObjectsFromCatalog(move.CleanupSchemaObjects, cleanupName),
			AttachedObjects:           markerObjectsFromCatalog(move.AttachedObjects, cleanupName),
			ExplicitlyMovedObjects:    markerObjectsFromCatalog(move.ExplicitMoveObjects, cleanupName),
			InternalToastObjects:      markerObjectsFromCatalog(move.InternalObjects, ""),
		})
	}
	for _, edge := range inventory.InheritanceEdges {
		parentID, parentFound := memberIDByOID[edge.ParentRelationOID]
		childID, childFound := memberIDByOID[edge.ChildRelationOID]
		if parentFound && childFound {
			marker.PartitionEdges = append(marker.PartitionEdges, archivalMarkerPartitionEdgeV1{
				ParentMemberID: parentID, ChildMemberID: childID, BoundExpression: "pending",
			})
		}
	}
	require.NoError(t, populateArchivalMarkerPartitionMetadata(inventory, &marker))
	_, err := marshalArchivalMarker(marker)
	require.NoError(t, err)
	return marker
}

func createMarkedSchemas(t *testing.T, pool *pgxpool.Pool, marker archivalMarkerV1) {
	t.Helper()
	encoded, err := marshalArchivalMarker(marker)
	require.NoError(t, err)
	for _, schemaName := range archivedMarkerSchemaNames(marker) {
		_, err := pool.Exec(t.Context(), fmt.Sprintf(
			"CREATE SCHEMA %s; COMMENT ON SCHEMA %s IS %s",
			schema.EscapeIdentifier(schemaName), schema.EscapeIdentifier(schemaName),
			escapeArchivalMarkerSQLLiteral(encoded),
		))
		require.NoError(t, err)
	}
}

func moveMarkedMembers(t *testing.T, pool *pgxpool.Pool, marker archivalMarkerV1) {
	t.Helper()
	depths := make(map[string]int, len(marker.Members))
	for changed := true; changed; {
		changed = false
		for _, edge := range marker.PartitionEdges {
			if depths[edge.ChildMemberID] < depths[edge.ParentMemberID]+1 {
				depths[edge.ChildMemberID] = depths[edge.ParentMemberID] + 1
				changed = true
			}
		}
	}
	members := slices.Clone(marker.Members)
	slices.SortFunc(members, func(a, b archivalMarkerMemberV1) int {
		if byDepth := depths[b.MemberID] - depths[a.MemberID]; byDepth != 0 {
			return byDepth
		}
		return strings.Compare(a.MemberID, b.MemberID)
	})
	for _, member := range members {
		for _, object := range member.ExplicitlyMovedObjects {
			_, err := pool.Exec(t.Context(), fmt.Sprintf(
				"ALTER STATISTICS %s.%s SET SCHEMA %s",
				schema.EscapeIdentifier(member.SourceTable.SchemaName), schema.EscapeIdentifier(object.Name),
				schema.EscapeIdentifier(member.CleanupTable.SchemaName),
			))
			require.NoError(t, err)
		}
		_, err := pool.Exec(t.Context(), fmt.Sprintf(
			"ALTER TABLE %s.%s SET SCHEMA %s",
			schema.EscapeIdentifier(member.SourceTable.SchemaName),
			schema.EscapeIdentifier(member.SourceTable.Name),
			schema.EscapeIdentifier(member.CleanupTable.SchemaName),
		))
		require.NoError(t, err)
	}
}

func mustCatalogRelationByName(
	t *testing.T,
	inventory schema.CatalogInventory,
	schemaName string,
	name string,
) schema.CatalogRelation {
	t.Helper()
	for _, relation := range inventory.Relations {
		if relation.SchemaName == schemaName && relation.Name == name &&
			(relation.Kind == schema.RelKindOrdinaryTable ||
				relation.Kind == schema.RelKindPartitionedTable) {
			return relation
		}
	}
	require.FailNow(t, "catalog relation not found", "relation: %s.%s", schemaName, name)
	return schema.CatalogRelation{}
}

func mustCatalogRelationByOID(
	t *testing.T,
	inventory schema.CatalogInventory,
	oid uint32,
) schema.CatalogRelation {
	t.Helper()
	for _, relation := range inventory.Relations {
		if relation.OID == oid {
			return relation
		}
	}
	require.FailNow(t, "catalog relation not found", "OID: %d", oid)
	return schema.CatalogRelation{}
}
