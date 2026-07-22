package diff

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/internal/testdb"
)

func TestPartitionArchivalPostgresCompleteTree(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	_, err := db.ConnPool.Exec(t.Context(), `
		CREATE SCHEMA stage15_root;
		CREATE SCHEMA stage15_branch;
		CREATE SCHEMA stage15_leaf;
		CREATE TABLE stage15_root.events (
			event_date DATE NOT NULL,
			tenant_id INTEGER NOT NULL,
			payload TEXT NOT NULL,
			CONSTRAINT events_payload_check CHECK (length(payload) > 0)
		) PARTITION BY RANGE (event_date);
		CREATE TABLE stage15_branch.events_2026
			PARTITION OF stage15_root.events
			FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')
			PARTITION BY RANGE (tenant_id);
		CREATE TABLE stage15_leaf.events_large
			PARTITION OF stage15_branch.events_2026 FOR VALUES FROM (100) TO (1000);
		CREATE TABLE stage15_leaf.events_small
			PARTITION OF stage15_branch.events_2026 FOR VALUES FROM (0) TO (100);
		CREATE SEQUENCE stage15_root.events_owned_seq OWNED BY stage15_root.events.tenant_id;
		CREATE SEQUENCE stage15_branch.events_2026_owned_seq
			OWNED BY stage15_branch.events_2026.tenant_id;
		CREATE SEQUENCE stage15_leaf.events_large_owned_seq
			OWNED BY stage15_leaf.events_large.tenant_id;
		CREATE SEQUENCE stage15_leaf.events_small_owned_seq
			OWNED BY stage15_leaf.events_small.tenant_id;
		CREATE INDEX events_payload_idx ON stage15_root.events (payload);
		CREATE FUNCTION stage15_root.events_trigger_fn() RETURNS trigger
			LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END';
		CREATE TRIGGER events_trigger BEFORE INSERT ON stage15_root.events
			FOR EACH ROW EXECUTE FUNCTION stage15_root.events_trigger_fn();
		CREATE STATISTICS events_small_stats ON tenant_id, payload
			FROM stage15_leaf.events_small;
		INSERT INTO stage15_root.events VALUES
			('2026-02-01', 10, repeat('small-', 500)),
			('2026-03-01', 200, repeat('large-', 500));
	`)
	require.NoError(t, err)

	before, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
	require.NoError(t, err)
	root := mustCatalogRelationByName(t, before.Inventory, "stage15_root", "events")
	request, marker := partitionArchivalDatabaseRequest(t, before.Inventory, root, nil,
		"20260722T091011123456Z_ABCDEFGH")
	statements, err := generatePlainTableArchivalStatements(request)
	require.NoError(t, err)

	moveDDL := archivalMoveDDL(statements)
	require.Len(t, moveDDL, 4)
	assert.Contains(t, moveDDL[0], `"events_large"`)
	assert.Contains(t, moveDDL[1], `"events_small"`)
	assert.Contains(t, moveDDL[2], `"events_2026"`)
	assert.Contains(t, moveDDL[3], `"events"`)
	assert.Equal(t, cloneMarkerObject(markerRoot(marker).CleanupTable), preparedCleanupRoot(t, request))
	assertSingleCleanupRootIntent(t, request, markerRoot(marker).CleanupTable)

	for _, statement := range statements {
		_, err = db.ConnPool.Exec(t.Context(), statement.ToSQL())
		require.NoError(t, err, statement.DDL)
	}

	after, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
	require.NoError(t, err)
	for _, member := range marker.Members {
		assertPlainTableMovedIdentities(t, before.Inventory, after.Inventory, member)
		assertPlainTableAttachedMetadata(t, before.Inventory, after.Inventory, member.SourceTable.OID)
		assert.Equal(t, member.SourceTable.OID,
			postgresRelationOID(t, db.ConnPool, member.CleanupTable.SchemaName, member.CleanupTable.Name))
	}
	assertPartitionMarkerMetadataPreserved(t, after.Inventory, marker)

	rootMember := markerRoot(marker)
	_, err = db.ConnPool.Exec(t.Context(), fmt.Sprintf(
		"INSERT INTO %s.%s VALUES ('2026-04-01', 20, 'routed-after-move')",
		schema.EscapeIdentifier(rootMember.CleanupTable.SchemaName),
		schema.EscapeIdentifier(rootMember.CleanupTable.Name),
	))
	require.NoError(t, err)
	var rowCount int
	err = db.ConnPool.QueryRow(t.Context(), fmt.Sprintf(
		"SELECT count(*) FROM %s.%s",
		schema.EscapeIdentifier(rootMember.CleanupTable.SchemaName),
		schema.EscapeIdentifier(rootMember.CleanupTable.Name),
	)).Scan(&rowCount)
	require.NoError(t, err)
	assert.Equal(t, 3, rowCount)
}

func TestPartitionArchivalPostgresDetachedSubtree(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	_, err := db.ConnPool.Exec(t.Context(), `
		CREATE SCHEMA stage15_active;
		CREATE SCHEMA stage15_subtree;
		CREATE SCHEMA stage15_subtree_leaf;
		CREATE TABLE stage15_active.metrics (
			bucket INTEGER NOT NULL,
			shard INTEGER NOT NULL,
			payload TEXT NOT NULL
		) PARTITION BY RANGE (bucket);
		CREATE TABLE stage15_subtree.metrics_old
			PARTITION OF stage15_active.metrics FOR VALUES FROM (0) TO (100)
			PARTITION BY RANGE (shard);
		CREATE TABLE stage15_subtree_leaf.a_metrics_old
			PARTITION OF stage15_subtree.metrics_old FOR VALUES FROM (0) TO (50);
		CREATE TABLE stage15_subtree_leaf.b_metrics_old
			PARTITION OF stage15_subtree.metrics_old FOR VALUES FROM (50) TO (100);
		CREATE INDEX metrics_payload_idx ON stage15_active.metrics (payload);
		CREATE FUNCTION stage15_active.metrics_trigger_fn() RETURNS trigger
			LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END';
		CREATE TRIGGER metrics_trigger BEFORE INSERT ON stage15_active.metrics
			FOR EACH ROW EXECUTE FUNCTION stage15_active.metrics_trigger_fn();
		INSERT INTO stage15_active.metrics VALUES (10, 10, 'a'), (20, 70, 'b');
	`)
	require.NoError(t, err)

	before, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
	require.NoError(t, err)
	parent := mustCatalogRelationByName(t, before.Inventory, "stage15_active", "metrics")
	root := mustCatalogRelationByName(t, before.Inventory, "stage15_subtree", "metrics_old")
	request, marker := partitionArchivalDatabaseRequest(t, before.Inventory, root, &parent,
		"20260722T091011123456Z_12345678")
	statements, err := generatePlainTableArchivalStatements(request)
	require.NoError(t, err)

	detachIndex := slices.IndexFunc(statements, func(statement Statement) bool {
		return strings.Contains(statement.DDL, " DETACH PARTITION ")
	})
	require.NotEqual(t, -1, detachIndex)
	for idx, statement := range statements {
		if strings.Contains(statement.DDL, " SET SCHEMA ") &&
			strings.HasPrefix(statement.DDL, "ALTER TABLE") {
			assert.Less(t, idx, detachIndex)
		}
	}
	detach := statements[detachIndex]
	assert.Contains(t, detach.DDL, schema.EscapeIdentifier(
		markerRoot(marker).CleanupTable.SchemaName,
	))
	assert.ElementsMatch(t, []MigrationHazardType{
		MigrationHazardTypeAcquiresAccessExclusiveLock, MigrationHazardTypeCorrectness,
	}, statementHazardTypes(detach))
	require.Len(t, marker.LostParentAttachments, 1)
	assert.Equal(t, parent.OID, marker.LostParentAttachments[0].ParentTable.OID)
	assert.Equal(t, "FOR VALUES FROM (0) TO (100)", marker.LostParentAttachments[0].BoundExpression)
	assert.NotEmpty(t, marker.LostParentAttachments[0].PartitionedIndexAttachments)
	assert.NotEmpty(t, marker.LostParentAttachments[0].ClonedTriggers)
	assert.Equal(t, cloneMarkerObject(markerRoot(marker).CleanupTable), preparedCleanupRoot(t, request))
	assertSingleCleanupRootIntent(t, request, markerRoot(marker).CleanupTable)

	for _, statement := range statements[:detachIndex] {
		_, err = db.ConnPool.Exec(t.Context(), statement.ToSQL())
		require.NoError(t, err, statement.DDL)
	}
	attached, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
	require.NoError(t, err)
	resolution, err := resolveArchivedState("archive", attached, schema.SchemaSnapshot{})
	require.NoError(t, err)
	require.Len(t, resolution.CandidateGroups, 1)
	candidate := resolution.CandidateGroups[0]
	assert.Equal(t, archivedCandidateGroupStatePartialResumable, candidate.State)
	assert.True(t, candidate.Resume.RemainingPartitionDetach)
	assert.Empty(t, candidate.Resume.RemainingMemberMoves)
	markerText, err := marshalArchivalMarker(marker)
	require.NoError(t, err)
	resumeRequest := plainTableArchivalRequest{
		CurrentInventory: attached.Inventory,
		Groups: []plainTableArchivalGroupRequest{{
			FinalizedMarker: markerText, Resume: &candidate.Resume,
			DetachedSubtree: request.Groups[0].DetachedSubtree,
		}},
		SourcePreflight: sourceSafetyPreflightResult{
			ValidatedTableRelationOIDs: plainMarkerRelationOIDs(marker),
		},
		DependencyClosure: archivedDependencyClosureResult{
			ValidatedGroupIDs: []archivalGroupID{marker.GroupID},
			DependencyValidatedCandidateGroups: []dependencyValidatedArchivedCandidateGroup{{
				Candidate: candidate,
			}},
		},
	}
	resumeStatements, err := generatePlainTableArchivalStatements(resumeRequest)
	require.NoError(t, err)
	assert.Empty(t, archivalMoveStatements(resumeStatements))
	require.Len(t, slices.DeleteFunc(slices.Clone(resumeStatements), func(statement Statement) bool {
		return !strings.Contains(statement.DDL, " DETACH PARTITION ")
	}), 1)
	for _, statement := range resumeStatements {
		_, err = db.ConnPool.Exec(t.Context(), statement.ToSQL())
		require.NoError(t, err, statement.DDL)
	}
	after, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
	require.NoError(t, err)
	for _, member := range marker.Members {
		assertDetachedPartitionMemberIdentities(t, before.Inventory, after.Inventory, member)
	}
	assertPartitionMarkerMetadataPreserved(t, after.Inventory, marker)
	for _, index := range marker.LostParentAttachments[0].PartitionedIndexAttachments {
		assert.False(t, isCatalogInheritanceEdge(after.Inventory,
			index.ParentIndex.OID, index.ChildIndex.OID))
	}
	for _, trigger := range marker.LostParentAttachments[0].ClonedTriggers {
		assert.Zero(t, catalogTriggerByOID(after.Inventory, trigger.ChildTrigger.OID).OID)
	}
	rootMember := markerRoot(marker)
	movedRoot := mustCatalogRelationByOID(t, after.Inventory, root.OID)
	assert.False(t, movedRoot.IsPartition)
	assert.Equal(t, rootMember.CleanupTable.SchemaName, movedRoot.SchemaName)
	assert.False(t, isCatalogInheritanceEdge(after.Inventory, parent.OID, root.OID))

	_, err = db.ConnPool.Exec(t.Context(), `INSERT INTO stage15_active.metrics VALUES (10, 10, 'no-route')`)
	require.Error(t, err)
	_, err = db.ConnPool.Exec(t.Context(), fmt.Sprintf(
		"INSERT INTO %s.%s VALUES (10, 10, 'subtree-route')",
		schema.EscapeIdentifier(rootMember.CleanupTable.SchemaName),
		schema.EscapeIdentifier(rootMember.CleanupTable.Name),
	))
	require.NoError(t, err)
}

func TestPartitionArchivalReplacementAwareGraphSuppressesChildDeletes(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	_, err := db.ConnPool.Exec(t.Context(), `
		CREATE TABLE public.graph_tree (
			id INTEGER,
			payload TEXT CHECK (payload IS NOT NULL)
		) PARTITION BY RANGE (id);
		CREATE TABLE public.graph_branch PARTITION OF public.graph_tree
			FOR VALUES FROM (0) TO (100) PARTITION BY RANGE (id);
		CREATE TABLE public.graph_leaf PARTITION OF public.graph_branch FOR VALUES FROM (0) TO (50);
		CREATE INDEX graph_payload_idx ON public.graph_tree (payload);
		CREATE SEQUENCE public.graph_leaf_owned OWNED BY public.graph_leaf.id;
		CREATE FUNCTION public.graph_trigger_fn() RETURNS trigger
			LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END';
		CREATE TRIGGER graph_trigger BEFORE INSERT ON public.graph_tree
			FOR EACH ROW EXECUTE FUNCTION public.graph_trigger_fn();
		CREATE TRIGGER graph_leaf_trigger BEFORE UPDATE ON public.graph_leaf
			FOR EACH ROW EXECUTE FUNCTION public.graph_trigger_fn();
		CREATE POLICY graph_leaf_policy ON public.graph_leaf USING (id >= 0);
	`)
	require.NoError(t, err)

	current, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
	require.NoError(t, err)
	root := mustCatalogRelationByName(t, current.Inventory, "public", "graph_tree")
	request, marker := partitionArchivalDatabaseRequest(t, current.Inventory, root, nil,
		"20260722T091011123456Z_GHIJKLMN")
	diff, _, err := buildSchemaDiff(current.Schema, schema.Schema{})
	require.NoError(t, err)
	dispositions := make(tableDispositions, len(marker.Members))
	for _, member := range marker.Members {
		dispositions[markerTableName(member.SourceTable).GetName()] = tableDisposition{
			Kind: tableDispositionKindArchivalMove, GroupID: marker.GroupID,
			RelationOID: member.SourceTable.OID,
		}
	}

	statements, err := generateReplacementAwareSchemaSQL(
		newSchemaSQLGenerator(&deterministicRandReader{}, &planOptions{}),
		diff, dispositions, request,
	)
	require.NoError(t, err)
	moveDDL := archivalMoveDDL(statements)
	require.Len(t, moveDDL, 3)
	assert.Contains(t, moveDDL[0], `"graph_leaf"`)
	assert.Contains(t, moveDDL[1], `"graph_branch"`)
	assert.Contains(t, moveDDL[2], `"graph_tree"`)
	ddl := replacementAwareDDL(statements)
	for _, unwanted := range []string{
		"DROP TABLE", "DROP INDEX", "DROP SEQUENCE", "DROP TRIGGER", "DROP POLICY", "DROP CONSTRAINT",
	} {
		assert.NotContains(t, ddl, unwanted)
	}
	assertSingleCleanupRootIntent(t, request, markerRoot(marker).CleanupTable)
}

func TestPartitionArchivalPostgresRejectsUnsupportedInheritance(t *testing.T) {
	factory := testdb.MustNewFactory(t)

	for _, testCase := range []struct {
		name       string
		ddl        string
		rootSchema string
		rootName   string
		contains   string
	}{
		{
			name: "traditional inheritance",
			ddl: `
				CREATE TABLE public.stage15_parent (id INTEGER);
				CREATE TABLE public.stage15_child () INHERITS (public.stage15_parent);
			`,
			rootSchema: "public", rootName: "stage15_parent", contains: "traditional_inheritance",
		},
		{
			name: "multiple inheritance",
			ddl: `
				CREATE TABLE public.stage15_parent_a (a INTEGER);
				CREATE TABLE public.stage15_parent_b (b INTEGER);
				CREATE TABLE public.stage15_child ()
					INHERITS (public.stage15_parent_a, public.stage15_parent_b);
			`,
			rootSchema: "public", rootName: "stage15_child", contains: "multiple_inheritance",
		},
		{
			name: "foreign table",
			ddl: `
				CREATE FOREIGN DATA WRAPPER stage15_fdw NO HANDLER;
				CREATE SERVER stage15_server FOREIGN DATA WRAPPER stage15_fdw;
				CREATE FOREIGN TABLE public.stage15_foreign (id INTEGER) SERVER stage15_server;
			`,
			rootSchema: "public", rootName: "stage15_foreign", contains: `classification "foreign"`,
		},
		{
			name: "foreign partition",
			ddl: `
				CREATE FOREIGN DATA WRAPPER stage15_fdw NO HANDLER;
				CREATE SERVER stage15_server FOREIGN DATA WRAPPER stage15_fdw;
				CREATE TABLE public.stage15_root (id INTEGER) PARTITION BY RANGE (id);
				CREATE FOREIGN TABLE public.stage15_foreign
					PARTITION OF public.stage15_root FOR VALUES FROM (0) TO (10)
					SERVER stage15_server;
			`,
			rootSchema: "public", rootName: "stage15_root", contains: "foreign_partition",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			db := factory.CreateDatabase(t)
			_, err := db.ConnPool.Exec(t.Context(), testCase.ddl)
			require.NoError(t, err)
			snapshot, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
			require.NoError(t, err)
			root := mustCatalogArchivalRelationByName(t, snapshot.Inventory,
				testCase.rootSchema, testCase.rootName)
			_, err = buildCompletePartitionTreeArchivalNameAllocationRequest(
				snapshot.Inventory, root.OID,
			)
			require.ErrorContains(t, err, testCase.contains)
		})
	}
}

func TestPartitionArchivalPostgresPartialResume(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	_, err := db.ConnPool.Exec(t.Context(), `
		CREATE TABLE public.resume_tree (id INTEGER, payload TEXT) PARTITION BY RANGE (id);
		CREATE TABLE public.resume_tree_a PARTITION OF public.resume_tree FOR VALUES FROM (0) TO (10);
		CREATE TABLE public.resume_tree_b PARTITION OF public.resume_tree FOR VALUES FROM (10) TO (20);
		INSERT INTO public.resume_tree VALUES (1, 'a'), (11, 'b');
	`)
	require.NoError(t, err)
	before, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
	require.NoError(t, err)
	root := mustCatalogRelationByName(t, before.Inventory, "public", "resume_tree")
	request, marker := partitionArchivalDatabaseRequest(t, before.Inventory, root, nil,
		"20260722T091011123456Z_QRSTUVWX")
	statements, err := generatePlainTableArchivalStatements(request)
	require.NoError(t, err)

	_, err = db.ConnPool.Exec(t.Context(), statements[0].ToSQL())
	require.NoError(t, err)
	moves := archivalMoveStatements(statements)
	require.Len(t, moves, 3)
	_, err = db.ConnPool.Exec(t.Context(), moves[0].ToSQL())
	require.NoError(t, err)

	partial, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
	require.NoError(t, err)
	resolution, err := resolveArchivedState("archive", partial, schema.SchemaSnapshot{})
	require.NoError(t, err)
	require.Len(t, resolution.CandidateGroups, 1)
	candidate := resolution.CandidateGroups[0]
	assert.Equal(t, archivedCandidateGroupStatePartialResumable, candidate.State)
	assert.Len(t, candidate.Resume.RemainingMemberMoves, 2)

	markerText, err := marshalArchivalMarker(marker)
	require.NoError(t, err)
	resumeRequest := plainTableArchivalRequest{
		CurrentInventory: partial.Inventory,
		Groups: []plainTableArchivalGroupRequest{{
			FinalizedMarker: markerText, Resume: &candidate.Resume,
		}},
		SourcePreflight: sourceSafetyPreflightResult{
			ValidatedTableRelationOIDs: plainMarkerRelationOIDs(marker),
		},
		DependencyClosure: archivedDependencyClosureResult{
			ValidatedGroupIDs: []archivalGroupID{marker.GroupID},
			DependencyValidatedCandidateGroups: []dependencyValidatedArchivedCandidateGroup{{
				Candidate: candidate,
			}},
		},
	}
	resumeStatements, err := generatePlainTableArchivalStatements(resumeRequest)
	require.NoError(t, err)
	assert.Len(t, archivalMoveStatements(resumeStatements), 2)
	for _, statement := range resumeStatements {
		_, err = db.ConnPool.Exec(t.Context(), statement.ToSQL())
		require.NoError(t, err, statement.DDL)
	}
}

func TestPartitionArchivalPostgresIsolationAcrossMembers(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	_, err := db.ConnPool.Exec(t.Context(), `
		CREATE TABLE public.stage15_reference (id INTEGER PRIMARY KEY);
		CREATE TABLE public.stage15_isolated (
			id INTEGER NOT NULL,
			ref_id INTEGER,
			payload TEXT
		) PARTITION BY RANGE (id);
		CREATE TABLE public.a_stage15_isolated
			PARTITION OF public.stage15_isolated FOR VALUES FROM (0) TO (100);
		CREATE TABLE public.b_stage15_isolated
			PARTITION OF public.stage15_isolated FOR VALUES FROM (100) TO (200);
		CREATE UNIQUE INDEX stage15_isolated_a_id_idx ON public.a_stage15_isolated (id);
		ALTER TABLE public.b_stage15_isolated ADD CONSTRAINT same_group_fk
			FOREIGN KEY (ref_id) REFERENCES public.a_stage15_isolated (id);
		ALTER TABLE public.a_stage15_isolated ADD CONSTRAINT boundary_fk
			FOREIGN KEY (ref_id) REFERENCES public.stage15_reference (id);
		GRANT SELECT ON public.b_stage15_isolated TO PUBLIC;
		CREATE PUBLICATION stage15_isolated_publication FOR TABLE public.a_stage15_isolated;
		CREATE STATISTICS public.stage15_isolated_b_stats ON ref_id, payload
			FROM public.b_stage15_isolated;
		INSERT INTO public.stage15_reference VALUES (1);
		INSERT INTO public.stage15_isolated VALUES (1, 1, 'a'), (101, 1, 'b');
	`)
	require.NoError(t, err)
	before, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
	require.NoError(t, err)
	root := mustCatalogRelationByName(t, before.Inventory, "public", "stage15_isolated")
	request, marker := partitionArchivalDatabaseRequest(t, before.Inventory, root, nil,
		"20260722T091011123456Z_IJKLMNOP")

	archivedOIDs := make(map[uint32]struct{}, len(marker.Members))
	for _, member := range marker.Members {
		archivedOIDs[member.SourceTable.OID] = struct{}{}
	}
	for _, foreignKey := range before.Inventory.ForeignKeys {
		_, owning := archivedOIDs[foreignKey.OwningRelationOID]
		_, referenced := archivedOIDs[foreignKey.ReferencedRelationOID]
		if !owning && !referenced {
			continue
		}
		direction, _ := classifySourceSafetyForeignKeyDirection(foreignKey,
			catalogRelationsByOID(before.Inventory, archivedOIDs))
		var triggerOIDs []uint32
		for _, trigger := range before.Inventory.Triggers {
			if trigger.ConstraintOID == foreignKey.OID {
				triggerOIDs = append(triggerOIDs, trigger.OID)
			}
		}
		request.SourcePreflight.ForeignKeys = append(request.SourcePreflight.ForeignKeys,
			sourceSafetyForeignKey{ForeignKey: foreignKey, Direction: direction, TriggerOIDs: triggerOIDs})
		if !owning || !referenced {
			marker.OriginalForeignKeys = append(marker.OriginalForeignKeys,
				archivalMarkerForeignKey(foreignKey))
		}
	}
	for _, publication := range before.Inventory.PublicationRelations {
		if _, archived := archivedOIDs[publication.RelationOID]; !archived {
			continue
		}
		request.SourcePreflight.PublicationRelations = append(
			request.SourcePreflight.PublicationRelations, publication,
		)
		marker.OriginalPublicationMemberships = append(marker.OriginalPublicationMemberships,
			archivalMarkerPublicationMembership(publication))
	}
	addresses := make([]schema.CatalogDependencyObject, 0, len(marker.Members))
	for _, member := range marker.Members {
		addresses = append(addresses, schema.CatalogDependencyObject{
			ClassOID: pgClassCatalogOID, ObjectOID: member.SourceTable.OID,
		})
	}
	revokes, err := before.Inventory.PlanACLRevokes(addresses)
	require.NoError(t, err)
	for _, revoke := range revokes.Revokes {
		record, err := archivalMarkerACLRecord(before.Inventory, revoke.Grant)
		require.NoError(t, err)
		marker.OriginalACLs = append(marker.OriginalACLs, record)
	}

	prepared := preparedArchivalGroup{id: marker.GroupID}
	for _, member := range marker.Members {
		prepared.members = append(prepared.members, preparedArchivalMember{marker: member})
	}
	groupByOID := archivalGroupByRelationOID([]preparedArchivalGroup{prepared})
	for idx := range marker.Members {
		member := &marker.Members[idx]
		move, err := before.Inventory.ExpectedTableMove(member.SourceTable.OID)
		require.NoError(t, err)
		move = filterPlainTableIsolationObjects(before.Inventory, move, request.SourcePreflight, groupByOID)
		member.AutomaticallyMovedObjects = markerObjectsFromCatalog(
			move.CleanupSchemaObjects, member.CleanupTable.SchemaName,
		)
		member.AttachedObjects = markerObjectsFromCatalog(move.AttachedObjects, member.CleanupTable.SchemaName)
	}
	markerText, err := marshalArchivalMarker(marker)
	require.NoError(t, err)
	request.Groups[0].FinalizedMarker = markerText

	statements, err := generatePlainTableArchivalStatements(request)
	require.NoError(t, err)
	assert.Contains(t, statementHazardTypes(
		findStatementContaining(t, statements, "DROP CONSTRAINT \"boundary_fk\""),
	),
		MigrationHazardTypeCorrectness)
	for _, statement := range statements {
		_, err = db.ConnPool.Exec(t.Context(), statement.ToSQL())
		require.NoError(t, err, statement.DDL)
	}
	after, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
	require.NoError(t, err)
	assert.True(t, slices.ContainsFunc(after.Inventory.ForeignKeys, func(
		foreignKey schema.CatalogForeignKey,
	) bool {
		return foreignKey.Name == "same_group_fk"
	}))
	assert.False(t, slices.ContainsFunc(after.Inventory.ForeignKeys, func(
		foreignKey schema.CatalogForeignKey,
	) bool {
		return foreignKey.Name == "boundary_fk"
	}))
	assert.Empty(t, after.Inventory.PublicationRelations)
	assert.False(t, slices.ContainsFunc(after.Inventory.ACLGrants, func(grant schema.CatalogACLGrant) bool {
		return grant.Object.ObjectOID == mustMarkerMemberByName(marker,
			"b_stage15_isolated").SourceTable.OID &&
			grant.GranteeIsPublic && grant.Privilege == "SELECT"
	}))
}

func TestPartitionArchivalRejectsInconsistentSubtreeIntent(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	_, err := db.ConnPool.Exec(t.Context(), `
		CREATE TABLE public.stage15_parent (id INTEGER) PARTITION BY RANGE (id);
		CREATE TABLE public.stage15_child PARTITION OF public.stage15_parent FOR VALUES FROM (0) TO (10);
	`)
	require.NoError(t, err)
	snapshot, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
	require.NoError(t, err)
	parent := mustCatalogRelationByName(t, snapshot.Inventory, "public", "stage15_parent")
	child := mustCatalogRelationByName(t, snapshot.Inventory, "public", "stage15_child")
	request, _ := partitionArchivalDatabaseRequest(t, snapshot.Inventory, child, &parent,
		"20260722T091011123456Z_YZABCDEF")
	request.Groups[0].DetachedSubtree.ParentRelationOID = child.OID
	_, err = generatePlainTableArchivalStatements(request)
	require.ErrorContains(t, err, "parent intent")
}

func partitionArchivalDatabaseRequest(
	t *testing.T,
	inventory schema.CatalogInventory,
	root schema.CatalogRelation,
	activeParent *schema.CatalogRelation,
	groupID archivalGroupID,
) (plainTableArchivalRequest, archivalMarkerV1) {
	t.Helper()
	timestamp, nonce, err := parseArchivalGroupID(groupID)
	require.NoError(t, err)
	nameRequest, err := buildCompletePartitionTreeArchivalNameAllocationRequest(inventory, root.OID)
	require.NoError(t, err)
	dependencySchema, err := buildArchivalDependencySchemaName("archive", timestamp, nonce)
	require.NoError(t, err)
	marker := archivalMarkerV1{
		Version: archivalMarkerVersion, GroupID: groupID,
		ExclusiveDependencySchemas: []archivalMarkerSchemaIdentity{{Name: dependencySchema}},
		CleanupDigest:              cleanupOperationDigest("sha256:" + strings.Repeat("0", 64)),
	}
	allocation := &archivalGroupNameAllocation{
		GroupID: groupID, Timestamp: timestamp, Nonce: nonce,
		DependencySchemaName:        dependencySchema,
		EscapedDependencySchemaName: schema.EscapeIdentifier(dependencySchema),
	}
	memberIDByOID := make(map[uint32]string, len(nameRequest.Members))
	for _, requested := range nameRequest.Members {
		relation := mustCatalogRelationByOID(t, inventory, requested.RelationOID)
		cleanupSchema, err := buildArchivalSchemaName(
			"archive", relation.SchemaName, relation.Name, timestamp, nonce,
		)
		require.NoError(t, err)
		move, err := inventory.ExpectedTableMove(relation.OID)
		require.NoError(t, err)
		memberID := fmt.Sprintf("member-%d", relation.OID)
		memberIDByOID[relation.OID] = memberID
		marker.Members = append(marker.Members, archivalMarkerMemberV1{
			MemberID: memberID,
			SourceTable: markerObject(relation.OID, archivalMarkerObjectKindTable,
				relation.SchemaName, relation.Name),
			CleanupTable: markerObject(relation.OID, archivalMarkerObjectKindTable,
				cleanupSchema, relation.Name),
			AutomaticallyMovedObjects: markerObjectsFromCatalog(move.CleanupSchemaObjects, cleanupSchema),
			AttachedObjects:           markerObjectsFromCatalog(move.AttachedObjects, cleanupSchema),
			ExplicitlyMovedObjects:    markerObjectsFromCatalog(move.ExplicitMoveObjects, cleanupSchema),
			InternalToastObjects:      markerObjectsFromCatalog(move.InternalObjects, ""),
		})
		allocation.Members = append(allocation.Members, archivalPhysicalMemberNameAllocation{
			RelationOID: relation.OID, SourceSchemaName: relation.SchemaName, SourceTableName: relation.Name,
			CleanupSchemaName: cleanupSchema, EscapedCleanupSchemaName: schema.EscapeIdentifier(cleanupSchema),
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

	var detachedRequest *archivalDetachedSubtreeRequest
	if activeParent != nil {
		group := preparedArchivalGroup{id: groupID, marker: marker}
		require.NoError(t, populatePreparedArchivalMembers(&group))
		parentIdentity := markerObject(activeParent.OID, archivalMarkerObjectKindTable,
			activeParent.SchemaName, activeParent.Name)
		lost, err := archivalLostParentAttachmentMetadata(inventory, group, markerRoot(marker), parentIdentity)
		require.NoError(t, err)
		marker.LostParentAttachments = []archivalMarkerLostParentAttachmentV1{lost}
		removeLostParentClonedTriggersFromMarker(&marker)
		detachedRequest = &archivalDetachedSubtreeRequest{
			RootRelationOID: root.OID, ParentRelationOID: activeParent.OID,
		}
	}
	markerText, err := marshalArchivalMarker(marker)
	require.NoError(t, err)
	oids := plainMarkerRelationOIDs(marker)
	return plainTableArchivalRequest{
		CurrentInventory: inventory,
		Groups: []plainTableArchivalGroupRequest{{
			Allocation: allocation, FinalizedMarker: markerText, DetachedSubtree: detachedRequest,
		}},
		SourcePreflight: sourceSafetyPreflightResult{ValidatedTableRelationOIDs: oids},
		DependencyClosure: archivedDependencyClosureResult{
			ValidatedGroupIDs: []archivalGroupID{groupID},
		},
	}, marker
}

func archivalMoveStatements(statements []Statement) []Statement {
	return slices.DeleteFunc(slices.Clone(statements), func(statement Statement) bool {
		return !strings.HasPrefix(statement.DDL, "ALTER TABLE") ||
			!strings.Contains(statement.DDL, " SET SCHEMA ")
	})
}

func archivalMoveDDL(statements []Statement) []string {
	moves := archivalMoveStatements(statements)
	result := make([]string, 0, len(moves))
	for _, move := range moves {
		result = append(result, move.DDL)
	}
	return result
}

func markerRoot(marker archivalMarkerV1) archivalMarkerMemberV1 {
	children := make(map[string]struct{}, len(marker.PartitionEdges))
	for _, edge := range marker.PartitionEdges {
		children[edge.ChildMemberID] = struct{}{}
	}
	for _, member := range marker.Members {
		if _, child := children[member.MemberID]; !child {
			return member
		}
	}
	return archivalMarkerMemberV1{}
}

func preparedCleanupRoot(t *testing.T, request plainTableArchivalRequest) archivalMarkerObjectIdentity {
	t.Helper()
	groups, err := preparePlainTableArchivalGroups(request)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	return groups[0].cleanupRoot
}

func assertSingleCleanupRootIntent(
	t *testing.T,
	request plainTableArchivalRequest,
	expected archivalMarkerObjectIdentity,
) {
	t.Helper()
	groups, err := preparePlainTableArchivalGroups(request)
	require.NoError(t, err)
	intents := archivalCleanupRootIntents(groups)
	require.Len(t, intents, 1)
	assert.Equal(t, cloneMarkerObject(expected), intents[0].Root)
}

func statementHazardTypes(statement Statement) []MigrationHazardType {
	result := make([]MigrationHazardType, 0, len(statement.Hazards))
	for _, hazard := range statement.Hazards {
		result = append(result, hazard.Type)
	}
	return result
}

func plainMarkerRelationOIDs(marker archivalMarkerV1) []uint32 {
	result := make([]uint32, 0, len(marker.Members))
	for _, member := range marker.Members {
		result = append(result, member.SourceTable.OID)
	}
	slices.Sort(result)
	return result
}

func assertPartitionMarkerMetadataPreserved(
	t *testing.T,
	inventory schema.CatalogInventory,
	marker archivalMarkerV1,
) {
	t.Helper()
	for _, edge := range marker.PartitionEdges {
		parent := markerMemberByID(marker, edge.ParentMemberID)
		child := markerMemberByID(marker, edge.ChildMemberID)
		attachment, err := uniquePartitionAttachment(inventory,
			parent.SourceTable.OID, child.SourceTable.OID)
		require.NoError(t, err)
		assert.Equal(t, edge.BoundExpression, attachment.BoundExpression)
		for _, index := range edge.PartitionedIndexAttachments {
			assert.True(t, isCatalogInheritanceEdge(inventory,
				index.ParentIndex.OID, index.ChildIndex.OID))
		}
		for _, trigger := range edge.ClonedTriggers {
			actual := catalogTriggerByOID(inventory, trigger.ChildTrigger.OID)
			assert.Equal(t, trigger.ParentTrigger.OID, actual.ParentTriggerOID)
		}
	}
}

func assertDetachedPartitionMemberIdentities(
	t *testing.T,
	before schema.CatalogInventory,
	after schema.CatalogInventory,
	member archivalMarkerMemberV1,
) {
	t.Helper()
	beforeMove, err := before.ExpectedTableMove(member.SourceTable.OID)
	require.NoError(t, err)
	afterMove, err := after.ExpectedTableMove(member.SourceTable.OID)
	require.NoError(t, err)
	assert.NoError(t, validateExactMarkerObjectSet("automatically moved",
		member.AutomaticallyMovedObjects,
		markerObjectsFromCatalog(afterMove.CleanupSchemaObjects, member.CleanupTable.SchemaName)))
	assert.NoError(t, validateExactMarkerObjectSet("attached", member.AttachedObjects,
		markerObjectsFromCatalog(afterMove.AttachedObjects, member.CleanupTable.SchemaName)))
	assert.NoError(t, validateExactMarkerObjectSet("TOAST", member.InternalToastObjects,
		markerObjectsFromCatalog(afterMove.InternalObjects, "")))
	assert.Equal(t, catalogObjectOIDs(beforeMove.CleanupSchemaObjects),
		catalogObjectOIDs(afterMove.CleanupSchemaObjects))
	assert.Equal(t, catalogObjectOIDs(beforeMove.InternalObjects),
		catalogObjectOIDs(afterMove.InternalObjects))
}

func markerMemberByID(marker archivalMarkerV1, memberID string) archivalMarkerMemberV1 {
	for _, member := range marker.Members {
		if member.MemberID == memberID {
			return member
		}
	}
	return archivalMarkerMemberV1{}
}

func catalogTriggerByOID(inventory schema.CatalogInventory, oid uint32) schema.CatalogTrigger {
	for _, trigger := range inventory.Triggers {
		if trigger.OID == oid {
			return trigger
		}
	}
	return schema.CatalogTrigger{}
}

func catalogRelationsByOID(
	inventory schema.CatalogInventory,
	oids map[uint32]struct{},
) map[uint32]schema.CatalogRelation {
	result := make(map[uint32]schema.CatalogRelation, len(oids))
	for _, relation := range inventory.Relations {
		if _, ok := oids[relation.OID]; ok {
			result[relation.OID] = relation
		}
	}
	return result
}

func findStatementContaining(t *testing.T, statements []Statement, fragment string) Statement {
	t.Helper()
	for _, statement := range statements {
		if strings.Contains(statement.DDL, fragment) {
			return statement
		}
	}
	require.FailNow(t, "statement not found", fragment)
	return Statement{}
}

func mustMarkerMemberByName(marker archivalMarkerV1, name string) archivalMarkerMemberV1 {
	for _, member := range marker.Members {
		if member.SourceTable.Name == name {
			return member
		}
	}
	return archivalMarkerMemberV1{}
}

func mustCatalogArchivalRelationByName(
	t *testing.T,
	inventory schema.CatalogInventory,
	schemaName string,
	name string,
) schema.CatalogRelation {
	t.Helper()
	for _, relation := range inventory.Relations {
		if relation.SchemaName == schemaName && relation.Name == name {
			return relation
		}
	}
	require.FailNow(t, "catalog archival relation not found", "relation: %s.%s", schemaName, name)
	return schema.CatalogRelation{}
}
