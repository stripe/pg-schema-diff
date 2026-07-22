package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/internal/testdb"
)

func TestGlobalCleanupPostgresDropsRetainedStateAndLeavesManagedObjects(t *testing.T) {
	t.Parallel()

	factory := testdb.MustNewFactory(t)
	currentDB := factory.CreateDatabase(t)
	_, err := currentDB.ConnPool.Exec(t.Context(), `
		CREATE SCHEMA managed;
		CREATE SCHEMA deps;
		CREATE SEQUENCE deps.archived_numbers AS bigint START 40;
		CREATE TABLE managed.archived (
			id bigint PRIMARY KEY,
			number bigint NOT NULL DEFAULT nextval('deps.archived_numbers'),
			payload text NOT NULL
		);
		CREATE INDEX archived_payload_idx ON managed.archived (payload);
		CREATE TABLE managed.keep (id bigint PRIMARY KEY, payload text NOT NULL);
		INSERT INTO managed.archived (id, payload) VALUES (1, 'retained data');
		INSERT INTO managed.keep VALUES (1, 'managed data');
	`)
	require.NoError(t, err)

	targetDB := factory.CreateDatabase(t)
	_, err = targetDB.ConnPool.Exec(t.Context(), `
		CREATE SCHEMA managed;
		CREATE SCHEMA deps;
		CREATE TABLE managed.keep (id bigint PRIMARY KEY, payload text NOT NULL);
	`)
	require.NoError(t, err)

	current, err := schema.GetSchemaSnapshot(t.Context(), currentDB.ConnPool)
	require.NoError(t, err)
	target, err := schema.GetSchemaSnapshot(t.Context(), targetDB.ConnPool)
	require.NoError(t, err)
	archived := mustCatalogRelationByName(t, current.Inventory, "managed", "archived")
	groupID := archivalGroupID("20260721T091011123456Z_AAAAAAAA")
	regularRequest, marker := plainTableArchivalDatabaseRequest(
		t, current.Inventory, archived, groupID,
	)
	preflight, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		ProposedTableRelationOIDs: []uint32{archived.OID},
	})
	require.NoError(t, err)
	closure, err := planArchivedDependencyClosure(archivedDependencyClosureRequest{
		CurrentSnapshot: current, TargetSnapshot: target, SourcePreflight: preflight,
		ProposedGroups: []archivedDependencyClosureGroupRequest{{
			GroupID: groupID, TableRelationOIDs: []uint32{archived.OID},
			DependencySchemaName: regularRequest.Groups[0].Allocation.DependencySchemaName,
		}},
	})
	require.NoError(t, err)
	require.Len(t, closure.Assignments, 1)
	assert.Equal(t, archivalMarkerObjectKindSequence, closure.Assignments[0].Source.Kind)

	marker.ExclusiveDependencyObjects = []archivalMarkerObjectIdentity{
		closure.Assignments[0].Destination,
	}
	marker.SharedCleanupComponentGroupEdges = incidentArchivedDependencyEdges(
		groupID, closure.SharedGroupEdges,
	)
	regularRequest.Groups[0].FinalizedMarker, err = marshalArchivalMarker(marker)
	require.NoError(t, err)
	regularRequest.TargetSchema = target.Schema
	regularRequest.SourcePreflight = preflight
	regularRequest.DependencyClosure = closure
	groups, err := preparePlainTableArchivalGroups(regularRequest)
	require.NoError(t, err)
	groups, err = finalizeGlobalCleanupComponentInputs(groups, closure)
	require.NoError(t, err)
	isolation, err := planArchivalIsolation(
		current.Inventory, target.Schema, preflight, closure, groups,
	)
	require.NoError(t, err)

	cleanup, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: current.Inventory, PredictedGroups: groups,
		RootIntents: archivalCleanupRootIntents(groups), DependencyClosure: closure,
		Isolation: isolation,
	})
	require.NoError(t, err)
	require.Len(t, cleanup.FinalizedMarkers, 1)
	regularRequest.Groups[0].FinalizedMarker = cleanup.FinalizedMarkers[0].Text
	regularStatements, err := generatePlainTableArchivalStatements(regularRequest)
	require.NoError(t, err)
	for _, statement := range regularStatements {
		_, err = currentDB.ConnPool.Exec(t.Context(), statement.ToSQL())
		require.NoError(t, err, statement.DDL)
	}

	cleanupSchema := marker.Members[0].CleanupTable.SchemaName
	dependencySchema := marker.ExclusiveDependencySchemas[0].Name
	assert.Equal(t, archived.OID,
		postgresRelationOID(t, currentDB.ConnPool, cleanupSchema, "archived"))
	var retainedPayload string
	err = currentDB.ConnPool.QueryRow(
		t.Context(),
		"SELECT payload FROM "+schema.EscapeIdentifier(cleanupSchema)+".archived WHERE id = 1",
	).Scan(&retainedPayload)
	require.NoError(t, err)
	assert.Equal(t, "retained data", retainedPayload)
	assert.NotZero(t, postgresRelationOID(t, currentDB.ConnPool, dependencySchema, "archived_numbers"))

	for _, statement := range cleanup.CleanupStatements {
		_, err = currentDB.ConnPool.Exec(t.Context(), statement.ToSQL())
		require.NoError(t, err, statement.DDL)
	}
	assert.False(t, postgresSchemaExists(t, currentDB.ConnPool, cleanupSchema))
	assert.False(t, postgresSchemaExists(t, currentDB.ConnPool, dependencySchema))
	assert.Zero(t, postgresRelationOID(t, currentDB.ConnPool, "deps", "archived_numbers"))
	assert.NotZero(t, postgresRelationOID(t, currentDB.ConnPool, "managed", "keep"))
	var managedPayload string
	err = currentDB.ConnPool.QueryRow(
		t.Context(),
		"SELECT payload FROM managed.keep WHERE id = 1",
	).Scan(&managedPayload)
	require.NoError(t, err)
	assert.Equal(t, "managed data", managedPayload)

	afterCleanup, err := schema.GetSchemaSnapshot(t.Context(), currentDB.ConnPool)
	require.NoError(t, err)
	resolution, err := resolveArchivedState("archive", afterCleanup, target)
	require.NoError(t, err)
	assert.Empty(t, resolution.CandidateGroups)
	empty, err := planGlobalCleanup(globalCleanupPlanRequest{})
	require.NoError(t, err)
	assert.Empty(t, empty.Operations)
	assert.Empty(t, empty.CleanupStatements)
}
