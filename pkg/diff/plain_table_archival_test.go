package diff

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

func TestPlainTableArchivalGraphDeterministicPhaseOrdering(t *testing.T) {
	t.Parallel()

	first := plainTableArchivalUnitRequest(t, "20260721T091011123456Z_ABCDEFGH", 20, "z_source", "z_table")
	second := plainTableArchivalUnitRequest(t, "20260721T091011123456Z_12345678", 10, "a_source", "a_table")
	request := mergePlainTableArchivalUnitRequests(first, second)

	groups, err := preparePlainTableArchivalGroups(request)
	require.NoError(t, err)
	isolation, err := planArchivalIsolation(request.CurrentInventory, request.TargetSchema,
		request.SourcePreflight, request.DependencyClosure, groups)
	require.NoError(t, err)
	graph, err := buildPlainTableArchivalGraph(request.CurrentInventory, groups, isolation)
	require.NoError(t, err)
	vertices, err := graph.TopologicallySortWithPriority(func(a, b sqlVertex) bool {
		return a.GetPriority() < b.GetPriority()
	})
	require.NoError(t, err)
	var vertexIDs []string
	for _, vertex := range vertices {
		if _, barrier := vertex.id.(plainTableArchivalPhaseBarrierID); barrier {
			continue
		}
		vertexIDs = append(vertexIDs, vertex.GetId())
	}
	assert.Equal(t, []string{
		"archival:01:20260721T091011123456Z_12345678:group_initialization",
		"archival:01:20260721T091011123456Z_ABCDEFGH:group_initialization",
		"archival:02:20260721T091011123456Z_12345678:marker_refresh",
		"archival:02:20260721T091011123456Z_ABCDEFGH:marker_refresh",
		"archival:04:20260721T091011123456Z_12345678:table_move",
		"archival:04:20260721T091011123456Z_ABCDEFGH:table_move",
		"archival:08:20260721T091011123456Z_12345678:catalog_assertion",
		"archival:08:20260721T091011123456Z_ABCDEFGH:catalog_assertion",
	}, vertexIDs)

	statements, err := generatePlainTableArchivalStatements(request)
	require.NoError(t, err)
	require.Len(t, statements, 6)
	assert.Contains(t, statements[0].DDL, second.Groups[0].Allocation.DependencySchemaName)
	assert.Contains(t, statements[1].DDL, first.Groups[0].Allocation.DependencySchemaName)
	assert.Equal(t, fmt.Sprintf("ALTER TABLE %s.%s SET SCHEMA %s",
		schema.EscapeIdentifier("a_source"), schema.EscapeIdentifier("a_table"),
		second.Groups[0].Allocation.Members[0].EscapedCleanupSchemaName), statements[2].DDL)
	assert.Equal(t, fmt.Sprintf("ALTER TABLE %s.%s SET SCHEMA %s",
		schema.EscapeIdentifier("z_source"), schema.EscapeIdentifier("z_table"),
		first.Groups[0].Allocation.Members[0].EscapedCleanupSchemaName), statements[3].DDL)
	assert.Contains(t, statements[4].DDL, "archival marker mismatch")
	assert.Contains(t, statements[5].DDL, "archival marker mismatch")
}

func TestPlainTableArchivalInitializationQuotesIdentifiersAndMarker(t *testing.T) {
	t.Parallel()

	allocation := &archivalGroupNameAllocation{}
	statements := renderPlainTableArchivalInitialization(preparedArchivalGroup{
		id: "group", markerText: "marker's finalized payload", allocation: allocation,
		marker: archivalMarkerV1{
			Members: []archivalMarkerMemberV1{{CleanupTable: archivalMarkerObjectIdentity{
				SchemaName: `archive"member`,
			}}},
			ExclusiveDependencySchemas: []archivalMarkerSchemaIdentity{{Name: `archive"dependency`}},
		},
	})
	require.Len(t, statements, 1)
	assert.Contains(t, statements[0].DDL, `CREATE SCHEMA "archive""dependency"`)
	assert.Contains(t, statements[0].DDL, `CREATE SCHEMA "archive""member"`)
	assert.Contains(t, statements[0].DDL, `IS 'marker''s finalized payload'`)
	assert.NotContains(t, statements[0].DDL, "IF NOT EXISTS")
	assert.Equal(t, []MigrationHazard{migrationHazardArchivalSchemaLockdown}, statements[0].Hazards)
}

func TestPlainTableArchivalResumeReusesValidatedSchemas(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		tableMoved bool
	}{
		{name: "empty initialized"},
		{name: "partial marker refresh", tableMoved: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			request := plainTableArchivalUnitRequest(
				t, "20260721T091011123456Z_ABCDEFGH", 10, "source", "accounts",
			)
			allocation := request.Groups[0].Allocation
			oldMarkerText := request.Groups[0].FinalizedMarker
			oldMarker, err := parseArchivalMarker(oldMarkerText)
			require.NoError(t, err)
			schemaNames := archivedMarkerSchemaNames(oldMarker)
			request.CurrentInventory.Schemas = append(
				request.CurrentInventory.Schemas,
				schema.CatalogSchema{OID: 2, Name: schemaNames[0], Comment: oldMarkerText},
				schema.CatalogSchema{OID: 3, Name: schemaNames[1], Comment: oldMarkerText},
			)
			if tc.tableMoved {
				movePlainTableUnitInventory(&request.CurrentInventory,
					oldMarker.Members[0].CleanupTable.SchemaName, 2)
			}
			resume := archivedGroupResumeDescriptor{
				GroupID: oldMarker.GroupID,
				RemainingMarkerUpdates: []archivedMarkerUpdateDescriptor{
					{SchemaName: schemaNames[0], Marker: oldMarkerText},
					{SchemaName: schemaNames[1], Marker: oldMarkerText},
				},
			}
			if !tc.tableMoved {
				resume.RemainingMemberMoves = []archivedMemberMoveDescriptor{{
					MemberID:         oldMarker.Members[0].MemberID,
					RelationOID:      oldMarker.Members[0].SourceTable.OID,
					SourceTable:      oldMarker.Members[0].SourceTable,
					DestinationTable: oldMarker.Members[0].CleanupTable,
				}}
			}
			candidate := structurallyValidArchivedCandidateGroup{
				GroupID: oldMarker.GroupID, State: archivedCandidateGroupStateEmptyInitialized,
				Marker: oldMarker, SchemaNames: schemaNames,
				ExpectedDependencySchemaName: allocation.DependencySchemaName, Resume: resume,
			}
			if tc.tableMoved {
				candidate.State = archivedCandidateGroupStatePartialResumable
			}
			newMarker := oldMarker
			newMarker.CleanupDigest = cleanupOperationDigest("sha256:" + strings.Repeat("1", 64))
			newMarkerText, err := marshalArchivalMarker(newMarker)
			require.NoError(t, err)
			request.Groups[0] = plainTableArchivalGroupRequest{
				FinalizedMarker: newMarkerText, Resume: &resume,
			}
			request.DependencyClosure.DependencyValidatedCandidateGroups = []dependencyValidatedArchivedCandidateGroup{
				{Candidate: candidate},
			}

			statements, err := generatePlainTableArchivalStatements(request)
			require.NoError(t, err)
			for _, statement := range statements {
				assert.NotContains(t, statement.DDL, "CREATE SCHEMA")
			}
			if tc.tableMoved {
				require.Len(t, statements, 2)
				assert.NotContains(t, statements[0].DDL, "ALTER TABLE")
			} else {
				require.Len(t, statements, 3)
				assert.Contains(t, statements[1].DDL, "ALTER TABLE")
			}
			assert.Contains(t, statements[0].DDL, newMarkerText)
			assert.Contains(t, statements[len(statements)-1].DDL, "archival marker mismatch")
		})
	}
}

func TestPlainTableArchivalFailsClosedForLaterStageCases(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		mutate func(*plainTableArchivalRequest)
	}{
		{name: "declarative partition", mutate: func(r *plainTableArchivalRequest) {
			r.CurrentInventory.Relations[0].IsPartition = true
		}},
		{name: "traditional inheritance", mutate: func(r *plainTableArchivalRequest) {
			r.CurrentInventory.InheritanceEdges = []schema.CatalogInheritanceEdge{{
				ParentRelationOID: 90, ChildRelationOID: 10,
			}}
		}},
		{name: "multiple inheritance", mutate: func(r *plainTableArchivalRequest) {
			r.CurrentInventory.InheritanceEdges = []schema.CatalogInheritanceEdge{
				{ParentRelationOID: 90, ChildRelationOID: 10},
				{ParentRelationOID: 91, ChildRelationOID: 10},
			}
		}},
		{name: "foreign table", mutate: func(r *plainTableArchivalRequest) {
			r.CurrentInventory.Relations[0].Kind = schema.RelKindForeignTable
		}},
		{name: "incoming dependency", mutate: func(r *plainTableArchivalRequest) {
			r.SourcePreflight.IncomingDependencies = []sourceSafetyIncomingDependency{{}}
		}},
		{name: "cross boundary foreign key", mutate: func(r *plainTableArchivalRequest) {
			r.SourcePreflight.ForeignKeys = []sourceSafetyForeignKey{{
				Direction: sourceSafetyForeignKeyDirectionIncoming,
			}}
		}},
		{name: "explicit publication", mutate: func(r *plainTableArchivalRequest) {
			r.SourcePreflight.PublicationRelations = []schema.CatalogPublicationRelation{{OID: 1}}
		}},
		{name: "schema publication", mutate: func(r *plainTableArchivalRequest) {
			r.SourcePreflight.PublicationSchemas = []schema.CatalogPublicationSchema{{OID: 1}}
		}},
		{name: "all tables publication", mutate: func(r *plainTableArchivalRequest) {
			r.CurrentInventory.Publications = []schema.CatalogPublication{{
				Name: "all_tables", PublishesAllTables: true,
			}}
		}},
		{name: "extended statistics", mutate: func(r *plainTableArchivalRequest) {
			r.CurrentInventory.ExtendedStatistics = []schema.CatalogExtendedStatistic{{
				OID: 80, SchemaName: "source", Name: "accounts_stats", RelationOID: 10,
			}}
		}},
		{name: "dependency assignment", mutate: func(r *plainTableArchivalRequest) {
			r.DependencyClosure.Assignments = []archivedDependencyAssignment{{
				GroupID: r.DependencyClosure.ValidatedGroupIDs[0],
			}}
		}},
		{name: "shared dependency edge", mutate: func(r *plainTableArchivalRequest) {
			r.DependencyClosure.SharedGroupEdges = []archivalMarkerSharedGroupEdgeV1{{
				FirstGroupID: r.DependencyClosure.ValidatedGroupIDs[0], SecondGroupID: "other",
			}}
		}},
		{name: "exclusive dependency classification", mutate: func(r *plainTableArchivalRequest) {
			r.DependencyClosure.Objects = []archivedDependencyClosureObject{{
				Identity:       archivalMarkerObjectIdentity{Name: "dependency"},
				Classification: archivedDependencyClassificationExclusiveMovable,
			}}
		}},
		{name: "enabled event trigger", mutate: func(r *plainTableArchivalRequest) {
			r.CurrentInventory.EventTriggers = []schema.CatalogEventTrigger{{
				Name: "ddl_trigger", EnabledMode: "O",
			}}
		}},
		{name: "extension member", mutate: func(r *plainTableArchivalRequest) {
			r.CurrentInventory.ExtensionMembers = []schema.CatalogExtensionMember{{
				ExtensionName: "unsafe", Object: schema.CatalogDependencyObject{
					ClassOID: pgClassCatalogOID, ObjectOID: 10, ObjectType: "table",
				},
			}}
		}},
		{name: "missing preflight validation", mutate: func(r *plainTableArchivalRequest) {
			r.SourcePreflight.ValidatedTableRelationOIDs = nil
		}},
		{name: "missing dependency closure validation", mutate: func(r *plainTableArchivalRequest) {
			r.DependencyClosure.ValidatedGroupIDs = nil
		}},
		{name: "ACL revoke marker", mutate: func(r *plainTableArchivalRequest) {
			marker, err := parseArchivalMarker(r.Groups[0].FinalizedMarker)
			require.NoError(t, err)
			marker.OriginalACLs = []archivalMarkerACLRecordV1{{
				ObjectClass: "table", Object: marker.Members[0].SourceTable,
				OwnerName: "owner", GrantorName: "owner", GranteeName: "reader", Privilege: "SELECT",
			}}
			r.Groups[0].FinalizedMarker, err = marshalArchivalMarker(marker)
			require.NoError(t, err)
		}},
		{name: "ACL revoke inventory", mutate: func(r *plainTableArchivalRequest) {
			r.CurrentInventory.Roles = []schema.CatalogRole{{OID: 1, Name: "owner"}}
			r.CurrentInventory.ACLGrants = []schema.CatalogACLGrant{{
				ObjectClass: schema.CatalogACLObjectClassTable,
				Object: schema.CatalogDependencyObject{
					ClassOID: pgClassCatalogOID, ObjectOID: 10,
				},
				OwnerOID: 1, OwnerName: "owner", GrantorOID: 1, GrantorName: "owner",
				GranteeName: schema.CatalogPublicRoleName, GranteeIsPublic: true, Privilege: "SELECT",
			}}
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			request := plainTableArchivalUnitRequest(
				t, "20260721T091011123456Z_ABCDEFGH", 10, "source", "accounts",
			)
			tc.mutate(&request)
			_, err := generatePlainTableArchivalStatements(request)
			require.Error(t, err)
		})
	}
}

func TestPlainTableArchivalSupportsLeaflessPartitionedRoot(t *testing.T) {
	t.Parallel()

	request := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_ABCDEFGH", 10, "source", "accounts",
	)
	request.CurrentInventory.Relations[0].Kind = schema.RelKindPartitionedTable

	statements, err := generatePlainTableArchivalStatements(request)
	require.NoError(t, err)
	assert.Contains(t, replacementAwareDDL(statements),
		`ALTER TABLE "source"."accounts" SET SCHEMA`)
}

func TestPlainTableArchivalStageValidationProofs(t *testing.T) {
	t.Parallel()

	current, target := sourceSafetyBaseSnapshots()
	preflight, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		ProposedTableRelationOIDs: []uint32{10},
	})
	require.NoError(t, err)
	assert.Equal(t, []uint32{10}, preflight.ValidatedTableRelationOIDs)

	closure, err := planArchivedDependencyClosure(archivedDependencyClosureRequest{
		CurrentSnapshot: current, TargetSnapshot: target, SourcePreflight: preflight,
		ProposedGroups: []archivedDependencyClosureGroupRequest{{
			GroupID: "proof-group", TableRelationOIDs: []uint32{10},
			DependencySchemaName: "proof_dependencies",
		}},
	})
	require.NoError(t, err)
	assert.Equal(t, []archivalGroupID{"proof-group"}, closure.ValidatedGroupIDs)
}

func TestPlainTableArchivalStatementsAreNonDestructive(t *testing.T) {
	t.Parallel()

	request := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_ABCDEFGH", 10, "source", "accounts",
	)
	statements, err := generatePlainTableArchivalStatements(request)
	require.NoError(t, err)
	for _, statement := range statements {
		upperDDL := strings.ToUpper(statement.DDL)
		assert.NotContains(t, upperDDL, "DROP TABLE")
		assert.NotContains(t, upperDDL, "TRUNCATE")
		assert.NotContains(t, upperDDL, "DELETE FROM")
		for _, hazard := range statement.Hazards {
			assert.NotEqual(t, MigrationHazardTypeDeletesData, hazard.Type)
			assert.NotEqual(t, MigrationHazardType("TABLE_REMOVAL"), hazard.Type)
		}
	}
	require.Len(t, statements[1].Hazards, 1)
	assert.Equal(t, MigrationHazardTypeAcquiresAccessExclusiveLock, statements[1].Hazards[0].Type)
}

func plainTableArchivalUnitRequest(
	t *testing.T,
	groupID archivalGroupID,
	relationOID uint32,
	sourceSchema string,
	tableName string,
) plainTableArchivalRequest {
	t.Helper()
	timestamp, nonce, err := parseArchivalGroupID(groupID)
	require.NoError(t, err)
	cleanupSchema, err := buildArchivalSchemaName("archive", sourceSchema, tableName, timestamp, nonce)
	require.NoError(t, err)
	dependencySchema, err := buildArchivalDependencySchemaName("archive", timestamp, nonce)
	require.NoError(t, err)
	inventory := schema.CatalogInventory{
		Schemas: []schema.CatalogSchema{{OID: 1, Name: sourceSchema}},
		Relations: []schema.CatalogRelation{{
			OID: relationOID, SchemaOID: 1, SchemaName: sourceSchema, Name: tableName,
			Kind: schema.RelKindOrdinaryTable,
		}},
		Tables: []schema.CatalogTable{{RelationOID: relationOID}},
	}
	move, err := inventory.ExpectedTableMove(relationOID)
	require.NoError(t, err)
	member := archivalMarkerMemberV1{
		MemberID: fmt.Sprintf("member-%d", relationOID),
		SourceTable: markerObject(
			relationOID, archivalMarkerObjectKindTable, sourceSchema, tableName,
		),
		CleanupTable: markerObject(
			relationOID, archivalMarkerObjectKindTable, cleanupSchema, tableName,
		),
		AutomaticallyMovedObjects: markerObjectsFromCatalog(move.CleanupSchemaObjects, cleanupSchema),
		AttachedObjects:           markerObjectsFromCatalog(move.AttachedObjects, cleanupSchema),
		ExplicitlyMovedObjects:    markerObjectsFromCatalog(move.ExplicitMoveObjects, cleanupSchema),
		InternalToastObjects:      markerObjectsFromCatalog(move.InternalObjects, ""),
	}
	marker := archivalMarkerV1{
		Version: archivalMarkerVersion, GroupID: groupID, Members: []archivalMarkerMemberV1{member},
		ExclusiveDependencySchemas: []archivalMarkerSchemaIdentity{{Name: dependencySchema}},
		CleanupDigest:              cleanupOperationDigest("sha256:" + strings.Repeat("0", 64)),
	}
	markerText, err := marshalArchivalMarker(marker)
	require.NoError(t, err)
	allocation := &archivalGroupNameAllocation{
		GroupID: groupID, Nonce: nonce, Timestamp: timestamp,
		DependencySchemaName: dependencySchema, EscapedDependencySchemaName: schema.EscapeIdentifier(dependencySchema),
		Members: []archivalPhysicalMemberNameAllocation{{
			RelationOID: relationOID, SourceSchemaName: sourceSchema, SourceTableName: tableName,
			CleanupSchemaName: cleanupSchema, EscapedCleanupSchemaName: schema.EscapeIdentifier(cleanupSchema),
		}},
	}
	return plainTableArchivalRequest{
		CurrentInventory: inventory,
		Groups: []plainTableArchivalGroupRequest{{
			Allocation: allocation, FinalizedMarker: markerText,
		}},
		SourcePreflight: sourceSafetyPreflightResult{ValidatedTableRelationOIDs: []uint32{relationOID}},
		DependencyClosure: archivedDependencyClosureResult{
			ValidatedGroupIDs: []archivalGroupID{groupID},
		},
	}
}

func mergePlainTableArchivalUnitRequests(
	requests ...plainTableArchivalRequest,
) plainTableArchivalRequest {
	var result plainTableArchivalRequest
	for _, request := range requests {
		result.CurrentInventory.Schemas = append(result.CurrentInventory.Schemas,
			request.CurrentInventory.Schemas...)
		result.CurrentInventory.Relations = append(result.CurrentInventory.Relations,
			request.CurrentInventory.Relations...)
		result.CurrentInventory.Tables = append(result.CurrentInventory.Tables,
			request.CurrentInventory.Tables...)
		result.Groups = append(result.Groups, request.Groups...)
		result.SourcePreflight.ValidatedTableRelationOIDs = append(
			result.SourcePreflight.ValidatedTableRelationOIDs,
			request.SourcePreflight.ValidatedTableRelationOIDs...,
		)
		result.DependencyClosure.ValidatedGroupIDs = append(
			result.DependencyClosure.ValidatedGroupIDs,
			request.DependencyClosure.ValidatedGroupIDs...,
		)
	}
	return result
}

func movePlainTableUnitInventory(inventory *schema.CatalogInventory, destination string, schemaOID uint32) {
	for idx := range inventory.Relations {
		inventory.Relations[idx].SchemaName = destination
		inventory.Relations[idx].SchemaOID = schemaOID
	}
	for idx := range inventory.Types {
		inventory.Types[idx].SchemaName = destination
		inventory.Types[idx].SchemaOID = schemaOID
	}
	for idx := range inventory.Indexes {
		inventory.Indexes[idx].SchemaName = destination
		inventory.Indexes[idx].SchemaOID = schemaOID
	}
	for idx := range inventory.Constraints {
		inventory.Constraints[idx].SchemaName = destination
		inventory.Constraints[idx].SchemaOID = schemaOID
	}
	for idx := range inventory.Sequences {
		inventory.Sequences[idx].SchemaName = destination
		inventory.Sequences[idx].SchemaOID = schemaOID
	}
}
