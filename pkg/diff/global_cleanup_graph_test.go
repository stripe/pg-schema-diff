package diff

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

func TestPlanGlobalCleanupDeterministicOperationsMarkersAndRendering(t *testing.T) {
	t.Parallel()

	first := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_AAAAAAAA", 10, "source", "accounts",
	)
	second := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_BBBBBBBB", 20, "source", "orders",
	)
	request := mergePlainTableArchivalUnitRequests(first, second)
	groups, err := preparePlainTableArchivalGroups(request)
	require.NoError(t, err)
	groupsBefore := make([]preparedArchivalGroup, 0, len(groups))
	for _, group := range groups {
		groupsBefore = append(groupsBefore, clonePreparedArchivalGroup(group))
	}
	planRequest := globalCleanupPlanRequest{
		CurrentInventory: request.CurrentInventory, PredictedGroups: groups,
		RootIntents: archivalCleanupRootIntents(groups), DependencyClosure: request.DependencyClosure,
	}

	result, err := planGlobalCleanup(planRequest)
	require.NoError(t, err)
	assert.Equal(t, cleanupOperationDigest(
		"sha256:75963c563a4699ca99b9ccb9529ca7faf282e2054ca3635a9ce4432e1f2f8ed6",
	),
		result.FinalizedMarkers[0].Payload.CleanupDigest)
	require.Len(t, result.Operations, 6)
	assert.Equal(t, []cleanupOperationKind{
		cleanupOperationKindDropTable, cleanupOperationKindDropTable,
		cleanupOperationKindDropSchema, cleanupOperationKindDropSchema,
		cleanupOperationKindDropSchema, cleanupOperationKindDropSchema,
	}, cleanupOperationKinds(result.Operations))
	for _, statement := range result.CleanupStatements {
		assert.Contains(t, statement.DDL, " RESTRICT")
		assert.NotContains(t, statement.DDL, "CASCADE")
	}

	reversed := planRequest
	reversed.PredictedGroups = slices.Clone(planRequest.PredictedGroups)
	reversed.RootIntents = slices.Clone(planRequest.RootIntents)
	reversed.DependencyClosure.ValidatedGroupIDs = slices.Clone(
		planRequest.DependencyClosure.ValidatedGroupIDs,
	)
	slices.Reverse(reversed.PredictedGroups)
	slices.Reverse(reversed.RootIntents)
	slices.Reverse(reversed.DependencyClosure.ValidatedGroupIDs)
	repeated, err := planGlobalCleanup(reversed)
	require.NoError(t, err)
	assert.Equal(t, result, repeated)
	assert.Equal(t, groupsBefore, planRequest.PredictedGroups,
		"planner must not mutate prepared groups")
}

func TestPlanGlobalCleanupUsesOneRootForPartitionTreesAndDetachedSubtrees(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		members int
	}{
		{name: "ordinary table", members: 1},
		{name: "complete partition tree", members: 3},
		{name: "detached subtree", members: 2},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			group := cleanupPartitionGroup(testCase.members)
			result, err := planGlobalCleanup(globalCleanupPlanRequest{
				PredictedGroups: []preparedArchivalGroup{group},
				RootIntents:     archivalCleanupRootIntents([]preparedArchivalGroup{group}),
				DependencyClosure: archivedDependencyClosureResult{
					ValidatedGroupIDs: []archivalGroupID{group.id},
				},
			})
			require.NoError(t, err)
			var tableDrops []cleanupOperationV1
			for _, operation := range result.Operations {
				if operation.Kind == cleanupOperationKindDropTable {
					tableDrops = append(tableDrops, operation)
				}
			}
			require.Len(t, tableDrops, 1)
			assert.Zero(t, compareMarkerObjects(group.cleanupRoot, tableDrops[0].Object))
			assert.NotContains(t, result.CleanupStatements[0].DDL, "child")
		})
	}
}

func TestPlanGlobalCleanupDependencyOrderHazardsAndDigest(t *testing.T) {
	t.Parallel()

	first := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_AAAAAAAA", 10, "source", "accounts",
	)
	second := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_BBBBBBBB", 20, "source", "orders",
	)
	request := mergePlainTableArchivalUnitRequests(first, second)
	groupIDs := slices.Clone(request.DependencyClosure.ValidatedGroupIDs)
	ownerID := groupIDs[0]
	ownerDependencySchema := first.Groups[0].Allocation.DependencySchemaName
	typeSource := markerObject(300, archivalMarkerObjectKindType, "deps", "status")
	typeDestination := markerObject(300, archivalMarkerObjectKindType, ownerDependencySchema, "status")
	functionSource := markerFunction(301, "deps", "normalize", "deps.status")
	functionDestination := markerFunction(301, ownerDependencySchema, "normalize", "deps.status")
	request.CurrentInventory.Types = []schema.CatalogType{{
		OID: 300, SchemaName: "deps", Name: "status", Kind: schema.CatalogTypeKindEnum,
	}}
	request.CurrentInventory.Routines = []schema.CatalogRoutine{{
		OID: 301, SchemaName: "deps", Name: "normalize", Kind: "f", IdentityArguments: "deps.status",
	}}
	request.CurrentInventory.Dependencies = []schema.CatalogDependency{{
		Dependent:  schema.CatalogDependencyObject{ClassOID: pgProcCatalogOID, ObjectOID: 301},
		Referenced: schema.CatalogDependencyObject{ClassOID: pgTypeCatalogOID, ObjectOID: 300},
		Type:       "n",
	}}
	request.DependencyClosure.Objects = []archivedDependencyClosureObject{
		cleanupClosureObject(typeSource, groupIDs, ownerID, ownerDependencySchema, pgTypeCatalogOID),
		cleanupClosureObject(functionSource, groupIDs, ownerID, ownerDependencySchema, pgProcCatalogOID),
	}
	request.DependencyClosure.Assignments = []archivedDependencyAssignment{
		{GroupID: ownerID, Source: typeSource, Destination: typeDestination},
		{GroupID: ownerID, Source: functionSource, Destination: functionDestination},
	}
	request.DependencyClosure.SharedGroupEdges = completeArchivedDependencyGroupEdges(groupIDs)
	isolation := archivalIsolationPlan{DependencyMoves: []archivalObjectMoveOperation{
		{GroupID: ownerID, Source: typeSource, Destination: typeDestination},
		{GroupID: ownerID, Source: functionSource, Destination: functionDestination},
	}}
	groups, err := preparePlainTableArchivalGroups(request)
	require.NoError(t, err)

	result, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: request.CurrentInventory, PredictedGroups: groups,
		RootIntents: archivalCleanupRootIntents(groups), DependencyClosure: request.DependencyClosure,
		Isolation: isolation,
	})
	require.NoError(t, err)
	functionIndex := cleanupStatementIndex(result.CleanupStatements, "DROP FUNCTION")
	typeIndex := cleanupStatementIndex(result.CleanupStatements, "DROP TYPE")
	firstSchemaIndex := cleanupStatementIndex(result.CleanupStatements, "DROP SCHEMA")
	assert.Greater(t, functionIndex, 1, "both roots must be dropped before shared dependencies")
	assert.Greater(t, typeIndex, functionIndex, "dependent function must be dropped before its type")
	assert.Greater(t, firstSchemaIndex, typeIndex, "schema drops must be last")

	for idx := 0; idx < 2; idx++ {
		assert.Equal(t, []MigrationHazardType{
			MigrationHazardTypeDeletesData,
			MigrationHazardTypeAcquiresAccessExclusiveLock,
		}, cleanupHazardTypes(result.CleanupStatements[idx]))
	}
	assert.Equal(t, []MigrationHazardType{
		MigrationHazardTypeCorrectness,
		MigrationHazardTypeAcquiresAccessExclusiveLock,
	}, cleanupHazardTypes(result.CleanupStatements[functionIndex]))
	assert.Equal(t, []MigrationHazardType{
		MigrationHazardTypeAcquiresAccessExclusiveLock,
	}, cleanupHazardTypes(result.CleanupStatements[firstSchemaIndex]))

	digestBefore, err := computeCleanupOperationDigest(result.Operations)
	require.NoError(t, err)
	result.CleanupStatements[0].Hazards = []MigrationHazard{{
		Type: "CHANGED", Message: "presentation only",
	}}
	digestAfter, err := computeCleanupOperationDigest(result.Operations)
	require.NoError(t, err)
	assert.Equal(t, digestBefore, digestAfter)
}

func TestPlanGlobalCleanupExistingOnlyReconstructsAndValidatesDigest(t *testing.T) {
	t.Parallel()

	newRequest := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_AAAAAAAA", 10, "source", "accounts",
	)
	groups, err := preparePlainTableArchivalGroups(newRequest)
	require.NoError(t, err)
	initial, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: newRequest.CurrentInventory, PredictedGroups: groups,
		RootIntents: archivalCleanupRootIntents(groups), DependencyClosure: newRequest.DependencyClosure,
	})
	require.NoError(t, err)
	require.Len(t, initial.FinalizedMarkers, 1)

	existingInventory := cleanupExistingInventory(
		newRequest.CurrentInventory, initial.FinalizedMarkers[0],
	)
	candidate := cleanupCandidate(initial.FinalizedMarkers[0])
	existingClosure := archivedDependencyClosureResult{
		ValidatedGroupIDs: []archivalGroupID{candidate.GroupID},
		DependencyValidatedCandidateGroups: []dependencyValidatedArchivedCandidateGroup{{
			Candidate: candidate,
		}},
	}
	existing, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: existingInventory, DependencyClosure: existingClosure,
	})
	require.NoError(t, err)
	assert.Empty(t, existing.FinalizedRegularGroups)
	assert.Empty(t, existing.MarkerUpdateStatements)
	assert.Equal(t, initial.Operations, existing.Operations)
	assert.Equal(t, initial.CleanupStatements, existing.CleanupStatements)
	assert.Equal(t, []archivalGroupID{candidate.GroupID}, existing.TrustedGroupIDs)

	staleCandidate := candidate
	staleCandidate.Marker.CleanupDigest = cleanupOperationDigest("sha256:" + strings.Repeat("1", 64))
	staleText, marshalErr := marshalArchivalMarker(staleCandidate.Marker)
	require.NoError(t, marshalErr)
	staleInventory := existingInventory
	staleInventory.Schemas = slices.Clone(existingInventory.Schemas)
	for idx := range staleInventory.Schemas {
		staleInventory.Schemas[idx].Comment = staleText
	}
	staleClosure := existingClosure
	staleClosure.DependencyValidatedCandidateGroups = []dependencyValidatedArchivedCandidateGroup{{
		Candidate: staleCandidate,
	}}
	_, err = planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: staleInventory, DependencyClosure: staleClosure,
	})
	require.ErrorContains(t, err, "stale cleanup digest")

	inconsistentInventory := existingInventory
	inconsistentInventory.Schemas = slices.Clone(existingInventory.Schemas)
	for idx := range inconsistentInventory.Schemas {
		if inconsistentInventory.Schemas[idx].Name == candidate.SchemaNames[0] {
			inconsistentInventory.Schemas[idx].Comment = "tampered"
		}
	}
	_, err = planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: inconsistentInventory, DependencyClosure: existingClosure,
	})
	require.ErrorContains(t, err, "inconsistent marker copy")

	malformedCandidate := candidate
	malformedCandidate.Marker.CleanupDigest = "malformed"
	malformedClosure := existingClosure
	malformedClosure.DependencyValidatedCandidateGroups = []dependencyValidatedArchivedCandidateGroup{{
		Candidate: malformedCandidate,
	}}
	_, err = planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: existingInventory, DependencyClosure: malformedClosure,
	})
	require.ErrorContains(t, err, "cleanup digest")

	edgeCandidate := candidate
	edgeCandidate.Marker.SharedCleanupComponentGroupEdges = []archivalMarkerSharedGroupEdgeV1{{
		FirstGroupID: candidate.GroupID, SecondGroupID: "missing-group",
	}}
	edgeText, marshalErr := marshalArchivalMarker(edgeCandidate.Marker)
	require.NoError(t, marshalErr)
	edgeInventory := existingInventory
	edgeInventory.Schemas = slices.Clone(existingInventory.Schemas)
	for idx := range edgeInventory.Schemas {
		edgeInventory.Schemas[idx].Comment = edgeText
	}
	edgeClosure := existingClosure
	edgeClosure.DependencyValidatedCandidateGroups = []dependencyValidatedArchivedCandidateGroup{{
		Candidate: edgeCandidate,
	}}
	_, err = planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: edgeInventory, DependencyClosure: edgeClosure,
	})
	require.ErrorContains(t, err, "shared component edges")
}

func TestPlanGlobalCleanupLaterSharedDependencyMoveUpdatesExistingMarker(t *testing.T) {
	t.Parallel()

	oldRequest := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_AAAAAAAA", 10, "source", "accounts",
	)
	oldGroups, err := preparePlainTableArchivalGroups(oldRequest)
	require.NoError(t, err)
	oldPlan, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: oldRequest.CurrentInventory, PredictedGroups: oldGroups,
		RootIntents:       archivalCleanupRootIntents(oldGroups),
		DependencyClosure: oldRequest.DependencyClosure,
	})
	require.NoError(t, err)
	oldFinalized := oldPlan.FinalizedMarkers[0]
	oldCandidate := cleanupCandidate(oldFinalized)

	newRequest := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_BBBBBBBB", 20, "source", "orders",
	)
	newGroups, err := preparePlainTableArchivalGroups(newRequest)
	require.NoError(t, err)
	newGroupID := newGroups[0].id
	oldGroupID := oldCandidate.GroupID
	dependencySource := markerObject(300, archivalMarkerObjectKindType, "deps", "status")
	dependencyDestination := markerObject(
		300, archivalMarkerObjectKindType,
		oldFinalized.Payload.ExclusiveDependencySchemas[0].Name, "status",
	)
	inventory := cleanupExistingInventory(oldRequest.CurrentInventory, oldFinalized)
	inventory.Relations = append(inventory.Relations, newRequest.CurrentInventory.Relations...)
	inventory.Tables = append(inventory.Tables, newRequest.CurrentInventory.Tables...)
	inventory.Types = append(inventory.Types, schema.CatalogType{
		OID: 300, SchemaName: "deps", Name: "status", Kind: schema.CatalogTypeKindEnum,
	})
	groupIDs := []archivalGroupID{oldGroupID, newGroupID}
	slices.Sort(groupIDs)
	closure := archivedDependencyClosureResult{
		ValidatedGroupIDs: groupIDs,
		Objects: []archivedDependencyClosureObject{
			cleanupClosureObject(dependencySource, groupIDs, oldGroupID,
				dependencyDestination.SchemaName, pgTypeCatalogOID),
		},
		Assignments: []archivedDependencyAssignment{{
			GroupID: oldGroupID, Source: dependencySource, Destination: dependencyDestination,
		}},
		SharedGroupEdges: completeArchivedDependencyGroupEdges(groupIDs),
		DependencyValidatedCandidateGroups: []dependencyValidatedArchivedCandidateGroup{{
			Candidate: oldCandidate,
		}},
	}
	componentGroups, err := finalizeGlobalCleanupComponentInputs(newGroups, closure)
	require.NoError(t, err)
	isolation, err := planArchivalIsolation(
		inventory, schema.Schema{}, sourceSafetyPreflightResult{}, closure, componentGroups,
	)
	require.NoError(t, err)
	require.Equal(t, []archivalObjectMoveOperation{{
		GroupID: oldGroupID, Source: dependencySource, Destination: dependencyDestination,
	}}, isolation.DependencyMoves)

	result, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: inventory, PredictedGroups: componentGroups,
		RootIntents: archivalCleanupRootIntents(componentGroups), DependencyClosure: closure,
		Isolation: isolation,
	})
	require.NoError(t, err)
	require.Len(t, result.MarkerUpdateStatements, 1)
	for _, schemaName := range oldCandidate.SchemaNames {
		assert.Contains(t, result.MarkerUpdateStatements[0].DDL, schema.EscapeIdentifier(schemaName))
	}
	require.Len(t, result.FinalizedMarkers, 2)
	assert.Equal(
		t,
		result.FinalizedMarkers[0].Payload.CleanupDigest,
		result.FinalizedMarkers[1].Payload.CleanupDigest,
	)
	assert.Equal(t, completeArchivedDependencyGroupEdges(groupIDs),
		result.FinalizedMarkers[0].Payload.SharedCleanupComponentGroupEdges)
	assert.Equal(t, 1, cleanupOperationKindCount(result.Operations, cleanupOperationKindDropType),
		"a shared dependency must have one owner and one drop")

	duplicateClosure := closure
	duplicateClosure.Assignments = append(slices.Clone(closure.Assignments), closure.Assignments[0])
	_, err = planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: inventory, PredictedGroups: componentGroups,
		RootIntents: archivalCleanupRootIntents(componentGroups), DependencyClosure: duplicateClosure,
		Isolation: isolation,
	})
	require.ErrorContains(t, err, "exactly one is required")

	missingMove := isolation
	missingMove.DependencyMoves = nil
	_, err = planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: inventory, PredictedGroups: componentGroups,
		RootIntents: archivalCleanupRootIntents(componentGroups), DependencyClosure: closure,
		Isolation: missingMove,
	})
	require.ErrorContains(t, err, "missing the cleanup dependency move")
}

func TestPlanArchivedDependencyClosureDefersProposedCandidateComponentDelta(t *testing.T) {
	t.Parallel()

	request := newArchivedDependencyTestRequest(10, 20)
	request.ProposedGroups = []archivedDependencyClosureGroupRequest{{
		GroupID: "group-b", TableRelationOIDs: []uint32{20},
		DependencySchemaName: "archive_dep_b",
	}}
	request.CandidateGroups = []structurallyValidArchivedCandidateGroup{
		closureCandidateGroup("group-a", 10, "archive_dep_a"),
	}
	enumAddress := closureAddress(pgTypeCatalogOID, 100, "deps", "status", "deps.status")
	request.CurrentSnapshot.Inventory.Types = []schema.CatalogType{
		closureType(100, "deps", "status", schema.CatalogTypeKindEnum),
	}
	request.CurrentSnapshot.Inventory.EnumLabels = []schema.CatalogEnumLabel{{
		TypeOID: 100, SortOrder: 1, Label: "ready",
	}}
	request.CurrentSnapshot.Inventory.Dependencies = []schema.CatalogDependency{
		closureDependency(closureTableAddress(10, "managed", "first"), enumAddress),
		closureDependency(closureTableAddress(20, "managed", "second"), enumAddress),
	}

	result, err := planArchivedDependencyClosure(request)
	require.NoError(t, err)
	require.Len(t, result.DependencyValidatedCandidateGroups, 1)
	require.Len(t, result.Assignments, 1)
	assert.Equal(t, archivalGroupID("group-a"), result.Assignments[0].GroupID)
	assert.Equal(t, "archive_dep_a", result.Assignments[0].Destination.SchemaName)
	assert.Equal(t, []archivalMarkerSharedGroupEdgeV1{{
		FirstGroupID: "group-a", SecondGroupID: "group-b",
	}}, result.SharedGroupEdges)
}

func TestPlanGlobalCleanupFinalizedMarkerFeedsInitializationWithoutPlaceholder(t *testing.T) {
	t.Parallel()

	request := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_AAAAAAAA", 10, "source", "accounts",
	)
	groups, err := preparePlainTableArchivalGroups(request)
	require.NoError(t, err)
	originalMarker := groups[0].markerText
	result, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: request.CurrentInventory, PredictedGroups: groups,
		RootIntents: archivalCleanupRootIntents(groups), DependencyClosure: request.DependencyClosure,
	})
	require.NoError(t, err)
	require.Len(t, result.FinalizedRegularGroups, 1)
	require.Len(t, result.FinalizedMarkers, 1)
	assert.NotEqual(t, originalMarker, result.FinalizedMarkers[0].Text)
	initialization := renderPlainTableArchivalInitialization(result.FinalizedRegularGroups[0])
	require.Len(t, initialization, 1)
	assert.Contains(t, initialization[0].DDL,
		escapeArchivalMarkerSQLLiteral(result.FinalizedMarkers[0].Text))
	parsed, err := parseArchivalMarker(result.FinalizedMarkers[0].Text)
	require.NoError(t, err)
	digest, err := computeCleanupOperationDigest(result.Operations)
	require.NoError(t, err)
	assert.Equal(t, digest, parsed.CleanupDigest)
}

func TestPlanGlobalCleanupFinalizedMarkerFeedsResumeRefresh(t *testing.T) {
	t.Parallel()

	newRequest := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_AAAAAAAA", 10, "source", "accounts",
	)
	newGroups, err := preparePlainTableArchivalGroups(newRequest)
	require.NoError(t, err)
	initial, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: newRequest.CurrentInventory, PredictedGroups: newGroups,
		RootIntents:       archivalCleanupRootIntents(newGroups),
		DependencyClosure: newRequest.DependencyClosure,
	})
	require.NoError(t, err)
	finalized := initial.FinalizedMarkers[0]
	candidate := cleanupCandidate(finalized)
	candidate.State = archivedCandidateGroupStateEmptyInitialized
	member := finalized.Payload.Members[0]
	candidate.Resume = archivedGroupResumeDescriptor{
		GroupID: candidate.GroupID,
		RemainingMemberMoves: []archivedMemberMoveDescriptor{{
			MemberID: member.MemberID, RelationOID: member.SourceTable.OID,
			SourceTable: member.SourceTable, DestinationTable: member.CleanupTable,
		}},
		RemainingMarkerUpdates: markerUpdatesForSchemas(candidate.SchemaNames, finalized.Text),
	}
	resumeInventory := newRequest.CurrentInventory
	for _, schemaName := range candidate.SchemaNames {
		resumeInventory.Schemas = append(resumeInventory.Schemas, schema.CatalogSchema{
			OID: uint32(1000 + len(resumeInventory.Schemas)), Name: schemaName, Comment: finalized.Text,
		})
	}
	resumeRequest := plainTableArchivalRequest{
		CurrentInventory: resumeInventory,
		Groups: []plainTableArchivalGroupRequest{{
			FinalizedMarker: finalized.Text, Resume: &candidate.Resume,
		}},
		SourcePreflight: sourceSafetyPreflightResult{
			ValidatedTableRelationOIDs: []uint32{member.SourceTable.OID},
		},
		DependencyClosure: archivedDependencyClosureResult{
			ValidatedGroupIDs: []archivalGroupID{candidate.GroupID},
			DependencyValidatedCandidateGroups: []dependencyValidatedArchivedCandidateGroup{{
				Candidate: candidate,
			}},
		},
	}
	resumeGroups, err := preparePlainTableArchivalGroups(resumeRequest)
	require.NoError(t, err)
	result, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: resumeInventory, PredictedGroups: resumeGroups,
		RootIntents:       archivalCleanupRootIntents(resumeGroups),
		DependencyClosure: resumeRequest.DependencyClosure,
	})
	require.NoError(t, err)
	require.Len(t, result.FinalizedRegularGroups, 1)
	refresh := renderPlainTableArchivalMarkerRefresh(result.FinalizedRegularGroups[0])
	require.Len(t, refresh, 1)
	assert.Contains(t, refresh[0].DDL,
		escapeArchivalMarkerSQLLiteral(result.FinalizedMarkers[0].Text))
	parsed, err := parseArchivalMarker(result.FinalizedMarkers[0].Text)
	require.NoError(t, err)
	digest, err := computeCleanupOperationDigest(result.Operations)
	require.NoError(t, err)
	assert.Equal(t, digest, parsed.CleanupDigest)
}

func TestRenderGlobalCleanupStatementsSignaturesSequencesAndIndexHazards(t *testing.T) {
	t.Parallel()

	operations := []cleanupOperationV1{
		cleanupDropOperation("01-table", cleanupOperationKindDropTable,
			markerObject(1, archivalMarkerObjectKindTable, `archive"schema`, `table"name`)),
		cleanupDropOperation("02-sequence", cleanupOperationKindDropSequence,
			markerObject(2, archivalMarkerObjectKindSequence, "archive", "numbers")),
		cleanupDropOperation("03-function", cleanupOperationKindDropFunction,
			markerFunction(3, "archive", "normalize", "integer, text")),
		cleanupDropOperation("04-operator", cleanupOperationKindDropOperator,
			archivedDependencyMarkerObject(4, archivalMarkerObjectKindOperator,
				"archive", "===", "integer", "text")),
		cleanupDropOperation("05-collation", cleanupOperationKindDropCollation,
			markerObject(5, archivalMarkerObjectKindCollation, "archive", "words")),
		cleanupDropOperation("06-type", cleanupOperationKindDropType,
			markerObject(6, archivalMarkerObjectKindType, "archive", "status")),
	}
	group := globalCleanupGroup{
		cleanupRoot: operations[0].Object, hasIndexes: true,
	}
	statements, err := renderGlobalCleanupStatements(operations, []globalCleanupGroup{group})
	require.NoError(t, err)
	assert.Equal(t, `DROP TABLE "archive""schema"."table""name" RESTRICT`, statements[0].DDL)
	assert.Equal(t, "DROP SEQUENCE \"archive\".\"numbers\" RESTRICT", statements[1].DDL)
	assert.Equal(t, "DROP FUNCTION \"archive\".\"normalize\"(integer, text) RESTRICT", statements[2].DDL)
	assert.Equal(t, "DROP OPERATOR \"archive\".=== (integer, text) RESTRICT", statements[3].DDL)
	assert.Equal(t, []MigrationHazardType{
		MigrationHazardTypeDeletesData,
		MigrationHazardTypeAcquiresAccessExclusiveLock,
		MigrationHazardTypeIndexDropped,
	}, cleanupHazardTypes(statements[0]))
	assert.Equal(t, []MigrationHazardType{
		MigrationHazardTypeDeletesData,
		MigrationHazardTypeAcquiresAccessExclusiveLock,
	}, cleanupHazardTypes(statements[1]))
	for _, statement := range statements {
		assert.Contains(t, statement.DDL, " RESTRICT")
		assert.NotContains(t, statement.DDL, "CASCADE")
	}
}

func cleanupPartitionGroup(memberCount int) preparedArchivalGroup {
	groupID := archivalGroupID("20260721T091011123456Z_AAAAAAAA")
	marker := archivalMarkerV1{
		Version: archivalMarkerVersion, GroupID: groupID,
		ExclusiveDependencySchemas: []archivalMarkerSchemaIdentity{{Name: "archive_dependencies"}},
		CleanupDigest:              cleanupOperationDigest("sha256:" + strings.Repeat("0", 64)),
	}
	for idx := range memberCount {
		name := "root"
		if idx > 0 {
			name = "child_" + string(rune('a'+idx-1))
		}
		object := markerObject(uint32(100+idx), archivalMarkerObjectKindTable,
			"archive_"+name, name)
		marker.Members = append(marker.Members, archivalMarkerMemberV1{
			MemberID: name,
			SourceTable: markerObject(uint32(100+idx), archivalMarkerObjectKindTable,
				"source", name),
			CleanupTable: object, AutomaticallyMovedObjects: []archivalMarkerObjectIdentity{object},
		})
		if idx > 0 {
			marker.PartitionEdges = append(marker.PartitionEdges, archivalMarkerPartitionEdgeV1{
				ParentMemberID: "root", ChildMemberID: name, BoundExpression: "FOR VALUES IN (1)",
			})
		}
	}
	root := marker.Members[0].CleanupTable
	return preparedArchivalGroup{id: groupID, marker: marker, cleanupRoot: root}
}

func cleanupClosureObject(
	source archivalMarkerObjectIdentity,
	groupIDs []archivalGroupID,
	ownerID archivalGroupID,
	destinationSchema string,
	classOID uint32,
) archivedDependencyClosureObject {
	classification := archivedDependencyClassificationExclusiveMovable
	if len(groupIDs) > 1 {
		classification = archivedDependencyClassificationSharedArchivedOnly
	}
	return archivedDependencyClosureObject{
		Kind: source.Kind,
		Address: schema.CatalogDependencyObject{
			ClassOID: classOID, ObjectOID: source.OID,
			SchemaName: source.SchemaName, Name: source.Name,
		},
		Identity: cloneMarkerObject(source), Classification: classification,
		GroupIDs: slices.Clone(groupIDs), OwnerGroupID: ownerID,
		DestinationSchema: destinationSchema, Movable: true,
	}
}

func cleanupExistingInventory(
	inventory schema.CatalogInventory,
	finalized globalCleanupFinalizedMarker,
) schema.CatalogInventory {
	result := inventory
	result.Relations = slices.Clone(inventory.Relations)
	for _, member := range finalized.Payload.Members {
		for idx := range result.Relations {
			if result.Relations[idx].OID == member.SourceTable.OID {
				result.Relations[idx].SchemaName = member.CleanupTable.SchemaName
			}
		}
	}
	for _, schemaName := range archivedMarkerSchemaNames(finalized.Payload) {
		result.Schemas = append(result.Schemas, schema.CatalogSchema{
			OID: uint32(1000 + len(result.Schemas)), Name: schemaName, Comment: finalized.Text,
		})
	}
	return result
}

func cleanupCandidate(finalized globalCleanupFinalizedMarker) structurallyValidArchivedCandidateGroup {
	return structurallyValidArchivedCandidateGroup{
		GroupID: finalized.GroupID, State: archivedCandidateGroupStateCompleteCandidate,
		Marker: finalized.Payload, SchemaNames: archivedMarkerSchemaNames(finalized.Payload),
		ExpectedDependencySchemaName: finalized.Payload.ExclusiveDependencySchemas[0].Name,
		Resume:                       archivedGroupResumeDescriptor{GroupID: finalized.GroupID},
	}
}

func cleanupDropOperation(
	id string,
	kind cleanupOperationKind,
	object archivalMarkerObjectIdentity,
) cleanupOperationV1 {
	return cleanupOperationV1{
		Version: cleanupOperationCodecVersion, ID: id, Kind: kind,
		Object: object, Restrict: true,
	}
}

func cleanupOperationKinds(operations []cleanupOperationV1) []cleanupOperationKind {
	result := make([]cleanupOperationKind, 0, len(operations))
	for _, operation := range operations {
		result = append(result, operation.Kind)
	}
	return result
}

func cleanupOperationKindCount(operations []cleanupOperationV1, kind cleanupOperationKind) int {
	count := 0
	for _, operation := range operations {
		if operation.Kind == kind {
			count++
		}
	}
	return count
}

func cleanupStatementIndex(statements []Statement, prefix string) int {
	return slices.IndexFunc(statements, func(statement Statement) bool {
		return strings.HasPrefix(statement.DDL, prefix)
	})
}

func cleanupHazardTypes(statement Statement) []MigrationHazardType {
	result := make([]MigrationHazardType, 0, len(statement.Hazards))
	for _, hazard := range statement.Hazards {
		result = append(result, hazard.Type)
	}
	return result
}
