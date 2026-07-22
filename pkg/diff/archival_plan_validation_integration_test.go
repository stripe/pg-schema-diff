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
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

func TestArchivalPlanValidationTwoPhasesPreserveThenDeleteRowsWithoutChangingTarget(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	request, _, cleanupSchema := newArchivalValidationFixture(t, factory)
	request.DataProbes = []archivalValidationDataProbe{
		{
			SetupDDL: "INSERT INTO managed.archived VALUES (1, 'retained')",
			RetentionQuery: fmt.Sprintf("SELECT payload FROM %s.archived WHERE id = 1",
				schema.EscapeIdentifier(cleanupSchema)),
			RetentionExpected: "retained",
			CleanupQuery: fmt.Sprintf("SELECT (to_regclass(%s) IS NULL)::text",
				schema.EscapeLiteral(schema.EscapeIdentifier(cleanupSchema)+`.archived`)),
			CleanupExpected: "true",
		},
		{
			SetupDDL:          "INSERT INTO managed.keep VALUES (1, 'managed')",
			RetentionQuery:    "SELECT payload FROM managed.keep WHERE id = 1",
			RetentionExpected: "managed",
			CleanupQuery:      "SELECT payload FROM managed.keep WHERE id = 1",
			CleanupExpected:   "managed",
		},
	}

	require.NoError(t, validateArchivalPlanTwoPhase(t.Context(), request))
}

func TestArchivalPlanValidationExistingOnlyUsesEmptyOrdinaryPhase(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	initial, sourceDB, cleanupSchema := newArchivalValidationFixture(t, factory)
	for _, statement := range initial.OrdinaryStatements {
		_, err := sourceDB.ConnPool.Exec(t.Context(), statement.ToSQL())
		require.NoError(t, err, statement.DDL)
	}

	current, err := schema.GetSchemaSnapshot(t.Context(), sourceDB.ConnPool,
		schema.WithExcludeSchemaPatterns("archive.*"))
	require.NoError(t, err)
	resolution, err := resolveArchivedState("archive", current, initial.TargetSnapshot)
	require.NoError(t, err)
	require.Len(t, resolution.CandidateGroups, 1)
	preflight, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: initial.TargetSnapshot,
		ProposedTableRelationOIDs: plainMarkerRelationOIDs(resolution.CandidateGroups[0].Marker),
	})
	require.NoError(t, err)
	closure, err := planArchivedDependencyClosure(archivedDependencyClosureRequest{
		CurrentSnapshot: current, TargetSnapshot: initial.TargetSnapshot,
		CandidateGroups: resolution.CandidateGroups, SourcePreflight: preflight,
	})
	require.NoError(t, err)
	cleanup, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: current.Inventory, DependencyClosure: closure,
	})
	require.NoError(t, err)
	request := archivalPlanValidationRequest{
		TempDBFactory: factory, Prefix: "archive", CurrentSnapshot: current,
		TargetSnapshot: initial.TargetSnapshot, Cleanup: cleanup,
		SourcePreflight: preflight, DependencyClosure: closure,
		ManagedSchemaOptions: []schema.GetSchemaOpt{
			schema.WithExcludeSchemaPatterns("archive.*"),
		},
		DataProbes: []archivalValidationDataProbe{{
			SetupDDL: fmt.Sprintf("INSERT INTO %s.archived VALUES (2, 'existing')",
				schema.EscapeIdentifier(cleanupSchema)),
			RetentionQuery: fmt.Sprintf("SELECT payload FROM %s.archived WHERE id = 2",
				schema.EscapeIdentifier(cleanupSchema)),
			RetentionExpected: "existing",
			CleanupQuery: fmt.Sprintf("SELECT (to_regclass(%s) IS NULL)::text",
				schema.EscapeLiteral(schema.EscapeIdentifier(cleanupSchema)+`.archived`)),
			CleanupExpected: "true",
		}},
	}
	assert.Empty(t, request.OrdinaryStatements)
	require.NoError(t, validateArchivalPlanTwoPhase(t.Context(), request))
}

func TestArchivalPlanValidationResumesInitializedGroupBeforePostconditions(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	initial, sourceDB, cleanupSchema := newArchivalValidationFixture(t, factory)
	initialization := slices.IndexFunc(initial.OrdinaryStatements, func(statement Statement) bool {
		return strings.Contains(statement.DDL, "CREATE SCHEMA") &&
			strings.Contains(statement.DDL, "COMMENT ON SCHEMA")
	})
	require.NotEqual(t, -1, initialization)
	_, err := sourceDB.ConnPool.Exec(t.Context(), initial.OrdinaryStatements[initialization].ToSQL())
	require.NoError(t, err)

	current, err := schema.GetSchemaSnapshot(t.Context(), sourceDB.ConnPool,
		schema.WithExcludeSchemaPatterns("archive.*"))
	require.NoError(t, err)
	resolution, err := resolveArchivedState("archive", current, initial.TargetSnapshot)
	require.NoError(t, err)
	require.Len(t, resolution.CandidateGroups, 1)
	candidate := resolution.CandidateGroups[0]
	assert.Equal(t, archivedCandidateGroupStateEmptyInitialized, candidate.State)
	preflight, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: initial.TargetSnapshot,
		ProposedTableRelationOIDs: plainMarkerRelationOIDs(candidate.Marker),
	})
	require.NoError(t, err)
	closure, err := planArchivedDependencyClosure(archivedDependencyClosureRequest{
		CurrentSnapshot: current, TargetSnapshot: initial.TargetSnapshot,
		CandidateGroups: resolution.CandidateGroups, SourcePreflight: preflight,
	})
	require.NoError(t, err)
	regularRequest := plainTableArchivalRequest{
		CurrentInventory: current.Inventory, TargetSchema: initial.TargetSnapshot.Schema,
		Groups: []plainTableArchivalGroupRequest{{
			FinalizedMarker: initial.Cleanup.FinalizedMarkers[0].Text, Resume: &candidate.Resume,
		}},
		SourcePreflight: preflight, DependencyClosure: closure,
	}
	groups, err := preparePlainTableArchivalGroups(regularRequest)
	require.NoError(t, err)
	groups, err = finalizeGlobalCleanupComponentInputs(groups, closure)
	require.NoError(t, err)
	isolation, err := planArchivalIsolation(current.Inventory, initial.TargetSnapshot.Schema,
		preflight, closure, groups)
	require.NoError(t, err)
	cleanup, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: current.Inventory, PredictedGroups: groups,
		RootIntents: archivalCleanupRootIntents(groups), DependencyClosure: closure,
		Isolation: isolation,
	})
	require.NoError(t, err)
	regularRequest.Groups[0].FinalizedMarker = cleanup.FinalizedMarkers[0].Text
	regularRequest.ComponentInitializationManaged = true
	diff, _, err := buildSchemaDiff(current.Schema, initial.TargetSnapshot.Schema)
	require.NoError(t, err)
	member := candidate.Marker.Members[0]
	ordinary, err := generateReplacementAwareSchemaSQL(
		newSchemaSQLGenerator(&deterministicRandReader{}, &planOptions{}), diff,
		tableDispositions{markerTableName(member.SourceTable).GetName(): {
			Kind: tableDispositionKindArchivalMove, GroupID: candidate.GroupID,
			RelationOID: member.SourceTable.OID,
		}}, regularRequest,
	)
	require.NoError(t, err)
	ordinary = append(cleanup.ComponentInitializationStatements, ordinary...)
	request := archivalPlanValidationRequest{
		TempDBFactory: factory, Prefix: "archive", CurrentSnapshot: current,
		TargetSnapshot: initial.TargetSnapshot, OrdinaryStatements: ordinary, Cleanup: cleanup,
		SourcePreflight: preflight, DependencyClosure: closure, Isolation: isolation,
		ManagedSchemaOptions: []schema.GetSchemaOpt{
			schema.WithExcludeSchemaPatterns("archive.*"),
		},
		DataProbes: []archivalValidationDataProbe{{
			SetupDDL: "INSERT INTO managed.archived VALUES (3, 'resumed')",
			RetentionQuery: fmt.Sprintf("SELECT payload FROM %s.archived WHERE id = 3",
				schema.EscapeIdentifier(cleanupSchema)),
			RetentionExpected: "resumed",
			CleanupQuery: fmt.Sprintf("SELECT (to_regclass(%s) IS NULL)::text",
				schema.EscapeLiteral(schema.EscapeIdentifier(cleanupSchema)+`.archived`)),
			CleanupExpected: "true",
		}},
	}
	require.NoError(t, validateArchivalPlanTwoPhase(t.Context(), request))
}

func TestArchivalPlanValidationSurfacesLaterRestrictDependencyFailure(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	request, _, cleanupSchema := newArchivalValidationFixture(t, factory)
	request.PostRetentionSetupStatements = []Statement{{DDL: fmt.Sprintf(
		"CREATE VIEW managed.late_dependency AS SELECT * FROM %s.archived",
		schema.EscapeIdentifier(cleanupSchema),
	)}}

	err := validateArchivalPlanTwoPhase(t.Context(), request)
	require.ErrorContains(t, err, "running archival cleanup statements")
	require.ErrorContains(t, err, "other objects depend on it")
}

func TestArchivalPlanValidationSourcePreflightFindsExcludedExternalDependency(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	initial, sourceDB, _ := newArchivalValidationFixture(t, factory)
	_, err := sourceDB.ConnPool.Exec(t.Context(), `
		CREATE SCHEMA excluded;
		CREATE VIEW excluded.archived_reader AS SELECT * FROM managed.archived;
	`)
	require.NoError(t, err)
	current, err := schema.GetSchemaSnapshot(t.Context(), sourceDB.ConnPool,
		schema.WithIncludeSchemaPatterns("managed"))
	require.NoError(t, err)
	archived := mustCatalogRelationByName(t, current.Inventory, "managed", "archived")

	_, err = runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: initial.TargetSnapshot,
		ProposedTableRelationOIDs: []uint32{archived.OID},
	})
	require.ErrorContains(t, err, "persistent view")
	require.ErrorContains(t, err, "excluded.archived_reader")
}

func TestArchivalPlanValidationRejectsStaleDigestAndIsolationProducts(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	request, _, _ := newArchivalValidationFixture(t, factory)

	t.Run("stale cleanup digest", func(t *testing.T) {
		mutated := request
		mutated.Cleanup = cloneArchivalValidationCleanup(request.Cleanup)
		mutated.Cleanup.FinalizedMarkers[0].Payload.CleanupDigest =
			cleanupOperationDigest("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		text, err := marshalArchivalMarker(mutated.Cleanup.FinalizedMarkers[0].Payload)
		require.NoError(t, err)
		mutated.Cleanup.FinalizedMarkers[0].Text = text
		err = validateArchivalPlanTwoPhase(t.Context(), mutated)
		require.ErrorContains(t, err, "stale cleanup digest")
	})

	t.Run("isolation mismatch", func(t *testing.T) {
		mutated := request
		mutated.Isolation = archivalIsolationPlan{}
		err := validateArchivalPlanTwoPhase(t.Context(), mutated)
		require.ErrorContains(t, err, "stage 14 isolation product")
	})
}

func TestArchivalPlanValidationRejectsCleanupTargetsBeforeTemporaryExecution(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	request, _, _ := newArchivalValidationFixture(t, factory)
	managed := mustCatalogRelationByName(t, request.CurrentSnapshot.Inventory, "managed", "keep")

	for _, testCase := range []struct {
		name   string
		mutate func(*archivalPlanValidationRequest)
		match  string
	}{
		{
			name: "managed table",
			mutate: func(request *archivalPlanValidationRequest) {
				request.Cleanup.Operations[0].Object = markerObject(
					managed.OID, archivalMarkerObjectKindTable, "managed", "keep",
				)
			},
			match: "outside a trusted archival schema",
		},
		{
			name: "unknown schema",
			mutate: func(request *archivalPlanValidationRequest) {
				idx := slices.IndexFunc(request.Cleanup.Operations, func(operation cleanupOperationV1) bool {
					return operation.Kind == cleanupOperationKindDropSchema
				})
				require.NotEqual(t, -1, idx)
				request.Cleanup.Operations[idx].Object.Name = "unknown_archive"
			},
			match: "targets unknown schema",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			mutated := request
			mutated.Cleanup = cloneArchivalValidationCleanup(request.Cleanup)
			testCase.mutate(&mutated)
			err := validateArchivalPlanTwoPhase(t.Context(), mutated)
			require.ErrorContains(t, err, testCase.match)
		})
	}
}

func TestArchivalPlanValidationFailsClosedForUnmodeledSecurityMetadata(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	request, _, _ := newArchivalValidationFixture(t, factory)
	archived := mustCatalogRelationByName(t, request.CurrentSnapshot.Inventory, "managed", "archived")
	request.CurrentSnapshot.Inventory.SecurityLabels = append(
		request.CurrentSnapshot.Inventory.SecurityLabels,
		schema.CatalogSecurityLabel{RelationOID: archived.OID, Provider: "unsupported_provider", Label: "secret"},
	)

	err := validateArchivalPlanTwoPhase(t.Context(), request)
	require.ErrorContains(t, err, "not synthetically reconstructable")
}

func TestArchivalPlanValidationRegeneratesSyntheticOIDsInsteadOfClaimingProductionOIDSQL(t *testing.T) {
	source := schema.CatalogInventory{Relations: []schema.CatalogRelation{{
		OID: 10, SchemaName: "managed", Name: "archived", Kind: schema.RelKindOrdinaryTable,
	}}}
	temporary := schema.CatalogInventory{Relations: []schema.CatalogRelation{{
		OID: 110, SchemaName: "managed", Name: "archived", Kind: schema.RelKindOrdinaryTable,
	}}}
	marker := globalCleanupFinalizedMarker{GroupID: "group", Payload: archivalMarkerV1{
		Members: []archivalMarkerMemberV1{{
			SourceTable:  markerObject(10, archivalMarkerObjectKindTable, "managed", "archived"),
			CleanupTable: markerObject(10, archivalMarkerObjectKindTable, "archive_group", "archived"),
		}},
	}}

	translation, err := buildArchivalValidationOIDTranslation(source, temporary,
		[]globalCleanupFinalizedMarker{marker})
	require.NoError(t, err)
	assert.Equal(t, uint32(110), translation.translateCatalog(pgClassCatalogOID, 10))
	assert.Equal(t, uint32(10), marker.Payload.Members[0].SourceTable.OID,
		"the persisted production marker must remain immutable")
}

func TestArchivalPlanValidationCompleteAndDetachedRecursivePartitions(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	for _, detached := range []bool{false, true} {
		name := "complete tree"
		if detached {
			name = "detached subtree"
		}
		t.Run(name, func(t *testing.T) {
			request, _ := newPartitionArchivalValidationFixture(t, factory, detached)
			require.NoError(t, validateArchivalPlanTwoPhase(t.Context(), request))
		})
	}
}

func TestArchivalPlanValidationPartialPartitionResumeReachesPredictedCompleteState(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	initial, sourceDB := newPartitionArchivalValidationFixture(t, factory, false)
	initialization := slices.IndexFunc(initial.OrdinaryStatements, func(statement Statement) bool {
		return strings.Contains(statement.DDL, "CREATE SCHEMA") &&
			strings.Contains(statement.DDL, "COMMENT ON SCHEMA")
	})
	require.NotEqual(t, -1, initialization)
	_, err := sourceDB.ConnPool.Exec(t.Context(), initial.OrdinaryStatements[initialization].ToSQL())
	require.NoError(t, err)
	moves := archivalMoveStatements(initial.OrdinaryStatements)
	require.Len(t, moves, 4)
	_, err = sourceDB.ConnPool.Exec(t.Context(), moves[0].ToSQL())
	require.NoError(t, err)

	current, err := schema.GetSchemaSnapshot(t.Context(), sourceDB.ConnPool,
		schema.WithExcludeSchemaPatterns("archive.*"))
	require.NoError(t, err)
	resolution, err := resolveArchivedState("archive", current, initial.TargetSnapshot)
	require.NoError(t, err)
	require.Len(t, resolution.CandidateGroups, 1)
	candidate := resolution.CandidateGroups[0]
	assert.Equal(t, archivedCandidateGroupStatePartialResumable, candidate.State)
	assert.Len(t, candidate.Resume.RemainingMemberMoves, 3)
	request := resumedArchivalValidationRequest(t, factory, current, initial.TargetSnapshot, candidate)
	request.ManagedSchemaOptions = []schema.GetSchemaOpt{
		schema.WithExcludeSchemaPatterns("archive.*"),
	}
	root := markerRoot(candidate.Marker)
	request.DataProbes = []archivalValidationDataProbe{{
		SetupDDL: `INSERT INTO "root"."events" VALUES (2, 'partial-row')`,
		RetentionQuery: fmt.Sprintf("SELECT count(*)::text FROM %s.%s",
			schema.EscapeIdentifier(root.CleanupTable.SchemaName),
			schema.EscapeIdentifier(root.CleanupTable.Name)),
		RetentionExpected: "1",
		CleanupQuery: fmt.Sprintf("SELECT (to_regclass(%s) IS NULL)::text",
			schema.EscapeLiteral(schema.EscapeIdentifier(root.CleanupTable.SchemaName)+"."+
				schema.EscapeIdentifier(root.CleanupTable.Name))),
		CleanupExpected: "true",
	}}
	require.NoError(t, validateArchivalPlanTwoPhase(t.Context(), request))
}

func newArchivalValidationFixture(
	t *testing.T,
	factory *testdb.Factory,
) (archivalPlanValidationRequest, *tempdb.Database, string) {
	t.Helper()
	sourceDB := factory.CreateDatabase(t)
	_, err := sourceDB.ConnPool.Exec(t.Context(), `
		CREATE SCHEMA managed;
		CREATE TABLE managed.archived (
			id bigint PRIMARY KEY,
			payload text NOT NULL
		);
		CREATE INDEX archived_payload_idx ON managed.archived (payload);
		GRANT SELECT ON managed.archived TO PUBLIC;
		CREATE TABLE managed.keep (
			id bigint PRIMARY KEY,
			payload text NOT NULL
		);
	`)
	require.NoError(t, err)
	targetDB := factory.CreateDatabase(t)
	_, err = targetDB.ConnPool.Exec(t.Context(), `
		CREATE SCHEMA managed;
		CREATE TABLE managed.keep (
			id bigint PRIMARY KEY,
			payload text NOT NULL
		);
	`)
	require.NoError(t, err)
	current, err := schema.GetSchemaSnapshot(t.Context(), sourceDB.ConnPool)
	require.NoError(t, err)
	target, err := schema.GetSchemaSnapshot(t.Context(), targetDB.ConnPool)
	require.NoError(t, err)
	archived := mustCatalogRelationByName(t, current.Inventory, "managed", "archived")
	regularRequest, marker := plainTableArchivalDatabaseRequest(t, current.Inventory, archived,
		"20260722T101112123456Z_VALDAT01")
	preflight, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		ProposedTableRelationOIDs: []uint32{archived.OID},
	})
	require.NoError(t, err)
	closure, err := planArchivedDependencyClosure(archivedDependencyClosureRequest{
		CurrentSnapshot: current, TargetSnapshot: target, SourcePreflight: preflight,
		ProposedGroups: []archivedDependencyClosureGroupRequest{{
			GroupID: marker.GroupID, TableRelationOIDs: []uint32{archived.OID},
			DependencySchemaName: regularRequest.Groups[0].Allocation.DependencySchemaName,
		}},
	})
	require.NoError(t, err)
	marker.ExclusiveDependencyObjects = nil
	marker.SharedCleanupComponentGroupEdges = closure.SharedGroupEdges
	aclPlan, err := current.Inventory.PlanACLRevokes([]schema.CatalogDependencyObject{{
		ClassOID: pgClassCatalogOID, ObjectOID: archived.OID,
	}})
	require.NoError(t, err)
	for _, revoke := range aclPlan.Revokes {
		record, err := archivalMarkerACLRecord(current.Inventory, revoke.Grant)
		require.NoError(t, err)
		marker.OriginalACLs = append(marker.OriginalACLs, record)
	}
	regularRequest.Groups[0].FinalizedMarker, err = marshalArchivalMarker(marker)
	require.NoError(t, err)
	regularRequest.TargetSchema = target.Schema
	regularRequest.SourcePreflight = preflight
	regularRequest.DependencyClosure = closure
	groups, err := preparePlainTableArchivalGroups(regularRequest)
	require.NoError(t, err)
	groups, err = finalizeGlobalCleanupComponentInputs(groups, closure)
	require.NoError(t, err)
	isolation, err := planArchivalIsolation(current.Inventory, target.Schema, preflight, closure, groups)
	require.NoError(t, err)
	cleanup, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: current.Inventory, PredictedGroups: groups,
		RootIntents: archivalCleanupRootIntents(groups), DependencyClosure: closure,
		Isolation: isolation,
	})
	require.NoError(t, err)
	regularRequest.Groups[0].FinalizedMarker = cleanup.FinalizedMarkers[0].Text
	regularRequest.ComponentInitializationManaged = true
	diff, _, err := buildSchemaDiff(current.Schema, target.Schema)
	require.NoError(t, err)
	dispositions := tableDispositions{
		markerTableName(marker.Members[0].SourceTable).GetName(): {
			Kind: tableDispositionKindArchivalMove, GroupID: marker.GroupID,
			RelationOID: marker.Members[0].SourceTable.OID,
		},
	}
	ordinary, err := generateReplacementAwareSchemaSQL(
		newSchemaSQLGenerator(&deterministicRandReader{}, &planOptions{}),
		diff, dispositions, regularRequest,
	)
	require.NoError(t, err)
	ordinary = append(cleanup.ComponentInitializationStatements, ordinary...)
	return archivalPlanValidationRequest{
		TempDBFactory: factory, Prefix: "archive", CurrentSnapshot: current,
		TargetSnapshot: target, OrdinaryStatements: ordinary, Cleanup: cleanup,
		SourcePreflight: preflight, DependencyClosure: closure, Isolation: isolation,
	}, sourceDB, marker.Members[0].CleanupTable.SchemaName
}

func cloneArchivalValidationCleanup(cleanup globalCleanupPlanResult) globalCleanupPlanResult {
	result := cleanup
	result.Operations = cloneCleanupOperations(cleanup.Operations)
	result.CleanupStatements = slices.Clone(cleanup.CleanupStatements)
	result.FinalizedMarkers = slices.Clone(cleanup.FinalizedMarkers)
	result.FinalizedRegularGroups = slices.Clone(cleanup.FinalizedRegularGroups)
	result.ComponentInitializationStatements = slices.Clone(
		cleanup.ComponentInitializationStatements,
	)
	result.TrustedGroupIDs = slices.Clone(cleanup.TrustedGroupIDs)
	result.TrustedSchemaNames = slices.Clone(cleanup.TrustedSchemaNames)
	return result
}

func newPartitionArchivalValidationFixture(
	t *testing.T,
	factory *testdb.Factory,
	detached bool,
) (archivalPlanValidationRequest, *tempdb.Database) {
	t.Helper()
	sourceDB := factory.CreateDatabase(t)
	targetDB := factory.CreateDatabase(t)
	var sourceDDL string
	var targetDDL string
	var rootSchema string
	var rootName string
	var parent *schema.CatalogRelation
	if detached {
		sourceDDL = `
			CREATE SCHEMA active;
			CREATE SCHEMA branch;
			CREATE SCHEMA leaf;
			CREATE TABLE active.events (id integer NOT NULL, payload text NOT NULL)
				PARTITION BY RANGE (id);
			CREATE TABLE branch.old_events PARTITION OF active.events
				FOR VALUES FROM (0) TO (100) PARTITION BY RANGE (id);
			CREATE TABLE leaf.old_events_a PARTITION OF branch.old_events
				FOR VALUES FROM (0) TO (50);
			CREATE TABLE leaf.old_events_b PARTITION OF branch.old_events
				FOR VALUES FROM (50) TO (100);
		`
		targetDDL = `
			CREATE SCHEMA active;
			CREATE SCHEMA branch;
			CREATE SCHEMA leaf;
			CREATE TABLE active.events (id integer NOT NULL, payload text NOT NULL)
				PARTITION BY RANGE (id);
		`
		rootSchema, rootName = "branch", "old_events"
	} else {
		sourceDDL = `
			CREATE SCHEMA root;
			CREATE SCHEMA branch;
			CREATE SCHEMA leaf;
			CREATE TABLE root.events (id integer NOT NULL, payload text NOT NULL)
				PARTITION BY RANGE (id);
			CREATE TABLE branch.events_2026 PARTITION OF root.events
				FOR VALUES FROM (0) TO (100) PARTITION BY RANGE (id);
			CREATE TABLE leaf.events_a PARTITION OF branch.events_2026
				FOR VALUES FROM (0) TO (50);
			CREATE TABLE leaf.events_b PARTITION OF branch.events_2026
				FOR VALUES FROM (50) TO (100);
		`
		targetDDL = `CREATE SCHEMA root; CREATE SCHEMA branch; CREATE SCHEMA leaf;`
		rootSchema, rootName = "root", "events"
	}
	_, err := sourceDB.ConnPool.Exec(t.Context(), sourceDDL)
	require.NoError(t, err)
	_, err = targetDB.ConnPool.Exec(t.Context(), targetDDL)
	require.NoError(t, err)
	current, err := schema.GetSchemaSnapshot(t.Context(), sourceDB.ConnPool)
	require.NoError(t, err)
	target, err := schema.GetSchemaSnapshot(t.Context(), targetDB.ConnPool)
	require.NoError(t, err)
	root := mustCatalogRelationByName(t, current.Inventory, rootSchema, rootName)
	if detached {
		value := mustCatalogRelationByName(t, current.Inventory, "active", "events")
		parent = &value
	}
	regularRequest, marker := partitionArchivalDatabaseRequest(t, current.Inventory, root, parent,
		"20260722T111213123456Z_PARTVAL1")
	relationOIDs := plainMarkerRelationOIDs(marker)
	preflight, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		ProposedTableRelationOIDs: relationOIDs,
	})
	require.NoError(t, err)
	closure, err := planArchivedDependencyClosure(archivedDependencyClosureRequest{
		CurrentSnapshot: current, TargetSnapshot: target, SourcePreflight: preflight,
		ProposedGroups: []archivedDependencyClosureGroupRequest{{
			GroupID: marker.GroupID, TableRelationOIDs: relationOIDs,
			DependencySchemaName: regularRequest.Groups[0].Allocation.DependencySchemaName,
		}},
	})
	require.NoError(t, err)
	marker.ExclusiveDependencyObjects = nil
	for _, assignment := range closure.Assignments {
		if assignment.GroupID == marker.GroupID {
			marker.ExclusiveDependencyObjects = append(marker.ExclusiveDependencyObjects,
				assignment.Destination)
		}
	}
	marker.SharedCleanupComponentGroupEdges = closure.SharedGroupEdges
	regularRequest.Groups[0].FinalizedMarker, err = marshalArchivalMarker(marker)
	require.NoError(t, err)
	regularRequest.TargetSchema = target.Schema
	regularRequest.SourcePreflight = preflight
	regularRequest.DependencyClosure = closure
	groups, err := preparePlainTableArchivalGroups(regularRequest)
	require.NoError(t, err)
	groups, err = finalizeGlobalCleanupComponentInputs(groups, closure)
	require.NoError(t, err)
	isolation, err := planArchivalIsolation(current.Inventory, target.Schema, preflight, closure, groups)
	require.NoError(t, err)
	cleanup, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: current.Inventory, PredictedGroups: groups,
		RootIntents: archivalCleanupRootIntents(groups), DependencyClosure: closure,
		Isolation: isolation,
	})
	require.NoError(t, err)
	regularRequest.Groups[0].FinalizedMarker = cleanup.FinalizedMarkers[0].Text
	regularRequest.ComponentInitializationManaged = true
	diff, _, err := buildSchemaDiff(current.Schema, target.Schema)
	require.NoError(t, err)
	dispositions := make(tableDispositions, len(marker.Members))
	for _, member := range marker.Members {
		dispositions[markerTableName(member.SourceTable).GetName()] = tableDisposition{
			Kind: tableDispositionKindArchivalMove, GroupID: marker.GroupID,
			RelationOID: member.SourceTable.OID,
		}
	}
	ordinary, err := generateReplacementAwareSchemaSQL(
		newSchemaSQLGenerator(&deterministicRandReader{}, &planOptions{}),
		diff, dispositions, regularRequest,
	)
	require.NoError(t, err)
	ordinary = append(cleanup.ComponentInitializationStatements, ordinary...)
	rootMember := markerRoot(cleanup.FinalizedMarkers[0].Payload)
	setupTable := schema.EscapeIdentifier(root.SchemaName) + "." + schema.EscapeIdentifier(root.Name)
	if detached {
		setupTable = `"active"."events"`
	}
	return archivalPlanValidationRequest{
		TempDBFactory: factory, Prefix: "archive", CurrentSnapshot: current,
		TargetSnapshot: target, OrdinaryStatements: ordinary, Cleanup: cleanup,
		SourcePreflight: preflight, DependencyClosure: closure, Isolation: isolation,
		DataProbes: []archivalValidationDataProbe{{
			SetupDDL: fmt.Sprintf("INSERT INTO %s VALUES (1, 'partition-row')", setupTable),
			RetentionQuery: fmt.Sprintf("SELECT count(*)::text FROM %s.%s",
				schema.EscapeIdentifier(rootMember.CleanupTable.SchemaName),
				schema.EscapeIdentifier(rootMember.CleanupTable.Name)),
			RetentionExpected: "1",
			CleanupQuery: fmt.Sprintf("SELECT (to_regclass(%s) IS NULL)::text",
				schema.EscapeLiteral(schema.EscapeIdentifier(rootMember.CleanupTable.SchemaName)+"."+
					schema.EscapeIdentifier(rootMember.CleanupTable.Name))),
			CleanupExpected: "true",
		}},
	}, sourceDB
}

func resumedArchivalValidationRequest(
	t *testing.T,
	factory *testdb.Factory,
	current schema.SchemaSnapshot,
	target schema.SchemaSnapshot,
	candidate structurallyValidArchivedCandidateGroup,
) archivalPlanValidationRequest {
	t.Helper()
	relationOIDs := plainMarkerRelationOIDs(candidate.Marker)
	preflight, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		ProposedTableRelationOIDs: relationOIDs,
	})
	require.NoError(t, err)
	closure, err := planArchivedDependencyClosure(archivedDependencyClosureRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		CandidateGroups: []structurallyValidArchivedCandidateGroup{candidate},
		SourcePreflight: preflight,
	})
	require.NoError(t, err)
	markerText, err := marshalArchivalMarker(candidate.Marker)
	require.NoError(t, err)
	regularRequest := plainTableArchivalRequest{
		CurrentInventory: current.Inventory, TargetSchema: target.Schema,
		Groups: []plainTableArchivalGroupRequest{{
			FinalizedMarker: markerText, Resume: &candidate.Resume,
		}},
		SourcePreflight: preflight, DependencyClosure: closure,
	}
	groups, err := preparePlainTableArchivalGroups(regularRequest)
	require.NoError(t, err)
	groups, err = finalizeGlobalCleanupComponentInputs(groups, closure)
	require.NoError(t, err)
	isolation, err := planArchivalIsolation(current.Inventory, target.Schema, preflight, closure, groups)
	require.NoError(t, err)
	cleanup, err := planGlobalCleanup(globalCleanupPlanRequest{
		CurrentInventory: current.Inventory, PredictedGroups: groups,
		RootIntents: archivalCleanupRootIntents(groups), DependencyClosure: closure,
		Isolation: isolation,
	})
	require.NoError(t, err)
	regularRequest.Groups[0].FinalizedMarker = cleanup.FinalizedMarkers[0].Text
	regularRequest.ComponentInitializationManaged = true
	diff, _, err := buildSchemaDiff(current.Schema, target.Schema)
	require.NoError(t, err)
	dispositions := make(tableDispositions, len(candidate.Resume.RemainingMemberMoves))
	for _, move := range candidate.Resume.RemainingMemberMoves {
		dispositions[markerTableName(move.SourceTable).GetName()] = tableDisposition{
			Kind: tableDispositionKindArchivalMove, GroupID: candidate.GroupID,
			RelationOID: move.RelationOID,
		}
	}
	ordinary, err := generateReplacementAwareSchemaSQL(
		newSchemaSQLGenerator(&deterministicRandReader{}, &planOptions{}),
		diff, dispositions, regularRequest,
	)
	require.NoError(t, err)
	ordinary = append(cleanup.ComponentInitializationStatements, ordinary...)
	return archivalPlanValidationRequest{
		TempDBFactory: factory, Prefix: "archive", CurrentSnapshot: current,
		TargetSnapshot: target, OrdinaryStatements: ordinary, Cleanup: cleanup,
		SourcePreflight: preflight, DependencyClosure: closure, Isolation: isolation,
	}
}
