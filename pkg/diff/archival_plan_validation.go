package diff

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

// archivalPlanValidationRequest is the dormant Stage 17 boundary. It is kept
// separate from assertValidPlan until archival activation in Stage 19.
type archivalPlanValidationRequest struct {
	TempDBFactory tempdb.Factory
	Logger        *slog.Logger
	Prefix        string

	CurrentSnapshot schema.SchemaSnapshot
	TargetSnapshot  schema.SchemaSnapshot

	OrdinaryStatements []Statement
	Cleanup            globalCleanupPlanResult
	SourcePreflight    sourceSafetyPreflightResult
	DependencyClosure  archivedDependencyClosureResult
	Isolation          archivalIsolationPlan

	// ManagedSchemaOptions are the caller's ordinary schema filters. Trusted
	// archival schemas are excluded separately and only after their markers and
	// catalogs have been validated.
	ManagedSchemaOptions []schema.GetSchemaOpt

	// DataProbes and PostRetentionSetupStatements are internal validation
	// fixtures. Stage 19 may derive probes for tables that admit a safe row; the
	// setup seam also proves that a later RESTRICT failure is surfaced rather
	// than converted to CASCADE behavior.
	DataProbes                   []archivalValidationDataProbe
	PostRetentionSetupStatements []Statement
}

type archivalValidationDataProbe struct {
	SetupDDL          string
	RetentionQuery    string
	RetentionExpected string
	CleanupQuery      string
	CleanupExpected   string
}

type archivalValidationOIDTranslation struct {
	bySourceAddress map[archivedDependencyAddressKey]uint32
	toastIdentities map[uint32]archivalMarkerObjectIdentity
}

type translatedArchivalValidationState struct {
	groups       []preparedArchivalGroup
	markers      []globalCleanupFinalizedMarker
	isolation    archivalIsolationPlan
	operations   []cleanupOperationV1
	statements   []Statement
	trustedNames []string
}

// validateArchivalPlanTwoPhase validates modeled archival behavior without
// changing public plan generation. Production OIDs are first proven against
// the source snapshot, then translated to identities reconstructed in one
// temporary database. The unmodified production-OID assertion SQL is never
// claimed as having run in the synthetic database.
func validateArchivalPlanTwoPhase(ctx context.Context, request archivalPlanValidationRequest) error {
	if err := validateArchivalPlanSourceFacts(request); err != nil {
		return fmt.Errorf("validating archival source facts: %w", err)
	}
	if request.TempDBFactory == nil {
		return fmt.Errorf("creating archival validation database: %w", errTempDbFactoryRequired)
	}
	logger := request.Logger
	if logger == nil {
		logger = slog.Default()
	}

	db, err := request.TempDBFactory.Create(ctx)
	if err != nil {
		return fmt.Errorf("creating archival validation database: %w", err)
	}
	defer func() {
		if err := db.Close(context.WithoutCancel(ctx)); err != nil {
			logger.ErrorContext(ctx, "failed to drop archival validation database", slog.Any("error", err))
		}
	}()

	reconstruction := request.CurrentSnapshot.UnfilteredSchema
	if reflect.DeepEqual(reconstruction, schema.Schema{}) {
		return fmt.Errorf("current snapshot has no unfiltered modeled schema for archival reconstruction")
	}
	reconstruction, deferredPartitionAttachments :=
		prepareArchivalValidationReconstruction(reconstruction)
	if err := setSchemaForEmptyDatabase(ctx, db, reconstruction, &planOptions{}); err != nil {
		return fmt.Errorf("reconstructing current modeled schema: %w", err)
	}
	for _, statement := range deferredPartitionAttachments {
		if _, err := db.ConnPool.Exec(ctx, statement.DDL); err != nil {
			return fmt.Errorf("reconstructing partitioned partition attachment: %w", err)
		}
	}
	if err := reconstructArchivalValidationFollowers(ctx, db, request); err != nil {
		return err
	}

	reconstructed, err := schema.GetSchemaSnapshot(ctx, db.ConnPool)
	if err != nil {
		return fmt.Errorf("fetching reconstructed archival schema: %w", err)
	}
	translation, err := buildArchivalValidationOIDTranslation(
		request.CurrentSnapshot.Inventory, reconstructed.Inventory, request.Cleanup.FinalizedMarkers,
	)
	if err != nil {
		return err
	}
	translated, err := translateArchivalValidationRequest(request, translation)
	if err != nil {
		return err
	}
	if err := installExistingTranslatedArchivalMarkers(ctx, db, request.CurrentSnapshot.Inventory,
		translated.markers); err != nil {
		return err
	}
	for idx, probe := range request.DataProbes {
		if _, err := db.ConnPool.Exec(ctx, probe.SetupDDL); err != nil {
			return fmt.Errorf("setting up archival data probe %d: %w", idx, err)
		}
	}

	if err := executeStatementsIgnoreTimeouts(ctx, db.ConnPool, translated.statements); err != nil {
		return fmt.Errorf("running archival retention statements: %w", err)
	}
	postRetention, err := schema.GetSchemaSnapshot(ctx, db.ConnPool)
	if err != nil {
		return fmt.Errorf("fetching post-retention schema: %w", err)
	}
	if err := validateArchivalRetentionPostcondition(ctx, db, request, translated, postRetention); err != nil {
		return fmt.Errorf("validating archival retention postcondition: %w", err)
	}
	for idx, probe := range request.DataProbes {
		if err := validateArchivalDataProbe(ctx, db, probe.RetentionQuery, probe.RetentionExpected); err != nil {
			return fmt.Errorf("validating archival retention data probe %d: %w", idx, err)
		}
	}

	for _, statement := range request.PostRetentionSetupStatements {
		if _, err := db.ConnPool.Exec(ctx, statement.ToSQL()); err != nil {
			return fmt.Errorf("installing post-retention validation state: %w", err)
		}
	}
	postRetention, err = schema.GetSchemaSnapshot(ctx, db.ConnPool)
	if err != nil {
		return fmt.Errorf("fetching cleanup precondition schema: %w", err)
	}
	if err := validateCleanupOperationTargets(postRetention.Inventory, translated.operations,
		translated.markers, true); err != nil {
		return fmt.Errorf("validating cleanup operation targets before execution: %w", err)
	}
	managedBeforeCleanup := archivalManagedCatalogMetadata(postRetention.Inventory,
		translated.trustedNames, translated.markers)

	if err := executeStatementsIgnoreTimeouts(ctx, db.ConnPool,
		request.Cleanup.CleanupStatements); err != nil {
		return fmt.Errorf("running archival cleanup statements: %w", err)
	}
	postCleanup, err := schema.GetSchemaSnapshot(ctx, db.ConnPool)
	if err != nil {
		return fmt.Errorf("fetching post-cleanup schema: %w", err)
	}
	if err := validateArchivalCleanupPostcondition(ctx, db, request, translated, postCleanup,
		managedBeforeCleanup); err != nil {
		return fmt.Errorf("validating archival cleanup postcondition: %w", err)
	}
	for idx, probe := range request.DataProbes {
		if err := validateArchivalDataProbe(ctx, db, probe.CleanupQuery, probe.CleanupExpected); err != nil {
			return fmt.Errorf("validating archival cleanup data probe %d: %w", idx, err)
		}
	}
	return nil
}

func prepareArchivalValidationReconstruction(source schema.Schema) (schema.Schema, []Statement) {
	result := source
	result.Tables = slices.Clone(source.Tables)
	var attachments []Statement
	for idx := range result.Tables {
		table := &result.Tables[idx]
		if !table.IsPartition() || !table.IsPartitioned() {
			continue
		}
		attachments = append(attachments, Statement{DDL: fmt.Sprintf(
			"ALTER TABLE %s ATTACH PARTITION %s %s",
			table.ParentTable.GetFQEscapedName(), table.GetFQEscapedName(), table.ForValues,
		)})
		table.ParentTable = nil
		table.ForValues = ""
	}
	return result, attachments
}

func validateArchivalPlanSourceFacts(request archivalPlanValidationRequest) error {
	if err := validateSchemaPartialArchivalPrefix(request.Prefix); err != nil {
		return err
	}
	if err := validateNoReservedArchivalTargetSchemas(request.Prefix,
		request.TargetSnapshot.Inventory); err != nil {
		return err
	}
	markersByID, err := validationFinalizedMarkersByID(request.Cleanup)
	if err != nil {
		return err
	}
	for _, finalized := range request.Cleanup.FinalizedMarkers {
		for _, schemaName := range archivedMarkerSchemaNames(finalized.Payload) {
			if err := validateArchivedMarkerNames(request.Prefix, schemaName, finalized.Payload); err != nil {
				return fmt.Errorf("validating finalized marker for group %q: %w", finalized.GroupID, err)
			}
		}
	}
	if err := validateCleanupOperationTargets(request.CurrentSnapshot.Inventory,
		request.Cleanup.Operations, request.Cleanup.FinalizedMarkers, false); err != nil {
		return err
	}
	if err := validateRenderedCleanupStatements(request.Cleanup); err != nil {
		return err
	}
	if err := validateCleanupMarkerDigests(request.Cleanup.Operations,
		request.Cleanup.FinalizedMarkers); err != nil {
		return err
	}

	var relationOIDs []uint32
	for _, marker := range request.Cleanup.FinalizedMarkers {
		for _, member := range marker.Payload.Members {
			relationOIDs = append(relationOIDs, member.SourceTable.OID)
		}
	}
	slices.Sort(relationOIDs)
	relationOIDs = slices.Compact(relationOIDs)
	preflight, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: request.CurrentSnapshot, TargetSnapshot: request.TargetSnapshot,
		ProposedTableRelationOIDs: relationOIDs,
	})
	if err != nil {
		return err
	}
	var additionalExpected []sourceSafetyExpectedRetainedObject
	for _, expected := range request.SourcePreflight.ExpectedRetainedObjects {
		if !slices.ContainsFunc(preflight.ExpectedRetainedObjects,
			func(baseline sourceSafetyExpectedRetainedObject) bool {
				return baseline.TableRelationOID == expected.TableRelationOID &&
					sameArchivedDependencyAddress(baseline.Address, expected.Address)
			}) {
			additionalExpected = append(additionalExpected, expected)
		}
	}
	if len(additionalExpected) > 0 {
		preflight, err = runSourceSafetyPreflight(sourceSafetyPreflightRequest{
			CurrentSnapshot: request.CurrentSnapshot, TargetSnapshot: request.TargetSnapshot,
			ProposedTableRelationOIDs: relationOIDs, ExpectedRetainedObjects: additionalExpected,
		})
		if err != nil {
			return err
		}
	}
	if !reflect.DeepEqual(preflight, request.SourcePreflight) {
		return fmt.Errorf("stage 10 source preflight product does not match the immutable source snapshot")
	}

	resolution, err := resolveArchivedState(request.Prefix, request.CurrentSnapshot, request.TargetSnapshot)
	if err != nil {
		return err
	}
	if err := validateArchivalValidationSourceCollisions(request, resolution); err != nil {
		return err
	}
	closureRequest := archivedDependencyClosureRequest{
		CurrentSnapshot: request.CurrentSnapshot, TargetSnapshot: request.TargetSnapshot,
		CandidateGroups: resolution.CandidateGroups, SourcePreflight: preflight,
	}
	for _, group := range request.Cleanup.FinalizedRegularGroups {
		if group.allocation == nil {
			continue
		}
		closureRequest.ProposedGroups = append(closureRequest.ProposedGroups,
			archivedDependencyClosureGroupRequest{
				GroupID: group.id, TableRelationOIDs: plainTableArchivalRelationOIDs([]preparedArchivalGroup{group}),
				DependencySchemaName: group.marker.ExclusiveDependencySchemas[0].Name,
			})
	}
	closure, err := planArchivedDependencyClosure(closureRequest)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(closure, request.DependencyClosure) {
		return fmt.Errorf("stage 11 dependency closure product does not match the immutable source snapshot")
	}

	isolation, err := planArchivalIsolation(request.CurrentSnapshot.Inventory,
		request.TargetSnapshot.Schema, preflight, closure, request.Cleanup.FinalizedRegularGroups)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(isolation, request.Isolation) {
		return fmt.Errorf("stage 14 isolation product does not match the source ACL and dependency facts")
	}
	if err := validateOrdinaryArchivalTargetScope(request, isolation); err != nil {
		return err
	}
	if err := validateArchivalStaticMetadata(request.CurrentSnapshot.Inventory,
		request.Cleanup.FinalizedMarkers, request.OrdinaryStatements); err != nil {
		return err
	}
	for _, group := range request.Cleanup.FinalizedRegularGroups {
		if _, ok := markersByID[group.id]; !ok {
			return fmt.Errorf("finalized regular group %q has no Stage 16 marker", group.id)
		}
		expected := renderPlainTableArchivalAssertion(group,
			mustArchivalIsolationGroup(request.Isolation, group.id), request.Isolation)
		if countStatementDDL(request.OrdinaryStatements, expected) != 1 {
			return fmt.Errorf("ordinary statements do not contain exactly one finalized assertion for group %q", group.id)
		}
	}
	return nil
}

func validateArchivalValidationSourceCollisions(
	request archivalPlanValidationRequest,
	resolution archivedStateResolution,
) error {
	existingBySchema := make(map[string]archivalGroupID)
	for _, candidate := range resolution.CandidateGroups {
		for _, schemaName := range candidate.SchemaNames {
			existingBySchema[schemaName] = candidate.GroupID
		}
	}
	currentSchemas, err := uniqueCatalogSchemas(request.CurrentSnapshot.Inventory)
	if err != nil {
		return err
	}
	for _, finalized := range request.Cleanup.FinalizedMarkers {
		for _, schemaName := range archivedMarkerSchemaNames(finalized.Payload) {
			if _, exists := currentSchemas[schemaName]; !exists {
				continue
			}
			if existingBySchema[schemaName] != finalized.GroupID {
				return fmt.Errorf("archival schema %q collides with state outside trusted group %q",
					schemaName, finalized.GroupID)
			}
		}
	}
	for _, group := range request.Cleanup.FinalizedRegularGroups {
		if group.allocation == nil {
			continue
		}
		var detached *archivalDetachedSubtreeRequest
		if group.detach != nil {
			detached = &archivalDetachedSubtreeRequest{
				RootRelationOID:   group.detach.root.SourceTable.OID,
				ParentRelationOID: group.detach.parent.OID,
			}
		}
		if err := validateNewArchivalGroup(request.CurrentSnapshot.Inventory, group, detached); err != nil {
			return fmt.Errorf("revalidating archival allocation and namespace collisions: %w", err)
		}
	}
	return nil
}

func validateOrdinaryArchivalTargetScope(
	request archivalPlanValidationRequest,
	isolation archivalIsolationPlan,
) error {
	graph, err := buildPlainTableArchivalGraph(request.CurrentSnapshot.Inventory,
		request.Cleanup.FinalizedRegularGroups, isolation)
	if err != nil {
		return err
	}
	expected, err := graph.toOrderedStatements()
	if err != nil {
		return err
	}
	allowed := make(map[string]struct{}, len(expected)+
		len(request.Cleanup.MarkerUpdateStatements))
	for _, statement := range expected {
		allowed[statement.DDL] = struct{}{}
	}
	for _, statement := range request.Cleanup.MarkerUpdateStatements {
		allowed[statement.DDL] = struct{}{}
	}
	for idx, statement := range request.OrdinaryStatements {
		if _, archivalStatement := allowed[statement.DDL]; archivalStatement {
			continue
		}
		for _, schemaName := range request.Cleanup.TrustedSchemaNames {
			if strings.Contains(statement.DDL, schema.EscapeIdentifier(schemaName)) {
				return fmt.Errorf("ordinary statement %d targets a trusted archival schema outside the typed archival graph", idx)
			}
		}
		for _, group := range request.Cleanup.FinalizedRegularGroups {
			for _, member := range group.members {
				if member.remainingMove == nil {
					continue
				}
				sourceName := schema.EscapeIdentifier(member.remainingMove.SourceTable.SchemaName) + "." +
					schema.EscapeIdentifier(member.remainingMove.SourceTable.Name)
				moveDDL := renderArchivalMemberMove(member)[0].DDL
				moveIndex := slices.IndexFunc(request.OrdinaryStatements, func(candidate Statement) bool {
					return candidate.DDL == moveDDL
				})
				if moveIndex < 0 {
					return fmt.Errorf("ordinary statements omit the archival move for member %q", member.marker.MemberID)
				}
				if idx < moveIndex && strings.Contains(statement.DDL, sourceName) {
					return fmt.Errorf("ordinary statement %d changes archival member %q before its move", idx,
						member.marker.MemberID)
				}
			}
		}
	}
	return nil
}

func validationFinalizedMarkersByID(
	cleanup globalCleanupPlanResult,
) (map[archivalGroupID]globalCleanupFinalizedMarker, error) {
	result := make(map[archivalGroupID]globalCleanupFinalizedMarker, len(cleanup.FinalizedMarkers))
	var ids []archivalGroupID
	var names []string
	for _, marker := range cleanup.FinalizedMarkers {
		if marker.GroupID != marker.Payload.GroupID {
			return nil, fmt.Errorf("finalized marker group ID %q disagrees with payload %q",
				marker.GroupID, marker.Payload.GroupID)
		}
		text, err := marshalArchivalMarker(marker.Payload)
		if err != nil {
			return nil, err
		}
		if text != marker.Text {
			return nil, fmt.Errorf("finalized marker for group %q is not its canonical payload", marker.GroupID)
		}
		if _, duplicate := result[marker.GroupID]; duplicate {
			return nil, fmt.Errorf("finalized marker group %q is duplicated", marker.GroupID)
		}
		result[marker.GroupID] = marker
		ids = append(ids, marker.GroupID)
		names = append(names, archivedMarkerSchemaNames(marker.Payload)...)
	}
	slices.Sort(ids)
	slices.Sort(names)
	names = slices.Compact(names)
	trustedIDs := slices.Clone(cleanup.TrustedGroupIDs)
	slices.Sort(trustedIDs)
	trustedNames := slices.Clone(cleanup.TrustedSchemaNames)
	slices.Sort(trustedNames)
	trustedNames = slices.Compact(trustedNames)
	if !slices.Equal(ids, trustedIDs) || !slices.Equal(names, trustedNames) {
		return nil, fmt.Errorf("stage 16 trusted groups or schemas do not match its finalized markers")
	}
	return result, nil
}

func validateRenderedCleanupStatements(cleanup globalCleanupPlanResult) error {
	groups := make([]globalCleanupGroup, 0, len(cleanup.FinalizedMarkers))
	for _, finalized := range cleanup.FinalizedMarkers {
		root, err := globalCleanupRootFromMarker(finalized.Payload)
		if err != nil {
			return err
		}
		groups = append(groups, globalCleanupGroup{
			id: finalized.GroupID, marker: finalized.Payload, cleanupRoot: root,
			hasIndexes: archivalMarkerHasIndexes(finalized.Payload),
		})
	}
	canonical, err := canonicalizeCleanupOperations(cleanup.Operations)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(canonical, cleanup.Operations) {
		return fmt.Errorf("stage 16 cleanup operations are not canonical")
	}
	rendered, err := renderGlobalCleanupStatements(cleanup.Operations, groups)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(rendered, cleanup.CleanupStatements) {
		return fmt.Errorf("stage 16 cleanup statements do not match its canonical operations")
	}
	return nil
}

func validateCleanupMarkerDigests(
	operations []cleanupOperationV1,
	markers []globalCleanupFinalizedMarker,
) error {
	byID := make(map[archivalGroupID]archivalMarkerV1, len(markers))
	for _, marker := range markers {
		byID[marker.GroupID] = marker.Payload
	}
	for _, finalized := range markers {
		component := map[archivalGroupID]struct{}{finalized.GroupID: {}}
		for changed := true; changed; {
			changed = false
			for _, marker := range markers {
				for _, edge := range marker.Payload.SharedCleanupComponentGroupEdges {
					_, first := component[edge.FirstGroupID]
					_, second := component[edge.SecondGroupID]
					if first && !second {
						component[edge.SecondGroupID] = struct{}{}
						changed = true
					}
					if second && !first {
						component[edge.FirstGroupID] = struct{}{}
						changed = true
					}
				}
			}
		}
		var componentOperations []cleanupOperationV1
		for _, operation := range operations {
			if cleanupOperationOwnedByComponent(operation, component, byID) {
				componentOperations = append(componentOperations, operation)
			}
		}
		digest, err := computeCleanupOperationDigest(componentOperations)
		if err != nil {
			return err
		}
		if digest != finalized.Payload.CleanupDigest {
			return fmt.Errorf("finalized marker for group %q has a stale cleanup digest", finalized.GroupID)
		}
	}
	return nil
}

func cleanupOperationOwnedByComponent(
	operation cleanupOperationV1,
	component map[archivalGroupID]struct{},
	markers map[archivalGroupID]archivalMarkerV1,
) bool {
	for groupID := range component {
		marker := markers[groupID]
		if operation.Kind == cleanupOperationKindDropSchema &&
			slices.Contains(archivedMarkerSchemaNames(marker), operation.Object.Name) {
			return true
		}
		root, err := globalCleanupRootFromMarker(marker)
		if err == nil && compareMarkerObjects(root, operation.Object) == 0 {
			return true
		}
		if containsMarkerObject(marker.ExclusiveDependencyObjects, operation.Object) {
			return true
		}
	}
	return false
}

func validateArchivalStaticMetadata(
	inventory schema.CatalogInventory,
	markers []globalCleanupFinalizedMarker,
	ordinary []Statement,
) error {
	for _, finalized := range markers {
		for _, member := range finalized.Payload.Members {
			relation, err := uniqueRelationByOID(inventory, member.SourceTable.OID)
			if err != nil {
				return err
			}
			if relation.Persistence == schema.RelationPersistenceTemporary {
				return fmt.Errorf("temporary archival relation %s.%s has no static preservation proof",
					relation.SchemaName, relation.Name)
			}
			for _, label := range inventory.SecurityLabels {
				if label.RelationOID == relation.OID {
					return fmt.Errorf("security label provider %q on archival relation %s.%s is not synthetically reconstructable",
						label.Provider, relation.SchemaName, relation.Name)
				}
			}
			for _, table := range inventory.Tables {
				if table.RelationOID == relation.OID && table.AccessMethodName != "" &&
					table.AccessMethodName != "heap" {
					return fmt.Errorf("table access method %q on archival relation %s.%s has no static preservation proof",
						table.AccessMethodName, relation.SchemaName, relation.Name)
				}
			}
			for _, statement := range ordinary {
				upper := strings.ToUpper(statement.DDL)
				if !strings.Contains(upper, "ALTER TABLE") {
					continue
				}
				for _, unsupported := range []string{
					" SET TABLESPACE ",
					" SET ACCESS METHOD ", " SECURITY LABEL ",
				} {
					if strings.Contains(upper, unsupported) &&
						(strings.Contains(statement.DDL, member.SourceTable.Name) ||
							strings.Contains(statement.DDL, member.CleanupTable.Name)) {
						return fmt.Errorf("ordinary SQL changes statically preserved metadata for archival member %q",
							member.MemberID)
					}
				}
			}
		}
	}
	return nil
}

func mustArchivalIsolationGroup(
	plan archivalIsolationPlan,
	groupID archivalGroupID,
) archivalIsolationGroupPlan {
	group, _ := archivalIsolationGroupPlanByID(plan.Groups, groupID)
	return group
}

func countStatementDDL(statements []Statement, ddl string) int {
	count := 0
	for _, statement := range statements {
		if statement.DDL == ddl {
			count++
		}
	}
	return count
}

func reconstructArchivalValidationFollowers(
	ctx context.Context,
	db *tempdb.Database,
	request archivalPlanValidationRequest,
) error {
	trustedRelations := make(map[uint32]struct{})
	for _, finalized := range request.Cleanup.FinalizedMarkers {
		for _, member := range finalized.Payload.Members {
			trustedRelations[member.SourceTable.OID] = struct{}{}
		}
	}
	for _, rule := range request.CurrentSnapshot.Inventory.Rules {
		if _, trusted := trustedRelations[rule.RelationOID]; !trusted || rule.Name == "_RETURN" {
			continue
		}
		if _, err := db.ConnPool.Exec(ctx, rule.Definition); err != nil {
			return fmt.Errorf("reconstructing archived rule %q: %w", rule.Name, err)
		}
		if rule.Comment != "" {
			relation, err := uniqueRelationByOID(request.CurrentSnapshot.Inventory, rule.RelationOID)
			if err != nil {
				return err
			}
			ddl := fmt.Sprintf("COMMENT ON RULE %s ON %s.%s IS %s",
				schema.EscapeIdentifier(rule.Name), schema.EscapeIdentifier(relation.SchemaName),
				schema.EscapeIdentifier(relation.Name), schema.EscapeLiteral(rule.Comment))
			if _, err := db.ConnPool.Exec(ctx, ddl); err != nil {
				return fmt.Errorf("reconstructing archived rule comment %q: %w", rule.Name, err)
			}
		}
	}
	for _, statistic := range request.CurrentSnapshot.Inventory.ExtendedStatistics {
		if _, trusted := trustedRelations[statistic.RelationOID]; !trusted {
			continue
		}
		ddl, err := archivalValidationStatisticDDL(request.CurrentSnapshot.Inventory, statistic)
		if err != nil {
			return err
		}
		if _, err := db.ConnPool.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("reconstructing archived extended statistic %s.%s: %w",
				statistic.SchemaName, statistic.Name, err)
		}
	}
	return nil
}

func archivalValidationStatisticDDL(
	inventory schema.CatalogInventory,
	statistic schema.CatalogExtendedStatistic,
) (string, error) {
	relation, err := uniqueRelationByOID(inventory, statistic.RelationOID)
	if err != nil {
		return "", err
	}
	var expressions []string
	for _, number := range statistic.ColumnNumbers {
		found := false
		for _, column := range inventory.Columns {
			if column.RelationOID == statistic.RelationOID && column.Number == number {
				expressions = append(expressions, schema.EscapeIdentifier(column.Name))
				found = true
				break
			}
		}
		if !found {
			return "", fmt.Errorf("extended statistic %s.%s references missing column number %d",
				statistic.SchemaName, statistic.Name, number)
		}
	}
	expressions = append(expressions, statistic.Expressions...)
	if len(expressions) == 0 {
		return "", fmt.Errorf("extended statistic %s.%s has no reconstructable expressions",
			statistic.SchemaName, statistic.Name)
	}
	var kinds []string
	for _, kind := range statistic.Kinds {
		switch kind {
		case "d":
			kinds = append(kinds, "ndistinct")
		case "f":
			kinds = append(kinds, "dependencies")
		case "m":
			kinds = append(kinds, "mcv")
		case "e":
		default:
			return "", fmt.Errorf("extended statistic %s.%s has unsupported kind %q",
				statistic.SchemaName, statistic.Name, kind)
		}
	}
	kindSQL := ""
	if len(kinds) > 0 {
		kindSQL = " (" + strings.Join(kinds, ", ") + ")"
	}
	return fmt.Sprintf("CREATE STATISTICS %s.%s%s ON %s FROM %s.%s",
		schema.EscapeIdentifier(statistic.SchemaName), schema.EscapeIdentifier(statistic.Name), kindSQL,
		strings.Join(expressions, ", "), schema.EscapeIdentifier(relation.SchemaName),
		schema.EscapeIdentifier(relation.Name)), nil
}

func buildArchivalValidationOIDTranslation(
	source schema.CatalogInventory,
	temporary schema.CatalogInventory,
	markers []globalCleanupFinalizedMarker,
) (archivalValidationOIDTranslation, error) {
	translation := archivalValidationOIDTranslation{
		bySourceAddress: make(map[archivedDependencyAddressKey]uint32),
		toastIdentities: make(map[uint32]archivalMarkerObjectIdentity),
	}
	for _, finalized := range markers {
		add := func(object archivalMarkerObjectIdentity) error {
			if object.OID == 0 {
				return nil
			}
			translated, err := matchArchivalValidationObject(source, temporary, object)
			if err != nil {
				return fmt.Errorf("translating %s OID %d for group %q: %w",
					object.Kind, object.OID, finalized.GroupID, err)
			}
			key := archivedDependencyAddressKey{
				classOID: archivalMarkerCatalogClassOID(object.Kind), objectOID: object.OID,
			}
			if existing, duplicate := translation.bySourceAddress[key]; duplicate && existing != translated {
				return fmt.Errorf("source catalog address %d/%d maps to conflicting temporary OIDs %d and %d",
					key.classOID, key.objectOID, existing, translated)
			}
			translation.bySourceAddress[key] = translated
			if object.Kind == archivalMarkerObjectKindToastRelation {
				identity, count := currentCatalogObjectsWithOID(temporary, object.Kind, translated)
				if count != 1 {
					return fmt.Errorf("translated TOAST OID %d has %d temporary identities", translated, count)
				}
				translation.toastIdentities[object.OID] = identity
			}
			return nil
		}
		if err := walkArchivalMarkerObjects(finalized.Payload, add); err != nil {
			return archivalValidationOIDTranslation{}, err
		}
		translateTriggerMetadata := func(triggers []archivalMarkerClonedTriggerV1) error {
			for _, trigger := range triggers {
				if err := add(archivalMarkerObjectIdentity{
					Kind: archivalMarkerObjectKindFunction, OID: trigger.FunctionOID,
				}); err != nil {
					return err
				}
				if trigger.ConstraintOID != 0 {
					if err := add(archivalMarkerObjectIdentity{
						Kind: archivalMarkerObjectKindConstraint, OID: trigger.ConstraintOID,
					}); err != nil {
						return err
					}
				}
			}
			return nil
		}
		for _, edge := range finalized.Payload.PartitionEdges {
			if err := translateTriggerMetadata(edge.ClonedTriggers); err != nil {
				return archivalValidationOIDTranslation{}, err
			}
		}
		for _, lost := range finalized.Payload.LostParentAttachments {
			if err := translateTriggerMetadata(lost.ClonedTriggers); err != nil {
				return archivalValidationOIDTranslation{}, err
			}
		}
	}
	return translation, nil
}

func matchArchivalValidationObject(
	source schema.CatalogInventory,
	temporary schema.CatalogInventory,
	expected archivalMarkerObjectIdentity,
) (uint32, error) {
	actual, count := currentCatalogObjectsWithOID(source, expected.Kind, expected.OID)
	if count != 1 {
		return 0, fmt.Errorf("source catalog contains %d matching identities", count)
	}
	if expected.Kind == archivalMarkerObjectKindToastRelation {
		for _, relation := range source.Relations {
			if relation.ToastRelation == nil || relation.ToastRelation.OID != expected.OID {
				continue
			}
			for _, candidate := range temporary.Relations {
				if candidate.SchemaName == relation.SchemaName && candidate.Name == relation.Name &&
					candidate.ToastRelation != nil {
					return candidate.ToastRelation.OID, nil
				}
			}
		}
		return 0, fmt.Errorf("owning table TOAST identity was not reconstructed")
	}
	var matches []uint32
	for _, oid := range archivalValidationCandidateOIDs(temporary, expected.Kind) {
		candidate, candidateCount := currentCatalogObjectsWithOID(temporary, expected.Kind, oid)
		if candidateCount == 1 && candidate.SchemaName == actual.SchemaName &&
			candidate.Name == actual.Name && slices.Equal(candidate.IdentityArguments, actual.IdentityArguments) {
			matches = append(matches, oid)
		}
	}
	if len(matches) != 1 {
		return 0, fmt.Errorf("temporary catalog contains %d identity matches for %s",
			len(matches), markerObjectDisplayName(actual))
	}
	return matches[0], nil
}

func archivalValidationCandidateOIDs(
	inventory schema.CatalogInventory,
	kind archivalMarkerObjectKind,
) []uint32 {
	var result []uint32
	switch kind {
	case archivalMarkerObjectKindTable:
		for _, value := range inventory.Relations {
			result = append(result, value.OID)
		}
	case archivalMarkerObjectKindRowType, archivalMarkerObjectKindArrayType, archivalMarkerObjectKindType:
		for _, value := range inventory.Types {
			result = append(result, value.OID)
		}
	case archivalMarkerObjectKindIndex:
		for _, value := range inventory.Indexes {
			result = append(result, value.OID)
		}
	case archivalMarkerObjectKindConstraint:
		for _, value := range inventory.Constraints {
			result = append(result, value.OID)
		}
	case archivalMarkerObjectKindTrigger:
		for _, value := range inventory.Triggers {
			result = append(result, value.OID)
		}
	case archivalMarkerObjectKindRule:
		for _, value := range inventory.Rules {
			result = append(result, value.OID)
		}
	case archivalMarkerObjectKindPolicy:
		for _, value := range inventory.Policies {
			result = append(result, value.OID)
		}
	case archivalMarkerObjectKindOwnedSequence, archivalMarkerObjectKindSequence:
		for _, value := range inventory.Sequences {
			result = append(result, value.OID)
		}
	case archivalMarkerObjectKindExtendedStatistic:
		for _, value := range inventory.ExtendedStatistics {
			result = append(result, value.OID)
		}
	case archivalMarkerObjectKindFunction:
		for _, value := range inventory.Routines {
			result = append(result, value.OID)
		}
	case archivalMarkerObjectKindCollation:
		for _, value := range inventory.Collations {
			result = append(result, value.OID)
		}
	case archivalMarkerObjectKindOperator:
		for _, value := range inventory.Operators {
			result = append(result, value.OID)
		}
	case archivalMarkerObjectKindView, archivalMarkerObjectKindMaterializedView:
		for _, value := range inventory.Views {
			result = append(result, value.RelationOID)
		}
	}
	slices.Sort(result)
	return slices.Compact(result)
}

func walkArchivalMarkerObjects(marker archivalMarkerV1, visit func(archivalMarkerObjectIdentity) error) error {
	visitAll := func(objects ...archivalMarkerObjectIdentity) error {
		for _, object := range objects {
			if err := visit(object); err != nil {
				return err
			}
		}
		return nil
	}
	for _, member := range marker.Members {
		if err := visitAll(member.SourceTable, member.CleanupTable); err != nil {
			return err
		}
		if err := visitAll(slices.Concat(member.AutomaticallyMovedObjects, member.AttachedObjects,
			member.ExplicitlyMovedObjects, member.InternalToastObjects)...); err != nil {
			return err
		}
	}
	for _, edge := range marker.PartitionEdges {
		for _, attachment := range edge.PartitionedIndexAttachments {
			if err := visitAll(attachment.ParentIndex, attachment.ChildIndex); err != nil {
				return err
			}
		}
		for _, trigger := range edge.ClonedTriggers {
			if err := visitAll(trigger.ParentTrigger, trigger.ChildTrigger); err != nil {
				return err
			}
		}
	}
	for _, lost := range marker.LostParentAttachments {
		if err := visit(lost.ParentTable); err != nil {
			return err
		}
		for _, attachment := range lost.PartitionedIndexAttachments {
			if err := visitAll(attachment.ParentIndex, attachment.ChildIndex); err != nil {
				return err
			}
		}
		for _, trigger := range lost.ClonedTriggers {
			if err := visitAll(trigger.ParentTrigger, trigger.ChildTrigger); err != nil {
				return err
			}
		}
	}
	if err := visitAll(marker.ExclusiveDependencyObjects...); err != nil {
		return err
	}
	for _, acl := range marker.OriginalACLs {
		if err := visit(acl.Object); err != nil {
			return err
		}
	}
	for _, foreignKey := range marker.OriginalForeignKeys {
		if err := visitAll(foreignKey.OwningTable, foreignKey.ReferencedTable); err != nil {
			return err
		}
	}
	for _, publication := range marker.OriginalPublicationMemberships {
		if err := visit(publication.Table); err != nil {
			return err
		}
	}
	return nil
}

func translateArchivalValidationRequest(
	request archivalPlanValidationRequest,
	translation archivalValidationOIDTranslation,
) (translatedArchivalValidationState, error) {
	state := translatedArchivalValidationState{
		isolation:    translateArchivalIsolationPlan(request.Isolation, translation),
		operations:   translateCleanupOperations(request.Cleanup.Operations, translation),
		trustedNames: slices.Clone(request.Cleanup.TrustedSchemaNames),
	}
	markerTextTranslations := make(map[string]string, len(request.Cleanup.FinalizedMarkers))
	for _, finalized := range request.Cleanup.FinalizedMarkers {
		payload := translateArchivalMarker(finalized.Payload, translation)
		text, err := marshalArchivalMarker(payload)
		if err != nil {
			return translatedArchivalValidationState{}, fmt.Errorf(
				"marshaling translated marker for group %q: %w", finalized.GroupID, err,
			)
		}
		state.markers = append(state.markers, globalCleanupFinalizedMarker{
			GroupID: finalized.GroupID, Payload: payload, Text: text,
		})
		markerTextTranslations[finalized.Text] = text
	}
	translatedMarkerByID, _ := validationFinalizedMarkersByID(globalCleanupPlanResult{
		FinalizedMarkers: state.markers, TrustedGroupIDs: request.Cleanup.TrustedGroupIDs,
		TrustedSchemaNames: request.Cleanup.TrustedSchemaNames,
	})
	for _, group := range request.Cleanup.FinalizedRegularGroups {
		translated := clonePreparedArchivalGroup(group)
		finalized := translatedMarkerByID[group.id]
		translated.marker = finalized.Payload
		translated.markerText = finalized.Text
		translated.cleanupRoot = translateArchivalObject(group.cleanupRoot, translation)
		for idx := range translated.members {
			for _, member := range translated.marker.Members {
				if member.MemberID == translated.members[idx].marker.MemberID {
					translated.members[idx].marker = member
				}
			}
			if translated.members[idx].remainingMove != nil {
				move := *translated.members[idx].remainingMove
				move.RelationOID = translation.translateCatalog(pgClassCatalogOID, move.RelationOID)
				move.SourceTable = translateArchivalObject(move.SourceTable, translation)
				move.DestinationTable = translateArchivalObject(move.DestinationTable, translation)
				translated.members[idx].remainingMove = &move
			}
		}
		state.groups = append(state.groups, translated)
	}

	state.statements = slices.Clone(request.OrdinaryStatements)
	for idx := range state.statements {
		for sourceText, translatedText := range markerTextTranslations {
			state.statements[idx].DDL = strings.ReplaceAll(state.statements[idx].DDL,
				sourceText, translatedText)
		}
	}
	for idx, sourceGroup := range request.Cleanup.FinalizedRegularGroups {
		sourceAssertion := renderPlainTableArchivalAssertion(sourceGroup,
			mustArchivalIsolationGroup(request.Isolation, sourceGroup.id), request.Isolation)
		translatedGroup := state.groups[idx]
		translatedAssertion := renderPlainTableArchivalAssertion(translatedGroup,
			mustArchivalIsolationGroup(state.isolation, translatedGroup.id), state.isolation)
		found := 0
		for statementIdx := range state.statements {
			if request.OrdinaryStatements[statementIdx].DDL == sourceAssertion {
				state.statements[statementIdx].DDL = translatedAssertion
				found++
			}
		}
		if found != 1 {
			return translatedArchivalValidationState{}, fmt.Errorf(
				"could not deterministically translate the archival assertion for group %q", sourceGroup.id,
			)
		}
	}
	return state, nil
}

func (translation archivalValidationOIDTranslation) translateCatalog(classOID, oid uint32) uint32 {
	if oid == 0 {
		return 0
	}
	if translated, ok := translation.bySourceAddress[archivedDependencyAddressKey{
		classOID: classOID, objectOID: oid,
	}]; ok {
		return translated
	}
	return oid
}

func translateArchivalObject(
	object archivalMarkerObjectIdentity,
	translation archivalValidationOIDTranslation,
) archivalMarkerObjectIdentity {
	object = cloneMarkerObject(object)
	if object.Kind == archivalMarkerObjectKindToastRelation {
		if translated, ok := translation.toastIdentities[object.OID]; ok {
			return translated
		}
	}
	object.OID = translation.translateCatalog(archivalMarkerCatalogClassOID(object.Kind), object.OID)
	return object
}

func translateArchivalMarker(
	marker archivalMarkerV1,
	translation archivalValidationOIDTranslation,
) archivalMarkerV1 {
	marker = canonicalizeArchivalMarker(marker)
	translateObjects := func(objects []archivalMarkerObjectIdentity) []archivalMarkerObjectIdentity {
		result := slices.Clone(objects)
		for idx := range result {
			result[idx] = translateArchivalObject(result[idx], translation)
		}
		return result
	}
	for memberIdx := range marker.Members {
		member := &marker.Members[memberIdx]
		member.SourceTable = translateArchivalObject(member.SourceTable, translation)
		member.CleanupTable = translateArchivalObject(member.CleanupTable, translation)
		member.AutomaticallyMovedObjects = translateObjects(member.AutomaticallyMovedObjects)
		member.AttachedObjects = translateObjects(member.AttachedObjects)
		member.ExplicitlyMovedObjects = translateObjects(member.ExplicitlyMovedObjects)
		member.InternalToastObjects = translateObjects(member.InternalToastObjects)
	}
	translateAttachments := func(values []archivalMarkerPartitionedIndexAttachmentV1) {
		for idx := range values {
			values[idx].ParentIndex = translateArchivalObject(values[idx].ParentIndex, translation)
			values[idx].ChildIndex = translateArchivalObject(values[idx].ChildIndex, translation)
		}
	}
	translateTriggers := func(values []archivalMarkerClonedTriggerV1) {
		for idx := range values {
			values[idx].ParentTrigger = translateArchivalObject(values[idx].ParentTrigger, translation)
			values[idx].ChildTrigger = translateArchivalObject(values[idx].ChildTrigger, translation)
			values[idx].FunctionOID = translation.translateCatalog(pgProcCatalogOID, values[idx].FunctionOID)
			values[idx].ConstraintOID = translation.translateCatalog(pgConstraintCatalogOID,
				values[idx].ConstraintOID)
		}
	}
	for idx := range marker.PartitionEdges {
		translateAttachments(marker.PartitionEdges[idx].PartitionedIndexAttachments)
		translateTriggers(marker.PartitionEdges[idx].ClonedTriggers)
	}
	for idx := range marker.LostParentAttachments {
		lost := &marker.LostParentAttachments[idx]
		lost.ParentTable = translateArchivalObject(lost.ParentTable, translation)
		translateAttachments(lost.PartitionedIndexAttachments)
		translateTriggers(lost.ClonedTriggers)
	}
	marker.ExclusiveDependencyObjects = translateObjects(marker.ExclusiveDependencyObjects)
	for idx := range marker.OriginalACLs {
		marker.OriginalACLs[idx].Object = translateArchivalObject(
			marker.OriginalACLs[idx].Object, translation,
		)
	}
	for idx := range marker.OriginalForeignKeys {
		marker.OriginalForeignKeys[idx].OwningTable = translateArchivalObject(
			marker.OriginalForeignKeys[idx].OwningTable, translation,
		)
		marker.OriginalForeignKeys[idx].ReferencedTable = translateArchivalObject(
			marker.OriginalForeignKeys[idx].ReferencedTable, translation,
		)
	}
	for idx := range marker.OriginalPublicationMemberships {
		marker.OriginalPublicationMemberships[idx].Table = translateArchivalObject(
			marker.OriginalPublicationMemberships[idx].Table, translation,
		)
	}
	return canonicalizeArchivalMarker(marker)
}

func translateArchivalIsolationPlan(
	plan archivalIsolationPlan,
	translation archivalValidationOIDTranslation,
) archivalIsolationPlan {
	result := plan
	result.Groups = slices.Clone(plan.Groups)
	for idx := range result.Groups {
		group := &result.Groups[idx]
		group.OriginalACLs = slices.Clone(group.OriginalACLs)
		group.OriginalForeignKeys = cloneMarkerForeignKeys(group.OriginalForeignKeys)
		group.OriginalPublications = slices.Clone(group.OriginalPublications)
		group.SchemaPublications = slices.Clone(group.SchemaPublications)
		for aclIdx := range group.OriginalACLs {
			group.OriginalACLs[aclIdx].Object = translateArchivalObject(
				group.OriginalACLs[aclIdx].Object, translation,
			)
		}
		for fkIdx := range group.OriginalForeignKeys {
			group.OriginalForeignKeys[fkIdx].OwningTable = translateArchivalObject(
				group.OriginalForeignKeys[fkIdx].OwningTable, translation,
			)
			group.OriginalForeignKeys[fkIdx].ReferencedTable = translateArchivalObject(
				group.OriginalForeignKeys[fkIdx].ReferencedTable, translation,
			)
		}
		for publicationIdx := range group.OriginalPublications {
			group.OriginalPublications[publicationIdx].Table = translateArchivalObject(
				group.OriginalPublications[publicationIdx].Table, translation,
			)
		}
		for publicationIdx := range group.SchemaPublications {
			group.SchemaPublications[publicationIdx].TableOID = translation.translateCatalog(
				pgClassCatalogOID, group.SchemaPublications[publicationIdx].TableOID,
			)
		}
	}
	result.PreservedForeignKeys = slices.Clone(plan.PreservedForeignKeys)
	for idx := range result.PreservedForeignKeys {
		foreignKey := &result.PreservedForeignKeys[idx].ForeignKey
		foreignKey.OID = translation.translateCatalog(pgConstraintCatalogOID, foreignKey.OID)
		foreignKey.OwningRelationOID = translation.translateCatalog(pgClassCatalogOID,
			foreignKey.OwningRelationOID)
		foreignKey.ReferencedRelationOID = translation.translateCatalog(pgClassCatalogOID,
			foreignKey.ReferencedRelationOID)
	}
	return result
}

func translateCleanupOperations(
	operations []cleanupOperationV1,
	translation archivalValidationOIDTranslation,
) []cleanupOperationV1 {
	result := cloneCleanupOperations(operations)
	for idx := range result {
		result[idx].Object = translateArchivalObject(result[idx].Object, translation)
	}
	return result
}

func installExistingTranslatedArchivalMarkers(
	ctx context.Context,
	db *tempdb.Database,
	source schema.CatalogInventory,
	markers []globalCleanupFinalizedMarker,
) error {
	existing := make(map[string]struct{}, len(source.Schemas))
	for _, catalogSchema := range source.Schemas {
		existing[catalogSchema.Name] = struct{}{}
	}
	for _, finalized := range markers {
		for _, schemaName := range archivedMarkerSchemaNames(finalized.Payload) {
			if _, ok := existing[schemaName]; !ok {
				continue
			}
			ddl := fmt.Sprintf("COMMENT ON SCHEMA %s IS %s",
				schema.EscapeIdentifier(schemaName), schema.EscapeLiteral(finalized.Text))
			if _, err := db.ConnPool.Exec(ctx, ddl); err != nil {
				return fmt.Errorf("installing translated marker on existing schema %q: %w", schemaName, err)
			}
		}
	}
	return nil
}

func validateArchivalRetentionPostcondition(
	ctx context.Context,
	db *tempdb.Database,
	request archivalPlanValidationRequest,
	translated translatedArchivalValidationState,
	postRetention schema.SchemaSnapshot,
) error {
	resolution, err := resolveArchivedState(request.Prefix, postRetention, schema.SchemaSnapshot{})
	if err != nil {
		return err
	}
	var groupIDs []archivalGroupID
	var schemaNames []string
	markerByID := make(map[archivalGroupID]globalCleanupFinalizedMarker, len(translated.markers))
	for _, marker := range translated.markers {
		markerByID[marker.GroupID] = marker
	}
	for _, candidate := range resolution.CandidateGroups {
		if candidate.State != archivedCandidateGroupStateCompleteCandidate {
			return fmt.Errorf("translated archival group %q did not reach complete state", candidate.GroupID)
		}
		finalized, trusted := markerByID[candidate.GroupID]
		if !trusted || !reflect.DeepEqual(candidate.Marker, finalized.Payload) {
			return fmt.Errorf("post-retention marker for group %q is not the translated canonical marker",
				candidate.GroupID)
		}
		groupIDs = append(groupIDs, candidate.GroupID)
		schemaNames = append(schemaNames, candidate.SchemaNames...)
	}
	slices.Sort(groupIDs)
	slices.Sort(schemaNames)
	expectedIDs := make([]archivalGroupID, 0, len(translated.markers))
	for _, marker := range translated.markers {
		expectedIDs = append(expectedIDs, marker.GroupID)
	}
	slices.Sort(expectedIDs)
	if !slices.Equal(groupIDs, expectedIDs) || !slices.Equal(schemaNames, translated.trustedNames) {
		return fmt.Errorf("post-retention trusted groups or schemas differ from Stage 16")
	}
	for _, marker := range translated.markers {
		for _, member := range marker.Payload.Members {
			relation, err := uniqueRelationByOID(postRetention.Inventory, member.CleanupTable.OID)
			if err != nil {
				return err
			}
			if relation.SchemaName != member.CleanupTable.SchemaName ||
				relation.Name != member.CleanupTable.Name {
				return fmt.Errorf("member %q did not preserve its translated relation identity", member.MemberID)
			}
		}
	}
	comparisonOptions := archivalManagedComparisonOptions(request.ManagedSchemaOptions,
		translated.trustedNames)
	managed, err := schema.GetSchemaSnapshot(ctx, db.ConnPool, comparisonOptions...)
	if err != nil {
		return fmt.Errorf("fetching filtered post-retention schema: %w", err)
	}
	if err := assertMigratedSchemaMatchesTarget(managed.Schema,
		request.TargetSnapshot.Schema, &planOptions{}); err != nil {
		return fmt.Errorf("filtered managed schema does not match target: %w", err)
	}
	return nil
}

func archivalManagedComparisonOptions(
	options []schema.GetSchemaOpt,
	trustedNames []string,
) []schema.GetSchemaOpt {
	result := slices.Clone(options)
	if len(trustedNames) == 0 {
		return result
	}
	patterns := make([]string, 0, len(trustedNames))
	for _, name := range trustedNames {
		patterns = append(patterns, regexp.QuoteMeta(name))
	}
	return append(result, schema.WithExcludeSchemaPatterns(patterns...))
}

func validateCleanupOperationTargets(
	inventory schema.CatalogInventory,
	operations []cleanupOperationV1,
	markers []globalCleanupFinalizedMarker,
	requireDestinationIdentity bool,
) error {
	trustedSchemas := make(map[string]struct{})
	ownedObjects := make(map[string]struct{})
	cleanupRoots := make(map[string]struct{})
	for _, finalized := range markers {
		for _, schemaName := range archivedMarkerSchemaNames(finalized.Payload) {
			trustedSchemas[schemaName] = struct{}{}
		}
		root, err := globalCleanupRootFromMarker(finalized.Payload)
		if err != nil {
			return err
		}
		cleanupRoots[markerObjectIdentityKey(root)] = struct{}{}
		for _, object := range finalized.Payload.ExclusiveDependencyObjects {
			ownedObjects[markerObjectIdentityKey(object)] = struct{}{}
		}
	}
	for _, operation := range operations {
		if !operation.Restrict {
			return fmt.Errorf("cleanup operation %q does not use RESTRICT", operation.ID)
		}
		if operation.Kind == cleanupOperationKindDropSchema {
			if _, trusted := trustedSchemas[operation.Object.Name]; !trusted {
				return fmt.Errorf("cleanup operation %q targets unknown schema %q",
					operation.ID, operation.Object.Name)
			}
			continue
		}
		if _, trusted := trustedSchemas[operation.Object.SchemaName]; !trusted {
			return fmt.Errorf("cleanup operation %q targets object %s outside a trusted archival schema",
				operation.ID, markerObjectDisplayName(operation.Object))
		}
		key := markerObjectIdentityKey(operation.Object)
		if operation.Kind == cleanupOperationKindDropTable {
			if _, root := cleanupRoots[key]; !root {
				return fmt.Errorf("cleanup operation %q targets a table outside marker root ownership", operation.ID)
			}
		} else if _, owned := ownedObjects[key]; !owned {
			return fmt.Errorf("cleanup operation %q targets an object outside marker ownership", operation.ID)
		}
		if operation.Object.OID != 0 {
			actual, count := currentCatalogObjectsWithOID(inventory,
				operation.Object.Kind, operation.Object.OID)
			identityMatches := count == 1 && actual.Name == operation.Object.Name &&
				slices.Equal(actual.IdentityArguments, operation.Object.IdentityArguments)
			if requireDestinationIdentity {
				identityMatches = identityMatches && actual.SchemaName == operation.Object.SchemaName
			}
			if !identityMatches {
				return fmt.Errorf("cleanup operation %q target is absent or has a different catalog identity",
					operation.ID)
			}
		}
	}
	return nil
}

func validateArchivalCleanupPostcondition(
	ctx context.Context,
	db *tempdb.Database,
	request archivalPlanValidationRequest,
	translated translatedArchivalValidationState,
	postCleanup schema.SchemaSnapshot,
	managedBefore map[string]string,
) error {
	for _, schemaName := range translated.trustedNames {
		if catalogSchemaWithName(postCleanup.Inventory, schemaName) != nil {
			return fmt.Errorf("trusted archival schema %q remains after cleanup", schemaName)
		}
	}
	for _, operation := range translated.operations {
		if operation.Kind == cleanupOperationKindDropSchema || operation.Object.OID == 0 {
			continue
		}
		if _, count := currentCatalogObjectsWithOID(postCleanup.Inventory,
			operation.Object.Kind, operation.Object.OID); count != 0 {
			return fmt.Errorf("cleanup operation %q target remains after cleanup", operation.ID)
		}
	}
	resolution, err := resolveArchivedState(request.Prefix, postCleanup, schema.SchemaSnapshot{})
	if err != nil {
		return err
	}
	if len(resolution.CandidateGroups) != 0 {
		return fmt.Errorf("trusted archival cleanup remains after cleanup execution")
	}
	managedAfter := archivalManagedCatalogMetadata(postCleanup.Inventory,
		translated.trustedNames, translated.markers)
	if !reflect.DeepEqual(managedBefore, managedAfter) {
		for key, before := range managedBefore {
			if after, ok := managedAfter[key]; !ok || after != before {
				return fmt.Errorf("managed target object identity or metadata changed across cleanup: %s", key)
			}
		}
		for key := range managedAfter {
			if _, ok := managedBefore[key]; !ok {
				return fmt.Errorf("managed target object appeared during cleanup: %s", key)
			}
		}
	}
	managed, err := schema.GetSchemaSnapshot(ctx, db.ConnPool, request.ManagedSchemaOptions...)
	if err != nil {
		return fmt.Errorf("fetching complete post-cleanup managed schema: %w", err)
	}
	if err := assertMigratedSchemaMatchesTarget(managed.Schema,
		request.TargetSnapshot.Schema, &planOptions{}); err != nil {
		return fmt.Errorf("complete post-cleanup modeled schema does not match target: %w", err)
	}
	return nil
}

func archivalManagedCatalogMetadata(
	inventory schema.CatalogInventory,
	trustedNames []string,
	markers []globalCleanupFinalizedMarker,
) map[string]string {
	trustedSchemas := make(map[string]struct{}, len(trustedNames))
	for _, name := range trustedNames {
		trustedSchemas[name] = struct{}{}
	}
	owned := make(map[archivedDependencyAddressKey]struct{})
	for _, marker := range markers {
		_ = walkArchivalMarkerObjects(marker.Payload, func(
			object archivalMarkerObjectIdentity,
		) error {
			classOID := archivalMarkerCatalogClassOID(object.Kind)
			if classOID != 0 && object.OID != 0 {
				owned[archivedDependencyAddressKey{classOID: classOID, objectOID: object.OID}] = struct{}{}
			}
			return nil
		})
	}
	for _, catalogSchema := range inventory.Schemas {
		if _, trusted := trustedSchemas[catalogSchema.Name]; trusted {
			owned[archivedDependencyAddressKey{
				classOID: pgNamespaceCatalogOID, objectOID: catalogSchema.OID,
			}] = struct{}{}
		}
	}
	isOwned := func(classOID, objectOID uint32) bool {
		_, ok := owned[archivedDependencyAddressKey{classOID: classOID, objectOID: objectOID}]
		return ok
	}
	isTrustedSchema := func(name string) bool {
		_, ok := trustedSchemas[name]
		return ok
	}
	result := make(map[string]string)
	add := func(kind string, key any, value any) {
		encoded, _ := json.Marshal(value)
		result[fmt.Sprintf("%s:%v", kind, key)] = string(encoded)
	}
	for _, value := range inventory.Schemas {
		if !isTrustedSchema(value.Name) {
			add("schema", value.OID, value)
		}
	}
	for _, value := range inventory.Relations {
		if !isTrustedSchema(value.SchemaName) && !isOwned(pgClassCatalogOID, value.OID) {
			add("relation", value.OID, value)
		}
	}
	for _, value := range inventory.Types {
		if !isTrustedSchema(value.SchemaName) && !isOwned(pgTypeCatalogOID, value.OID) {
			add("type", value.OID, value)
		}
	}
	for _, value := range inventory.Tables {
		if !isOwned(pgClassCatalogOID, value.RelationOID) {
			add("table", value.RelationOID, value)
		}
	}
	for _, value := range inventory.Columns {
		if !isOwned(pgClassCatalogOID, value.RelationOID) {
			add("column", fmt.Sprintf("%d/%d", value.RelationOID, value.Number), value)
		}
	}
	for _, value := range inventory.Indexes {
		if !isOwned(pgClassCatalogOID, value.OID) &&
			!isOwned(pgClassCatalogOID, value.RelationOID) {
			add("index", value.OID, value)
		}
	}
	for _, value := range inventory.Constraints {
		if !isOwned(pgConstraintCatalogOID, value.OID) &&
			!isOwned(pgClassCatalogOID, value.RelationOID) {
			add("constraint", value.OID, value)
		}
	}
	for _, value := range inventory.Triggers {
		if !isOwned(pgTriggerCatalogOID, value.OID) &&
			!isOwned(pgClassCatalogOID, value.RelationOID) {
			add("trigger", value.OID, value)
		}
	}
	for _, value := range inventory.Rules {
		if !isOwned(pgRewriteCatalogOID, value.OID) &&
			!isOwned(pgClassCatalogOID, value.RelationOID) {
			add("rule", value.OID, value)
		}
	}
	for _, value := range inventory.Policies {
		if !isOwned(pgPolicyCatalogOID, value.OID) &&
			!isOwned(pgClassCatalogOID, value.RelationOID) {
			add("policy", value.OID, value)
		}
	}
	for _, value := range inventory.Sequences {
		if !isTrustedSchema(value.SchemaName) && !isOwned(pgClassCatalogOID, value.OID) {
			add("sequence", value.OID, value)
		}
	}
	for _, value := range inventory.OwnedSequences {
		if !isOwned(pgClassCatalogOID, value.SequenceOID) &&
			!isOwned(pgClassCatalogOID, value.RelationOID) {
			add("owned_sequence", value.SequenceOID, value)
		}
	}
	for _, value := range inventory.ExtendedStatistics {
		if !isTrustedSchema(value.SchemaName) && !isOwned(pgStatisticExtCatalogOID, value.OID) {
			add("statistic", value.OID, value)
		}
	}
	for _, value := range inventory.InheritanceEdges {
		if !isOwned(pgClassCatalogOID, value.ChildRelationOID) &&
			!isOwned(pgClassCatalogOID, value.ParentRelationOID) {
			add("inheritance", fmt.Sprintf("%d/%d", value.ParentRelationOID, value.ChildRelationOID), value)
		}
	}
	for _, value := range inventory.PartitionAttachments {
		if !isOwned(pgClassCatalogOID, value.RelationOID) &&
			!isOwned(pgClassCatalogOID, value.ParentRelationOID) {
			add("partition", fmt.Sprintf("%d/%d", value.ParentRelationOID, value.RelationOID), value)
		}
	}
	for _, value := range inventory.SecurityLabels {
		if !isOwned(pgClassCatalogOID, value.RelationOID) {
			add("security_label", fmt.Sprintf("%d/%d/%s", value.RelationOID,
				value.ColumnNumber, value.Provider), value)
		}
	}
	for _, value := range inventory.Dependencies {
		if !isOwned(value.Dependent.ClassOID, value.Dependent.ObjectOID) &&
			!isOwned(value.Referenced.ClassOID, value.Referenced.ObjectOID) {
			add("dependency", fmt.Sprintf("%d/%d/%d/%d/%s", value.Dependent.ClassOID,
				value.Dependent.ObjectOID, value.Referenced.ClassOID,
				value.Referenced.ObjectOID, value.Type), value)
		}
	}
	for _, value := range inventory.ForeignKeys {
		if !isOwned(pgConstraintCatalogOID, value.OID) &&
			!isOwned(pgClassCatalogOID, value.OwningRelationOID) &&
			!isOwned(pgClassCatalogOID, value.ReferencedRelationOID) {
			add("foreign_key", value.OID, value)
		}
	}
	for _, value := range inventory.Views {
		if !isTrustedSchema(value.SchemaName) && !isOwned(pgClassCatalogOID, value.RelationOID) {
			add("view", value.RelationOID, value)
		}
	}
	for _, value := range inventory.Routines {
		if !isTrustedSchema(value.SchemaName) && !isOwned(pgProcCatalogOID, value.OID) {
			add("routine", value.OID, value)
		}
	}
	for _, value := range inventory.Collations {
		if !isTrustedSchema(value.SchemaName) && !isOwned(pgCollationCatalogOID, value.OID) {
			add("collation", value.OID, value)
		}
	}
	for _, value := range inventory.Operators {
		if !isTrustedSchema(value.SchemaName) && !isOwned(pgOperatorCatalogOID, value.OID) {
			add("operator", value.OID, value)
		}
	}
	for _, value := range inventory.Extensions {
		add("extension", value.OID, value)
	}
	for _, value := range inventory.EventTriggers {
		add("event_trigger", value.OID, value)
	}
	for _, value := range inventory.Publications {
		add("publication", value.OID, value)
	}
	for _, value := range inventory.PublicationRelations {
		if !isOwned(pgClassCatalogOID, value.RelationOID) {
			add("publication_relation", value.OID, value)
		}
	}
	for _, value := range inventory.ACLGrants {
		if !isOwned(value.Object.ClassOID, value.Object.ObjectOID) {
			add("acl", fmt.Sprintf("%d/%d/%d/%s", value.Object.ClassOID, value.Object.ObjectOID,
				value.GranteeOID, value.Privilege), value)
		}
	}
	return result
}

func archivalMarkerCatalogClassOID(kind archivalMarkerObjectKind) uint32 {
	switch kind {
	case archivalMarkerObjectKindTable, archivalMarkerObjectKindIndex,
		archivalMarkerObjectKindOwnedSequence, archivalMarkerObjectKindToastRelation,
		archivalMarkerObjectKindSequence, archivalMarkerObjectKindView,
		archivalMarkerObjectKindMaterializedView:
		return pgClassCatalogOID
	case archivalMarkerObjectKindRowType, archivalMarkerObjectKindArrayType, archivalMarkerObjectKindType:
		return pgTypeCatalogOID
	case archivalMarkerObjectKindConstraint:
		return pgConstraintCatalogOID
	case archivalMarkerObjectKindTrigger:
		return pgTriggerCatalogOID
	case archivalMarkerObjectKindRule:
		return pgRewriteCatalogOID
	case archivalMarkerObjectKindPolicy:
		return pgPolicyCatalogOID
	case archivalMarkerObjectKindExtendedStatistic:
		return pgStatisticExtCatalogOID
	case archivalMarkerObjectKindFunction:
		return pgProcCatalogOID
	case archivalMarkerObjectKindCollation:
		return pgCollationCatalogOID
	case archivalMarkerObjectKindOperator:
		return pgOperatorCatalogOID
	default:
		return 0
	}
}

func validateArchivalDataProbe(
	ctx context.Context,
	db *tempdb.Database,
	query string,
	expected string,
) error {
	if query == "" {
		return nil
	}
	var actual string
	if err := db.ConnPool.QueryRow(ctx, query).Scan(&actual); err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("query returned %q instead of %q", actual, expected)
	}
	return nil
}
