package diff

import (
	"fmt"
	"slices"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

// buildCandidatePlanSnapshotHash builds the active plan-side v1 snapshot hash.
// Stage 16/17 owns marker validation; the shared hasher binds trusted identities to
// the immutable raw schema OID/name/comment facts instead of parsing markers a
// second, weaker way.
func buildCandidatePlanSnapshotHash(
	snapshot schema.SchemaSnapshot,
	cleanup globalCleanupPlanResult,
) (string, error) {
	trustedGroupIDs := make(map[archivalGroupID]struct{}, len(cleanup.TrustedGroupIDs))
	for _, groupID := range cleanup.TrustedGroupIDs {
		trustedGroupIDs[groupID] = struct{}{}
	}
	currentSchemas := make(map[string]struct{}, len(snapshot.Inventory.Schemas))
	for _, catalogSchema := range snapshot.Inventory.Schemas {
		currentSchemas[catalogSchema.Name] = struct{}{}
	}

	trustedGroups := make([]schema.SnapshotHashTrustedArchivalGroup, 0, len(cleanup.FinalizedMarkers))
	seenSourceGroups := make(map[archivalGroupID]struct{})
	for _, finalized := range cleanup.FinalizedMarkers {
		if _, trusted := trustedGroupIDs[finalized.GroupID]; !trusted {
			return "", fmt.Errorf("candidate snapshot hash marker group %q is not trusted by the cleanup result",
				finalized.GroupID)
		}
		allSchemaNames := archivedMarkerSchemaNames(finalized.Payload)
		sourceSchemaNames := make([]string, 0, len(allSchemaNames))
		for _, name := range allSchemaNames {
			if _, exists := currentSchemas[name]; exists {
				sourceSchemaNames = append(sourceSchemaNames, name)
			}
		}
		if len(sourceSchemaNames) == 0 {
			// Newly predicted groups are not part of the immutable source state.
			continue
		}
		if len(sourceSchemaNames) != len(allSchemaNames) {
			return "", fmt.Errorf("candidate snapshot hash trusted group %q is only partly present in the source inventory",
				finalized.GroupID)
		}
		trustedGroups = append(trustedGroups, schema.SnapshotHashTrustedArchivalGroup{
			GroupID: string(finalized.GroupID), SchemaNames: sourceSchemaNames,
		})
		seenSourceGroups[finalized.GroupID] = struct{}{}
	}
	for groupID := range trustedGroupIDs {
		if _, sourceGroup := seenSourceGroups[groupID]; sourceGroup {
			continue
		}
		if !slices.ContainsFunc(cleanup.FinalizedMarkers, func(
			marker globalCleanupFinalizedMarker,
		) bool {
			return marker.GroupID == groupID
		}) {
			return "", fmt.Errorf("candidate snapshot hash trusted group %q has no finalized marker", groupID)
		}
	}
	return schema.BuildSchemaSnapshotHashV1(snapshot, trustedGroups)
}
