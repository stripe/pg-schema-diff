package diff

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

func TestCandidatePlanSnapshotHashUsesSharedContract(t *testing.T) {
	t.Parallel()

	snapshot := schema.SchemaSnapshot{
		Schema: schema.Schema{NamedSchemas: []schema.NamedSchema{{Name: "public"}}},
		Inventory: schema.CatalogInventory{
			Schemas: []schema.CatalogSchema{{OID: 1, Name: "public"}},
			Relations: []schema.CatalogRelation{{
				OID: 10, SchemaOID: 1, SchemaName: "public", Name: "accounts", Kind: schema.RelKindOrdinaryTable,
			}},
		},
	}
	expected, err := schema.BuildSchemaSnapshotHashV1(snapshot, nil)
	require.NoError(t, err)
	actual, err := buildCandidatePlanSnapshotHash(snapshot, globalCleanupPlanResult{})
	require.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestCandidatePlanSnapshotHashBindsTrustedSourceMarkers(t *testing.T) {
	t.Parallel()

	const (
		groupID    archivalGroupID = "group-1"
		schemaName string          = "archive_public_accounts"
	)
	payload := minimalArchivalMarker()
	payload.Members[0].CleanupTable.SchemaName = schemaName
	payload.Members[0].AutomaticallyMovedObjects[0].SchemaName = schemaName
	marker, err := marshalArchivalMarker(payload)
	require.NoError(t, err)
	_, err = parseArchivalMarker(marker)
	require.NoError(t, err)
	snapshot := schema.SchemaSnapshot{Inventory: schema.CatalogInventory{Schemas: []schema.CatalogSchema{{
		OID: 42, Name: schemaName, Comment: marker,
	}}}}
	cleanup := globalCleanupPlanResult{
		TrustedGroupIDs: []archivalGroupID{groupID},
		FinalizedMarkers: []globalCleanupFinalizedMarker{{
			GroupID: groupID, Payload: payload,
			Text: marker,
		}},
	}

	actual, err := buildCandidatePlanSnapshotHash(snapshot, cleanup)
	require.NoError(t, err)
	expected, err := schema.BuildSchemaSnapshotHashV1(snapshot,
		[]schema.SnapshotHashTrustedArchivalGroup{{GroupID: string(groupID), SchemaNames: []string{schemaName}}})
	require.NoError(t, err)
	assert.Equal(t, expected, actual)

	changedPayload := payload
	changedPayload.CleanupDigest = cleanupOperationDigest("sha256:" + strings.Repeat("1", 64))
	changedMarker, err := marshalArchivalMarker(changedPayload)
	require.NoError(t, err)
	changedSnapshot := snapshot
	changedSnapshot.Inventory.Schemas = []schema.CatalogSchema{{
		OID: 42, Name: schemaName, Comment: changedMarker,
	}}
	changed, err := buildCandidatePlanSnapshotHash(changedSnapshot, cleanup)
	require.NoError(t, err)
	assert.NotEqual(t, actual, changed)

	cleanup.FinalizedMarkers[0].Payload = changedPayload
	cleanup.FinalizedMarkers[0].Text = changedMarker
	renderingChanged, err := buildCandidatePlanSnapshotHash(snapshot, cleanup)
	require.NoError(t, err)
	assert.Equal(t, actual, renderingChanged)
}

func TestCandidatePlanSnapshotHashExcludesGenerationAndRenderingState(t *testing.T) {
	t.Parallel()

	snapshot := schema.SchemaSnapshot{Schema: schema.Schema{
		NamedSchemas: []schema.NamedSchema{{Name: "public"}},
	}}
	first := globalCleanupPlanResult{
		CleanupStatements: []Statement{{
			DDL: "DROP TABLE rendered_first", SkipValidation: true,
			Hazards: []MigrationHazard{{Type: MigrationHazardTypeDeletesData, Message: "first message"}},
		}},
		ComponentInitializationStatements: []Statement{{DDL: "COMMENT ON SCHEMA rendered_first"}},
	}
	second := globalCleanupPlanResult{
		CleanupStatements: []Statement{
			{DDL: "DROP SCHEMA rendered_second"},
			{DDL: "DROP TABLE rendered_second", Hazards: []MigrationHazard{{
				Type: MigrationHazardTypeCorrectness, Message: "different message",
			}}},
		},
	}
	firstHash, err := buildCandidatePlanSnapshotHash(snapshot, first)
	require.NoError(t, err)
	secondHash, err := buildCandidatePlanSnapshotHash(snapshot, second)
	require.NoError(t, err)
	assert.Equal(t, firstHash, secondHash,
		"rendered SQL, statement ordering, validation flags, and hazard state are not immutable source facts")
}

func TestGenerateUsesVersionedSnapshotHash(t *testing.T) {
	t.Parallel()

	snapshot := schema.SchemaSnapshot{
		Hash:      "legacy-stage-18-hash",
		Inventory: schema.CatalogInventory{Schemas: []schema.CatalogSchema{{OID: 1, Name: "public"}}},
	}
	candidate, err := buildCandidatePlanSnapshotHash(snapshot, globalCleanupPlanResult{})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(candidate, schema.SchemaSnapshotHashV1Prefix))
	source := fakeSchemaSource{
		t: t,
		expectedDeps: schemaSourcePlanDeps{
			logger: slog.Default(), getSchemaOpts: nil,
		},
		snapshot: snapshot,
	}

	plan, err := Generate(t.Context(), source, source, WithDoNotValidatePlan())
	require.NoError(t, err)
	assert.Equal(t, candidate, plan.CurrentSchemaHash)
}
