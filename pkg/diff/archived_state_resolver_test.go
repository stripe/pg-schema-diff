package diff

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

const (
	resolverTestPrefix  = "archive"
	resolverTestGroupID = archivalGroupID("20260721T091011123456Z_ABCDEFGH")
)

type archivedResolverFixture struct {
	snapshot    schema.SchemaSnapshot
	marker      archivalMarkerV1
	cleanupName string
}

func TestResolveArchivedStateClassificationsAndResumeDescriptors(t *testing.T) {
	for _, testCase := range []struct {
		name                   string
		tableMoved             bool
		statisticMoved         bool
		expectedState          archivedCandidateGroupState
		expectedMemberMoves    int
		expectedStatisticMoves int
		expectedMarkerUpdates  int
	}{
		{
			name: "empty initialization", expectedState: archivedCandidateGroupStateEmptyInitialized,
			expectedMemberMoves: 1, expectedStatisticMoves: 1, expectedMarkerUpdates: 1,
		},
		{
			name: "partial resumable", tableMoved: true,
			expectedState:          archivedCandidateGroupStatePartialResumable,
			expectedStatisticMoves: 1, expectedMarkerUpdates: 1,
		},
		{
			name: "complete candidate", tableMoved: true, statisticMoved: true,
			expectedState: archivedCandidateGroupStateCompleteCandidate,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newArchivedResolverFixture(t, testCase.tableMoved, testCase.statisticMoved)
			before, err := json.Marshal(fixture.snapshot)
			require.NoError(t, err)

			resolution, err := resolveArchivedState(
				resolverTestPrefix, fixture.snapshot, schema.SchemaSnapshot{},
			)
			require.NoError(t, err)
			require.Len(t, resolution.CandidateGroups, 1)
			candidate := resolution.CandidateGroups[0]
			assert.Equal(t, resolverTestGroupID, candidate.GroupID)
			assert.Equal(t, testCase.expectedState, candidate.State)
			assert.Equal(t, []string{fixture.cleanupName}, candidate.SchemaNames)
			assert.Equal(t, []string{fixture.cleanupName},
				resolution.ProvisionalUntrustedSchemaNames)
			assert.Len(t, candidate.Resume.RemainingMemberMoves, testCase.expectedMemberMoves)
			assert.Len(t, candidate.Resume.RemainingExplicitObjectMoves, testCase.expectedStatisticMoves)
			assert.Len(t, candidate.Resume.RemainingMarkerUpdates, testCase.expectedMarkerUpdates)
			assert.Empty(t, candidate.Resume.RemainingDependencyObjectMoves)
			if testCase.expectedMemberMoves > 0 {
				move := candidate.Resume.RemainingMemberMoves[0]
				assert.Equal(t, "member-accounts", move.MemberID)
				assert.Equal(t, uint32(10), move.RelationOID)
				assert.Equal(t, "public", move.SourceTable.SchemaName)
				assert.Equal(t, fixture.cleanupName, move.DestinationTable.SchemaName)
			}
			if testCase.expectedStatisticMoves > 0 {
				move := candidate.Resume.RemainingExplicitObjectMoves[0]
				assert.Equal(t, "member-accounts", move.MemberID)
				assert.Equal(t, uint32(19), move.Source.OID)
				assert.Equal(t, "public", move.Source.SchemaName)
				assert.Equal(t, fixture.cleanupName, move.Destination.SchemaName)
			}
			after, err := json.Marshal(fixture.snapshot)
			require.NoError(t, err)
			assert.Equal(t, before, after, "resolution must not mutate snapshots")
		})
	}
}

func TestResolveArchivedStateRejectsInvalidCandidateMarkers(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		mutate   func(*archivedResolverFixture)
		contains string
	}{
		{
			name: "generated name without marker",
			mutate: func(f *archivedResolverFixture) {
				f.snapshot.Inventory.Schemas[1].Comment = ""
			},
			contains: "has no archival marker",
		},
		{
			name: "malformed marker",
			mutate: func(f *archivedResolverFixture) {
				f.snapshot.Inventory.Schemas[1].Comment = "not-a-marker"
			},
			contains: "invalid archival marker envelope",
		},
		{
			name: "wrong marker version",
			mutate: func(f *archivedResolverFixture) {
				f.snapshot.Inventory.Schemas[1].Comment =
					archivalMarkerEnvelopeNamespace + "v2:e30"
			},
			contains: "unsupported archival marker envelope version",
		},
		{
			name: "marker under wrong generated name",
			mutate: func(f *archivedResolverFixture) {
				wrongName, err := buildArchivalSchemaName(
					resolverTestPrefix, "public", "other", "20260721T091011123456Z", "ABCDEFGH",
				)
				require.NoError(t, err)
				f.snapshot.Inventory.Schemas[1].Name = wrongName
			},
			contains: "marker does not declare containing schema",
		},
		{
			name: "marker name disagreement",
			mutate: func(f *archivedResolverFixture) {
				f.marker.GroupID = "20260721T091011123456Z_IJKLMNOP"
				setFixtureMarkers(t, f)
			},
			contains: "disagrees with generated name",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newArchivedResolverFixture(t, false, false)
			testCase.mutate(&fixture)
			resolution, err := resolveArchivedState(
				resolverTestPrefix, fixture.snapshot, schema.SchemaSnapshot{},
			)
			assert.Empty(t, resolution.CandidateGroups)
			require.Error(t, err)
			assert.ErrorContains(t, err, testCase.contains)
		})
	}
}

func TestResolveArchivedStateRejectsInconsistentMarkerCopies(t *testing.T) {
	fixture := newArchivedDependencyResolverFixture(t, true)
	other := fixture.marker
	other.CleanupDigest = cleanupOperationDigest("sha256:" + strings.Repeat("f", 64))
	otherMarker, err := marshalArchivalMarker(other)
	require.NoError(t, err)
	fixture.snapshot.Inventory.Schemas[2].Comment = otherMarker

	_, err = resolveArchivedState(resolverTestPrefix, fixture.snapshot, schema.SchemaSnapshot{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "inconsistent marker payload")
}

func TestResolveArchivedStateAcceptsCanonicalEquivalentMarkerCopies(t *testing.T) {
	fixture := newArchivedDependencyResolverFixture(t, true)
	nonCanonical := canonicalizeArchivalMarker(fixture.marker)
	slices.Reverse(nonCanonical.Members[0].AutomaticallyMovedObjects)
	slices.Reverse(nonCanonical.ExclusiveDependencySchemas)
	slices.Reverse(nonCanonical.ExclusiveDependencyObjects)
	encoded, err := json.Marshal(nonCanonical)
	require.NoError(t, err)
	fixture.snapshot.Inventory.Schemas[2].Comment = archivalMarkerEnvelopeV1 +
		base64.RawURLEncoding.EncodeToString(encoded)

	resolution, err := resolveArchivedState(resolverTestPrefix, fixture.snapshot, schema.SchemaSnapshot{})
	require.NoError(t, err)
	require.Len(t, resolution.CandidateGroups, 1)
	assert.Equal(t, archivedCandidateGroupStateCompleteCandidate, resolution.CandidateGroups[0].State)
	assert.True(t, slices.IsSorted(resolution.CandidateGroups[0].SchemaNames))
}

func TestResolveArchivedStateValidatesExactTableLocalCatalogObjects(t *testing.T) {
	missingCases := []struct {
		name     string
		contains string
		mutate   func(*schema.CatalogInventory)
	}{
		{name: "relation", contains: "relation OID 10 is missing", mutate: func(i *schema.CatalogInventory) {
			i.Relations = slices.DeleteFunc(i.Relations,
				func(r schema.CatalogRelation) bool { return r.OID == 10 })
		}},
		{name: "type", contains: "row_type", mutate: func(i *schema.CatalogInventory) {
			i.Types = slices.DeleteFunc(i.Types, func(v schema.CatalogType) bool { return v.OID == 11 })
		}},
		{name: "index", contains: "index", mutate: func(i *schema.CatalogInventory) {
			i.Indexes = slices.DeleteFunc(i.Indexes, func(v schema.CatalogIndex) bool { return v.OID == 13 })
		}},
		{name: "constraint", contains: "constraint", mutate: func(i *schema.CatalogInventory) {
			i.Constraints = nil
		}},
		{name: "trigger", contains: "trigger", mutate: func(i *schema.CatalogInventory) { i.Triggers = nil }},
		{name: "rule", contains: "rule", mutate: func(i *schema.CatalogInventory) { i.Rules = nil }},
		{name: "policy", contains: "policy", mutate: func(i *schema.CatalogInventory) { i.Policies = nil }},
		{name: "owned sequence", contains: "owned sequence OID 18", mutate: func(i *schema.CatalogInventory) {
			i.Sequences = nil
		}},
		{
			name: "extended statistic", contains: "extended_statistic",
			mutate: func(i *schema.CatalogInventory) { i.ExtendedStatistics = nil },
		},
		{name: "TOAST", contains: "toast_relation", mutate: func(i *schema.CatalogInventory) { i.Relations[0].ToastRelation = nil }},
	}
	for _, testCase := range missingCases {
		t.Run("missing "+testCase.name, func(t *testing.T) {
			fixture := newArchivedResolverFixture(t, true, true)
			testCase.mutate(&fixture.snapshot.Inventory)
			_, err := resolveArchivedState(resolverTestPrefix, fixture.snapshot, schema.SchemaSnapshot{})
			require.Error(t, err)
			assert.ErrorContains(t, err, testCase.contains)
		})
	}

	unexpectedCases := []struct {
		name     string
		contains string
		mutate   func(*schema.CatalogInventory, string)
	}{
		{name: "relation", contains: "unexpected catalog object", mutate: func(
			i *schema.CatalogInventory, archive string,
		) {
			i.Relations = append(i.Relations, schema.CatalogRelation{
				OID: 90, SchemaOID: 2, SchemaName: archive, Name: "unexpected_table", Kind: schema.RelKindOrdinaryTable,
			})
		}},
		{name: "type", contains: "type", mutate: func(i *schema.CatalogInventory, archive string) {
			i.Types = append(i.Types, schema.CatalogType{
				OID: 91, SchemaOID: 2, SchemaName: archive, Name: "unexpected_type", Kind: schema.CatalogTypeKindEnum,
			})
		}},
		{name: "index", contains: "index", mutate: func(i *schema.CatalogInventory, archive string) {
			i.Indexes = append(i.Indexes, schema.CatalogIndex{
				OID: 92, SchemaOID: 2,
				SchemaName: archive, Name: "unexpected_idx", RelationOID: 10,
			})
		}},
		{name: "constraint", contains: "constraint", mutate: func(i *schema.CatalogInventory, archive string) {
			i.Constraints = append(i.Constraints, schema.CatalogConstraint{
				OID:       93,
				SchemaOID: 2, SchemaName: archive, Name: "unexpected_check", RelationOID: 10,
			})
		}},
		{name: "trigger", contains: "trigger", mutate: func(i *schema.CatalogInventory, _ string) {
			i.Triggers = append(i.Triggers, schema.CatalogTrigger{OID: 94, RelationOID: 10, Name: "unexpected_trigger"})
		}},
		{name: "rule", contains: "rule", mutate: func(i *schema.CatalogInventory, _ string) {
			i.Rules = append(i.Rules, schema.CatalogRule{OID: 95, RelationOID: 10, Name: "unexpected_rule"})
		}},
		{name: "policy", contains: "policy", mutate: func(i *schema.CatalogInventory, _ string) {
			i.Policies = append(i.Policies, schema.CatalogPolicy{OID: 96, RelationOID: 10, Name: "unexpected_policy"})
		}},
		{name: "owned sequence", contains: "owned_sequence", mutate: func(
			i *schema.CatalogInventory, archive string,
		) {
			i.Sequences = append(i.Sequences, schema.CatalogSequence{
				OID: 97, SchemaOID: 2,
				SchemaName: archive, Name: "unexpected_seq",
			})
			i.OwnedSequences = append(i.OwnedSequences, schema.CatalogOwnedSequence{
				SequenceOID: 97, RelationOID: 10,
			})
		}},
		{name: "extended statistic", contains: "extended_statistic", mutate: func(
			i *schema.CatalogInventory, archive string,
		) {
			i.ExtendedStatistics = append(i.ExtendedStatistics, schema.CatalogExtendedStatistic{
				OID: 98, SchemaOID: 2, SchemaName: archive, Name: "unexpected_stats", RelationOID: 10,
			})
		}},
		{name: "TOAST", contains: "toast_relation", mutate: func(i *schema.CatalogInventory, _ string) {
			i.Relations[0].ToastRelation = &schema.CatalogRelationIdentity{
				OID:       99,
				SchemaOID: 9, SchemaName: "pg_toast", Name: "pg_toast_99",
			}
		}},
	}
	for _, testCase := range unexpectedCases {
		t.Run("unexpected "+testCase.name, func(t *testing.T) {
			fixture := newArchivedResolverFixture(t, true, true)
			testCase.mutate(&fixture.snapshot.Inventory, fixture.cleanupName)
			_, err := resolveArchivedState(resolverTestPrefix, fixture.snapshot, schema.SchemaSnapshot{})
			require.Error(t, err)
			assert.ErrorContains(t, err, testCase.contains)
		})
	}

	t.Run("duplicate expected index identity", func(t *testing.T) {
		fixture := newArchivedResolverFixture(t, true, true)
		fixture.snapshot.Inventory.Indexes = append(
			fixture.snapshot.Inventory.Indexes, fixture.snapshot.Inventory.Indexes[0],
		)
		_, err := resolveArchivedState(resolverTestPrefix, fixture.snapshot, schema.SchemaSnapshot{})
		require.Error(t, err)
		assert.ErrorContains(t, err, "duplicate current automatically moved index object")
	})
}

func TestResolveArchivedStateUsesOriginalRelationOIDAcrossReplacement(t *testing.T) {
	t.Run("replacement before expected move is invalid", func(t *testing.T) {
		fixture := newArchivedResolverFixture(t, false, false)
		fixture.snapshot.Inventory.Relations = slices.DeleteFunc(
			fixture.snapshot.Inventory.Relations,
			func(relation schema.CatalogRelation) bool { return relation.OID == 10 },
		)
		fixture.snapshot.Inventory.Relations = append(fixture.snapshot.Inventory.Relations, schema.CatalogRelation{
			OID: 999, SchemaOID: 1, SchemaName: "public", Name: "accounts", Kind: schema.RelKindOrdinaryTable,
		})

		_, err := resolveArchivedState(resolverTestPrefix, fixture.snapshot, schema.SchemaSnapshot{})
		require.Error(t, err)
		assert.ErrorContains(t, err, "relation OID 10 is missing")
	})

	t.Run("same-name replacement after original move is valid", func(t *testing.T) {
		fixture := newArchivedResolverFixture(t, true, true)
		fixture.snapshot.Inventory.Relations = append(fixture.snapshot.Inventory.Relations, schema.CatalogRelation{
			OID: 999, SchemaOID: 1, SchemaName: "public", Name: "accounts", Kind: schema.RelKindOrdinaryTable,
		})

		resolution, err := resolveArchivedState(resolverTestPrefix, fixture.snapshot, schema.SchemaSnapshot{})
		require.NoError(t, err)
		assert.Equal(t, archivedCandidateGroupStateCompleteCandidate, resolution.CandidateGroups[0].State)
	})
}

func TestResolveArchivedStateRecursivePartitionGroupAndTopology(t *testing.T) {
	fixture := newArchivedPartitionResolverFixture(t)

	resolution, err := resolveArchivedState(resolverTestPrefix, fixture.snapshot, schema.SchemaSnapshot{})
	require.NoError(t, err)
	require.Len(t, resolution.CandidateGroups, 1)
	candidate := resolution.CandidateGroups[0]
	assert.Equal(t, archivedCandidateGroupStatePartialResumable, candidate.State)
	require.Len(t, candidate.Resume.RemainingMemberMoves, 1)
	assert.Equal(t, "member-leaf", candidate.Resume.RemainingMemberMoves[0].MemberID)
	assert.Len(t, candidate.SchemaNames, 3)

	for _, testCase := range []struct {
		name     string
		mutate   func(*schema.CatalogInventory)
		contains string
	}{
		{
			name: "missing attachment",
			mutate: func(inventory *schema.CatalogInventory) {
				inventory.PartitionAttachments = inventory.PartitionAttachments[:1]
			},
			contains: "missing expected partition attachment",
		},
		{
			name: "second parent",
			mutate: func(inventory *schema.CatalogInventory) {
				inventory.Relations = append(inventory.Relations, schema.CatalogRelation{
					OID: 999, SchemaName: "public", Name: "outside", Kind: schema.RelKindPartitionedTable,
				})
				inventory.InheritanceEdges = append(inventory.InheritanceEdges, schema.CatalogInheritanceEdge{
					ParentRelationOID: 999, ChildRelationOID: 120, SequenceNumber: 2,
				})
			},
			contains: "outside the archival group",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			invalid := newArchivedPartitionResolverFixture(t)
			testCase.mutate(&invalid.snapshot.Inventory)
			_, err := resolveArchivedState(resolverTestPrefix, invalid.snapshot, schema.SchemaSnapshot{})
			require.Error(t, err)
			assert.ErrorContains(t, err, testCase.contains)
		})
	}

	t.Run("multiple marker roots", func(t *testing.T) {
		invalid := newArchivedPartitionResolverFixture(t)
		invalid.marker.PartitionEdges = invalid.marker.PartitionEdges[:1]
		payload, err := json.Marshal(canonicalizeArchivalMarker(invalid.marker))
		require.NoError(t, err)
		invalidMarker := archivalMarkerEnvelopeV1 +
			base64.RawURLEncoding.EncodeToString(payload)
		for idx := range invalid.snapshot.Inventory.Schemas {
			if isReservedArchivalSchemaName(invalid.snapshot.Inventory.Schemas[idx].Name, resolverTestPrefix) {
				invalid.snapshot.Inventory.Schemas[idx].Comment = invalidMarker
			}
		}
		_, err = resolveArchivedState(resolverTestPrefix, invalid.snapshot, schema.SchemaSnapshot{})
		require.Error(t, err)
		assert.ErrorContains(t, err, "must contain 2 edges")
	})
}

func TestResolveArchivedStateDependencySchemasAreStructuralOnly(t *testing.T) {
	for _, moved := range []bool{false, true} {
		t.Run(fmt.Sprintf("moved_%t", moved), func(t *testing.T) {
			fixture := newArchivedDependencyResolverFixture(t, moved)
			assert.Empty(t, fixture.snapshot.Inventory.Dependencies,
				"Stage 9 must not require or recompute dependency closure")

			resolution, err := resolveArchivedState(resolverTestPrefix,
				fixture.snapshot, schema.SchemaSnapshot{})
			require.NoError(t, err)
			candidate := resolution.CandidateGroups[0]
			if moved {
				assert.Equal(t, archivedCandidateGroupStateCompleteCandidate, candidate.State)
				assert.Empty(t, candidate.Resume.RemainingDependencyObjectMoves)
			} else {
				assert.Equal(t, archivedCandidateGroupStatePartialResumable, candidate.State)
				require.Len(t, candidate.Resume.RemainingDependencyObjectMoves, 1)
				move := candidate.Resume.RemainingDependencyObjectMoves[0]
				assert.Equal(t, "public", move.Source.SchemaName)
				assert.Equal(t, fixture.marker.ExclusiveDependencySchemas[0].Name, move.Destination.SchemaName)
			}
		})
	}

	t.Run("unexpected dependency schema object", func(t *testing.T) {
		fixture := newArchivedDependencyResolverFixture(t, true)
		dependencySchema := fixture.marker.ExclusiveDependencySchemas[0].Name
		fixture.snapshot.Inventory.Routines = append(fixture.snapshot.Inventory.Routines, schema.CatalogRoutine{
			OID: 201, SchemaName: dependencySchema, Name: "unexpected", IdentityArguments: "",
		})
		_, err := resolveArchivedState(resolverTestPrefix, fixture.snapshot, schema.SchemaSnapshot{})
		require.Error(t, err)
		assert.ErrorContains(t, err, "unexpected catalog object")
	})

	t.Run("declared empty dependency schema", func(t *testing.T) {
		fixture := newArchivedDependencyResolverFixture(t, true)
		fixture.marker.ExclusiveDependencyObjects = nil
		fixture.snapshot.Inventory.Routines = nil
		setFixtureMarkers(t, &fixture)
		resolution, err := resolveArchivedState(resolverTestPrefix, fixture.snapshot, schema.SchemaSnapshot{})
		require.NoError(t, err)
		assert.Equal(t, archivedCandidateGroupStateCompleteCandidate, resolution.CandidateGroups[0].State)
	})
}

func TestResolveArchivedStateCustomPrefixTargetValidationAndInventoryDiscovery(t *testing.T) {
	fixture := newArchivedResolverFixture(t, true, true)
	customName, err := buildArchivalSchemaName(
		"deleted", "public", "accounts", "20260721T091011123456Z", "ABCDEFGH",
	)
	require.NoError(t, err)
	oldName := fixture.cleanupName
	fixture.cleanupName = customName
	for idx := range fixture.marker.Members {
		member := &fixture.marker.Members[idx]
		member.CleanupTable.SchemaName = customName
		for _, objects := range [][]archivalMarkerObjectIdentity{
			member.AutomaticallyMovedObjects, member.AttachedObjects, member.ExplicitlyMovedObjects,
		} {
			for objectIdx := range objects {
				objects[objectIdx].SchemaName = customName
			}
		}
	}
	for idx := range fixture.snapshot.Inventory.Schemas {
		if fixture.snapshot.Inventory.Schemas[idx].Name == oldName {
			fixture.snapshot.Inventory.Schemas[idx].Name = customName
		}
	}
	replaceInventorySchemaName(&fixture.snapshot.Inventory, oldName, customName)
	setFixtureMarkers(t, &fixture)

	resolution, err := resolveArchivedState("deleted", fixture.snapshot, schema.SchemaSnapshot{})
	require.NoError(t, err)
	assert.Equal(t, []string{customName}, resolution.ProvisionalUntrustedSchemaNames)
	assert.Empty(t, fixture.snapshot.Schema.NamedSchemas,
		"the resolver must discover directly from inventory even when modeled cleanup exclusion is disabled or empty")

	target := schema.SchemaSnapshot{Inventory: schema.CatalogInventory{
		Schemas: []schema.CatalogSchema{{Name: customName}},
	}}
	_, err = resolveArchivedState("deleted", schema.SchemaSnapshot{}, target)
	require.Error(t, err)
	assert.ErrorContains(t, err, "target schema")

	_, err = resolveArchivedState("bad-prefix", schema.SchemaSnapshot{}, schema.SchemaSnapshot{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "simple PostgreSQL identifier")
}

func TestResolveArchivedStateDeterministicOrdering(t *testing.T) {
	first := newArchivedPartitionResolverFixture(t)
	second := newArchivedPartitionResolverFixture(t)
	slices.Reverse(second.snapshot.Inventory.Schemas)
	slices.Reverse(second.snapshot.Inventory.Relations)
	slices.Reverse(second.snapshot.Inventory.InheritanceEdges)
	slices.Reverse(second.snapshot.Inventory.PartitionAttachments)

	resolutionA, err := resolveArchivedState(resolverTestPrefix, first.snapshot, schema.SchemaSnapshot{})
	require.NoError(t, err)
	resolutionB, err := resolveArchivedState(resolverTestPrefix, second.snapshot, schema.SchemaSnapshot{})
	require.NoError(t, err)
	assert.Equal(t, resolutionA, resolutionB)
	assert.True(t, slices.IsSorted(resolutionA.ProvisionalUntrustedSchemaNames))
}

func newArchivedResolverFixture(t *testing.T, tableMoved, statisticMoved bool) archivedResolverFixture {
	t.Helper()
	cleanupName, err := buildArchivalSchemaName(
		resolverTestPrefix, "public", "accounts", "20260721T091011123456Z", "ABCDEFGH",
	)
	require.NoError(t, err)
	tableSchema, tableSchemaOID := "public", uint32(1)
	if tableMoved {
		tableSchema, tableSchemaOID = cleanupName, 2
	}
	statisticSchema, statisticSchemaOID := "public", uint32(1)
	if statisticMoved {
		statisticSchema, statisticSchemaOID = cleanupName, 2
	}
	inventory := schema.CatalogInventory{
		Schemas: []schema.CatalogSchema{
			{OID: 1, Name: "public"},
			{OID: 2, Name: cleanupName},
		},
		Relations: []schema.CatalogRelation{
			{
				OID: 10, SchemaOID: tableSchemaOID, SchemaName: tableSchema, Name: "accounts",
				Kind: schema.RelKindOrdinaryTable, RowTypeOID: 11, ArrayTypeOID: 12,
				ToastRelation: &schema.CatalogRelationIdentity{
					OID: 20, SchemaOID: 9, SchemaName: "pg_toast", Name: "pg_toast_10",
				},
			},
			{OID: 13, SchemaOID: tableSchemaOID, SchemaName: tableSchema, Name: "accounts_pkey", Kind: schema.RelKindIndex},
			{OID: 18, SchemaOID: tableSchemaOID, SchemaName: tableSchema, Name: "accounts_id_seq", Kind: schema.RelKindSequence},
		},
		Types: []schema.CatalogType{
			{
				OID: 11, SchemaOID: tableSchemaOID, SchemaName: tableSchema, Name: "accounts",
				Kind: schema.CatalogTypeKindRow, RelationOID: 10,
			},
			{
				OID: 12, SchemaOID: tableSchemaOID, SchemaName: tableSchema, Name: "_accounts",
				Kind: schema.CatalogTypeKindArray, RelationOID: 10,
			},
		},
		Indexes: []schema.CatalogIndex{{
			OID: 13, SchemaOID: tableSchemaOID,
			SchemaName: tableSchema, Name: "accounts_pkey", RelationOID: 10,
		}},
		Constraints: []schema.CatalogConstraint{{
			OID: 14, SchemaOID: tableSchemaOID, SchemaName: tableSchema, Name: "accounts_pkey", RelationOID: 10,
		}},
		Triggers: []schema.CatalogTrigger{{OID: 15, RelationOID: 10, Name: "accounts_trigger"}},
		Rules:    []schema.CatalogRule{{OID: 16, RelationOID: 10, Name: "accounts_rule"}},
		Policies: []schema.CatalogPolicy{{OID: 17, RelationOID: 10, Name: "accounts_policy"}},
		Sequences: []schema.CatalogSequence{{
			OID: 18, SchemaOID: tableSchemaOID, SchemaName: tableSchema, Name: "accounts_id_seq",
		}},
		OwnedSequences: []schema.CatalogOwnedSequence{{SequenceOID: 18, RelationOID: 10, ColumnNumber: 1}},
		ExtendedStatistics: []schema.CatalogExtendedStatistic{{
			OID: 19, SchemaOID: statisticSchemaOID, SchemaName: statisticSchema,
			Name: "accounts_stats", RelationOID: 10,
		}},
	}
	move, err := inventory.ExpectedTableMove(10)
	require.NoError(t, err)
	cleanupTable := markerObject(10, archivalMarkerObjectKindTable, cleanupName, "accounts")
	marker := archivalMarkerV1{
		Version: archivalMarkerVersion,
		GroupID: resolverTestGroupID,
		Members: []archivalMarkerMemberV1{{
			MemberID:                  "member-accounts",
			SourceTable:               markerObject(10, archivalMarkerObjectKindTable, "public", "accounts"),
			CleanupTable:              cleanupTable,
			AutomaticallyMovedObjects: markerObjectsFromCatalog(move.CleanupSchemaObjects, cleanupName),
			AttachedObjects:           markerObjectsFromCatalog(move.AttachedObjects, cleanupName),
			ExplicitlyMovedObjects:    markerObjectsFromCatalog(move.ExplicitMoveObjects, cleanupName),
			InternalToastObjects:      markerObjectsFromCatalog(move.InternalObjects, ""),
		}},
		CleanupDigest: cleanupOperationDigest("sha256:" + strings.Repeat("0", 64)),
	}
	fixture := archivedResolverFixture{
		snapshot: schema.SchemaSnapshot{Inventory: inventory}, marker: marker, cleanupName: cleanupName,
	}
	setFixtureMarkers(t, &fixture)
	return fixture
}

func newArchivedDependencyResolverFixture(t *testing.T, dependencyMoved bool) archivedResolverFixture {
	t.Helper()
	fixture := newArchivedResolverFixture(t, true, true)
	dependencySchema, err := buildArchivalSchemaName(
		resolverTestPrefix, "dependency", "functions", "20260721T091011123456Z", "ABCDEFGH",
	)
	require.NoError(t, err)
	actualSchema := "public"
	if dependencyMoved {
		actualSchema = dependencySchema
	}
	fixture.marker.ExclusiveDependencySchemas = []archivalMarkerSchemaIdentity{{Name: dependencySchema}}
	fixture.marker.ExclusiveDependencyObjects = []archivalMarkerObjectIdentity{
		markerFunction(200, dependencySchema, "account_total"),
	}
	fixture.snapshot.Inventory.Schemas = append(fixture.snapshot.Inventory.Schemas,
		schema.CatalogSchema{OID: 3, Name: dependencySchema})
	fixture.snapshot.Inventory.Routines = append(fixture.snapshot.Inventory.Routines, schema.CatalogRoutine{
		OID: 200, SchemaOID: map[bool]uint32{false: 1, true: 3}[dependencyMoved],
		SchemaName: actualSchema, Name: "account_total",
	})
	setFixtureMarkers(t, &fixture)
	return fixture
}

func newArchivedPartitionResolverFixture(t *testing.T) archivedResolverFixture {
	t.Helper()
	timestamp, nonce := "20260721T091011123456Z", "ABCDEFGH"
	type memberSpec struct {
		id           string
		oid          uint32
		sourceSchema string
		name         string
		kind         schema.RelKind
		isPartition  bool
		moved        bool
		archiveOID   uint32
	}
	specs := []memberSpec{
		{
			id: "member-root", oid: 100, sourceSchema: "root_schema", name: "events",
			kind: schema.RelKindPartitionedTable, moved: true, archiveOID: 11,
		},
		{
			id: "member-branch", oid: 110, sourceSchema: "branch_schema", name: "events_2026",
			kind: schema.RelKindPartitionedTable, isPartition: true, moved: true, archiveOID: 12,
		},
		{
			id: "member-leaf", oid: 120, sourceSchema: "leaf_schema", name: "events_july",
			kind: schema.RelKindOrdinaryTable, isPartition: true, archiveOID: 13,
		},
	}
	inventory := schema.CatalogInventory{
		Schemas: []schema.CatalogSchema{
			{OID: 1, Name: "root_schema"}, {OID: 2, Name: "branch_schema"}, {OID: 3, Name: "leaf_schema"},
		},
		InheritanceEdges: []schema.CatalogInheritanceEdge{
			{ParentRelationOID: 100, ChildRelationOID: 110, SequenceNumber: 1},
			{ParentRelationOID: 110, ChildRelationOID: 120, SequenceNumber: 1},
		},
		PartitionAttachments: []schema.CatalogPartitionAttachment{
			{ParentRelationOID: 100, RelationOID: 110, BoundExpression: "FOR VALUES FROM (0) TO (100)"},
			{ParentRelationOID: 110, RelationOID: 120, BoundExpression: "FOR VALUES FROM (0) TO (10)"},
		},
	}
	marker := archivalMarkerV1{
		Version: archivalMarkerVersion, GroupID: resolverTestGroupID,
		PartitionEdges: []archivalMarkerPartitionEdgeV1{
			{ParentMemberID: "member-root", ChildMemberID: "member-branch"},
			{ParentMemberID: "member-branch", ChildMemberID: "member-leaf"},
		},
		CleanupDigest: cleanupOperationDigest("sha256:" + strings.Repeat("0", 64)),
	}
	for _, spec := range specs {
		cleanupName, err := buildArchivalSchemaName(
			resolverTestPrefix, spec.sourceSchema, spec.name, timestamp, nonce,
		)
		require.NoError(t, err)
		currentSchema, currentSchemaOID := spec.sourceSchema, uint32(1)
		if spec.moved {
			currentSchema, currentSchemaOID = cleanupName, spec.archiveOID
		}
		inventory.Schemas = append(inventory.Schemas, schema.CatalogSchema{
			OID: spec.archiveOID, Name: cleanupName,
		})
		inventory.Relations = append(inventory.Relations, schema.CatalogRelation{
			OID: spec.oid, SchemaOID: currentSchemaOID, SchemaName: currentSchema, Name: spec.name,
			Kind: spec.kind, IsPartition: spec.isPartition,
		})
		cleanupTable := markerObject(spec.oid, archivalMarkerObjectKindTable, cleanupName, spec.name)
		marker.Members = append(marker.Members, archivalMarkerMemberV1{
			MemberID:                  spec.id,
			SourceTable:               markerObject(spec.oid, archivalMarkerObjectKindTable, spec.sourceSchema, spec.name),
			CleanupTable:              cleanupTable,
			AutomaticallyMovedObjects: []archivalMarkerObjectIdentity{cleanupTable},
		})
	}
	fixture := archivedResolverFixture{snapshot: schema.SchemaSnapshot{Inventory: inventory}, marker: marker}
	setFixtureMarkers(t, &fixture)
	return fixture
}

func setFixtureMarkers(t *testing.T, fixture *archivedResolverFixture) {
	t.Helper()
	marker, err := marshalArchivalMarker(fixture.marker)
	require.NoError(t, err)
	declared := archivedMarkerSchemaNames(fixture.marker)
	for idx := range fixture.snapshot.Inventory.Schemas {
		if slices.Contains(declared, fixture.snapshot.Inventory.Schemas[idx].Name) {
			fixture.snapshot.Inventory.Schemas[idx].Comment = marker
		}
	}
}

func replaceInventorySchemaName(inventory *schema.CatalogInventory, oldName, newName string) {
	for idx := range inventory.Relations {
		if inventory.Relations[idx].SchemaName == oldName {
			inventory.Relations[idx].SchemaName = newName
		}
	}
	for idx := range inventory.Types {
		if inventory.Types[idx].SchemaName == oldName {
			inventory.Types[idx].SchemaName = newName
		}
	}
	for idx := range inventory.Indexes {
		if inventory.Indexes[idx].SchemaName == oldName {
			inventory.Indexes[idx].SchemaName = newName
		}
	}
	for idx := range inventory.Constraints {
		if inventory.Constraints[idx].SchemaName == oldName {
			inventory.Constraints[idx].SchemaName = newName
		}
	}
	for idx := range inventory.Sequences {
		if inventory.Sequences[idx].SchemaName == oldName {
			inventory.Sequences[idx].SchemaName = newName
		}
	}
	for idx := range inventory.ExtendedStatistics {
		if inventory.ExtendedStatistics[idx].SchemaName == oldName {
			inventory.ExtendedStatistics[idx].SchemaName = newName
		}
	}
}
