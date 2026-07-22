package diff

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

func TestArchivalNamesUseSingleTimestampCapturedByGenerate(t *testing.T) {
	clockTime := time.Date(2026, time.July, 21, 9, 10, 11, 123456789,
		time.FixedZone("test", -7*60*60))
	clockReads := 0
	var capturedOptions *planOptions
	captureOpt := func(opts *planOptions) {
		capturedOptions = opts
		opts.now = func() time.Time {
			clockReads++
			return clockTime
		}
		opts.randReader = bytes.NewReader([]byte{
			0, 1, 2, 3, 4, 5, 6, 7,
			8, 9, 10, 11, 12, 13, 14, 15,
		})
	}
	source := fakeSchemaSource{
		t: t,
		expectedDeps: schemaSourcePlanDeps{
			logger:        slog.Default(),
			getSchemaOpts: make([]schema.GetSchemaOpt, 1),
		},
		snapshot: schema.SchemaSnapshot{Hash: "snapshot-hash"},
	}

	_, err := Generate(t.Context(), source, source, WithDoNotValidatePlan(), captureOpt)
	require.NoError(t, err)
	require.NotNil(t, capturedOptions)
	assert.Equal(t, 1, clockReads)

	inventory := schema.CatalogInventory{Relations: []schema.CatalogRelation{
		{OID: 2, SchemaName: "second", Name: "table_name", Kind: schema.RelKindOrdinaryTable},
		{OID: 1, SchemaName: "first", Name: "table_name", Kind: schema.RelKindOrdinaryTable},
	}}
	allocations, err := allocateArchivalNames(
		capturedOptions,
		inventory,
		schema.CatalogInventory{},
		[]archivalGroupNameAllocationRequest{
			ordinaryArchivalNameRequest(2),
			ordinaryArchivalNameRequest(1),
		},
	)
	require.NoError(t, err)
	require.Len(t, allocations, 2)
	assert.Equal(t, "20260721T161011123456Z", allocations[0].Timestamp)
	assert.Equal(t, allocations[0].Timestamp, allocations[1].Timestamp)
	assert.Equal(t, []string{"ABCDEFGH", "IJKLMNOP"},
		[]string{allocations[0].Nonce, allocations[1].Nonce})
}

func TestAllocateArchivalNamesUsesGenerationContextAndDeterministicOrder(t *testing.T) {
	timestamp := time.Date(2026, time.July, 21, 9, 10, 11, 123456789,
		time.FixedZone("test", -7*60*60))
	inventory := schema.CatalogInventory{Relations: []schema.CatalogRelation{
		{OID: 2, SchemaName: "z_schema", Name: "z_table", Kind: schema.RelKindOrdinaryTable},
		{OID: 1, SchemaName: "a_schema", Name: "a_table", Kind: schema.RelKindOrdinaryTable},
	}}
	requests := []archivalGroupNameAllocationRequest{
		ordinaryArchivalNameRequest(2),
		ordinaryArchivalNameRequest(1),
	}
	planOpts := archivalNameTestPlanOptions(timestamp, bytes.NewReader([]byte{
		0, 1, 2, 3, 4, 5, 6, 7,
		8, 9, 10, 11, 12, 13, 14, 15,
	}))

	allocations, err := allocateArchivalNames(planOpts, inventory, schema.CatalogInventory{}, requests)
	require.NoError(t, err)
	require.Len(t, allocations, 2)

	assert.Equal(t, "20260721T161011123456Z", allocations[0].Timestamp)
	assert.Equal(t, allocations[0].Timestamp, allocations[1].Timestamp)
	assert.Equal(t, "ABCDEFGH", allocations[0].Nonce)
	assert.Equal(t, "IJKLMNOP", allocations[1].Nonce)
	assert.Equal(t, archivalGroupID("20260721T161011123456Z_ABCDEFGH"), allocations[0].GroupID)
	assert.Equal(t, archivalGroupID("20260721T161011123456Z_IJKLMNOP"), allocations[1].GroupID)
	assert.Equal(t, "archive_dep_o_20260721T161011123456Z_ABCDEFGH",
		allocations[0].DependencySchemaName)
	assert.Equal(t, `"archive_dep_o_20260721T161011123456Z_ABCDEFGH"`,
		allocations[0].EscapedDependencySchemaName)
	assert.Equal(t, uint32(1), allocations[0].Members[0].RelationOID)
	assert.Equal(t, uint32(2), allocations[1].Members[0].RelationOID)
	assert.Equal(
		t,
		"archive_a_schema_a_table_20260721T161011123456Z_ABCDEFGH",
		allocations[0].Members[0].CleanupSchemaName,
	)
}

func TestAllocateArchivalNamesReturnsRawAndEscapedUnicodeNames(t *testing.T) {
	inventory := schema.CatalogInventory{Relations: []schema.CatalogRelation{{
		OID: 1, SchemaName: `MiXed "Schema`, Name: `táb"le`, Kind: schema.RelKindOrdinaryTable,
	}}}

	allocations, err := allocateArchivalNames(
		archivalNameTestPlanOptions(archivalNameTestTimestamp(), bytes.NewReader(make([]byte, 8))),
		inventory,
		schema.CatalogInventory{},
		[]archivalGroupNameAllocationRequest{ordinaryArchivalNameRequest(1)},
	)
	require.NoError(t, err)
	require.Len(t, allocations, 1)
	require.Len(t, allocations[0].Members, 1)

	member := allocations[0].Members[0]
	expectedRaw := `archive_MiXed "Schema_táb"le_20260721T091011123456Z_AAAAAAAA`
	assert.Equal(t, `MiXed "Schema`, member.SourceSchemaName)
	assert.Equal(t, `táb"le`, member.SourceTableName)
	assert.Equal(t, expectedRaw, member.CleanupSchemaName)
	assert.Equal(t, `"archive_MiXed ""Schema_táb""le_20260721T091011123456Z_AAAAAAAA"`,
		member.EscapedCleanupSchemaName)
	assert.True(t, utf8.ValidString(member.CleanupSchemaName))
}

func TestAllocateArchivalNamesPropagatesRandomReaderFailures(t *testing.T) {
	inventory := schema.CatalogInventory{Relations: []schema.CatalogRelation{{
		OID: 1, SchemaName: "public", Name: "table_name", Kind: schema.RelKindOrdinaryTable,
	}}}
	request := []archivalGroupNameAllocationRequest{ordinaryArchivalNameRequest(1)}
	expectedErr := errors.New("random source failed")

	for _, testCase := range []struct {
		name   string
		reader io.Reader
		err    error
	}{
		{name: "reader error", reader: archivalNameErrorReader{err: expectedErr}, err: expectedErr},
		{name: "short read", reader: bytes.NewReader(make([]byte, 7)), err: io.ErrUnexpectedEOF},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			allocations, err := allocateArchivalNames(
				archivalNameTestPlanOptions(archivalNameTestTimestamp(), testCase.reader),
				inventory,
				schema.CatalogInventory{},
				request,
			)
			assert.Nil(t, allocations)
			require.ErrorIs(t, err, testCase.err)
		})
	}
}

func TestBuildArchivalSchemaNameEnforcesIdentifierLimit(t *testing.T) {
	timestamp := "20260721T091011123456Z"

	t.Run("exact 63 byte boundary", func(t *testing.T) {
		name, err := buildArchivalSchemaName(
			"archive", strings.Repeat("s", 10), strings.Repeat("t", 12), timestamp, "ABCDEFGH",
		)
		require.NoError(t, err)
		assert.Len(t, name, maxPostgresIdentifierSize)
		assert.Contains(t, name, strings.Repeat("s", 10)+"_"+strings.Repeat("t", 12))
	})

	t.Run("maximum prefix and one rune preservation", func(t *testing.T) {
		prefix := "abcdefghijklmnopqrstu"
		name, err := buildArchivalSchemaName(prefix, "😀suffix", "😁suffix", timestamp, "ABCDEFGH")
		require.NoError(t, err)
		assert.Len(t, name, maxPostgresIdentifierSize)
		assert.Equal(t, prefix+"_😀_😁_"+timestamp+"_ABCDEFGH", name)
		assert.True(t, utf8.ValidString(name))
	})

	t.Run("UTF-8 rune boundary", func(t *testing.T) {
		name, err := buildArchivalSchemaName("archive", "éééééé", "界界界界界", timestamp, "ABCDEFGH")
		require.NoError(t, err)
		assert.Equal(t, "archive_ééééé_界界界界_"+timestamp+"_ABCDEFGH", name)
		assert.Len(t, name, maxPostgresIdentifierSize)
		assert.True(t, utf8.ValidString(name))
	})

	t.Run("impossible minimum", func(t *testing.T) {
		sourceSchema, sourceTable, err := truncateArchivalSourceComponents("😀schema", "😁table", 7)
		assert.Empty(t, sourceSchema)
		assert.Empty(t, sourceTable)
		assert.ErrorContains(t, err, "one rune from each source component requires 8 bytes")
	})

	t.Run("invalid UTF-8", func(t *testing.T) {
		_, _, err := truncateArchivalSourceComponents(string([]byte{0xff}), "table", 20)
		assert.ErrorContains(t, err, "valid UTF-8")
	})
}

func TestAllocateArchivalNamesRejectsDuplicateTruncatedNames(t *testing.T) {
	prefix := "abcdefghijklmnopqrstu"
	inventory := schema.CatalogInventory{
		Relations: []schema.CatalogRelation{
			{OID: 1, SchemaName: "😀root", Name: "😁root", Kind: schema.RelKindPartitionedTable},
			{OID: 2, SchemaName: "😀child", Name: "😁child", Kind: schema.RelKindOrdinaryTable, IsPartition: true},
		},
		InheritanceEdges: []schema.CatalogInheritanceEdge{{
			ChildRelationOID:  2,
			ParentRelationOID: 1, SequenceNumber: 1,
		}},
	}
	request, err := buildCompletePartitionTreeArchivalNameAllocationRequest(inventory, 1)
	require.NoError(t, err)
	planOpts := archivalNameTestPlanOptions(archivalNameTestTimestamp(), bytes.NewReader(make([]byte, 8)))
	planOpts.schemaPartialArchivalPrefix = prefix

	allocations, err := allocateArchivalNames(
		planOpts, inventory, schema.CatalogInventory{}, []archivalGroupNameAllocationRequest{request},
	)
	assert.Nil(t, allocations)
	assert.ErrorContains(t, err, "duplicate generated archival schema name")
}

func TestAllocateArchivalNamesRejectsUnfilteredSchemaCollisions(t *testing.T) {
	const generatedName = "archive_public_table_name_20260721T091011123456Z_AAAAAAAA"
	baseInventory := schema.CatalogInventory{Relations: []schema.CatalogRelation{{
		OID: 1, SchemaName: "public", Name: "table_name", Kind: schema.RelKindOrdinaryTable,
	}}}

	for _, testCase := range []struct {
		name              string
		currentInventory  schema.CatalogInventory
		targetInventory   schema.CatalogInventory
		expectedErrorText string
	}{
		{
			name: "current unfiltered inventory",
			currentInventory: schema.CatalogInventory{
				Relations: baseInventory.Relations,
				Schemas:   []schema.CatalogSchema{{Name: generatedName}},
			},
			expectedErrorText: "collides with a current schema",
		},
		{
			name:              "target unfiltered inventory",
			currentInventory:  baseInventory,
			targetInventory:   schema.CatalogInventory{Schemas: []schema.CatalogSchema{{Name: generatedName}}},
			expectedErrorText: "collides with a target schema",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			allocations, err := allocateArchivalNames(
				archivalNameTestPlanOptions(archivalNameTestTimestamp(), bytes.NewReader(make([]byte, 8))),
				testCase.currentInventory,
				testCase.targetInventory,
				[]archivalGroupNameAllocationRequest{ordinaryArchivalNameRequest(1)},
			)
			assert.Nil(t, allocations)
			assert.ErrorContains(t, err, testCase.expectedErrorText)
		})
	}
}

func TestAllocateArchivalNamesRejectsDependencySchemaCollisions(t *testing.T) {
	const dependencySchema = "archive_dep_o_20260721T091011123456Z_AAAAAAAA"
	baseInventory := schema.CatalogInventory{Relations: []schema.CatalogRelation{{
		OID: 1, SchemaName: "public", Name: "table_name", Kind: schema.RelKindOrdinaryTable,
	}}}
	for _, testCase := range []struct {
		name     string
		current  schema.CatalogInventory
		target   schema.CatalogInventory
		contains string
	}{
		{
			name: "current schema", current: schema.CatalogInventory{
				Relations: baseInventory.Relations, Schemas: []schema.CatalogSchema{{Name: dependencySchema}},
			}, contains: "collides with a current schema",
		},
		{
			name: "target schema", current: baseInventory,
			target:   schema.CatalogInventory{Schemas: []schema.CatalogSchema{{Name: dependencySchema}}},
			contains: "collides with a target schema",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			allocations, err := allocateArchivalNames(
				archivalNameTestPlanOptions(archivalNameTestTimestamp(), bytes.NewReader(make([]byte, 8))),
				testCase.current, testCase.target,
				[]archivalGroupNameAllocationRequest{ordinaryArchivalNameRequest(1)},
			)
			assert.Nil(t, allocations)
			assert.ErrorContains(t, err, testCase.contains)
		})
	}

	t.Run("member allocation", func(t *testing.T) {
		inventory := schema.CatalogInventory{Relations: []schema.CatalogRelation{{
			OID: 1, SchemaName: "dep", Name: "o", Kind: schema.RelKindOrdinaryTable,
		}}}
		allocations, err := allocateArchivalNames(
			archivalNameTestPlanOptions(archivalNameTestTimestamp(), bytes.NewReader(make([]byte, 8))),
			inventory, schema.CatalogInventory{},
			[]archivalGroupNameAllocationRequest{ordinaryArchivalNameRequest(1)},
		)
		assert.Nil(t, allocations)
		assert.ErrorContains(t, err, "duplicate generated archival schema name")
	})
}

func TestAllocateArchivalNamesRejectsReservedTargetGrammar(t *testing.T) {
	reserved := "archive_source_schema_source_table_20260721T091011123456Z_Ab0$_z9Q"
	allocations, err := allocateArchivalNames(
		archivalNameTestPlanOptions(archivalNameTestTimestamp(), bytes.NewReader(nil)),
		schema.CatalogInventory{},
		schema.CatalogInventory{Schemas: []schema.CatalogSchema{{Name: reserved}}},
		nil,
	)
	assert.Nil(t, allocations)
	assert.ErrorContains(t, err, "reserved archival naming grammar")

	for _, name := range []string{
		"other_source_table_20260721T091011123456Z_ABCDEFGH",
		"archive_sourcetable_20260721T091011123456Z_ABCDEFGH",
		"archive_source_table_20260721T091011.123456Z_ABCDEFGH",
		"archive_source_table_20260230T091011123456Z_ABCDEFGH",
		"archive_source_table_20260721T091011123456Z_ABCDEFG",
		"archive_source_table_20260721T091011123456Z_ABC-1234",
	} {
		assert.False(t, isReservedArchivalSchemaName(name, "archive"), name)
	}
	assert.True(t, isReservedArchivalSchemaName(reserved, "archive"))

	falsePositiveTarget := schema.CatalogInventory{Schemas: []schema.CatalogSchema{
		{Name: "archive_source_table_20260230T091011123456Z_ABCDEFGH"},
		{Name: "archive_source_table_20260721T091011123456Z_ABC-1234"},
	}}
	allocations, err = allocateArchivalNames(
		archivalNameTestPlanOptions(archivalNameTestTimestamp(), bytes.NewReader(nil)),
		schema.CatalogInventory{}, falsePositiveTarget, nil,
	)
	require.NoError(t, err)
	assert.Empty(t, allocations)
}

func TestAllocateArchivalNamesValidatesDestinationNamespaces(t *testing.T) {
	t.Run("allows linked table row type", func(t *testing.T) {
		inventory := schema.CatalogInventory{
			Relations: []schema.CatalogRelation{{
				OID: 1, SchemaName: "public", Name: "table_name", Kind: schema.RelKindOrdinaryTable,
			}},
			Types: []schema.CatalogType{{
				OID: 2, SchemaName: "public", Name: "table_name", Kind: schema.CatalogTypeKindRow, RelationOID: 1,
			}},
		}
		allocations, err := allocateArchivalNames(
			archivalNameTestPlanOptions(archivalNameTestTimestamp(), bytes.NewReader(make([]byte, 8))),
			inventory,
			schema.CatalogInventory{},
			[]archivalGroupNameAllocationRequest{ordinaryArchivalNameRequest(1)},
		)
		require.NoError(t, err)
		assert.Len(t, allocations, 1)
	})

	t.Run("rejects type namespace conflict", func(t *testing.T) {
		inventory := schema.CatalogInventory{
			Relations: []schema.CatalogRelation{{
				OID: 1, SchemaName: "public", Name: "shared_name", Kind: schema.RelKindOrdinaryTable,
			}},
			Types: []schema.CatalogType{
				{
					OID: 2, SchemaName: "public", Name: "shared_name", Kind: schema.CatalogTypeKindRow,
					RelationOID: 1,
				},
				{
					OID: 3, SchemaName: "other", Name: "shared_name", Kind: schema.CatalogTypeKindArray,
					RelationOID: 1,
				},
			},
		}
		assertArchivalAllocationError(t, inventory, ordinaryArchivalNameRequest(1),
			"conflicting type objects")
	})

	t.Run("rejects index and owned sequence conflict", func(t *testing.T) {
		inventory := schema.CatalogInventory{
			Relations: []schema.CatalogRelation{{
				OID: 1, SchemaName: "public", Name: "table_name", Kind: schema.RelKindOrdinaryTable,
			}},
			Indexes: []schema.CatalogIndex{{OID: 2, Name: "shared_name", RelationOID: 1}},
			Sequences: []schema.CatalogSequence{{
				OID: 3, SchemaName: "other", Name: "shared_name",
			}},
			OwnedSequences: []schema.CatalogOwnedSequence{{SequenceOID: 3, RelationOID: 1}},
		}
		assertArchivalAllocationError(t, inventory, ordinaryArchivalNameRequest(1),
			"conflicting relation objects named \"shared_name\"")
	})

	t.Run("rejects explicit dependency conflict with no partial output", func(t *testing.T) {
		inventory := schema.CatalogInventory{Relations: []schema.CatalogRelation{
			{OID: 1, SchemaName: "a", Name: "valid", Kind: schema.RelKindOrdinaryTable},
			{OID: 2, SchemaName: "b", Name: "invalid", Kind: schema.RelKindOrdinaryTable},
		}}
		invalidRequest := ordinaryArchivalNameRequest(2)
		invalidRequest.Members[0].ExplicitDependencyObjects = []schema.CatalogObjectIdentity{
			{Kind: schema.CatalogObjectKindExtendedStatistic, OID: 10, Name: "duplicate"},
			{Kind: schema.CatalogObjectKindExtendedStatistic, OID: 11, Name: "duplicate"},
		}
		allocations, err := allocateArchivalNames(
			archivalNameTestPlanOptions(archivalNameTestTimestamp(), bytes.NewReader(make([]byte, 16))),
			inventory,
			schema.CatalogInventory{},
			[]archivalGroupNameAllocationRequest{invalidRequest, ordinaryArchivalNameRequest(1)},
		)
		assert.Nil(t, allocations)
		assert.ErrorContains(t, err, "conflicting extended statistic objects")
	})
}

func TestBuildCompletePartitionTreeArchivalNameAllocationRequest(t *testing.T) {
	inventory := schema.CatalogInventory{
		Relations: []schema.CatalogRelation{
			{OID: 1, SchemaName: "z_root", Name: "same_name", Kind: schema.RelKindPartitionedTable},
			{
				OID: 2, SchemaName: "a_branch", Name: "same_name", Kind: schema.RelKindPartitionedTable,
				IsPartition: true,
			},
			{OID: 3, SchemaName: "m_leaf", Name: "same_name", Kind: schema.RelKindOrdinaryTable, IsPartition: true},
		},
		InheritanceEdges: []schema.CatalogInheritanceEdge{
			{ChildRelationOID: 3, ParentRelationOID: 2, SequenceNumber: 1},
			{ChildRelationOID: 2, ParentRelationOID: 1, SequenceNumber: 1},
		},
	}

	request, err := buildCompletePartitionTreeArchivalNameAllocationRequest(inventory, 1)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), request.RootRelationOID)
	assert.Equal(t, []uint32{2, 3, 1}, archivalMemberRequestOIDs(request.Members))

	allocations, err := allocateArchivalNames(
		archivalNameTestPlanOptions(archivalNameTestTimestamp(), bytes.NewReader(make([]byte, 8))),
		inventory,
		schema.CatalogInventory{},
		[]archivalGroupNameAllocationRequest{request},
	)
	require.NoError(t, err)
	require.Len(t, allocations, 1)
	assert.Equal(t, []uint32{2, 3, 1}, archivalMemberAllocationOIDs(allocations[0].Members))
	for _, member := range allocations[0].Members {
		assert.Contains(t, member.CleanupSchemaName, string(allocations[0].GroupID))
	}
	assert.NotEqual(t, allocations[0].Members[0].CleanupSchemaName,
		allocations[0].Members[1].CleanupSchemaName,
		"same table names in different source schemas must receive different cleanup schemas")
}

func TestBuildCompletePartitionTreeRejectsUnsupportedInheritance(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		inventory schema.CatalogInventory
		rootOID   uint32
		contains  string
	}{
		{
			name: "foreign root",
			inventory: schema.CatalogInventory{Relations: []schema.CatalogRelation{{
				OID: 1, SchemaName: "public", Name: "foreign_table", Kind: schema.RelKindForeignTable,
			}}},
			rootOID:  1,
			contains: "unsupported classification \"foreign\"",
		},
		{
			name: "traditional inheritance",
			inventory: schema.CatalogInventory{
				Relations: []schema.CatalogRelation{
					{OID: 1, SchemaName: "public", Name: "parent", Kind: schema.RelKindOrdinaryTable},
					{OID: 2, SchemaName: "public", Name: "child", Kind: schema.RelKindOrdinaryTable},
				},
				InheritanceEdges: []schema.CatalogInheritanceEdge{{
					ChildRelationOID: 2, ParentRelationOID: 1,
				}},
			},
			rootOID:  1,
			contains: "traditional_inheritance",
		},
		{
			name: "foreign partition",
			inventory: schema.CatalogInventory{
				Relations: []schema.CatalogRelation{
					{OID: 1, SchemaName: "public", Name: "root", Kind: schema.RelKindPartitionedTable},
					{
						OID: 2, SchemaName: "public", Name: "foreign_child", Kind: schema.RelKindForeignTable,
						IsPartition: true,
					},
				},
				InheritanceEdges: []schema.CatalogInheritanceEdge{{
					ChildRelationOID: 2, ParentRelationOID: 1,
				}},
			},
			rootOID:  1,
			contains: "foreign_partition",
		},
		{
			name: "multiple inheritance",
			inventory: schema.CatalogInventory{
				Relations: []schema.CatalogRelation{
					{OID: 1, SchemaName: "public", Name: "root", Kind: schema.RelKindPartitionedTable},
					{OID: 2, SchemaName: "public", Name: "other", Kind: schema.RelKindPartitionedTable},
					{
						OID: 3, SchemaName: "public", Name: "child", Kind: schema.RelKindOrdinaryTable,
						IsPartition: true,
					},
				},
				InheritanceEdges: []schema.CatalogInheritanceEdge{
					{ChildRelationOID: 3, ParentRelationOID: 1, SequenceNumber: 1},
					{ChildRelationOID: 3, ParentRelationOID: 2, SequenceNumber: 2},
				},
			},
			rootOID:  1,
			contains: "has 2 inheritance parents",
		},
		{
			name: "cyclic topology",
			inventory: schema.CatalogInventory{
				Relations: []schema.CatalogRelation{
					{
						OID: 1, SchemaName: "public", Name: "root", Kind: schema.RelKindPartitionedTable,
						IsPartition: true,
					},
					{
						OID: 2, SchemaName: "public", Name: "child", Kind: schema.RelKindPartitionedTable,
						IsPartition: true,
					},
				},
				InheritanceEdges: []schema.CatalogInheritanceEdge{
					{ChildRelationOID: 2, ParentRelationOID: 1, SequenceNumber: 1},
					{ChildRelationOID: 1, ParentRelationOID: 2, SequenceNumber: 1},
				},
			},
			rootOID:  1,
			contains: "contains an inheritance cycle",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			request, err := buildCompletePartitionTreeArchivalNameAllocationRequest(
				testCase.inventory, testCase.rootOID,
			)
			assert.Empty(t, request.Members)
			assert.ErrorContains(t, err, testCase.contains)
		})
	}
}

func TestAllocateArchivalNamesRejectsIncompletePartitionTree(t *testing.T) {
	inventory := schema.CatalogInventory{
		Relations: []schema.CatalogRelation{
			{OID: 1, SchemaName: "public", Name: "root", Kind: schema.RelKindPartitionedTable},
			{OID: 2, SchemaName: "public", Name: "child", Kind: schema.RelKindOrdinaryTable, IsPartition: true},
		},
		InheritanceEdges: []schema.CatalogInheritanceEdge{{ChildRelationOID: 2, ParentRelationOID: 1}},
	}
	allocations, err := allocateArchivalNames(
		archivalNameTestPlanOptions(archivalNameTestTimestamp(), bytes.NewReader(make([]byte, 8))),
		inventory,
		schema.CatalogInventory{},
		[]archivalGroupNameAllocationRequest{{
			RootRelationOID: 1,
			Members:         []archivalPhysicalMemberNameAllocationRequest{{RelationOID: 1}},
		}},
	)
	assert.Nil(t, allocations)
	assert.ErrorContains(t, err, "complete declarative partition tree")
}

func ordinaryArchivalNameRequest(relationOID uint32) archivalGroupNameAllocationRequest {
	return archivalGroupNameAllocationRequest{
		RootRelationOID: relationOID,
		Members: []archivalPhysicalMemberNameAllocationRequest{{
			RelationOID: relationOID,
		}},
	}
}

func archivalNameTestTimestamp() time.Time {
	return time.Date(2026, time.July, 21, 9, 10, 11, 123456789, time.UTC)
}

func archivalNameTestPlanOptions(timestamp time.Time, random io.Reader) *planOptions {
	return &planOptions{
		schemaPartialArchivalPrefix: "archive",
		generationTimestamp:         timestamp,
		randReader:                  random,
	}
}

type archivalNameErrorReader struct {
	err error
}

func (r archivalNameErrorReader) Read([]byte) (int, error) {
	return 0, r.err
}

func assertArchivalAllocationError(
	t *testing.T,
	inventory schema.CatalogInventory,
	request archivalGroupNameAllocationRequest,
	expected string,
) {
	t.Helper()
	allocations, err := allocateArchivalNames(
		archivalNameTestPlanOptions(archivalNameTestTimestamp(), bytes.NewReader(make([]byte, 8))),
		inventory,
		schema.CatalogInventory{},
		[]archivalGroupNameAllocationRequest{request},
	)
	assert.Nil(t, allocations)
	assert.ErrorContains(t, err, expected)
}

func archivalMemberRequestOIDs(members []archivalPhysicalMemberNameAllocationRequest) []uint32 {
	result := make([]uint32, 0, len(members))
	for _, member := range members {
		result = append(result, member.RelationOID)
	}
	return result
}

func archivalMemberAllocationOIDs(members []archivalPhysicalMemberNameAllocation) []uint32 {
	result := make([]uint32, 0, len(members))
	for _, member := range members {
		result = append(result, member.RelationOID)
	}
	return result
}
