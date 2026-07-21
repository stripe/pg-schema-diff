package diff

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanupOperationsCanonicalTopologicalOrder(t *testing.T) {
	operations := cleanupOperationFixture()
	slices.Reverse(operations)
	for idx := range operations {
		slices.Reverse(operations[idx].DependsOn)
	}

	ordered, err := canonicalizeCleanupOperations(operations)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"drop-table-a",
		"drop-schema-a",
		"drop-table-b",
		"drop-schema-b",
		"drop-type-status",
		"drop-schema-dependencies",
	}, cleanupOperationIDs(ordered))

	canonicalJSON, err := marshalCanonicalCleanupOperations(operations)
	require.NoError(t, err)
	assert.Equal(
		t,
		`[{"version":1,"id":"drop-table-a","kind":"drop_table","object":{"kind":"table","oid":100,"schema_name":"archive_a","name":"accounts","identity_arguments":[]},"depends_on":[],"restrict":true},{"version":1,"id":"drop-schema-a","kind":"drop_schema","object":{"kind":"schema","oid":0,"schema_name":"","name":"archive_a","identity_arguments":[]},"depends_on":["drop-table-a"],"restrict":true},{"version":1,"id":"drop-table-b","kind":"drop_table","object":{"kind":"table","oid":200,"schema_name":"archive_b","name":"orders","identity_arguments":[]},"depends_on":[],"restrict":true},{"version":1,"id":"drop-schema-b","kind":"drop_schema","object":{"kind":"schema","oid":0,"schema_name":"","name":"archive_b","identity_arguments":[]},"depends_on":["drop-table-b"],"restrict":true},{"version":1,"id":"drop-type-status","kind":"drop_type","object":{"kind":"type","oid":300,"schema_name":"archive_dependencies","name":"status","identity_arguments":[]},"depends_on":["drop-table-a","drop-table-b"],"restrict":true},{"version":1,"id":"drop-schema-dependencies","kind":"drop_schema","object":{"kind":"schema","oid":0,"schema_name":"","name":"archive_dependencies","identity_arguments":[]},"depends_on":["drop-type-status"],"restrict":true}]`,
		string(canonicalJSON),
	)
}

func TestCleanupOperationGraphValidation(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		mutate   func(*[]cleanupOperationV1)
		contains string
	}{
		{
			name: "unsupported codec version",
			mutate: func(operations *[]cleanupOperationV1) {
				(*operations)[0].Version = 2
			},
			contains: "unsupported version",
		},
		{
			name: "missing operation ID",
			mutate: func(operations *[]cleanupOperationV1) {
				(*operations)[0].ID = ""
			},
			contains: "operation ID is required",
		},
		{
			name: "duplicate operation ID",
			mutate: func(operations *[]cleanupOperationV1) {
				(*operations)[1].ID = (*operations)[0].ID
			},
			contains: "duplicate cleanup operation ID",
		},
		{
			name: "unsupported kind",
			mutate: func(operations *[]cleanupOperationV1) {
				(*operations)[0].Kind = "drop_database"
			},
			contains: "unsupported operation kind",
		},
		{
			name: "missing object identity",
			mutate: func(operations *[]cleanupOperationV1) {
				(*operations)[0].Object.Name = ""
			},
			contains: "object name is required",
		},
		{
			name: "kind identity mismatch",
			mutate: func(operations *[]cleanupOperationV1) {
				(*operations)[0].Object.Kind = archivalMarkerObjectKindSequence
			},
			contains: "expected object kind",
		},
		{
			name: "missing edge",
			mutate: func(operations *[]cleanupOperationV1) {
				(*operations)[0].DependsOn = []string{"missing"}
			},
			contains: "depends on missing operation",
		},
		{
			name: "self edge",
			mutate: func(operations *[]cleanupOperationV1) {
				(*operations)[0].DependsOn = []string{(*operations)[0].ID}
			},
			contains: "self-edge",
		},
		{
			name: "duplicate edge",
			mutate: func(operations *[]cleanupOperationV1) {
				(*operations)[3].DependsOn = append(
					(*operations)[3].DependsOn,
					(*operations)[3].DependsOn[0],
				)
			},
			contains: "duplicate dependency edge",
		},
		{
			name: "cycle",
			mutate: func(operations *[]cleanupOperationV1) {
				(*operations)[0].DependsOn = []string{"drop-type-status"}
			},
			contains: "contains a cycle",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			operations := cleanupOperationFixture()
			testCase.mutate(&operations)
			_, err := canonicalizeCleanupOperations(operations)
			require.Error(t, err)
			assert.ErrorContains(t, err, testCase.contains)
		})
	}
}

func TestCleanupOperationAllowedKinds(t *testing.T) {
	for _, testCase := range []struct {
		kind       cleanupOperationKind
		objectKind archivalMarkerObjectKind
	}{
		{kind: cleanupOperationKindDropTable, objectKind: archivalMarkerObjectKindTable},
		{kind: cleanupOperationKindDropSequence, objectKind: archivalMarkerObjectKindSequence},
		{kind: cleanupOperationKindDropFunction, objectKind: archivalMarkerObjectKindFunction},
		{kind: cleanupOperationKindDropType, objectKind: archivalMarkerObjectKindType},
		{kind: cleanupOperationKindDropCollation, objectKind: archivalMarkerObjectKindCollation},
		{kind: cleanupOperationKindDropOperator, objectKind: archivalMarkerObjectKindOperator},
		{kind: cleanupOperationKindDropSchema, objectKind: archivalMarkerObjectKindSchema},
	} {
		t.Run(string(testCase.kind), func(t *testing.T) {
			object := markerObject(1, testCase.objectKind, "archive", "object")
			if testCase.objectKind == archivalMarkerObjectKindSchema {
				object.OID = 0
				object.SchemaName = ""
			}
			_, err := canonicalizeCleanupOperations([]cleanupOperationV1{
				{
					Version: cleanupOperationCodecVersion,
					ID:      "operation", Kind: testCase.kind, Object: object, Restrict: true,
				},
			})
			require.NoError(t, err)
		})
	}
}

func TestCleanupOperationDigestStableVector(t *testing.T) {
	digest, err := computeCleanupOperationDigest(cleanupOperationFixture())
	require.NoError(t, err)
	assert.Equal(
		t,
		cleanupOperationDigest("sha256:53138289ea3bc2a69926d7f5bffc2e4ddfc7dba2feaa080353cd273b50d254bf"),
		digest,
	)

	parsed, err := parseCleanupOperationDigest(digest.String())
	require.NoError(t, err)
	assert.Equal(t, digest, parsed)

	emptyDigest, err := computeCleanupOperationDigest(nil)
	require.NoError(t, err)
	assert.Equal(
		t,
		cleanupOperationDigest("sha256:45f673ff015ee42394c3cf2b62e2ae44b4e3484949f2322473a8d36c9c68e469"),
		emptyDigest,
	)
}

func TestCleanupOperationDigestExcludesPresentationMetadata(t *testing.T) {
	type digestFixture struct {
		Operations  []cleanupOperationV1
		RenderedSQL []string
		Hazards     []MigrationHazard
		MarkerText  string
	}
	fixtureA := digestFixture{
		Operations: cleanupOperationFixture(),
		RenderedSQL: []string{
			`DROP TABLE "archive_a"."accounts" RESTRICT`,
			`DROP SCHEMA "archive_a" RESTRICT`,
		},
		Hazards: []MigrationHazard{
			{Type: MigrationHazardTypeDeletesData, Message: "rows are deleted"},
			{Type: MigrationHazardTypeAcquiresAccessExclusiveLock, Message: "table lock"},
		},
		MarkerText: "marker-a",
	}
	fixtureB := digestFixture{
		Operations: fixtureA.Operations,
		RenderedSQL: []string{
			"DROP  TABLE archive_a.accounts\nRESTRICT",
			"DROP SCHEMA archive_a RESTRICT",
		},
		Hazards: []MigrationHazard{
			{Type: MigrationHazardTypeAcquiresAccessExclusiveLock, Message: "changed message"},
			{Type: MigrationHazardTypeDeletesData, Message: "another changed message"},
		},
		MarkerText: "entirely different marker text",
	}

	digestA, err := computeCleanupOperationDigest(fixtureA.Operations)
	require.NoError(t, err)
	digestB, err := computeCleanupOperationDigest(fixtureB.Operations)
	require.NoError(t, err)
	assert.Equal(t, digestA, digestB)
}

func TestCleanupOperationDigestChangesWithSemantics(t *testing.T) {
	baselineOperations := cleanupOperationFixture()
	baseline, err := computeCleanupOperationDigest(baselineOperations)
	require.NoError(t, err)

	for _, testCase := range []struct {
		name   string
		mutate func(*[]cleanupOperationV1)
	}{
		{
			name: "operation kind",
			mutate: func(operations *[]cleanupOperationV1) {
				(*operations)[0].Kind = cleanupOperationKindDropSequence
				(*operations)[0].Object.Kind = archivalMarkerObjectKindSequence
			},
		},
		{
			name: "object identity",
			mutate: func(operations *[]cleanupOperationV1) {
				(*operations)[0].Object.Name = "different_accounts"
			},
		},
		{
			name: "object catalog OID",
			mutate: func(operations *[]cleanupOperationV1) {
				(*operations)[0].Object.OID++
			},
		},
		{
			name: "dependency edge",
			mutate: func(operations *[]cleanupOperationV1) {
				(*operations)[2].DependsOn = []string{"drop-table-a"}
			},
		},
		{
			name: "restrict behavior",
			mutate: func(operations *[]cleanupOperationV1) {
				(*operations)[0].Restrict = false
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			changedOperations := cleanupOperationFixture()
			testCase.mutate(&changedOperations)
			changed, err := computeCleanupOperationDigest(changedOperations)
			require.NoError(t, err)
			assert.NotEqual(t, baseline, changed)
		})
	}
}

func TestCleanupOperationDigestStrictParsing(t *testing.T) {
	for _, encoded := range []string{
		"",
		"md5:00000000000000000000000000000000",
		"sha256:1234",
		"sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"sha256:gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
	} {
		t.Run(encoded, func(t *testing.T) {
			_, err := parseCleanupOperationDigest(encoded)
			require.Error(t, err)
		})
	}
}

func cleanupOperationFixture() []cleanupOperationV1 {
	return []cleanupOperationV1{
		{
			Version: cleanupOperationCodecVersion,
			ID:      "drop-table-a", Kind: cleanupOperationKindDropTable,
			Object:   markerObject(100, archivalMarkerObjectKindTable, "archive_a", "accounts"),
			Restrict: true,
		},
		{
			Version: cleanupOperationCodecVersion,
			ID:      "drop-schema-a", Kind: cleanupOperationKindDropSchema,
			Object:    archivalMarkerObjectIdentity{Kind: archivalMarkerObjectKindSchema, Name: "archive_a"},
			DependsOn: []string{"drop-table-a"}, Restrict: true,
		},
		{
			Version: cleanupOperationCodecVersion,
			ID:      "drop-table-b", Kind: cleanupOperationKindDropTable,
			Object:   markerObject(200, archivalMarkerObjectKindTable, "archive_b", "orders"),
			Restrict: true,
		},
		{
			Version: cleanupOperationCodecVersion,
			ID:      "drop-type-status", Kind: cleanupOperationKindDropType,
			Object:    markerObject(300, archivalMarkerObjectKindType, "archive_dependencies", "status"),
			DependsOn: []string{"drop-table-b", "drop-table-a"}, Restrict: true,
		},
		{
			Version: cleanupOperationCodecVersion,
			ID:      "drop-schema-b", Kind: cleanupOperationKindDropSchema,
			Object:    archivalMarkerObjectIdentity{Kind: archivalMarkerObjectKindSchema, Name: "archive_b"},
			DependsOn: []string{"drop-table-b"}, Restrict: true,
		},
		{
			Version: cleanupOperationCodecVersion,
			ID:      "drop-schema-dependencies", Kind: cleanupOperationKindDropSchema,
			Object:    archivalMarkerObjectIdentity{Kind: archivalMarkerObjectKindSchema, Name: "archive_dependencies"},
			DependsOn: []string{"drop-type-status"}, Restrict: true,
		},
	}
}

func cleanupOperationIDs(operations []cleanupOperationV1) []string {
	ids := make([]string, 0, len(operations))
	for _, operation := range operations {
		ids = append(ids, operation.ID)
	}
	return ids
}
