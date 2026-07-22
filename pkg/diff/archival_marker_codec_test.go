package diff

import (
	"encoding/base64"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArchivalMarkerExactEnvelopeRoundTrip(t *testing.T) {
	payload := minimalArchivalMarker()
	marker, err := marshalArchivalMarker(payload)
	require.NoError(t, err)

	const expected = "pg-schema-diff:schema-partial-archival:v1:eyJ2ZXJzaW9uIjoxLCJncm91cF9pZCI6Imdyb3VwLTEiLCJtZW1iZXJzIjpbeyJtZW1iZXJfaWQiOiJtZW1iZXItMSIsInNvdXJjZV90YWJsZSI6eyJraW5kIjoidGFibGUiLCJvaWQiOjEwMCwic2NoZW1hX25hbWUiOiJwdWJsaWMiLCJuYW1lIjoiYWNjb3VudHMiLCJpZGVudGl0eV9hcmd1bWVudHMiOltdfSwiY2xlYW51cF90YWJsZSI6eyJraW5kIjoidGFibGUiLCJvaWQiOjEwMCwic2NoZW1hX25hbWUiOiJhcmNoaXZlX2FjY291bnRzIiwibmFtZSI6ImFjY291bnRzIiwiaWRlbnRpdHlfYXJndW1lbnRzIjpbXX0sImF1dG9tYXRpY2FsbHlfbW92ZWRfb2JqZWN0cyI6W3sia2luZCI6InRhYmxlIiwib2lkIjoxMDAsInNjaGVtYV9uYW1lIjoiYXJjaGl2ZV9hY2NvdW50cyIsIm5hbWUiOiJhY2NvdW50cyIsImlkZW50aXR5X2FyZ3VtZW50cyI6W119XSwiYXR0YWNoZWRfb2JqZWN0cyI6W10sImV4cGxpY2l0bHlfbW92ZWRfb2JqZWN0cyI6W10sImludGVybmFsX3RvYXN0X29iamVjdHMiOltdfV0sInBhcnRpdGlvbl9lZGdlcyI6W10sImxvc3RfcGFyZW50X2F0dGFjaG1lbnRzIjpbXSwiZXhjbHVzaXZlX2RlcGVuZGVuY3lfc2NoZW1hcyI6W10sImV4Y2x1c2l2ZV9kZXBlbmRlbmN5X29iamVjdHMiOltdLCJzaGFyZWRfY2xlYW51cF9jb21wb25lbnRfZ3JvdXBfZWRnZXMiOltdLCJvcmlnaW5hbF9hY2xzIjpbXSwib3JpZ2luYWxfZm9yZWlnbl9rZXlzIjpbXSwib3JpZ2luYWxfcHVibGljYXRpb25fbWVtYmVyc2hpcHMiOltdLCJjbGVhbnVwX2RpZ2VzdCI6InNoYTI1NjowMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwIn0"
	assert.Equal(t, expected, marker)
	assert.NotContains(t, strings.TrimPrefix(marker, archivalMarkerEnvelopeV1), "=")

	parsed, err := parseArchivalMarker(marker)
	require.NoError(t, err)
	assert.Equal(t, canonicalizeArchivalMarker(payload), parsed)
}

func TestArchivalMarkerStrictParsing(t *testing.T) {
	payload := minimalArchivalMarker()
	marker, err := marshalArchivalMarker(payload)
	require.NoError(t, err)
	payloadJSON, err := json.Marshal(canonicalizeArchivalMarker(payload))
	require.NoError(t, err)

	withUnknownField := strings.TrimSuffix(string(payloadJSON), "}") + `,"unknown":true}`
	withPayloadVersion := strings.Replace(string(payloadJSON), `"version":1`, `"version":2`, 1)
	withoutMemberID := strings.Replace(string(payloadJSON), `"member_id":"member-1",`, "", 1)
	withoutLostParentAttachments := strings.Replace(
		string(payloadJSON), `"lost_parent_attachments":[],`, "", 1,
	)
	withNullField := strings.Replace(string(payloadJSON), `"version":1`, `"version":null`, 1)

	for _, testCase := range []struct {
		name     string
		marker   string
		contains string
	}{
		{
			name: "invalid envelope", marker: "other:v1:e30",
			contains: "invalid archival marker envelope",
		},
		{name: "unsupported envelope version", marker: strings.Replace(marker, ":v1:", ":v2:", 1), contains: "unsupported"},
		{
			name: "missing envelope version", marker: archivalMarkerEnvelopeNamespace,
			contains: "invalid archival marker envelope",
		},
		{
			name: "invalid base64", marker: archivalMarkerEnvelopeV1 + "%%%",
			contains: "decoding archival marker payload",
		},
		{name: "padded base64", marker: marker + "=", contains: "unpadded base64url"},
		{
			name: "base64 with ignored newline", marker: marker + "\n",
			contains: "canonical unpadded base64url",
		},
		{name: "unknown JSON field", marker: markerForJSON(withUnknownField), contains: "unknown field"},
		{name: "missing required JSON field", marker: markerForJSON(withoutMemberID), contains: "required JSON field"},
		{
			name: "missing required lost-parent array", marker: markerForJSON(withoutLostParentAttachments),
			contains: "lost_parent_attachments is missing",
		},
		{name: "null required JSON field", marker: markerForJSON(withNullField), contains: "must not be null"},
		{
			name: "unsupported payload version", marker: markerForJSON(withPayloadVersion),
			contains: "unsupported payload version",
		},
		{name: "trailing JSON", marker: markerForJSON(string(payloadJSON) + `{}`), contains: "trailing JSON"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := parseArchivalMarker(testCase.marker)
			require.Error(t, err)
			assert.ErrorContains(t, err, testCase.contains)
		})
	}
}

func TestArchivalMarkerPartitionFieldsAreRequired(t *testing.T) {
	payload, err := json.Marshal(canonicalizeArchivalMarker(representativeArchivalMarker(t)))
	require.NoError(t, err)

	for _, field := range []string{
		"bound_expression", "partitioned_index_attachments", "cloned_triggers",
	} {
		t.Run(field, func(t *testing.T) {
			var raw map[string]any
			require.NoError(t, json.Unmarshal(payload, &raw))
			edges := raw["partition_edges"].([]any)
			delete(edges[0].(map[string]any), field)
			encoded, err := json.Marshal(raw)
			require.NoError(t, err)
			_, err = parseArchivalMarker(markerForJSON(string(encoded)))
			require.ErrorContains(t, err, "."+field+" is missing")
		})
	}
}

func TestEscapeArchivalMarkerSQLLiteral(t *testing.T) {
	assert.Equal(
		t,
		`'pg-schema-diff:schema-partial-archival:v1:quote''\backslash_雪'`,
		escapeArchivalMarkerSQLLiteral(`pg-schema-diff:schema-partial-archival:v1:quote'\backslash_雪`),
	)
}

func TestArchivalMarkerValidation(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		mutate   func(*archivalMarkerV1)
		contains string
	}{
		{
			name: "payload version",
			mutate: func(marker *archivalMarkerV1) {
				marker.Version = 2
			},
			contains: "unsupported payload version",
		},
		{
			name: "missing group ID",
			mutate: func(marker *archivalMarkerV1) {
				marker.GroupID = ""
			},
			contains: "group ID is required",
		},
		{
			name: "missing catalog OID",
			mutate: func(marker *archivalMarkerV1) {
				marker.Members[0].SourceTable.OID = 0
			},
			contains: "object catalog OID is required",
		},
		{
			name: "missing members",
			mutate: func(marker *archivalMarkerV1) {
				marker.Members = nil
			},
			contains: "at least one group member",
		},
		{
			name: "duplicate member ID",
			mutate: func(marker *archivalMarkerV1) {
				marker.Members[1].MemberID = marker.Members[0].MemberID
			},
			contains: "duplicate member ID",
		},
		{
			name: "duplicate member schema",
			mutate: func(marker *archivalMarkerV1) {
				oldSchema := marker.Members[1].CleanupTable.SchemaName
				newSchema := marker.Members[0].CleanupTable.SchemaName
				marker.Members[1].CleanupTable.SchemaName = newSchema
				for idx := range marker.Members[1].AutomaticallyMovedObjects {
					if marker.Members[1].AutomaticallyMovedObjects[idx].SchemaName == oldSchema {
						marker.Members[1].AutomaticallyMovedObjects[idx].SchemaName = newSchema
					}
				}
				for idx := range marker.Members[1].AttachedObjects {
					if marker.Members[1].AttachedObjects[idx].SchemaName == oldSchema {
						marker.Members[1].AttachedObjects[idx].SchemaName = newSchema
					}
				}
			},
			contains: "duplicate member cleanup schema",
		},
		{
			name: "duplicate source table identity",
			mutate: func(marker *archivalMarkerV1) {
				marker.Members[1].SourceTable = marker.Members[0].SourceTable
				marker.Members[1].CleanupTable.OID = marker.Members[0].SourceTable.OID
				marker.Members[1].CleanupTable.Name = marker.Members[0].SourceTable.Name
				marker.Members[1].AutomaticallyMovedObjects[0] = marker.Members[1].CleanupTable
			},
			contains: "duplicate member source table identity",
		},
		{
			name: "duplicate local identity",
			mutate: func(marker *archivalMarkerV1) {
				marker.Members[0].AutomaticallyMovedObjects = append(
					marker.Members[0].AutomaticallyMovedObjects,
					marker.Members[0].AutomaticallyMovedObjects[0],
				)
			},
			contains: "duplicate automatically moved object identity",
		},
		{
			name: "duplicate dependency schema",
			mutate: func(marker *archivalMarkerV1) {
				marker.ExclusiveDependencySchemas = append(
					marker.ExclusiveDependencySchemas,
					marker.ExclusiveDependencySchemas[0],
				)
			},
			contains: "duplicate exclusive dependency schema",
		},
		{
			name: "duplicate dependency object",
			mutate: func(marker *archivalMarkerV1) {
				marker.ExclusiveDependencyObjects = append(
					marker.ExclusiveDependencyObjects,
					marker.ExclusiveDependencyObjects[0],
				)
			},
			contains: "duplicate exclusive dependency object identity",
		},
		{
			name: "missing topology member",
			mutate: func(marker *archivalMarkerV1) {
				marker.PartitionEdges[0].ChildMemberID = "missing"
			},
			contains: "missing child member",
		},
		{
			name: "topology cycle",
			mutate: func(marker *archivalMarkerV1) {
				third := marker.Members[1]
				third.MemberID = "member-third"
				third.SourceTable.OID = 250
				third.SourceTable.Name = "orders_2026"
				third.CleanupTable.OID = 250
				third.CleanupTable.SchemaName = "archive_orders_2026"
				third.CleanupTable.Name = "orders_2026"
				third.AutomaticallyMovedObjects = []archivalMarkerObjectIdentity{third.CleanupTable}
				third.AttachedObjects = nil
				marker.Members = append(marker.Members, third)
				marker.PartitionEdges = []archivalMarkerPartitionEdgeV1{
					{
						ParentMemberID: "member-child", ChildMemberID: "member-third",
						BoundExpression: "FOR VALUES IN (2026)",
					},
					{
						ParentMemberID: "member-third", ChildMemberID: "member-child",
						BoundExpression: "FOR VALUES IN (2025)",
					},
				}
			},
			contains: "cyclic",
		},
		{
			name: "duplicate topology edge",
			mutate: func(marker *archivalMarkerV1) {
				marker.PartitionEdges = append(marker.PartitionEdges, marker.PartitionEdges[0])
			},
			contains: "duplicate partition edge",
		},
		{
			name: "shared edge outside group",
			mutate: func(marker *archivalMarkerV1) {
				marker.SharedCleanupComponentGroupEdges[0] = archivalMarkerSharedGroupEdgeV1{
					FirstGroupID: "other-a", SecondGroupID: "other-b",
				}
			},
			contains: "does not contain marker group",
		},
		{
			name: "duplicate shared edge in reverse",
			mutate: func(marker *archivalMarkerV1) {
				edge := marker.SharedCleanupComponentGroupEdges[0]
				marker.SharedCleanupComponentGroupEdges = append(
					marker.SharedCleanupComponentGroupEdges,
					archivalMarkerSharedGroupEdgeV1{FirstGroupID: edge.SecondGroupID, SecondGroupID: edge.FirstGroupID},
				)
			},
			contains: "duplicate shared cleanup-component edge",
		},
		{
			name: "malformed cleanup digest",
			mutate: func(marker *archivalMarkerV1) {
				marker.CleanupDigest = "sha256:1234"
			},
			contains: "cleanup digest",
		},
		{
			name: "ACL object class mismatch",
			mutate: func(marker *archivalMarkerV1) {
				marker.OriginalACLs[0].ObjectClass = "table"
			},
			contains: "expected object kind",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			marker := representativeArchivalMarker(t)
			testCase.mutate(&marker)
			_, err := marshalArchivalMarker(marker)
			require.Error(t, err)
			assert.ErrorContains(t, err, testCase.contains)
		})
	}
}

func TestArchivalMarkerCanonicalOrdering(t *testing.T) {
	markerA := representativeArchivalMarker(t)
	markerB := representativeArchivalMarker(t)

	slices.Reverse(markerB.Members)
	slices.Reverse(markerB.ExclusiveDependencySchemas)
	slices.Reverse(markerB.ExclusiveDependencyObjects)
	slices.Reverse(markerB.SharedCleanupComponentGroupEdges)
	for idx := range markerB.SharedCleanupComponentGroupEdges {
		edge := &markerB.SharedCleanupComponentGroupEdges[idx]
		edge.FirstGroupID, edge.SecondGroupID = edge.SecondGroupID, edge.FirstGroupID
	}
	slices.Reverse(markerB.OriginalACLs)
	slices.Reverse(markerB.OriginalPublicationMemberships)
	for idx := range markerB.OriginalPublicationMemberships {
		slices.Reverse(markerB.OriginalPublicationMemberships[idx].ColumnNames)
	}
	for idx := range markerB.Members {
		slices.Reverse(markerB.Members[idx].AutomaticallyMovedObjects)
		slices.Reverse(markerB.Members[idx].AttachedObjects)
	}
	for idx := range markerB.PartitionEdges {
		slices.Reverse(markerB.PartitionEdges[idx].PartitionedIndexAttachments)
		slices.Reverse(markerB.PartitionEdges[idx].ClonedTriggers)
	}

	encodedA, err := marshalArchivalMarker(markerA)
	require.NoError(t, err)
	encodedB, err := marshalArchivalMarker(markerB)
	require.NoError(t, err)
	assert.Equal(t, encodedA, encodedB)
}

func TestArchivalMarkerRepresentativePayload(t *testing.T) {
	payload := representativeArchivalMarker(t)
	marker, err := marshalArchivalMarker(payload)
	require.NoError(t, err)
	parsed, err := parseArchivalMarker(marker)
	require.NoError(t, err)

	require.Len(t, parsed.Members, 2)
	assert.NotEmpty(t, parsed.PartitionEdges)
	var root archivalMarkerMemberV1
	for _, member := range parsed.Members {
		if member.MemberID == "member-root" {
			root = member
		}
	}
	assert.NotEmpty(t, root.AutomaticallyMovedObjects)
	assert.NotEmpty(t, root.AttachedObjects)
	assert.NotEmpty(t, root.ExplicitlyMovedObjects)
	assert.NotEmpty(t, root.InternalToastObjects)
	assert.NotEmpty(t, parsed.ExclusiveDependencySchemas)
	assert.NotEmpty(t, parsed.ExclusiveDependencyObjects)
	assert.NotEmpty(t, parsed.SharedCleanupComponentGroupEdges)
	assert.NotEmpty(t, parsed.OriginalACLs)
	assert.NotEmpty(t, parsed.OriginalForeignKeys)
	assert.NotEmpty(t, parsed.OriginalPublicationMemberships)
}

func TestArchivalMarkerDetachedSubtreeMetadataContract(t *testing.T) {
	marker := representativeArchivalMarker(t)
	marker.LostParentAttachments = []archivalMarkerLostParentAttachmentV1{{
		RootMemberID: "member-root",
		ParentTable: markerObject(90, archivalMarkerObjectKindTable,
			"active", "orders_by_year"),
		BoundExpression: "FOR VALUES IN (2025)",
		PartitionedIndexAttachments: []archivalMarkerPartitionedIndexAttachmentV1{{
			ParentIndex: markerObject(91, archivalMarkerObjectKindIndex,
				"active", "orders_by_year_idx"),
			ChildIndex: markerObject(101, archivalMarkerObjectKindIndex,
				"archive_orders", "orders_pkey"),
		}},
		ClonedTriggers: []archivalMarkerClonedTriggerV1{{
			ParentTrigger: markerObject(92, archivalMarkerObjectKindTrigger,
				"active", "orders_audit"),
			ChildTrigger: markerObject(110, archivalMarkerObjectKindTrigger,
				"archive_orders", "active_orders_audit"),
			FunctionOID: 500, Type: 7, EnabledMode: "O",
			Definition: "CREATE TRIGGER orders_audit BEFORE INSERT ON public.orders",
		}},
	}}
	encoded, err := marshalArchivalMarker(marker)
	require.NoError(t, err)
	parsed, err := parseArchivalMarker(encoded)
	require.NoError(t, err)
	assert.Equal(t, canonicalizeArchivalMarker(marker), parsed)

	for _, tc := range []struct {
		name     string
		mutate   func(*archivalMarkerV1)
		contains string
	}{
		{
			name: "multiple lost parents", mutate: func(value *archivalMarkerV1) {
				value.LostParentAttachments = append(value.LostParentAttachments,
					value.LostParentAttachments[0])
			}, contains: "at most one lost parent",
		},
		{
			name: "missing lost root", mutate: func(value *archivalMarkerV1) {
				value.LostParentAttachments[0].RootMemberID = "missing"
			}, contains: "missing root member",
		},
		{
			name: "lost root is a child", mutate: func(value *archivalMarkerV1) {
				value.LostParentAttachments[0].RootMemberID = "member-child"
			}, contains: "not the partition topology root",
		},
		{
			name: "wrong index kind", mutate: func(value *archivalMarkerV1) {
				value.LostParentAttachments[0].PartitionedIndexAttachments[0].ChildIndex.Kind = archivalMarkerObjectKindTable
			}, contains: "expected object kind \"index\"",
		},
		{
			name: "missing trigger function", mutate: func(value *archivalMarkerV1) {
				value.LostParentAttachments[0].ClonedTriggers[0].FunctionOID = 0
			}, contains: "function OID is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalid := canonicalizeArchivalMarker(marker)
			tc.mutate(&invalid)
			_, err := marshalArchivalMarker(invalid)
			require.ErrorContains(t, err, tc.contains)
		})
	}
}

func minimalArchivalMarker() archivalMarkerV1 {
	cleanupTable := markerObject(100, archivalMarkerObjectKindTable, "archive_accounts", "accounts")
	return archivalMarkerV1{
		Version: archivalMarkerVersion,
		GroupID: "group-1",
		Members: []archivalMarkerMemberV1{
			{
				MemberID:                  "member-1",
				SourceTable:               markerObject(100, archivalMarkerObjectKindTable, "public", "accounts"),
				CleanupTable:              cleanupTable,
				AutomaticallyMovedObjects: []archivalMarkerObjectIdentity{cleanupTable},
			},
		},
		CleanupDigest: cleanupOperationDigest("sha256:" + strings.Repeat("0", 64)),
	}
}

func representativeArchivalMarker(t *testing.T) archivalMarkerV1 {
	t.Helper()
	digest, err := computeCleanupOperationDigest(cleanupOperationFixture())
	require.NoError(t, err)

	rootTable := markerObject(100, archivalMarkerObjectKindTable, "archive_orders", "orders")
	childTable := markerObject(200, archivalMarkerObjectKindTable, "archive_orders_2025", "orders_2025")
	rootIndexA := markerObject(101, archivalMarkerObjectKindIndex, "archive_orders", "orders_pkey")
	rootIndexB := markerObject(108, archivalMarkerObjectKindIndex, "archive_orders", "orders_total_idx")
	childIndexA := markerObject(201, archivalMarkerObjectKindIndex, "archive_orders_2025", "orders_2025_pkey")
	childIndexB := markerObject(202, archivalMarkerObjectKindIndex, "archive_orders_2025", "orders_2025_total_idx")
	rootTriggerA := markerObject(104, archivalMarkerObjectKindTrigger, "archive_orders", "orders_audit")
	rootTriggerB := markerObject(109, archivalMarkerObjectKindTrigger, "archive_orders", "orders_notify")
	childTriggerA := markerObject(204, archivalMarkerObjectKindTrigger, "archive_orders_2025", "orders_audit")
	childTriggerB := markerObject(205, archivalMarkerObjectKindTrigger, "archive_orders_2025", "orders_notify")
	return archivalMarkerV1{
		Version: archivalMarkerVersion,
		GroupID: "group-a",
		Members: []archivalMarkerMemberV1{
			{
				MemberID:     "member-root",
				SourceTable:  markerObject(100, archivalMarkerObjectKindTable, "public", "orders"),
				CleanupTable: rootTable,
				AutomaticallyMovedObjects: []archivalMarkerObjectIdentity{
					rootIndexA,
					rootIndexB,
					markerObject(102, archivalMarkerObjectKindArrayType, "archive_orders", "_orders"),
					rootTable,
					markerObject(103, archivalMarkerObjectKindRowType, "archive_orders", "orders"),
				},
				AttachedObjects: []archivalMarkerObjectIdentity{
					rootTriggerA,
					rootTriggerB,
					markerObject(105, archivalMarkerObjectKindPolicy, "archive_orders", "tenant_policy"),
				},
				ExplicitlyMovedObjects: []archivalMarkerObjectIdentity{
					markerObject(106, archivalMarkerObjectKindExtendedStatistic, "archive_orders", "orders_stats"),
				},
				InternalToastObjects: []archivalMarkerObjectIdentity{
					markerObject(107, archivalMarkerObjectKindToastRelation, "pg_toast", "pg_toast_12345"),
				},
			},
			{
				MemberID:                  "member-child",
				SourceTable:               markerObject(200, archivalMarkerObjectKindTable, "history", "orders_2025"),
				CleanupTable:              childTable,
				AutomaticallyMovedObjects: []archivalMarkerObjectIdentity{childTable, childIndexA, childIndexB},
				AttachedObjects:           []archivalMarkerObjectIdentity{childTriggerA, childTriggerB},
			},
		},
		PartitionEdges: []archivalMarkerPartitionEdgeV1{
			{
				ParentMemberID: "member-root", ChildMemberID: "member-child",
				BoundExpression: "FOR VALUES IN (2025)",
				PartitionedIndexAttachments: []archivalMarkerPartitionedIndexAttachmentV1{
					{ParentIndex: rootIndexB, ChildIndex: childIndexB},
					{ParentIndex: rootIndexA, ChildIndex: childIndexA},
				},
				ClonedTriggers: []archivalMarkerClonedTriggerV1{
					{
						ParentTrigger: rootTriggerB, ChildTrigger: childTriggerB,
						FunctionOID: 501, Type: 7, EnabledMode: "O", Definition: "trigger-b",
					},
					{
						ParentTrigger: rootTriggerA, ChildTrigger: childTriggerA,
						FunctionOID: 500, Type: 7, EnabledMode: "O", Definition: "trigger-a",
					},
				},
			},
		},
		ExclusiveDependencySchemas: []archivalMarkerSchemaIdentity{
			{Name: "archive_dependencies_b"},
			{Name: "archive_dependencies_a"},
		},
		ExclusiveDependencyObjects: []archivalMarkerObjectIdentity{
			markerObject(300, archivalMarkerObjectKindType, "archive_dependencies_b", "order_status"),
			markerFunction(301, "archive_dependencies_a", "order_total", "archive_orders.orders"),
		},
		SharedCleanupComponentGroupEdges: []archivalMarkerSharedGroupEdgeV1{
			{FirstGroupID: "group-c", SecondGroupID: "group-a"},
			{FirstGroupID: "group-a", SecondGroupID: "group-b"},
		},
		OriginalACLs: []archivalMarkerACLRecordV1{
			{
				ObjectClass: "function", Object: markerFunction(301, "public", "order_total", "public.orders"),
				OwnerName: "owner", GrantorName: "owner", GranteeName: "reporter", Privilege: "EXECUTE",
			},
			{
				ObjectClass: "table", Object: markerObject(100, archivalMarkerObjectKindTable, "public", "orders"),
				ColumnName: "total", OwnerName: "owner", GrantorName: "owner", GranteeName: "PUBLIC",
				GranteeIsPublic: true, Privilege: "SELECT", IsGrantable: true,
			},
		},
		OriginalForeignKeys: []archivalMarkerForeignKeyV1{
			{
				Name:            "invoices_order_id_fkey",
				OwningTable:     markerObject(400, archivalMarkerObjectKindTable, "billing", "invoices"),
				ReferencedTable: markerObject(100, archivalMarkerObjectKindTable, "public", "orders"),
				Columns: []archivalMarkerForeignKeyColumnV1{
					{OwningColumnName: "order_id", ReferencedColumnName: "id"},
				},
				MatchType: "SIMPLE", UpdateAction: "NO ACTION", DeleteAction: "RESTRICT",
				IsDeferrable: true, IsInitiallyDeferred: false, IsValidated: true,
				Definition: "FOREIGN KEY (order_id) REFERENCES public.orders(id)",
			},
		},
		OriginalPublicationMemberships: []archivalMarkerPublicationMembershipV1{
			{
				PublicationName: "orders_publication",
				Table:           markerObject(100, archivalMarkerObjectKindTable, "public", "orders"),
				ColumnNames:     []string{"total", "id"}, RowFilter: "tenant_id = 42",
			},
			{
				PublicationName: "audit_publication",
				Table:           markerObject(200, archivalMarkerObjectKindTable, "history", "orders_2025"),
				ColumnNames:     []string{"id"},
			},
		},
		CleanupDigest: digest,
	}
}

func markerObject(oid uint32, kind archivalMarkerObjectKind, schemaName, name string) archivalMarkerObjectIdentity {
	return archivalMarkerObjectIdentity{Kind: kind, OID: oid, SchemaName: schemaName, Name: name}
}

func markerFunction(oid uint32, schemaName, name string, arguments ...string) archivalMarkerObjectIdentity {
	return archivalMarkerObjectIdentity{
		Kind: archivalMarkerObjectKindFunction, OID: oid, SchemaName: schemaName, Name: name,
		IdentityArguments: arguments,
	}
}

func markerForJSON(payload string) string {
	return archivalMarkerEnvelopeV1 + base64.RawURLEncoding.EncodeToString([]byte(payload))
}
