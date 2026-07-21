package diff

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

func TestPlanArchivedDependencyClosureExclusiveMovableKinds(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		kind    archivalMarkerObjectKind
		address schema.CatalogDependencyObject
		add     func(*schema.CatalogInventory)
	}{
		{
			name: "enum", kind: archivalMarkerObjectKindType,
			address: closureAddress(pgTypeCatalogOID, 100, "deps", "status", "deps.status"),
			add: func(inventory *schema.CatalogInventory) {
				inventory.Types = append(inventory.Types,
					closureType(100, "deps", "status", schema.CatalogTypeKindEnum))
				inventory.EnumLabels = append(inventory.EnumLabels,
					schema.CatalogEnumLabel{TypeOID: 100, SortOrder: 1, Label: "ready"})
			},
		},
		{
			name: "domain", kind: archivalMarkerObjectKindType,
			address: closureAddress(pgTypeCatalogOID, 101, "deps", "positive", "deps.positive"),
			add: func(inventory *schema.CatalogInventory) {
				inventory.Types = append(inventory.Types,
					closureType(101, "deps", "positive", schema.CatalogTypeKindDomain))
				inventory.DomainConstraints = append(inventory.DomainConstraints, schema.CatalogDomainConstraint{
					OID: 1001, TypeOID: 101, Name: "positive", Definition: "CHECK ((VALUE > 0))",
				})
			},
		},
		{
			name: "composite", kind: archivalMarkerObjectKindType,
			address: closureAddress(pgTypeCatalogOID, 102, "deps", "pair", "deps.pair"),
			add: func(inventory *schema.CatalogInventory) {
				inventory.Types = append(inventory.Types,
					closureType(102, "deps", "pair", schema.CatalogTypeKindComposite))
			},
		},
		{
			name: "range", kind: archivalMarkerObjectKindType,
			address: closureAddress(pgTypeCatalogOID, 103, "deps", "span", "deps.span"),
			add: func(inventory *schema.CatalogInventory) {
				inventory.Types = append(inventory.Types,
					closureType(103, "deps", "span", schema.CatalogTypeKindRange))
			},
		},
		{
			name: "multirange", kind: archivalMarkerObjectKindType,
			address: closureAddress(pgTypeCatalogOID, 104, "deps", "spans", "deps.spans"),
			add: func(inventory *schema.CatalogInventory) {
				inventory.Types = append(inventory.Types,
					closureType(104, "deps", "spans", schema.CatalogTypeKindMultirange))
			},
		},
		{
			name: "collation", kind: archivalMarkerObjectKindCollation,
			address: closureAddress(pgCollationCatalogOID, 105, "deps", "words", "deps.words"),
			add: func(inventory *schema.CatalogInventory) {
				inventory.Collations = append(inventory.Collations, schema.CatalogCollation{
					OID: 105, SchemaName: "deps", Name: "words", Provider: "c",
					IsDeterministic: true, Encoding: -1, Collate: "C", CType: "C",
				})
			},
		},
		{
			name: "function", kind: archivalMarkerObjectKindFunction,
			address: closureAddress(pgProcCatalogOID, 106, "deps", "normalize", "deps.normalize(integer)"),
			add: func(inventory *schema.CatalogInventory) {
				inventory.Routines = append(inventory.Routines,
					closureRoutine(106, "deps", "normalize", "integer"))
			},
		},
		{
			name: "operator", kind: archivalMarkerObjectKindOperator,
			address: closureAddress(pgOperatorCatalogOID, 107, "deps", "===", "deps.===(integer,integer)"),
			add: func(inventory *schema.CatalogInventory) {
				inventory.Operators = append(inventory.Operators, schema.CatalogOperator{
					OID: 107, SchemaName: "deps", Name: "===", Kind: "b",
					LeftType: "integer", RightType: "integer", ResultType: "boolean",
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			request := newArchivedDependencyTestRequest(10)
			tc.add(&request.CurrentSnapshot.Inventory)
			request.CurrentSnapshot.Inventory.Dependencies = append(
				request.CurrentSnapshot.Inventory.Dependencies,
				closureDependency(closureTableAddress(10, "managed", "archived"), tc.address),
			)

			result, err := planArchivedDependencyClosure(request)
			require.NoError(t, err)
			require.Len(t, result.Assignments, 1)
			assert.Equal(t, tc.kind, result.Assignments[0].Source.Kind)
			assert.Equal(t, tc.address.ObjectOID, result.Assignments[0].Source.OID)
			assert.Equal(t, "archive_dep_a", result.Assignments[0].Destination.SchemaName)
			require.Len(t, result.Objects, 1)
			assert.Equal(t, archivedDependencyClassificationExclusiveMovable,
				result.Objects[0].Classification)
			assert.Empty(t, result.SharedGroupEdges)
		})
	}
}

func TestPlanArchivedDependencyClosureStandaloneSequence(t *testing.T) {
	t.Parallel()

	request := newArchivedDependencyTestRequest(10)
	sequenceAddress := closureAddress(pgClassCatalogOID, 100, "deps", "numbers", "deps.numbers")
	integerAddress := closureAddress(pgTypeCatalogOID, 23, "pg_catalog", "int4", "integer")
	request.CurrentSnapshot.Inventory.Sequences = []schema.CatalogSequence{{
		OID: 100, SchemaName: "deps", Name: "numbers", DataTypeOID: 23,
		StartValue: 1, IncrementValue: 1, MaxValue: 100, MinValue: 1, CacheSize: 1,
	}}
	request.CurrentSnapshot.Inventory.Dependencies = []schema.CatalogDependency{
		closureDependency(closureTableAddress(10, "managed", "archived"), sequenceAddress),
		closureDependency(sequenceAddress, integerAddress),
	}
	request.TargetSnapshot.Inventory.Dependencies = []schema.CatalogDependency{
		closureDependency(
			closureAddress(pgNamespaceCatalogOID, 2200, "pg_catalog", "pg_catalog", "pg_catalog"),
			integerAddress,
		),
	}

	result, err := planArchivedDependencyClosure(request)
	require.NoError(t, err)
	require.Len(t, result.Assignments, 1)
	assert.Equal(t, archivalMarkerObjectKindSequence, result.Assignments[0].Source.Kind)
	assert.Contains(t, result.Objects, archivedDependencyClosureObject{
		Kind:     archivalMarkerObjectKindType,
		Address:  integerAddress,
		Identity: archivedDependencyMarkerObject(23, archivalMarkerObjectKindType, "pg_catalog", "int4"),
		SemanticDefinition: canonicalArchivedDependencyDefinition(struct {
			CatalogClass uint32
			Identity     string
		}{pgTypeCatalogOID, "integer"}),
		Classification: archivedDependencyClassificationTargetCompatible,
		GroupIDs:       []archivalGroupID{"group-a"},
	})
}

func TestPlanArchivedDependencyClosureTransitiveSupportFunctionsAndCycles(t *testing.T) {
	t.Parallel()

	request := newArchivedDependencyTestRequest(10)
	typeAddress := closureAddress(pgTypeCatalogOID, 100, "deps", "token", "deps.token")
	functionAddress := closureAddress(pgProcCatalogOID, 200, "deps", "token_in", "deps.token_in(cstring)")
	operatorAddress := closureAddress(pgOperatorCatalogOID, 300, "deps", "~", "deps.~(integer,integer)")
	request.CurrentSnapshot.Inventory.Types = []schema.CatalogType{
		closureType(100, "deps", "token", schema.CatalogTypeKindBase),
	}
	request.CurrentSnapshot.Inventory.Routines = []schema.CatalogRoutine{
		closureRoutine(200, "deps", "token_in", "cstring"),
	}
	request.CurrentSnapshot.Inventory.Operators = []schema.CatalogOperator{{
		OID: 300, SchemaName: "deps", Name: "~", Kind: "b", LeftType: "integer",
		RightType: "integer", ResultType: "boolean", FunctionOID: 200,
	}}
	request.CurrentSnapshot.Inventory.TypeSupportFunctions = []schema.CatalogTypeSupportFunction{{
		TypeOID: 100, Role: "input", FunctionOID: 200, FunctionSchemaName: "deps",
		FunctionName: "token_in", FunctionIdentityArguments: "cstring",
	}}
	request.CurrentSnapshot.Inventory.Dependencies = []schema.CatalogDependency{
		closureDependency(closureTableAddress(10, "managed", "archived"), typeAddress),
		closureDependency(typeAddress, functionAddress),
		closureDependency(functionAddress, operatorAddress),
		closureDependency(operatorAddress, functionAddress),
	}

	first, err := planArchivedDependencyClosure(request)
	require.NoError(t, err)
	require.Len(t, first.Assignments, 3)
	assert.Equal(t, []archivalMarkerObjectKind{
		archivalMarkerObjectKindFunction,
		archivalMarkerObjectKindOperator,
		archivalMarkerObjectKindType,
	}, closureAssignmentKinds(first.Assignments))

	slices.Reverse(request.CurrentSnapshot.Inventory.Dependencies)
	slices.Reverse(request.CurrentSnapshot.Inventory.TypeSupportFunctions)
	second, err := planArchivedDependencyClosure(request)
	require.NoError(t, err)
	assert.Equal(t, first, second)
}

func TestPlanArchivedDependencyClosureTargetClassifications(t *testing.T) {
	t.Parallel()

	t.Run("compatible managed dependency", func(t *testing.T) {
		request := closureEnumRequestWithTarget(false)
		request.TargetSnapshot.Inventory.EnumLabels[0].SortOrder = 17
		result, err := planArchivedDependencyClosure(request)
		require.NoError(t, err)
		require.Len(t, result.Objects, 1)
		assert.Equal(t, archivedDependencyClassificationTargetCompatible,
			result.Objects[0].Classification)
		assert.Empty(t, result.Assignments)
	})

	t.Run("shared with managed target table", func(t *testing.T) {
		request := closureEnumRequestWithTarget(true)
		result, err := planArchivedDependencyClosure(request)
		require.NoError(t, err)
		require.Len(t, result.Objects, 1)
		assert.Equal(t, archivedDependencyClassificationSharedWithTarget,
			result.Objects[0].Classification)
		assert.Empty(t, result.Assignments)
	})

	t.Run("incompatible exclusive replacement moves old identity", func(t *testing.T) {
		request := closureEnumRequestWithTarget(false)
		request.TargetSnapshot.Inventory.EnumLabels[0].Label = "incompatible"
		result, err := planArchivedDependencyClosure(request)
		require.NoError(t, err)
		require.Len(t, result.Assignments, 1)
		assert.Equal(t, archivedDependencyClassificationExclusiveMovable,
			result.Objects[0].Classification)
	})
}

func TestPlanArchivedDependencyClosureSharedArchivedOwnershipAndEdges(t *testing.T) {
	t.Parallel()

	request := newArchivedDependencyTestRequest(20, 10)
	request.ProposedGroups = []archivedDependencyClosureGroupRequest{
		{GroupID: "group-b", TableRelationOIDs: []uint32{20}, DependencySchemaName: "archive_dep_b"},
		{GroupID: "group-a", TableRelationOIDs: []uint32{10}, DependencySchemaName: "archive_dep_a"},
	}
	enumAddress := closureAddress(pgTypeCatalogOID, 100, "deps", "status", "deps.status")
	request.CurrentSnapshot.Inventory.Types = []schema.CatalogType{
		closureType(100, "deps", "status", schema.CatalogTypeKindEnum),
	}
	request.CurrentSnapshot.Inventory.EnumLabels = []schema.CatalogEnumLabel{{
		TypeOID: 100, SortOrder: 1, Label: "ready",
	}}
	request.CurrentSnapshot.Inventory.Dependencies = []schema.CatalogDependency{
		closureDependency(closureTableAddress(20, "managed", "second"), enumAddress),
		closureDependency(closureTableAddress(10, "managed", "first"), enumAddress),
	}

	result, err := planArchivedDependencyClosure(request)
	require.NoError(t, err)
	require.Len(t, result.Objects, 1)
	assert.Equal(t, archivedDependencyClassificationSharedArchivedOnly,
		result.Objects[0].Classification)
	assert.Equal(t, []archivalGroupID{"group-a", "group-b"}, result.Objects[0].GroupIDs)
	assert.Equal(t, archivalGroupID("group-a"), result.Objects[0].OwnerGroupID)
	require.Len(t, result.Assignments, 1)
	assert.Equal(t, archivalGroupID("group-a"), result.Assignments[0].GroupID)
	assert.Equal(t, "archive_dep_a", result.Assignments[0].Destination.SchemaName)
	assert.Equal(t, []archivalMarkerSharedGroupEdgeV1{{
		FirstGroupID: "group-a", SecondGroupID: "group-b",
	}}, result.SharedGroupEdges)
}

func TestPlanArchivedDependencyClosureRejectsUnsafeDependencies(t *testing.T) {
	t.Parallel()

	t.Run("unknown user dependency", func(t *testing.T) {
		request := newArchivedDependencyTestRequest(10)
		request.CurrentSnapshot.Inventory.Dependencies = []schema.CatalogDependency{
			closureDependency(
				closureTableAddress(10, "managed", "archived"),
				closureAddress(9999, 100, "deps", "unknown", "deps.unknown"),
			),
		}
		_, err := planArchivedDependencyClosure(request)
		require.ErrorContains(t, err, "unsupported/non-movable dependency")
	})

	t.Run("non-movable platform dependency removed", func(t *testing.T) {
		request := newArchivedDependencyTestRequest(10)
		request.CurrentSnapshot.Inventory.Dependencies = []schema.CatalogDependency{
			closureDependency(
				closureTableAddress(10, "managed", "archived"),
				closureAddress(pgTypeCatalogOID, 23, "pg_catalog", "int4", "integer"),
			),
		}
		_, err := planArchivedDependencyClosure(request)
		require.ErrorContains(t, err, "unsupported/non-movable dependency")
	})

	t.Run("external source consumer prevents exclusive movement", func(t *testing.T) {
		request := newArchivedDependencyTestRequest(10)
		functionAddress := closureAddress(pgProcCatalogOID, 100, "deps", "value", "deps.value()")
		request.CurrentSnapshot.Inventory.Relations = append(
			request.CurrentSnapshot.Inventory.Relations,
			schema.CatalogRelation{OID: 20, SchemaName: "external", Name: "consumer", Kind: schema.RelKindOrdinaryTable},
		)
		request.CurrentSnapshot.Inventory.Routines = []schema.CatalogRoutine{
			closureRoutine(100, "deps", "value", ""),
		}
		request.CurrentSnapshot.Inventory.Dependencies = []schema.CatalogDependency{
			closureDependency(closureTableAddress(10, "managed", "archived"), functionAddress),
			closureDependency(closureTableAddress(20, "external", "consumer"), functionAddress),
		}
		_, err := planArchivedDependencyClosure(request)
		require.ErrorContains(t, err, "also used by")
	})
}

func TestPlanArchivedDependencyClosureExtensionSafety(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		mutate      func(*schema.SchemaSnapshot)
		expectError string
	}{
		{name: "unchanged extension"},
		{
			name: "extension drop",
			mutate: func(target *schema.SchemaSnapshot) {
				target.Inventory.Types = nil
				target.Inventory.Extensions = nil
				target.Inventory.ExtensionMembers = nil
			},
			expectError: "unsupported/non-movable dependency",
		},
		{
			name: "extension update",
			mutate: func(target *schema.SchemaSnapshot) {
				target.Inventory.Extensions[0].Version = "2.0"
			},
			expectError: "replaced incompatibly",
		},
		{
			name: "extension schema change",
			mutate: func(target *schema.SchemaSnapshot) {
				target.Inventory.Extensions[0].SchemaName = "other"
			},
			expectError: "replaced incompatibly",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := closureExtensionRequest()
			if tc.mutate != nil {
				tc.mutate(&request.TargetSnapshot)
			}
			result, err := planArchivedDependencyClosure(request)
			if tc.expectError != "" {
				require.ErrorContains(t, err, tc.expectError)
				return
			}
			require.NoError(t, err)
			assert.Empty(t, result.Assignments)
			require.Len(t, result.Objects, 2)
			for _, object := range result.Objects {
				assert.Equal(t, archivedDependencyClassificationTargetCompatible, object.Classification)
				assert.False(t, object.Movable)
			}
		})
	}
}

func TestPlanArchivedDependencyClosureRejectsAmbiguousTargetIdentity(t *testing.T) {
	t.Parallel()

	request := closureEnumRequestWithTarget(false)
	duplicate := request.TargetSnapshot.Inventory.Types[0]
	duplicate.OID++
	request.TargetSnapshot.Inventory.Types = append(request.TargetSnapshot.Inventory.Types, duplicate)
	duplicateLabel := request.TargetSnapshot.Inventory.EnumLabels[0]
	duplicateLabel.TypeOID = duplicate.OID
	request.TargetSnapshot.Inventory.EnumLabels = append(
		request.TargetSnapshot.Inventory.EnumLabels, duplicateLabel,
	)

	_, err := planArchivedDependencyClosure(request)
	require.ErrorContains(t, err, "ambiguous target identity")
}

func TestPlanArchivedDependencyClosureCandidateMarkerValidation(t *testing.T) {
	t.Parallel()

	_, expectedObject := closureCandidateRequest()
	tests := []struct {
		name        string
		mutate      func(*archivedDependencyClosureRequest)
		expectError string
	}{
		{name: "valid dependency claims"},
		{
			name: "dependency schema mismatch",
			mutate: func(request *archivedDependencyClosureRequest) {
				request.CandidateGroups[0].Marker.ExclusiveDependencySchemas[0].Name = "wrong_schema"
				request.CandidateGroups[0].Marker.ExclusiveDependencyObjects[0].SchemaName = "wrong_schema"
			},
			expectError: "dependency schema claim",
		},
		{
			name: "object OID mismatch",
			mutate: func(request *archivedDependencyClosureRequest) {
				request.CandidateGroups[0].Marker.ExclusiveDependencyObjects[0].OID++
			},
			expectError: "exclusive dependency mismatch",
		},
		{
			name: "object identity mismatch",
			mutate: func(request *archivedDependencyClosureRequest) {
				request.CandidateGroups[0].Marker.ExclusiveDependencyObjects[0].Name = "other"
			},
			expectError: "exclusive dependency mismatch",
		},
		{
			name: "missing dependency",
			mutate: func(request *archivedDependencyClosureRequest) {
				request.CandidateGroups[0].Marker.ExclusiveDependencyObjects = nil
			},
			expectError: "missing exclusive dependency",
		},
		{
			name: "extra dependency",
			mutate: func(request *archivedDependencyClosureRequest) {
				extra := expectedObject
				extra.OID++
				extra.Name = "extra"
				request.CandidateGroups[0].Marker.ExclusiveDependencyObjects = append(
					request.CandidateGroups[0].Marker.ExclusiveDependencyObjects, extra,
				)
			},
			expectError: "extra exclusive dependency",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request, _ := closureCandidateRequest()
			if tc.mutate != nil {
				tc.mutate(&request)
			}
			result, err := planArchivedDependencyClosure(request)
			if tc.expectError != "" {
				require.ErrorContains(t, err, tc.expectError)
				return
			}
			require.NoError(t, err)
			require.Len(t, result.DependencyValidatedCandidateGroups, 1)
			assert.Equal(t, archivalGroupID("group-a"),
				result.DependencyValidatedCandidateGroups[0].Candidate.GroupID)
		})
	}
}

func TestPlanArchivedDependencyClosureCandidateSharedEdges(t *testing.T) {
	t.Parallel()

	request := newArchivedDependencyTestRequest(10, 20)
	request.ProposedGroups = nil
	request.CandidateGroups = []structurallyValidArchivedCandidateGroup{
		closureCandidateGroup("group-a", 10, "archive_dep_a"),
		closureCandidateGroup("group-b", 20, "archive_dep_b"),
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
	edge := archivalMarkerSharedGroupEdgeV1{FirstGroupID: "group-a", SecondGroupID: "group-b"}
	request.CandidateGroups[0].Marker.ExclusiveDependencyObjects = []archivalMarkerObjectIdentity{
		archivedDependencyMarkerObject(100, archivalMarkerObjectKindType, "archive_dep_a", "status"),
	}
	for idx := range request.CandidateGroups {
		request.CandidateGroups[idx].Marker.SharedCleanupComponentGroupEdges = []archivalMarkerSharedGroupEdgeV1{edge}
	}

	result, err := planArchivedDependencyClosure(request)
	require.NoError(t, err)
	require.Len(t, result.DependencyValidatedCandidateGroups, 2)
	assert.Equal(t, []archivalMarkerSharedGroupEdgeV1{edge}, result.SharedGroupEdges)

	request.CandidateGroups[1].Marker.SharedCleanupComponentGroupEdges = nil
	_, err = planArchivedDependencyClosure(request)
	require.ErrorContains(t, err, "shared dependency edges do not match")
}

func TestPlanArchivedDependencyClosureDoesNotMutateSnapshots(t *testing.T) {
	t.Parallel()

	request := closureEnumRequestWithTarget(true)
	slices.Reverse(request.CurrentSnapshot.Inventory.Dependencies)
	slices.Reverse(request.CurrentSnapshot.Inventory.Relations)
	currentBefore, err := json.Marshal(request.CurrentSnapshot)
	require.NoError(t, err)
	targetBefore, err := json.Marshal(request.TargetSnapshot)
	require.NoError(t, err)

	_, err = planArchivedDependencyClosure(request)
	require.NoError(t, err)
	currentAfter, err := json.Marshal(request.CurrentSnapshot)
	require.NoError(t, err)
	targetAfter, err := json.Marshal(request.TargetSnapshot)
	require.NoError(t, err)
	assert.Equal(t, currentBefore, currentAfter)
	assert.Equal(t, targetBefore, targetAfter)
}

func newArchivedDependencyTestRequest(tableOIDs ...uint32) archivedDependencyClosureRequest {
	request := archivedDependencyClosureRequest{}
	for idx, oid := range tableOIDs {
		name := "archived"
		if idx == 0 && len(tableOIDs) > 1 {
			name = "first"
		} else if idx == 1 {
			name = "second"
		}
		request.CurrentSnapshot.Inventory.Relations = append(
			request.CurrentSnapshot.Inventory.Relations,
			schema.CatalogRelation{OID: oid, SchemaName: "managed", Name: name, Kind: schema.RelKindOrdinaryTable},
		)
		request.SourcePreflight.ExpectedRetainedObjects = append(
			request.SourcePreflight.ExpectedRetainedObjects,
			sourceSafetyExpectedRetainedObject{
				TableRelationOID: oid,
				Address:          closureTableAddress(oid, "managed", name),
			},
		)
	}
	if len(tableOIDs) == 1 {
		request.ProposedGroups = []archivedDependencyClosureGroupRequest{{
			GroupID: "group-a", TableRelationOIDs: []uint32{tableOIDs[0]},
			DependencySchemaName: "archive_dep_a",
		}}
	}
	return request
}

func closureEnumRequestWithTarget(sharedWithTable bool) archivedDependencyClosureRequest {
	request := newArchivedDependencyTestRequest(10)
	sourceAddress := closureAddress(pgTypeCatalogOID, 100, "deps", "status", "deps.status")
	request.CurrentSnapshot.Inventory.Types = []schema.CatalogType{
		closureType(100, "deps", "status", schema.CatalogTypeKindEnum),
	}
	request.CurrentSnapshot.Inventory.EnumLabels = []schema.CatalogEnumLabel{{
		TypeOID: 100, SortOrder: 1, Label: "ready",
	}}
	request.CurrentSnapshot.Inventory.Dependencies = []schema.CatalogDependency{
		closureDependency(closureTableAddress(10, "managed", "archived"), sourceAddress),
	}
	targetAddress := closureAddress(pgTypeCatalogOID, 500, "deps", "status", "deps.status")
	request.TargetSnapshot.Inventory.Types = []schema.CatalogType{
		closureType(500, "deps", "status", schema.CatalogTypeKindEnum),
	}
	request.TargetSnapshot.Inventory.EnumLabels = []schema.CatalogEnumLabel{{
		TypeOID: 500, SortOrder: 1, Label: "ready",
	}}
	if sharedWithTable {
		request.TargetSnapshot.Schema.Tables = []schema.Table{{
			SchemaQualifiedName: schema.SchemaQualifiedName{SchemaName: "managed", EscapedName: `"active"`},
		}}
		request.TargetSnapshot.Inventory.Relations = []schema.CatalogRelation{{
			OID: 600, SchemaName: "managed", Name: "active", Kind: schema.RelKindOrdinaryTable,
		}}
		request.TargetSnapshot.Inventory.Dependencies = []schema.CatalogDependency{
			closureDependency(closureTableAddress(600, "managed", "active"), targetAddress),
		}
	}
	return request
}

func closureExtensionRequest() archivedDependencyClosureRequest {
	request := newArchivedDependencyTestRequest(10)
	sourceTypeAddress := closureAddress(pgTypeCatalogOID, 100, "deps", "ext_value", "deps.ext_value")
	sourceExtensionAddress := closureAddress(pgExtensionCatalogOID, 200, "", "sample", "sample")
	request.CurrentSnapshot.Inventory.Types = []schema.CatalogType{{
		OID: 100, SchemaName: "deps", Name: "ext_value", Kind: schema.CatalogTypeKindBase,
		Extension: &schema.CatalogExtension{OID: 200, Name: "sample"},
	}}
	request.CurrentSnapshot.Inventory.Extensions = []schema.CatalogExtensionIdentity{{
		OID: 200, Name: "sample", SchemaName: "deps", Version: "1.0",
	}}
	request.CurrentSnapshot.Inventory.ExtensionMembers = []schema.CatalogExtensionMember{{
		ExtensionOID: 200, ExtensionName: "sample", Object: sourceTypeAddress,
	}}
	request.CurrentSnapshot.Inventory.Dependencies = []schema.CatalogDependency{
		closureDependency(closureTableAddress(10, "managed", "archived"), sourceTypeAddress),
		{Dependent: sourceTypeAddress, Referenced: sourceExtensionAddress, Type: "e"},
	}
	targetTypeAddress := closureAddress(pgTypeCatalogOID, 500, "deps", "ext_value", "deps.ext_value")
	request.TargetSnapshot.Inventory.Types = []schema.CatalogType{{
		OID: 500, SchemaName: "deps", Name: "ext_value", Kind: schema.CatalogTypeKindBase,
		Extension: &schema.CatalogExtension{OID: 900, Name: "sample"},
	}}
	request.TargetSnapshot.Inventory.Extensions = []schema.CatalogExtensionIdentity{{
		OID: 900, Name: "sample", SchemaName: "deps", Version: "1.0",
	}}
	request.TargetSnapshot.Inventory.ExtensionMembers = []schema.CatalogExtensionMember{{
		ExtensionOID: 900, ExtensionName: "sample", Object: targetTypeAddress,
	}}
	return request
}

func closureCandidateRequest() (archivedDependencyClosureRequest, archivalMarkerObjectIdentity) {
	request := newArchivedDependencyTestRequest(10)
	request.ProposedGroups = nil
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
		closureDependency(closureTableAddress(10, "managed", "archived"), enumAddress),
	}
	expected := archivedDependencyMarkerObject(
		100, archivalMarkerObjectKindType, "archive_dep_a", "status",
	)
	request.CandidateGroups[0].Marker.ExclusiveDependencyObjects = []archivalMarkerObjectIdentity{
		expected,
	}
	return request, expected
}

func closureCandidateGroup(
	groupID archivalGroupID,
	tableOID uint32,
	dependencySchema string,
) structurallyValidArchivedCandidateGroup {
	return structurallyValidArchivedCandidateGroup{
		GroupID: groupID, ExpectedDependencySchemaName: dependencySchema,
		Marker: archivalMarkerV1{
			GroupID: groupID,
			Members: []archivalMarkerMemberV1{{
				SourceTable: archivedDependencyMarkerObject(
					tableOID, archivalMarkerObjectKindTable, "managed", "archived",
				),
			}},
			ExclusiveDependencySchemas: []archivalMarkerSchemaIdentity{{Name: dependencySchema}},
		},
	}
}

func closureAddress(
	classOID uint32,
	objectOID uint32,
	schemaName string,
	name string,
	identity string,
) schema.CatalogDependencyObject {
	return schema.CatalogDependencyObject{
		ClassOID: classOID, ObjectOID: objectOID, SchemaName: schemaName,
		Name: name, Identity: identity,
	}
}

func closureTableAddress(oid uint32, schemaName, name string) schema.CatalogDependencyObject {
	return closureAddress(pgClassCatalogOID, oid, schemaName, name, schemaName+"."+name)
}

func closureDependency(
	dependent schema.CatalogDependencyObject,
	referenced schema.CatalogDependencyObject,
) schema.CatalogDependency {
	return schema.CatalogDependency{Dependent: dependent, Referenced: referenced, Type: "n"}
}

func closureType(
	oid uint32,
	schemaName string,
	name string,
	kind schema.CatalogTypeKind,
) schema.CatalogType {
	return schema.CatalogType{
		OID: oid, SchemaName: schemaName, Name: name, Kind: kind,
		RawKind: string(kind), IsDefined: true, Delimiter: ",", Alignment: "i", Storage: "p",
	}
}

func closureRoutine(oid uint32, schemaName, name, arguments string) schema.CatalogRoutine {
	return schema.CatalogRoutine{
		OID: oid, SchemaName: schemaName, Name: name, Kind: "f", LanguageName: "sql",
		IdentityArguments: arguments, Arguments: arguments, Result: "integer", Source: "SELECT 1",
		BodyForm:              schema.CatalogRoutineBodyFormSQLString,
		ReferenceTrackability: schema.CatalogRoutineReferenceTrackabilityUntrackable,
	}
}

func closureAssignmentKinds(assignments []archivedDependencyAssignment) []archivalMarkerObjectKind {
	result := make([]archivalMarkerObjectKind, 0, len(assignments))
	for _, assignment := range assignments {
		result = append(result, assignment.Source.Kind)
	}
	return result
}
