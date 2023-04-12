package graph

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddGetHasOperations(t *testing.T) {
	g := NewGraph[vertex]()

	// get missing vertex return Zero value
	assert.Zero(t, g.GetVertex("missing_vertex"))

	// add vertex
	v1 := NewV("v_1")
	g.AddVertex(v1)
	assert.True(t, g.HasVertexWithId("v_1"))
	assert.Equal(t, v1, g.GetVertex("v_1"))

	// override vertex
	newV1 := NewV("v_1")
	g.AddVertex(newV1)
	assert.True(t, g.HasVertexWithId("v_1"))
	assert.Equal(t, newV1, g.GetVertex("v_1"))
	assert.NotEqual(t, v1, g.GetVertex("v_1"))

	// add edge when target is missing
	assert.Error(t, g.AddEdge("v_1", "missing_vertex"))

	// add edge when source is missing
	assert.Error(t, g.AddEdge("missing_vertex", "v_1"))

	// add edge when both are present
	v2 := NewV("v_2")
	g.AddVertex(v2)
	assert.True(t, g.HasVertexWithId("v_2"))
	assert.NoError(t, g.AddEdge("v_1", "v_2"))
	assert.Equal(t, AdjacencyMatrix{
		"v_1": {
			"v_2": true,
		},
		"v_2": {},
	}, g.edges)

	// overriding vertex keeps edges
	g.AddVertex(NewV("v_1"))
	g.AddVertex(NewV("v_2"))
	assert.Equal(t, AdjacencyMatrix{
		"v_1": {
			"v_2": true,
		},
		"v_2": {},
	}, g.edges)

	// override edge
	assert.NoError(t, g.AddEdge("v_1", "v_2"))
	assert.Equal(t, AdjacencyMatrix{
		"v_1": {
			"v_2": true,
		},
		"v_2": {},
	}, g.edges)

	// allow cycles
	assert.NoError(t, g.AddEdge("v_2", "v_1"))
	assert.Equal(t, AdjacencyMatrix{
		"v_1": {
			"v_2": true,
		},
		"v_2": {
			"v_1": true,
		},
	}, g.edges)
}

func TestReverse(t *testing.T) {
	g := NewGraph[vertex]()
	g.AddVertex(NewV("v_1"))
	g.AddVertex(NewV("v_2"))
	g.AddVertex(NewV("v_3"))
	assert.NoError(t, g.AddEdge("v_1", "v_2"))
	assert.NoError(t, g.AddEdge("v_1", "v_3"))
	assert.NoError(t, g.AddEdge("v_2", "v_3"))
	g.Reverse()
	assert.Equal(t, AdjacencyMatrix{
		"v_1": {},
		"v_2": {
			"v_1": true,
		},
		"v_3": {
			"v_1": true,
			"v_2": true,
		},
	}, g.edges)
	assert.ElementsMatch(t, getVertexIds(g), []string{"v_1", "v_2", "v_3"})
}

func TestCopy(t *testing.T) {
	g := NewGraph[vertex]()
	g.AddVertex(NewV("shared_1"))
	g.AddVertex(NewV("shared_2"))
	g.AddVertex(NewV("shared_3"))
	assert.NoError(t, g.AddEdge("shared_1", "shared_3"))
	assert.NoError(t, g.AddEdge("shared_3", "shared_2"))

	gC := g.Copy()
	gC.AddVertex(NewV("g_copy_1"))
	gC.AddVertex(NewV("g_copy_2"))
	assert.NoError(t, gC.AddEdge("g_copy_1", "g_copy_2"))
	assert.NoError(t, gC.AddEdge("shared_3", "g_copy_1"))
	assert.NoError(t, gC.AddEdge("shared_3", "shared_1"))
	copyOverrideShared1 := NewV("shared_1")
	gC.AddVertex(copyOverrideShared1)

	g.AddVertex(NewV("g_1"))
	g.AddVertex(NewV("g_2"))
	g.AddVertex(NewV("g_3"))
	assert.NoError(t, g.AddEdge("g_3", "g_1"))
	assert.NoError(t, g.AddEdge("g_2", "shared_2"))
	assert.NoError(t, g.AddEdge("shared_2", "shared_3"))
	originalOverrideShared2 := NewV("shared_2")
	g.AddVertex(originalOverrideShared2)

	// validate nodes on copy
	assert.ElementsMatch(t, getVertexIds(gC), []string{
		"shared_1", "shared_2", "shared_3", "g_copy_1", "g_copy_2",
	})

	// validate nodes on original
	assert.ElementsMatch(t, getVertexIds(g), []string{
		"shared_1", "shared_2", "shared_3", "g_1", "g_2", "g_3",
	})

	// validate overrides weren't copied over and non-overriden shared nodes are the same
	assert.NotEqual(t, g.GetVertex("shared_1"), gC.GetVertex("shared_1"))
	assert.Equal(t, gC.GetVertex("shared_1"), copyOverrideShared1)
	assert.NotEqual(t, g.GetVertex("shared_2"), gC.GetVertex("shared_2"))
	assert.Equal(t, g.GetVertex("shared_2"), originalOverrideShared2)
	assert.Equal(t, g.GetVertex("shared_3"), gC.GetVertex("shared_3"))

	// validate edges
	assert.Equal(t, AdjacencyMatrix{
		"shared_1": {
			"shared_3": true,
		},
		"shared_2": {},
		"shared_3": {
			"shared_1": true,
			"shared_2": true,
			"g_copy_1": true,
		},
		"g_copy_1": {
			"g_copy_2": true,
		},
		"g_copy_2": {},
	}, gC.edges)
	assert.Equal(t, AdjacencyMatrix{
		"shared_1": {
			"shared_3": true,
		},
		"shared_2": {
			"shared_3": true,
		},
		"shared_3": {
			"shared_2": true,
		},
		"g_1": {},
		"g_2": {
			"shared_2": true,
		},
		"g_3": {
			"g_1": true,
		},
	}, g.edges)
}

func TestUnion(t *testing.T) {
	gA := NewGraph[vertex]()
	gA1 := NewV("a_1")
	gA.AddVertex(gA1)
	gA2 := NewV("a_2")
	gA.AddVertex(gA2)
	gA3 := NewV("a_3")
	gA.AddVertex(gA3)
	gAShared1 := NewV("shared_1")
	gA.AddVertex(gAShared1)
	gAShared2 := NewV("shared_2")
	gA.AddVertex(gAShared2)
	assert.NoError(t, gA.AddEdge("a_1", "a_2"))
	assert.NoError(t, gA.AddEdge("a_3", "a_1"))
	assert.NoError(t, gA.AddEdge("shared_1", "a_1"))

	gB := NewGraph[vertex]()
	gB1 := NewV("b_1")
	gB.AddVertex(gB1)
	gB2 := NewV("b_2")
	gB.AddVertex(gB2)
	gB3 := NewV("b_3")
	gB.AddVertex(gB3)
	gBShared1 := NewV("shared_1")
	gB.AddVertex(gBShared1)
	gBShared2 := NewV("shared_2")
	gB.AddVertex(gBShared2)
	assert.NoError(t, gB.AddEdge("b_3", "b_2"))
	assert.NoError(t, gB.AddEdge("b_3", "b_1"))
	assert.NoError(t, gB.AddEdge("shared_1", "b_2"))

	err := gA.Union(gB, func(old, new vertex) vertex {
		return vertex{
			id:  old.id,
			val: fmt.Sprintf("%s_%s", old.val, new.val),
		}
	})
	assert.NoError(t, err)

	// make sure non-shared nodes were not merged
	assert.Equal(t, gA1, gA.GetVertex("a_1"))
	assert.Equal(t, gA2, gA.GetVertex("a_2"))
	assert.Equal(t, gA3, gA.GetVertex("a_3"))
	assert.Equal(t, gB1, gA.GetVertex("b_1"))
	assert.Equal(t, gB2, gA.GetVertex("b_2"))
	assert.Equal(t, gB3, gA.GetVertex("b_3"))

	// check merged nodes
	assert.Equal(t, vertex{
		id:  "shared_1",
		val: fmt.Sprintf("%s_%s", gAShared1.val, gBShared1.val),
	}, gA.GetVertex("shared_1"))
	assert.Equal(t, vertex{
		id:  "shared_2",
		val: fmt.Sprintf("%s_%s", gAShared2.val, gBShared2.val),
	}, gA.GetVertex("shared_2"))

	// no extra nodes
	assert.ElementsMatch(t, getVertexIds(gA), []string{
		"a_1", "a_2", "a_3", "b_1", "b_2", "b_3", "shared_1", "shared_2",
	})

	assert.Equal(t, AdjacencyMatrix{
		"a_1": {
			"a_2": true,
		},
		"a_2": {},
		"a_3": {
			"a_1": true,
		},
		"b_1": {},
		"b_2": {},
		"b_3": {
			"b_1": true,
			"b_2": true,
		},
		"shared_1": {
			"a_1": true,
			"b_2": true,
		},
		"shared_2": {},
	}, gA.edges)
}

func TestUnionFailsIfMergeReturnsDifferentId(t *testing.T) {
	gA := NewGraph[vertex]()
	gA.AddVertex(NewV("shared_1"))
	gB := NewGraph[vertex]()
	gB.AddVertex(NewV("shared_1"))
	assert.Error(t, gA.Union(gB, func(old, new vertex) vertex {
		return vertex{
			id:  "different_id",
			val: old.val,
		}
	}))
}

func TestTopologicallySort(t *testing.T) {
	// Source: https://en.wikipedia.org/wiki/Topological_sorting#Examples
	g := NewGraph[vertex]()
	v5 := NewV("05")
	g.AddVertex(v5)
	v7 := NewV("07")
	g.AddVertex(v7)
	v3 := NewV("03")
	g.AddVertex(v3)
	v11 := NewV("11")
	g.AddVertex(v11)
	v8 := NewV("08")
	g.AddVertex(v8)
	v2 := NewV("02")
	g.AddVertex(v2)
	v9 := NewV("09")
	g.AddVertex(v9)
	v10 := NewV("10")
	g.AddVertex(v10)
	assert.NoError(t, g.AddEdge("05", "11"))
	assert.NoError(t, g.AddEdge("07", "11"))
	assert.NoError(t, g.AddEdge("07", "08"))
	assert.NoError(t, g.AddEdge("03", "08"))
	assert.NoError(t, g.AddEdge("03", "10"))
	assert.NoError(t, g.AddEdge("11", "02"))
	assert.NoError(t, g.AddEdge("11", "09"))
	assert.NoError(t, g.AddEdge("11", "10"))
	assert.NoError(t, g.AddEdge("08", "09"))

	orderedNodes, err := g.TopologicallySort()
	assert.NoError(t, err)
	assert.Equal(t, []vertex{
		v3, v5, v7, v8, v11, v2, v9, v10,
	}, orderedNodes)

	// Cycle should error
	assert.NoError(t, g.AddEdge("10", "07"))
	_, err = g.TopologicallySort()
	assert.Error(t, err)
}

func TestTopologicallySortWithPriority(t *testing.T) {
	// Source: https://en.wikipedia.org/wiki/Topological_sorting#Examples
	g := NewGraph[vertex]()
	v5 := NewV("05")
	g.AddVertex(v5)
	v7 := NewV("07")
	g.AddVertex(v7)
	v3 := NewV("03")
	g.AddVertex(v3)
	v11 := NewV("11")
	g.AddVertex(v11)
	v8 := NewV("08")
	g.AddVertex(v8)
	v2 := NewV("02")
	g.AddVertex(v2)
	v9 := NewV("09")
	g.AddVertex(v9)
	v10 := NewV("10")
	g.AddVertex(v10)
	assert.NoError(t, g.AddEdge("05", "11"))
	assert.NoError(t, g.AddEdge("07", "11"))
	assert.NoError(t, g.AddEdge("07", "08"))
	assert.NoError(t, g.AddEdge("03", "08"))
	assert.NoError(t, g.AddEdge("03", "10"))
	assert.NoError(t, g.AddEdge("11", "02"))
	assert.NoError(t, g.AddEdge("11", "09"))
	assert.NoError(t, g.AddEdge("11", "10"))
	assert.NoError(t, g.AddEdge("08", "09"))

	for _, tc := range []struct {
		name             string
		isLowerPriority  func(v1, v2 vertex) bool
		expectedOrdering []vertex
	}{
		{
			name: "largest-numbered available vertex first (string-based GetPriority)",
			isLowerPriority: IsLowerPriorityFromGetPriority(func(v vertex) string {
				return v.GetId()
			}),
			expectedOrdering: []vertex{v7, v5, v11, v3, v10, v8, v9, v2},
		},
		{
			name: "smallest-numbered available vertex first (numeric-based GetPriority)",
			isLowerPriority: IsLowerPriorityFromGetPriority(func(v vertex) int {
				idAsInt, err := strconv.Atoi(v.GetId())
				require.NoError(t, err)
				return -idAsInt
			}),
			expectedOrdering: []vertex{v3, v5, v7, v8, v11, v2, v9, v10},
		},
		{
			name: "fewest edges first (prioritize high id's for tie breakers)",
			isLowerPriority: func(v1, v2 vertex) bool {
				v1EdgeCount := getEdgeCount(g, v1)
				v2EdgeCount := getEdgeCount(g, v2)
				if v1EdgeCount == v2EdgeCount {
					// Break ties with ID
					return v1.GetId() < v2.GetId()
				}
				return v1EdgeCount > v2EdgeCount
			},
			expectedOrdering: []vertex{v5, v7, v3, v8, v11, v10, v9, v2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {

			// Highest number vertices should be prioritized first
			orderedNodes, err := g.TopologicallySortWithPriority(tc.isLowerPriority)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedOrdering, orderedNodes)
		})
	}

	// Cycle should error
	assert.NoError(t, g.AddEdge("10", "07"))
	_, err := g.TopologicallySort()
	assert.Error(t, err)
}

func getEdgeCount[V Vertex](g *Graph[V], v Vertex) int {
	edgeCount := 0
	for _, hasEdge := range g.edges[v.GetId()] {
		if hasEdge {
			edgeCount++
		}
	}
	return edgeCount
}

type vertex struct {
	id  string
	val string
}

func NewV(id string) vertex {
	uuid, err := uuid.NewUUID()
	if err != nil {
		panic(err)
	}
	return vertex{id: id, val: uuid.String()}
}

func (v vertex) GetId() string {
	return v.id
}

func getVertexIds(g *Graph[vertex]) []string {
	var output []string
	for id, _ := range g.verticesById {
		output = append(output, id)
	}
	return output
}
