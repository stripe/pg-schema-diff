package graph

import (
	"fmt"
	"io"
	"sort"
)

type Vertex interface {
	GetId() string
}

type AdjacencyMatrix map[string]map[string]bool

// Graph is a directed graph
type Graph[V Vertex] struct {
	verticesById map[string]V
	edges        AdjacencyMatrix
}

func NewGraph[V Vertex]() *Graph[V] {
	return &Graph[V]{
		verticesById: make(map[string]V),
		edges:        make(AdjacencyMatrix),
	}
}

// AddVertex adds a vertex to the graph.
// If the vertex already exists, it will override it and keep the edges
func (g *Graph[V]) AddVertex(v V) {
	g.verticesById[v.GetId()] = v
	if g.edges[v.GetId()] == nil {
		g.edges[v.GetId()] = make(map[string]bool)
	}
}

// AddEdge adds an edge to the graph. If the vertex doesn't exist, it will error
func (g *Graph[V]) AddEdge(sourceId, targetId string) error {
	if !g.HasVertexWithId(sourceId) {
		return fmt.Errorf("source %s does not exist", sourceId)
	}
	if !g.HasVertexWithId(targetId) {
		return fmt.Errorf("target %s does not exist", targetId)
	}
	g.edges[sourceId][targetId] = true

	return nil
}

// Union unions the graph with a new graph. If a vertex exists in both graphs,
// it uses the merge function to determine what the new vertex is
func (g *Graph[V]) Union(new *Graph[V], merge func(old, new V) V) error {
	for _, newV := range new.verticesById {
		if g.HasVertexWithId(newV.GetId()) {
			id := newV.GetId()
			// merge the vertices using the procedure defined by the user
			newV = merge(g.GetVertex(newV.GetId()), newV)
			if newV.GetId() != id {
				return fmt.Errorf("the merge function must return a vertex with the same id: "+
					"expected %s but found %s", id, newV.GetId())
			}
		}
		g.AddVertex(newV)
	}

	for source, adjacentEdgesMap := range new.edges {
		for target, isAdjacent := range adjacentEdgesMap {
			if isAdjacent {
				if err := g.AddEdge(source, target); err != nil {
					return fmt.Errorf("adding an edge from the new graph: %w", err)
				}
			}
		}
	}

	return nil
}

func (g *Graph[V]) GetVertex(id string) V {
	return g.verticesById[id]
}

func (g *Graph[V]) HasVertexWithId(id string) bool {
	_, hasVertex := g.verticesById[id]
	return hasVertex
}

// Reverse reverses the edges of the map. The sources become the sinks and vice versa.
func (g *Graph[V]) Reverse() {
	reversedEdges := make(AdjacencyMatrix)
	for vertexId, _ := range g.verticesById {
		reversedEdges[vertexId] = make(map[string]bool)
	}
	for source, adjacentEdgesMap := range g.edges {
		for target, isAdjacent := range adjacentEdgesMap {
			if isAdjacent {
				reversedEdges[target][source] = true
			}
		}
	}
	g.edges = reversedEdges
}

func (g *Graph[V]) Copy() *Graph[V] {
	verticesById := make(map[string]V)
	for id, v := range g.verticesById {
		verticesById[id] = v
	}

	edges := make(AdjacencyMatrix)
	for source, adjacentEdgesMap := range g.edges {
		edges[source] = make(map[string]bool)
		for target, isAdjacent := range adjacentEdgesMap {
			edges[source][target] = isAdjacent
		}
	}

	return &Graph[V]{
		verticesById: verticesById,
		edges:        edges,
	}
}

func (g *Graph[V]) TopologicallySort() ([]V, error) {
	return g.TopologicallySortWithPriority(func(_, _ V) bool {
		return false
	})
}

type Ordered interface {
	~int | ~string
}

func IsLowerPriorityFromGetPriority[V Vertex, P Ordered](getPriority func(V) P) func(V, V) bool {
	return func(v1 V, v2 V) bool {
		return getPriority(v1) < getPriority(v2)
	}
}

// TopologicallySortWithPriority returns a consistent topological sort of the graph taking a greedy approach to put
// high priority sources first. The output is deterministic. getPriority must be deterministic
func (g *Graph[V]) TopologicallySortWithPriority(isLowerPriority func(V, V) bool) ([]V, error) {
	// This uses mutation. Copy the graph
	graph := g.Copy()

	// The strategy of this algorithm:
	// 1. Count the number of incoming edges to each vertex
	// 2. Remove the sources and add them to the outputIds
	// 3. Decrement the number of incoming edges to each vertex adjacent to the sink
	// 4. Repeat 2-3 until the graph is empty

	// The number of outgoing in the reversed graph is the number of incoming in the original graph
	// In other words, a source in the graph (has no incoming edges) is a sink in the reversed graph
	// (has no outgoing edges)

	// To find the number of incoming edges in graph, just count the number of outgoing edges
	// in the reversed graph
	reversedGraph := graph.Copy()
	reversedGraph.Reverse()
	incomingEdgeCountByVertex := make(map[string]int)
	for vertex, reversedAdjacentEdges := range reversedGraph.edges {
		count := 0
		for _, isAdjacent := range reversedAdjacentEdges {
			if isAdjacent {
				count++
			}
		}
		incomingEdgeCountByVertex[vertex] = count
	}

	var output []V
	// Add the sinks to the output. Delete the sinks. Repeat.
	for len(graph.verticesById) > 0 {
		// Put all the sources into an array, so we can get a stable sort of them before identifying the one
		// with the highest priority
		var sources []V
		for sourceId, incomingEdgeCount := range incomingEdgeCountByVertex {
			if incomingEdgeCount == 0 {
				sources = append(sources, g.GetVertex(sourceId))
			}
		}
		sort.Slice(sources, func(i, j int) bool {
			return sources[i].GetId() < sources[j].GetId()
		})

		// Take the source with highest priority from the sorted array of sources
		indexOfSourceWithHighestPri := -1
		for i, source := range sources {
			if indexOfSourceWithHighestPri == -1 || isLowerPriority(sources[indexOfSourceWithHighestPri], source) {
				indexOfSourceWithHighestPri = i
			}
		}
		if indexOfSourceWithHighestPri == -1 {
			return nil, fmt.Errorf("cycle detected: %+v, %+v", graph, incomingEdgeCountByVertex)
		}
		sourceWithHighestPriority := sources[indexOfSourceWithHighestPri]

		output = append(output, sourceWithHighestPriority)

		// Remove source vertex from graph and update counts
		// Update incoming edge counts
		for target, hasEdge := range graph.edges[sourceWithHighestPriority.GetId()] {
			if hasEdge {
				incomingEdgeCountByVertex[target]--
			}
		}

		// Delete the vertex from graph
		delete(graph.verticesById, sourceWithHighestPriority.GetId())
		// We don't need to worry about any incoming edges referencing this vertex, since it was a sink
		delete(graph.edges, sourceWithHighestPriority.GetId())
		delete(incomingEdgeCountByVertex, sourceWithHighestPriority.GetId())
	}

	return output, nil
}

func (g *Graph[V]) EncodeDOT(w io.Writer, sortVertices bool) (err error) {
	builder := &dotBuilder{w}
	err = builder.init()
	if err != nil {
		return err
	}
	defer func() {
		err = builder.finish()
	}()

	vertexIds := make([]string, 0, len(g.verticesById))
	for k := range g.verticesById {
		vertexIds = append(vertexIds, k)
	}
	if sortVertices {
		// Sort the vertices so that the ordering of the output is deterministic,
		// mainly for testing purposes.
		sort.Strings(vertexIds)
	}

	nodeIdsByVertex := make(map[string]int)
	for i, vid := range vertexIds {
		err = builder.addNode(i, vid)
		if err != nil {
			return err
		}
		nodeIdsByVertex[vid] = i
	}

	for _, source := range vertexIds {
		adjacentEdgesMap := g.edges[source]
		for target, isAdjacent := range adjacentEdgesMap {
			if isAdjacent {
				err = builder.addEdge(nodeIdsByVertex[source], nodeIdsByVertex[target])
				if err != nil {
					return err
				}
			}
		}
	}

	return err
}
