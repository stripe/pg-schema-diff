package graph

import (
	"fmt"
	"io"
	"sort"
)

// dotBuilder wraps an io.Writer and writes a graph in DOT format
type dotBuilder struct {
	io.Writer
}

func newDotBuilder(w io.Writer) *dotBuilder {
	builder := dotBuilder{w}
	if err := builder.init(); err != nil {
		panic(err)
	}
	return &builder
}

// init initializes a graph
func (b *dotBuilder) init() error {
	_, err := fmt.Fprintln(b, `digraph G {`)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(b, `node [fontname="Helvetica,Arial,sans-serif"]`)
	if err != nil {
		return err
	}

	return nil
}

// finish closes the opening curly bracket of the current graph
func (b *dotBuilder) finish() error {
	_, err := fmt.Fprintln(b, "}")
	return err
}

// addNode adds a node to the graph
func (b *dotBuilder) addNode(id int, label string) error {
	attr := fmt.Sprintf(`label=%q`, label)
	_, err := fmt.Fprintf(b, "n%d [%s]\n", id, attr)
	return err
}

// addEdge adds an edge to the graph
func (b *dotBuilder) addEdge(from, to int) error {
	_, err := fmt.Fprintf(b, "n%d -> n%d\n", from, to)
	return err
}

// EncodeDOT encodes a graph in DOT format to enable visualization of the graph
func EncodeDOT[V Vertex](g *Graph[V], w io.Writer, sortVertices bool) error {
	builder := newDotBuilder(w)

	vertexIds := make([]string, 0, len(g.verticesById))
	for id := range g.verticesById {
		vertexIds = append(vertexIds, id)
	}
	if sortVertices {
		// Sort the vertices so that the ordering of the output is deterministic,
		// mainly for testing purposes.
		sort.Strings(vertexIds)
	}

	nodeIdsByVertex := make(map[string]int)
	for i, id := range vertexIds {
		err := builder.addNode(i, id)
		if err != nil {
			return fmt.Errorf("addNode(%d, %s): %w", i, id, err)
		}
		nodeIdsByVertex[id] = i
	}

	for _, source := range vertexIds {
		adjacentEdgesMap := g.edges[source]

		adjacentVerticesIds := make([]string, 0, len(adjacentEdgesMap))
		for targetId := range adjacentEdgesMap {
			adjacentVerticesIds = append(adjacentVerticesIds, targetId)
		}
		if sortVertices {
			sort.Strings(adjacentVerticesIds)
		}

		for _, target := range adjacentVerticesIds {
			if adjacentEdgesMap[target] {
				err := builder.addEdge(nodeIdsByVertex[source], nodeIdsByVertex[target])
				if err != nil {
					return fmt.Errorf(
						"addEdge(%d, %d): %w", nodeIdsByVertex[source], nodeIdsByVertex[target], err,
					)
				}
			}
		}
	}

	return builder.finish()
}
