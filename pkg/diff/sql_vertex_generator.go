package diff

import (
	"fmt"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

type diffType string

const (
	diffTypeDelete   diffType = "DELETE"
	diffTypeAddAlter diffType = "ADDALTER"
)

// partialSQLGraph represents SQL vertices and their dependencies. A dependency may reference a vertex
// in another partial graph.
type partialSQLGraph struct {
	vertices     []sqlVertex
	dependencies []dependency
}

func concatPartialGraphs(parts ...partialSQLGraph) partialSQLGraph {
	var vertices []sqlVertex
	var dependencies []dependency
	for _, part := range parts {
		vertices = append(vertices, part.vertices...)
		dependencies = append(dependencies, part.dependencies...)
	}
	return partialSQLGraph{
		vertices:     vertices,
		dependencies: dependencies,
	}
}

func graphFromPartials(parts partialSQLGraph) (*sqlGraph, error) {
	graph := newSqlGraph()
	for _, vertex := range parts.vertices {
		// It's possible the node already exists. merge it if it does
		if graph.HasVertexWithId(vertex.GetId()) {
			vertex = mergeVertices(graph.GetVertex(vertex.GetId()), vertex)
		}
		graph.AddVertex(vertex)
	}

	for _, dep := range parts.dependencies {
		// To maintain the correctness of the graph, we will add a dummy vertex for the missing dependencies
		addVertexIfNotExists(graph, dep.source)
		addVertexIfNotExists(graph, dep.target)

		if err := graph.AddEdge(dep.source.String(), dep.target.String()); err != nil {
			return nil, fmt.Errorf("adding edge from %s to %s: %w", dep.source, dep.target, err)
		}
	}

	return graph, nil
}

func mergeVertices(old, new sqlVertex) sqlVertex {
	priority := old.priority
	if new.priority != sqlPriorityUnset && (priority == sqlPriorityUnset || new.priority > priority) {
		// If one is unset, use the other. If both are set, use the higher priority.
		priority = new.priority
	}

	return sqlVertex{
		id:         old.id,
		priority:   priority,
		statements: append(old.statements, new.statements...),
	}
}

func addVertexIfNotExists(graph *sqlGraph, id sqlVertexId) {
	if !graph.HasVertexWithId(id.String()) {
		// Create a filler node
		graph.AddVertex(sqlVertex{
			id:         id,
			priority:   sqlPriorityUnset,
			statements: nil,
		})
	}
}

// sqlVertexGenerator generates partial SQL graphs for schema objects and their diffs.
type sqlVertexGenerator[S schema.Object, Diff diff[S]] interface {
	Add(S) (partialSQLGraph, error)
	Delete(S) (partialSQLGraph, error)
	// Alter generates the statements required to resolve the schema object to its new state using the
	// provided diff. Alter, e.g., with a table, might produce add/delete statements
	Alter(Diff) (partialSQLGraph, error)
}

func buildPartialSQLGraph(
	id sqlVertexId,
	priority sqlPriority,
	statements []Statement,
	dependencies []dependency,
) partialSQLGraph {
	return partialSQLGraph{
		vertices: []sqlVertex{{
			id:         id,
			priority:   priority,
			statements: statements,
		}},
		dependencies: dependencies,
	}
}

// generatePartialGraph generates a partial SQL graph for a schema object list diff using the provided generator.
func generatePartialGraph[S schema.Object, Diff diff[S]](generator sqlVertexGenerator[S, Diff], listDiff listDiff[S, Diff]) (partialSQLGraph, error) {
	var partialGraphs []partialSQLGraph
	for _, a := range listDiff.adds {
		v, err := generator.Add(a)
		if err != nil {
			return partialSQLGraph{}, fmt.Errorf("generating add SQL graph for %s: %w", a.GetName(), err)
		}
		partialGraphs = append(partialGraphs, v)
	}
	for _, d := range listDiff.deletes {
		v, err := generator.Delete(d)
		if err != nil {
			return partialSQLGraph{}, fmt.Errorf("generating delete SQL graph for %s: %w", d.GetName(), err)
		}
		partialGraphs = append(partialGraphs, v)
	}
	for _, a := range listDiff.alters {
		v, err := generator.Alter(a)
		if err != nil {
			return partialSQLGraph{}, fmt.Errorf("generating alter SQL graph for %s: %w", a.GetNew().GetName(), err)
		}
		partialGraphs = append(partialGraphs, v)
	}
	return concatPartialGraphs(partialGraphs...), nil
}
