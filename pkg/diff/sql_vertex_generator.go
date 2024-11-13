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

// partialSQLGraph represents the SQL statements and their dependencies. Every SQL statement is a
// vertex in the graph. This is different from a graph because a dependency may exist between a node
// within this part and a node in another part.
type partialSQLGraph struct {
	vertices     []sqlVertex
	dependencies []dependency
}

func newPartialSQLGraph() *partialSQLGraph {
	return &partialSQLGraph{
		vertices:     nil,
		dependencies: nil,
	}
}

func (s *partialSQLGraph) addVertex(v ...sqlVertex) {
	s.vertices = append(s.vertices, v...)
}

func (s *partialSQLGraph) addDependency(dep ...dependency) {
	s.dependencies = append(s.dependencies, dep...)
}

func (s *partialSQLGraph) statements() []Statement {
	var statements []Statement
	for _, vertex := range s.vertices {
		statements = append(statements, vertex.statements...)
	}
	return statements
}

func (s *partialSQLGraph) Clone() *partialSQLGraph {
	vertices := make([]sqlVertex, len(s.vertices))
	copy(vertices, s.vertices)
	dependencies := make([]dependency, len(s.dependencies))
	copy(dependencies, s.dependencies)
	return &partialSQLGraph{
		vertices:     vertices,
		dependencies: dependencies,
	}
}

func concatPartialGraphs(parts ...*partialSQLGraph) *partialSQLGraph {
	var vertices []sqlVertex
	var dependencies []dependency
	for _, part := range parts {
		vertices = append(vertices, part.vertices...)
		dependencies = append(dependencies, part.dependencies...)
	}
	return &partialSQLGraph{
		vertices:     vertices,
		dependencies: dependencies,
	}
}

func createNestedGraphForPartial(partial *partialSQLGraph, id nestedGraphSqlVertexId, priority sqlPriority) *partialSQLGraph {
	partial := partial.Clone()
	startVertex := sqlVertex{
		id:       id.WithNestedGraphMarker(subgraphMarkerStart),
		priority: priority,
	}
	endVertex := sqlVertex{
		id:       id.WithNestedGraphMarker(subgraphMarkerEnd),
		priority: priority,
	}

	var deps []dependency
	for _, vertex := range partial.vertices {
		// TODO(bplunkett) - build deps
	}
	partial.addVertex(startVertex, endVertex)
}

func graphFromPartials(parts *partialSQLGraph) (*sqlGraph, error) {
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

// sqlVertexGenerator generates SQL statements for a schema object and its diff. This is the canonical interface
// for SQL generation.
type sqlVertexGenerator[S schema.Object, Diff diff[S]] interface {
	Add(S) (*partialSQLGraph, error)
	Delete(S) (*partialSQLGraph, error)
	// Alter generates the statements required to resolve the schema object to its new state using the
	// provided diff. Alter, e.g., with a table, might produce add/delete statements
	Alter(Diff) (*partialSQLGraph, error)
}

// generatePartialGraph generates a partial for the given schema object list diff using the inutted generator.
func generatePartialGraph[S schema.Object, Diff diff[S]](generator sqlVertexGenerator[S, Diff], listDiff listDiff[S, Diff]) (*partialSQLGraph, error) {
	var partialGraphs []*partialSQLGraph
	for _, a := range listDiff.adds {
		v, err := generator.Add(a)
		if err != nil {
			return nil, fmt.Errorf("generating add statements for %s: %w", a.GetName(), err)
		}
		partialGraphs = append(partialGraphs, v)
	}
	for _, d := range listDiff.deletes {
		v, err := generator.Delete(d)
		if err != nil {
			return nil, fmt.Errorf("generating delete statements for %s: %w", d.GetName(), err)
		}
		partialGraphs = append(partialGraphs, v)
	}
	for _, a := range listDiff.alters {
		v, err := generator.Alter(a)
		if err != nil {
			return nil, fmt.Errorf("generating alter statements for %s: %w", a.GetNew().GetName(), err)
		}
		partialGraphs = append(partialGraphs, v)
	}
	return concatPartialGraphs(partialGraphs...), nil
}

// deprecated legacySqlVertexGenerator represents the "old" style for generating SQL vertices where the Add/Delete/Alter functions
// return a flat list of statements.
type legacySqlVertexGenerator[S schema.Object, Diff diff[S]] interface {
	sqlGenerator[S, Diff]
	// GetSQLVertexId gets the canonical vertex id to represent the schema object
	GetSQLVertexId(S, diffType) schemaObjNestedGraphSqlVertexId

	// GetAddAlterDependencies gets the dependencies of the SQL generated to resolve the AddAlter diff for the
	// schema objects. Dependencies can be formed on any other nodes in the SQL graph, even if the node has
	// no statements. If the diff is just an add, then old will be the zero value
	//
	// These dependencies can also be built in reverse: the SQL returned by the sqlVertexGenerator to resolve the
	// diff for the object must always be run before the SQL required to resolve another SQL vertex diff
	GetAddAlterDependencies(new S, old S) ([]dependency, error)

	// GetDeleteDependencies is the same as above but for deletes.
	// Invariant to maintain:
	// - If an object X depends on the delete for an object Y (generated by the sqlVertexGenerator), immediately after the
	// the (Y, diffTypeDelete) sqlVertex's SQL is run, Y must no longer be present in the schema; either the
	// (Y, diffTypeDelete) statements deleted Y or something that vertex depended on deleted Y. In other words, if a
	// delete is cascaded by another delete (e.g., index dropped by table drop) and the index SQL is empty,
	// the index delete vertex must still have dependency from itself to the object from which the delete cascades down from
	GetDeleteDependencies(S) ([]dependency, error)
}

type wrappedLegacySqlVertexGenerator[S schema.Object, Diff diff[S]] struct {
	generator legacySqlVertexGenerator[S, Diff]
}

func legacyToNewSqlVertexGenerator[S schema.Object, Diff diff[S]](generator legacySqlVertexGenerator[S, Diff]) sqlVertexGenerator[S, Diff] {
	return &wrappedLegacySqlVertexGenerator[S, Diff]{
		generator: generator,
	}
}

func (s *wrappedLegacySqlVertexGenerator[S, Diff]) Add(o S) (*partialSQLGraph, error) {
	statements, err := s.generator.Add(o)
	if err != nil {
		return nil, fmt.Errorf("generating sql: %w", err)
	}

	var zeroVal S
	deps, err := s.generator.GetAddAlterDependencies(o, zeroVal)
	if err != nil {
		return nil, fmt.Errorf("getting dependencies: %w", err)
	}

	return &partialSQLGraph{
		vertices: []sqlVertex{{
			id:         buildUUIDVertexId(),
			priority:   sqlPrioritySooner,
			statements: statements,
		}},
		dependencies: deps,
	}, nil
}

func (s *wrappedLegacySqlVertexGenerator[S, Diff]) Delete(o S) (*partialSQLGraph, error) {
	statements, err := s.generator.Delete(o)
	if err != nil {
		return nil, fmt.Errorf("generating sql: %w", err)
	}
	deps, err := s.generator.GetDeleteDependencies(o)
	if err != nil {
		return nil, fmt.Errorf("getting dependencies: %w", err)
	}

	return &partialSQLGraph{
		vertices: []sqlVertex{{
			id:         buildUUIDVertexId(),
			priority:   sqlPriorityLater,
			statements: statements,
		}},
		dependencies: deps,
	}, nil
}

func (s *wrappedLegacySqlVertexGenerator[S, Diff]) Alter(d Diff) (*partialSQLGraph, error) {
	statements, err := s.generator.Alter(d)
	if err != nil {
		return nil, fmt.Errorf("generating sql: %w", err)
	}
	deps, err := s.generator.GetAddAlterDependencies(d.GetNew(), d.GetOld())
	if err != nil {
		return nil, fmt.Errorf("getting dependencies: %w", err)
	}

	return &partialSQLGraph{
		vertices: []sqlVertex{{
			id:         buildUUIDVertexId(),
			priority:   sqlPrioritySooner,
			statements: statements,
		}},
		dependencies: deps,
	}, nil
}
