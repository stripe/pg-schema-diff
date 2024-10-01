package diff

import (
	"fmt"

	"github.com/stripe/pg-schema-diff/internal/graph"
)

// sqlVertexId is an interface for a vertex id in the SQL graph
type sqlVertexId interface {
	fmt.Stringer
}

// sqlPriority is an enum for the priority of a statement in the SQL graph, i.e., whether it should be run sooner
// or later in the topological sort of the graph. It can be thought of as a unary unit vector.
type sqlPriority int

const (
	// Indicates a statement should run as soon as possible. Usually, most adds will have this priority.
	sqlPrioritySooner sqlPriority = 1
	// sqlPriorityUnset is the default priority for a statement
	sqlPriorityUnset sqlPriority = 0
	// Indicates a statement should run as late as possible. Usually, most deletes will have this priority.
	sqlPriorityLater sqlPriority = -1
)

// schemaObjSqlVertexId is a vertex id for a standard schema object node, i.e., indicating the creation/deletiion
// of a schema object. It slots into the legacySqlVertexGenerator system.
type schemaObjSqlVertexId struct {
	objType  string
	objId    string
	diffType diffType
}

func buildSchemaObjVertexId(objType string, id string, diffType diffType) sqlVertexId {
	return schemaObjSqlVertexId{
		objType:  objType,
		objId:    id,
		diffType: diffType,
	}
}

func (s schemaObjSqlVertexId) String() string {
	return fmt.Sprintf("%s:%s:%s", s.objType, s.objId, s.diffType)
}

type sqlVertex struct {
	// id is used to identify the sql vertex
	id sqlVertexId

	// priority is used to determine if the sql vertex should be included sooner or later in the topological
	// sort of the graph
	priority sqlPriority

	// statements is the set of statements to run for this vertex
	statements []Statement
}

func (s sqlVertex) GetId() string {
	return s.id.String()
}

func (s sqlVertex) GetPriority() int {
	// Weight the priority (which is just a "direction") by the number of statements
	return len(s.statements) * int(s.priority)
}

// dependency indicates an edge between the SQL to resolve a diff for a source schema object and the SQL to resolve
// the diff of a target schema object.
//
// Most SchemaObjects will have two nodes in the SQL graph: a node for delete SQL and a node for add/alter SQL.
// These nodes will almost always be present in the sqlGraph even if the schema object is not being deleted (or added/altered).
// If a node is present for a schema object where the "diffType" is NOT occurring, it will just be a no-op (no SQl statements)
type dependency struct {
	// source must run before target
	source sqlVertexId
	// target must run after source
	target sqlVertexId
}

type dependencyBuilder struct {
	base sqlVertexId
}

func mustRun(id sqlVertexId) dependencyBuilder {
	return dependencyBuilder{
		base: id,
	}
}

func (d dependencyBuilder) before(id sqlVertexId) dependency {
	return dependency{
		source: d.base,
		target: id,
	}
}

func (d dependencyBuilder) after(id sqlVertexId) dependency {
	return dependency{
		source: id,
		target: d.base,
	}
}

// sqlGraph represents a dependency web of SQL statements
type sqlGraph struct {
	*graph.Graph[sqlVertex]
}

func newSqlGraph() *sqlGraph {
	return &sqlGraph{
		Graph: graph.NewGraph[sqlVertex](),
	}
}

func (s *sqlGraph) toOrderedStatements() ([]Statement, error) {
	vertices, err := s.TopologicallySortWithPriority(graph.IsLowerPriorityFromGetPriority(func(v sqlVertex) int {
		return v.GetPriority()
	}))
	if err != nil {
		return nil, fmt.Errorf("topologically sorting graph: %w", err)
	}
	var stmts []Statement
	for _, v := range vertices {
		stmts = append(stmts, v.statements...)
	}
	return stmts, nil
}
