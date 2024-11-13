package diff

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/stripe/pg-schema-diff/internal/graph"
)

// sqlVertexId is an interface for a vertex id in the SQL graph
type sqlVertexId interface {
	fmt.Stringer
}

// sqlPriority is an enum for the priority of a statement in the SQL graph, i.e., whether it should be run sooner
// or later in the topological sort of the graph. It can be thought of as a unary unit vector.
type sqlPriority int

// Used to indicate the start or end of an operation in a SQL graph. One could imagine an operation comprised of
// an entire subgraph of SQL statements.
type nestedGraphMarker string

const (
	markerUnset nestedGraphMarker = ""
	// subgraphMarkerStart is used to indicate the start of an operation
	subgraphMarkerStart nestedGraphMarker = "START"
	// subgraphMarkerEnd is used to indicate the end of an operation
	subgraphMarkerEnd nestedGraphMarker = "END"
)

type nestedGraphSqlVertexId interface {
	sqlVertexId
	WithNestedGraphMarker(marker nestedGraphMarker) sqlVertexId
}

const (
	// Indicates a statement should run as soon as possible. Usually, most adds will have this priority.
	sqlPrioritySooner sqlPriority = 1
	// sqlPriorityUnset is the default priority for a statement
	sqlPriorityUnset sqlPriority = 0
	// Indicates a statement should run as late as possible. Usually, most deletes will have this priority.
	sqlPriorityLater sqlPriority = -1
)

// schemaObjNestedGraphSqlVertexId is a vertex id for a standard schema object node, i.e., indicating the creation/deletion
// of a schema object. It slots into the legacySqlVertexGenerator system.
//
// Every schemaObjNestedGraphSqlVertexId should have an accompanying id with the opposite marker, i.e., start and end, because
// it effectively represents a subgraph.
type schemaObjNestedGraphSqlVertexId struct {
	objType    string
	objId      string
	diffType   diffType
	diffMarker nestedGraphMarker
}

func buildSchemaObjVertexId(objType string, id string, diffType diffType) schemaObjNestedGraphSqlVertexId {
	return schemaObjNestedGraphSqlVertexId{
		objType:    objType,
		objId:      id,
		diffType:   diffType,
		diffMarker: markerUnset,
	}
}

func (s schemaObjNestedGraphSqlVertexId) WithNestedGraphMarker(m nestedGraphMarker) sqlVertexId {
	s.diffMarker = m
	return s
}

func (s schemaObjNestedGraphSqlVertexId) String() string {
	return fmt.Sprintf("%T:%s:%s:%s:%s", s, s.objType, s.objId, s.diffType, s.diffMarker)
}

type uuidSqlVertexId struct {
	uuid uuid.UUID
}

// buildUUIDVertexId creates a new UUID vertex id. This is useful if the vertex is unlikely to have a dependency
// hard-coded.
func buildUUIDVertexId() sqlVertexId {
	return uuidSqlVertexId{
		uuid: uuid.New(),
	}
}

func (u uuidSqlVertexId) String() string {
	return fmt.Sprintf("%T:%s", u, u.uuid.String())
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
	base nestedGraphSqlVertexId
}

func mustRun(id nestedGraphSqlVertexId) dependencyBuilder {
	return dependencyBuilder{
		base: id,
	}
}

//func mustRunVertex(vertex sqlVertex) dependencyBuilder {
//	return dependencyBuilder{
//		base: vertex.id,
//	}
//}

func (d dependencyBuilder) before(id nestedGraphSqlVertexId) dependency {
	return dependency{
		source: d.base.WithNestedGraphMarker(subgraphMarkerEnd),
		target: id.WithNestedGraphMarker(subgraphMarkerStart),
	}
}

func (d dependencyBuilder) after(id nestedGraphSqlVertexId) dependency {
	return dependency{
		source: id.WithNestedGraphMarker(subgraphMarkerEnd),
		target: d.base.WithNestedGraphMarker(subgraphMarkerStart),
	}
}

//func (d dependencyBuilder) afterAll(ids ...sqlVertexId) []dependency {
//	var deps []dependency
//	for _, i := range ids {
//		deps = append(deps, d.after(i))
//	}
//	return deps
//}
//
//func (d dependencyBuilder) afterAllVertices(vertices ...sqlVertex) []dependency {
//	var ids []sqlVertexId
//	for _, v := range vertices {
//		ids = append(ids, v.id)
//	}
//	return d.afterAll(ids...)
//}

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
