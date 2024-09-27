package diff

import (
	"fmt"

	"github.com/stripe/pg-schema-diff/internal/graph"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type sqlVertex struct {
	// todo(bplunkett) - rename to statement id?
	// these ids will need to be globally unique
	ObjId      string
	Statements []Statement
	// todo(bplunkett) - this should not affect the id
	DiffType diffType
}

func (s sqlVertex) GetId() string {
	return fmt.Sprintf("%s_%s", s.DiffType, s.ObjId)
}

func buildTableVertexId(name schema.SchemaQualifiedName) string {
	return fmt.Sprintf("table_%s", name)
}

func buildIndexVertexId(name schema.SchemaQualifiedName) string {
	return fmt.Sprintf("index_%s", name)
}

// dependency indicates an edge between the SQL to resolve a diff for a source schema object and the SQL to resolve
// the diff of a target schema object
//
// Most SchemaObjects will have two nodes in the SQL graph: a node for delete SQL and a node for add/alter SQL.
// These nodes will almost always be present in the sqlGraph even if the schema object is not being deleted (or added/altered).
// If a node is present for a schema object where the "diffType" is NOT occurring, it will just be a no-op (no SQl statements)
type dependency struct {
	sourceObjId string
	sourceType  diffType

	targetObjId string
	targetType  diffType
}

type dependencyBuilder struct {
	valObjId string
	valType  diffType
}

func mustRun(schemaObjId string, schemaDiffType diffType) dependencyBuilder {
	return dependencyBuilder{
		valObjId: schemaObjId,
		valType:  schemaDiffType,
	}
}

func (d dependencyBuilder) before(valObjId string, valType diffType) dependency {
	return dependency{
		sourceType:  d.valType,
		sourceObjId: d.valObjId,

		targetType:  valType,
		targetObjId: valObjId,
	}
}

func (d dependencyBuilder) after(valObjId string, valType diffType) dependency {
	return dependency{
		sourceObjId: valObjId,
		sourceType:  valType,

		targetObjId: d.valObjId,
		targetType:  d.valType,
	}
}

// sqlGraph represents two dependency webs of SQL statements
type sqlGraph struct {
	*graph.Graph[sqlVertex]
}

func newSqlGraph() *sqlGraph {
	return &sqlGraph{
		Graph: graph.NewGraph[sqlVertex](),
	}
}

func (s *sqlGraph) toOrderedStatements() ([]Statement, error) {
	vertices, err := s.TopologicallySortWithPriority(graph.IsLowerPriorityFromGetPriority(
		func(vertex sqlVertex) int {
			multiplier := 1
			if vertex.DiffType == diffTypeDelete {
				multiplier = -1
			}
			// Prioritize adds/alters over deletes. Weight by number of statements. A 0 statement delete should be
			// prioritized over a 1 statement delete
			return len(vertex.Statements) * multiplier
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("topologically sorting graph: %w", err)
	}
	var stmts []Statement
	for _, v := range vertices {
		stmts = append(stmts, v.Statements...)
	}
	return stmts, nil
}
