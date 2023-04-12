package diff

import (
	"fmt"

	"github.com/stripe/pg-schema-diff/internal/graph"
)

type sqlVertex struct {
	ObjId      string
	Statements []Statement
	DiffType   diffType
}

func (s sqlVertex) GetId() string {
	return fmt.Sprintf("%s_%s", s.DiffType, s.ObjId)
}

func buildTableVertexId(name string) string {
	return fmt.Sprintf("table_%s", name)
}

func buildIndexVertexId(name string) string {
	return fmt.Sprintf("index_%s", name)
}

// sqlGraph represents two dependency webs of SQL statements
type sqlGraph graph.Graph[sqlVertex]

// union unions the two AddsAndAlters graphs and, separately, unions the two delete graphs
func (s *sqlGraph) union(sqlGraph *sqlGraph) error {
	if err := (*graph.Graph[sqlVertex])(s).Union((*graph.Graph[sqlVertex])(sqlGraph), mergeSQLVertices); err != nil {
		return fmt.Errorf("unioning the graphs: %w", err)
	}
	return nil
}

func mergeSQLVertices(old, new sqlVertex) sqlVertex {
	return sqlVertex{
		ObjId:      old.ObjId,
		DiffType:   old.DiffType,
		Statements: append(old.Statements, new.Statements...),
	}
}

func (s *sqlGraph) toOrderedStatements() ([]Statement, error) {
	vertices, err := (*graph.Graph[sqlVertex])(s).TopologicallySortWithPriority(graph.IsLowerPriorityFromGetPriority(
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
