package diff

import (
	"fmt"
	"sort"

	"github.com/stripe/pg-schema-diff/internal/graph"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

var ErrNotImplemented = fmt.Errorf("not implemented")
var errDuplicateIdentifier = fmt.Errorf("duplicate identifier")

type diffType string

const (
	diffTypeDelete   diffType = "DELETE"
	diffTypeAddAlter diffType = "ADDALTER"
)

type (
	diff[S schema.Object] interface {
		GetOld() S
		GetNew() S
	}

	// sqlGenerator is used to generate SQL that resolves diffs between lists
	sqlGenerator[S schema.Object, Diff diff[S]] interface {
		Add(S) ([]Statement, error)
		Delete(S) ([]Statement, error)
		// Alter generates the statements required to resolve the schema object to its new state using the
		// provided diff. Alter, e.g., with a table, might produce add/delete statements
		Alter(Diff) ([]Statement, error)
	}

	// dependency indicates an edge between the SQL to resolve a diff for a source schema object and the SQL to resolve
	// the diff of a target schema object
	//
	// Most SchemaObjects will have two nodes in the SQL graph: a node for delete SQL and a node for add/alter SQL.
	// These nodes will almost always be present in the sqlGraph even if the schema object is not being deleted (or added/altered).
	// If a node is present for a schema object where the "diffType" is NOT occurring, it will just be a no-op (no SQl statements)
	dependency struct {
		sourceObjId string
		sourceType  diffType

		targetObjId string
		targetType  diffType
	}
)

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

// sqlVertexGenerator is used to generate SQL statements for schema objects that have dependency webs
// with other schema objects. The schema object represents a vertex in the graph.
type sqlVertexGenerator[S schema.Object, Diff diff[S]] interface {
	sqlGenerator[S, Diff]
	// GetSQLVertexId gets the canonical vertex id to represent the schema object
	GetSQLVertexId(S) string

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

type (
	// listDiff represents the differences between two lists.
	listDiff[S schema.Object, Diff diff[S]] struct {
		adds    []S
		deletes []S
		// alters contains the diffs of any objects that persisted between two schemas
		alters []Diff
	}

	sqlGroupedByEffect[S schema.Object, Diff diff[S]] struct {
		Adds    []Statement
		Deletes []Statement
		// Alters might contain adds and deletes. For example, a set of alters for a table might add indexes.
		Alters []Statement
	}
)

func (ld listDiff[S, D]) isEmpty() bool {
	return len(ld.adds) == 0 || len(ld.alters) == 0 || len(ld.deletes) == 0
}

func (ld listDiff[S, D]) resolveToSQLGroupedByEffect(sqlGenerator sqlGenerator[S, D]) (sqlGroupedByEffect[S, D], error) {
	var adds, deletes, alters []Statement

	for _, a := range ld.adds {
		statements, err := sqlGenerator.Add(a)
		if err != nil {
			return sqlGroupedByEffect[S, D]{}, fmt.Errorf("generating SQL for add %s: %w", a.GetName(), err)
		}
		adds = append(adds, statements...)
	}
	for _, d := range ld.deletes {
		statements, err := sqlGenerator.Delete(d)
		if err != nil {
			return sqlGroupedByEffect[S, D]{}, fmt.Errorf("generating SQL for delete %s: %w", d.GetName(), err)
		}
		deletes = append(deletes, statements...)
	}
	for _, a := range ld.alters {
		statements, err := sqlGenerator.Alter(a)
		if err != nil {
			return sqlGroupedByEffect[S, D]{}, fmt.Errorf("generating SQL for diff %+v: %w", a, err)
		}
		alters = append(alters, statements...)
	}

	return sqlGroupedByEffect[S, D]{
		Adds:    adds,
		Deletes: deletes,
		Alters:  alters,
	}, nil
}

func (ld listDiff[S, D]) resolveToSQLGraph(generator sqlVertexGenerator[S, D]) (*sqlGraph, error) {
	graph := graph.NewGraph[sqlVertex]()

	for _, a := range ld.adds {
		statements, err := generator.Add(a)
		if err != nil {
			return nil, fmt.Errorf("generating SQL for add %s: %w", a.GetName(), err)
		}

		deps, err := generator.GetAddAlterDependencies(a, *new(S))
		if err != nil {
			return nil, fmt.Errorf("getting dependencies for add %s: %w", a.GetName(), err)
		}
		if err := addSQLVertexToGraph(graph, sqlVertex{
			ObjId:      generator.GetSQLVertexId(a),
			Statements: statements,
			DiffType:   diffTypeAddAlter,
		}, deps); err != nil {
			return nil, fmt.Errorf("adding SQL Vertex for add %s: %w", a.GetName(), err)
		}
	}

	for _, a := range ld.alters {
		statements, err := generator.Alter(a)
		if err != nil {
			return nil, fmt.Errorf("generating SQL for diff %+v: %w", a, err)
		}

		vertexId := generator.GetSQLVertexId(a.GetOld())
		vertexIdAfterAlter := generator.GetSQLVertexId(a.GetNew())
		if vertexIdAfterAlter != vertexId {
			return nil, fmt.Errorf("an alter lead to a node with a different id: old=%s, new=%s", vertexId, vertexIdAfterAlter)
		}

		deps, err := generator.GetAddAlterDependencies(a.GetNew(), a.GetOld())
		if err != nil {
			return nil, fmt.Errorf("getting dependencies for alter %s: %w", a.GetOld().GetName(), err)
		}

		if err := addSQLVertexToGraph(graph, sqlVertex{
			ObjId:      vertexId,
			Statements: statements,
			DiffType:   diffTypeAddAlter,
		}, deps); err != nil {
			return nil, fmt.Errorf("adding SQL Vertex for alter %s: %w", a.GetOld().GetName(), err)
		}
	}

	for _, d := range ld.deletes {
		statements, err := generator.Delete(d)
		if err != nil {
			return nil, fmt.Errorf("generating SQL for delete %s: %w", d.GetName(), err)
		}

		deps, err := generator.GetDeleteDependencies(d)
		if err != nil {
			return nil, fmt.Errorf("getting dependencies for delete %s: %w", d.GetName(), err)
		}

		if err := addSQLVertexToGraph(graph, sqlVertex{
			ObjId:      generator.GetSQLVertexId(d),
			Statements: statements,
			DiffType:   diffTypeDelete,
		}, deps); err != nil {
			return nil, fmt.Errorf("adding SQL Vertex for delete %s: %w", d.GetName(), err)
		}
	}

	return (*sqlGraph)(graph), nil
}

func addSQLVertexToGraph(graph *graph.Graph[sqlVertex], vertex sqlVertex, dependencies []dependency) error {
	// It's possible the node already exists. merge it if it does
	if graph.HasVertexWithId(vertex.GetId()) {
		vertex = mergeSQLVertices(graph.GetVertex(vertex.GetId()), vertex)
	}
	graph.AddVertex(vertex)
	for _, dep := range dependencies {
		if err := addDependency(graph, dep); err != nil {
			return fmt.Errorf("adding dependencies for %s: %w", vertex.GetId(), err)
		}
	}
	return nil
}

func addDependency(graph *graph.Graph[sqlVertex], dep dependency) error {
	sourceVertex := sqlVertex{
		ObjId:      dep.sourceObjId,
		DiffType:   dep.sourceType,
		Statements: nil,
	}
	targetVertex := sqlVertex{
		ObjId:      dep.targetObjId,
		DiffType:   dep.targetType,
		Statements: nil,
	}

	// To maintain the correctness of the graph, we will add a dummy vertex for the missing dependencies
	addVertexIfNotExists(graph, sourceVertex)
	addVertexIfNotExists(graph, targetVertex)

	if err := graph.AddEdge(sourceVertex.GetId(), targetVertex.GetId()); err != nil {
		return fmt.Errorf("adding edge from %s to %s: %w", sourceVertex.GetId(), targetVertex.GetId(), err)
	}

	return nil
}

func addVertexIfNotExists(graph *graph.Graph[sqlVertex], vertex sqlVertex) {
	if !graph.HasVertexWithId(vertex.GetId()) {
		graph.AddVertex(vertex)
	}
}

type schemaObjectEntry[S schema.Object] struct {
	index int //  index is the index the schema object in the list
	obj   S
}

// diffLists diffs two lists of schema objects using name.
// If an object is present in both lists, it will use buildDiff function to build the diffs between the two objects. If
// build diff returns as requiresRecreation, then the old schema object will be deleted and the new one will be added
//
// The List will outputted in a deterministic order by schema object name, which is important for tests
func diffLists[S schema.Object, Diff diff[S]](
	oldSchemaObjs, newSchemaObjs []S,
	buildDiff func(old, new S, oldIndex, newIndex int) (diff Diff, requiresRecreation bool, error error),
) (listDiff[S, Diff], error) {
	nameToOld := make(map[string]schemaObjectEntry[S])
	for oldIndex, oldSchemaObject := range oldSchemaObjs {
		if _, nameAlreadyTaken := nameToOld[oldSchemaObject.GetName()]; nameAlreadyTaken {
			return listDiff[S, Diff]{}, fmt.Errorf("multiple objects have identifier %s: %w", oldSchemaObject.GetName(), errDuplicateIdentifier)
		}
		// store the old schema object and its index. if an alteration, the index might be used in the diff, e.g., for columns
		nameToOld[oldSchemaObject.GetName()] = schemaObjectEntry[S]{
			obj:   oldSchemaObject,
			index: oldIndex,
		}
	}

	var adds []S
	var alters []Diff
	var deletes []S
	for newIndex, newSchemaObj := range newSchemaObjs {
		if oldSchemaObjAndIndex, hasOldSchemaObj := nameToOld[newSchemaObj.GetName()]; !hasOldSchemaObj {
			adds = append(adds, newSchemaObj)
		} else {
			delete(nameToOld, newSchemaObj.GetName())

			diff, requiresRecreation, err := buildDiff(oldSchemaObjAndIndex.obj, newSchemaObj, oldSchemaObjAndIndex.index, newIndex)
			if err != nil {
				return listDiff[S, Diff]{}, fmt.Errorf("diffing for %s: %w", newSchemaObj.GetName(), err)
			}
			if requiresRecreation {
				deletes = append(deletes, oldSchemaObjAndIndex.obj)
				adds = append(adds, newSchemaObj)
			} else {
				alters = append(alters, diff)
			}
		}
	}

	// Remaining schema objects in nameToOld have been deleted
	for _, d := range nameToOld {
		deletes = append(deletes, d.obj)
	}
	// Iterating through a map is non-deterministic in go, so we'll sort the deletes by schema object name
	sort.Slice(deletes, func(i, j int) bool {
		return deletes[i].GetName() < deletes[j].GetName()
	})

	return listDiff[S, Diff]{
		adds:    adds,
		deletes: deletes,
		alters:  alters,
	}, nil
}
