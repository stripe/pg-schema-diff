package diff

import (
	"fmt"
	"sort"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

var (
	ErrNotImplemented      = fmt.Errorf("not implemented")
	errDuplicateIdentifier = fmt.Errorf("duplicate identifier")
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
)

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
	return len(ld.adds) == 0 && len(ld.alters) == 0 && len(ld.deletes) == 0
}

func (ld listDiff[S, D]) resolveToSQLGroupedByEffect(sqlGenerator sqlGenerator[S, D]) (sqlGroupedByEffect[S, D], error) {
	var adds, deletes, alters []Statement

	for _, a := range ld.adds {
		statements, err := sqlGenerator.Add(a)
		if err != nil {
			return sqlGroupedByEffect[S, D]{}, fmt.Errorf(
				"generating SQL for add %s: %w", a.GetName(), err,
			)
		}
		adds = append(adds, statements...)
	}
	for _, d := range ld.deletes {
		statements, err := sqlGenerator.Delete(d)
		if err != nil {
			return sqlGroupedByEffect[S, D]{}, fmt.Errorf(
				"generating SQL for delete %s: %w", d.GetName(), err,
			)
		}
		deletes = append(deletes, statements...)
	}
	for _, a := range ld.alters {
		statements, err := sqlGenerator.Alter(a)
		if err != nil {
			return sqlGroupedByEffect[S, D]{}, fmt.Errorf(
				"generating SQL for diff %+v: %w", a, err,
			)
		}
		alters = append(alters, statements...)
	}

	return sqlGroupedByEffect[S, D]{
		Adds:    adds,
		Deletes: deletes,
		Alters:  alters,
	}, nil
}

// diffLists diffs two lists of schema objects using name.
// If an object is present in both lists, it will use buildDiff function to build the diffs between the two objects. If
// build diff returns as requiresRecreation, then the old schema object will be deleted and the new one will be added
//
// Additions and alterations preserve the new schema order. Deletions are sorted by name because they come from a map.
func diffLists[S schema.Object, Diff diff[S]](
	oldSchemaObjs, newSchemaObjs []S,
	buildDiff func(old, new S) (diff Diff, requiresRecreation bool, error error),
) (listDiff[S, Diff], error) {
	nameToOld := make(map[string]S)
	for _, oldSchemaObject := range oldSchemaObjs {
		if _, nameAlreadyTaken := nameToOld[oldSchemaObject.GetName()]; nameAlreadyTaken {
			return listDiff[S, Diff]{}, fmt.Errorf(
				"multiple objects have identifier %s: %w", oldSchemaObject.GetName(), errDuplicateIdentifier,
			)
		}
		nameToOld[oldSchemaObject.GetName()] = oldSchemaObject
	}

	var adds []S
	var alters []Diff
	var deletes []S
	for _, newSchemaObj := range newSchemaObjs {
		if oldSchemaObj, hasOldSchemaObj := nameToOld[newSchemaObj.GetName()]; !hasOldSchemaObj {
			adds = append(adds, newSchemaObj)
		} else {
			delete(nameToOld, newSchemaObj.GetName())

			diff, requiresRecreation, err := buildDiff(oldSchemaObj, newSchemaObj)
			if err != nil {
				return listDiff[S, Diff]{}, fmt.Errorf("diffing for %s: %w", newSchemaObj.GetName(), err)
			}
			if requiresRecreation {
				deletes = append(deletes, oldSchemaObj)
				adds = append(adds, newSchemaObj)
			} else {
				alters = append(alters, diff)
			}
		}
	}

	// Remaining schema objects in nameToOld have been deleted
	for _, d := range nameToOld {
		deletes = append(deletes, d)
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
