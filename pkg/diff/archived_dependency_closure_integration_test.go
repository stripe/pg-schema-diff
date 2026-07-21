package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/internal/testdb"
)

func TestPlanArchivedDependencyClosureDatabaseCatalogFixture(t *testing.T) {
	t.Parallel()

	factory := testdb.MustNewFactory(t)
	currentDB := factory.CreateDatabase(t)
	_, err := currentDB.ConnPool.Exec(t.Context(), `
		CREATE SCHEMA managed;
		CREATE SCHEMA deps;
		CREATE TYPE deps.status AS ENUM ('ready', 'done');
		CREATE DOMAIN deps.positive AS integer
			DEFAULT 1 CONSTRAINT positive_value CHECK (VALUE > 0);
		CREATE TYPE deps.pair AS (amount deps.positive, label text);
		CREATE TYPE deps.int_span AS RANGE (SUBTYPE = integer);
		CREATE COLLATION deps.c_copy FROM "C";
		CREATE FUNCTION deps.identity_value(value integer)
			RETURNS integer LANGUAGE SQL IMMUTABLE RETURN value;
		CREATE FUNCTION deps.integer_equal(left_value integer, right_value integer)
			RETURNS boolean LANGUAGE SQL IMMUTABLE
			RETURN left_value = right_value;
		CREATE OPERATOR deps.=== (
			LEFTARG = integer,
			RIGHTARG = integer,
			FUNCTION = deps.integer_equal
		);
		CREATE SEQUENCE deps.standalone_numbers AS bigint;
		CREATE TABLE managed.archived (
			status deps.status,
			positive deps.positive,
			pair deps.pair,
			span deps.int_span,
			spans deps.int_span_multirange,
			label text COLLATE deps.c_copy,
			number bigint DEFAULT nextval('deps.standalone_numbers'),
			compared integer DEFAULT deps.identity_value(1),
			CONSTRAINT compared_equal CHECK (
				compared OPERATOR(deps.===) compared
			)
		);
	`)
	require.NoError(t, err)
	targetDB := factory.CreateDatabase(t)
	current := sourceSafetyDatabaseSnapshot(t, currentDB, "managed", "deps")
	target := sourceSafetyDatabaseSnapshot(t, targetDB, "managed", "deps")
	tableOID := sourceSafetyRelationOID(t, current.Inventory, "managed", "archived")
	preflight, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		ProposedTableRelationOIDs: []uint32{tableOID},
	})
	require.NoError(t, err)

	result, err := planArchivedDependencyClosure(archivedDependencyClosureRequest{
		CurrentSnapshot: current, TargetSnapshot: target, SourcePreflight: preflight,
		ProposedGroups: []archivedDependencyClosureGroupRequest{{
			GroupID: "database-fixture", TableRelationOIDs: []uint32{tableOID},
			DependencySchemaName: "archive_fixture_dependencies",
		}},
	})
	require.NoError(t, err)

	assigned := make(map[string]archivalMarkerObjectKind)
	for _, assignment := range result.Assignments {
		assigned[assignment.Source.Name] = assignment.Source.Kind
		assert.Equal(t, "archive_fixture_dependencies", assignment.Destination.SchemaName)
		assert.Equal(t, archivalGroupID("database-fixture"), assignment.GroupID)
	}
	for name, kind := range map[string]archivalMarkerObjectKind{
		"status":              archivalMarkerObjectKindType,
		"positive":            archivalMarkerObjectKindType,
		"pair":                archivalMarkerObjectKindType,
		"int_span":            archivalMarkerObjectKindType,
		"int_span_multirange": archivalMarkerObjectKindType,
		"c_copy":              archivalMarkerObjectKindCollation,
		"identity_value":      archivalMarkerObjectKindFunction,
		"integer_equal":       archivalMarkerObjectKindFunction,
		"===":                 archivalMarkerObjectKindOperator,
		"standalone_numbers":  archivalMarkerObjectKindSequence,
	} {
		assert.Equal(t, kind, assigned[name], name)
	}

	status := catalogTypeForClosureTest(t, current.Inventory, "deps", "status")
	var supportFunctionOIDs []uint32
	for _, support := range current.Inventory.TypeSupportFunctions {
		if support.TypeOID == status.OID {
			supportFunctionOIDs = append(supportFunctionOIDs, support.FunctionOID)
		}
	}
	require.NotEmpty(t, supportFunctionOIDs)
	for _, supportOID := range supportFunctionOIDs {
		object := closureObjectByOID(t, result.Objects, pgProcCatalogOID, supportOID)
		assert.Equal(t, archivalMarkerObjectKindFunction, object.Kind)
		assert.False(t, object.Movable)
		assert.Equal(t, archivedDependencyClassificationTargetCompatible, object.Classification)
	}
	assert.Empty(t, result.SharedGroupEdges)
}

func catalogTypeForClosureTest(
	t *testing.T,
	inventory schema.CatalogInventory,
	schemaName string,
	name string,
) schema.CatalogType {
	t.Helper()
	for _, catalogType := range inventory.Types {
		if catalogType.SchemaName == schemaName && catalogType.Name == name {
			return catalogType
		}
	}
	require.FailNow(t, "catalog type not found", "%s.%s", schemaName, name)
	return schema.CatalogType{}
}

func closureObjectByOID(
	t *testing.T,
	objects []archivedDependencyClosureObject,
	classOID uint32,
	objectOID uint32,
) archivedDependencyClosureObject {
	t.Helper()
	for _, object := range objects {
		if object.Address.ClassOID == classOID && object.Address.ObjectOID == objectOID {
			return object
		}
	}
	require.FailNow(t, "closure object not found", "catalog address %d/%d", classOID, objectOID)
	return archivedDependencyClosureObject{}
}
