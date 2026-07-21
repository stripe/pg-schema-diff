package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/testdb"
)

func TestClassifyCatalogRoutineReferenceTrackability(t *testing.T) {
	testCases := []struct {
		name         string
		language     string
		hasSQLBody   bool
		bodyForm     CatalogRoutineBodyForm
		trackability CatalogRoutineReferenceTrackability
	}{
		{
			name: "SQL standard body", language: "sql", hasSQLBody: true,
			bodyForm:     CatalogRoutineBodyFormSQLStandard,
			trackability: CatalogRoutineReferenceTrackabilityCatalogTrackable,
		},
		{
			name: "SQL string body", language: "sql",
			bodyForm:     CatalogRoutineBodyFormSQLString,
			trackability: CatalogRoutineReferenceTrackabilityUntrackable,
		},
		{
			name: "PL/pgSQL", language: "plpgsql",
			bodyForm:     CatalogRoutineBodyFormPLPGSQL,
			trackability: CatalogRoutineReferenceTrackabilityUntrackable,
		},
		{
			name: "unknown language", language: "future_language", hasSQLBody: true,
			bodyForm:     CatalogRoutineBodyFormOther,
			trackability: CatalogRoutineReferenceTrackabilityUntrackable,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			bodyForm, trackability := ClassifyCatalogRoutineReferenceTrackability(
				testCase.language, testCase.hasSQLBody,
			)
			assert.Equal(t, testCase.bodyForm, bodyForm)
			assert.Equal(t, testCase.trackability, trackability)
		})
	}
}

func TestCatalogDependencyInventoryNormalize(t *testing.T) {
	inventory := CatalogInventory{
		Dependencies: []CatalogDependency{
			{IsShared: true, Dependent: CatalogDependencyObject{ClassOID: 2, ObjectOID: 1}},
			{Dependent: CatalogDependencyObject{ClassOID: 1, ObjectOID: 2}},
			{Dependent: CatalogDependencyObject{ClassOID: 1, ObjectOID: 1}},
		},
		Views: []CatalogView{
			{RelationOID: 2, SchemaName: "z", Name: "view", Options: []string{"z=1", "a=1"}},
			{RelationOID: 1, SchemaName: "a", Name: "view"},
		},
		Routines: []CatalogRoutine{
			{OID: 2, SchemaName: "z", Name: "routine"},
			{OID: 1, SchemaName: "a", Name: "routine"},
		},
		EventTriggers: []CatalogEventTrigger{
			{OID: 2, Name: "z", Tags: []string{"DROP TABLE", "CREATE TABLE"}},
			{OID: 1, Name: "a"},
		},
	}

	normalized := inventory.Normalize()
	assert.Equal(t, uint32(1), normalized.Dependencies[0].Dependent.ObjectOID)
	assert.False(t, normalized.Dependencies[0].IsShared)
	assert.True(t, normalized.Dependencies[2].IsShared)
	assert.Equal(t, "a", normalized.Views[0].SchemaName)
	assert.Equal(t, []string{"a=1", "z=1"}, normalized.Views[1].Options)
	assert.Equal(t, "a", normalized.Routines[0].SchemaName)
	assert.Equal(t, "a", normalized.EventTriggers[0].Name)
	assert.Equal(t, []string{"CREATE TABLE", "DROP TABLE"}, normalized.EventTriggers[1].Tags)
	assert.Equal(t, normalized, normalized.Normalize())
	assert.Equal(t, []string{"z=1", "a=1"}, inventory.Views[0].Options,
		"normalization must not mutate nested input slices")
}

func TestCatalogDependencyInventoryDatabaseFixtures(t *testing.T) {
	factory := testdb.MustNewFactory(t)

	t.Run("excluded dependencies, dependent objects, and routines", func(t *testing.T) {
		db := factory.CreateDatabase(t)
		_, err := db.ConnPool.Exec(t.Context(), `
			CREATE SCHEMA managed_dependency;
			CREATE TABLE managed_dependency.target (
				id INTEGER PRIMARY KEY,
				payload TEXT
			);
		`)
		require.NoError(t, err)

		before, err := GetSchemaSnapshot(t.Context(), db.ConnPool,
			WithIncludeSchemaPatterns("managed_dependency"))
		require.NoError(t, err)

		_, err = db.ConnPool.Exec(t.Context(), `
			CREATE SCHEMA excluded_dependency;
			CREATE TYPE excluded_dependency.wrapper AS (
				target_row managed_dependency.target,
				note TEXT
			);
			CREATE TABLE excluded_dependency.row_consumer (
				target_row managed_dependency.target,
				wrapped excluded_dependency.wrapper
			);
			CREATE VIEW excluded_dependency.target_view AS
				SELECT id, payload FROM managed_dependency.target;
			CREATE MATERIALIZED VIEW excluded_dependency.target_materialized AS
				SELECT id FROM managed_dependency.target WITH NO DATA;
			CREATE TABLE excluded_dependency.rule_audit (id INTEGER, target_count BIGINT);
			CREATE TABLE excluded_dependency.rule_source (id INTEGER);
			CREATE RULE target_rule AS ON INSERT TO excluded_dependency.rule_source
				DO ALSO INSERT INTO excluded_dependency.rule_audit
				VALUES (NEW.id, (SELECT count(*) FROM managed_dependency.target));

			CREATE FUNCTION excluded_dependency.atomic_reference(
				value managed_dependency.target
			) RETURNS INTEGER
			LANGUAGE SQL
			SET search_path = pg_catalog
			BEGIN ATOMIC
				SELECT (value).id;
			END;

			CREATE PROCEDURE excluded_dependency.atomic_procedure(
				value managed_dependency.target
			) LANGUAGE SQL
			BEGIN ATOMIC
				SELECT (value).id;
			END;

			CREATE FUNCTION excluded_dependency.string_reference()
			RETURNS INTEGER LANGUAGE SQL
			AS 'SELECT id FROM managed_dependency.target ORDER BY id LIMIT 1';

			CREATE FUNCTION excluded_dependency.plpgsql_reference()
			RETURNS INTEGER LANGUAGE plpgsql AS $function$
			BEGIN
				RETURN (SELECT id FROM managed_dependency.target ORDER BY id LIMIT 1);
			END
			$function$;

			CREATE FUNCTION excluded_dependency.dynamic_reference()
			RETURNS INTEGER LANGUAGE plpgsql AS $function$
			DECLARE result INTEGER;
			BEGIN
				EXECUTE 'SELECT id FROM managed_dependency.target ORDER BY id LIMIT 1'
					INTO result;
				RETURN result;
			END
			$function$;
		`)
		require.NoError(t, err)

		after, err := GetSchemaSnapshot(t.Context(), db.ConnPool,
			WithIncludeSchemaPatterns("managed_dependency"))
		require.NoError(t, err)
		assert.Equal(t, before.Schema, after.Schema)
		assert.Equal(t, before.Hash, after.Hash,
			"dependency inventory must not expand the legacy modeled-schema hash")
		assert.NotEqual(t, before.Inventory, after.Inventory)
		assert.Equal(t, after.Inventory, after.Inventory.Normalize())
		for _, namedSchema := range after.Schema.NamedSchemas {
			assert.NotEqual(t, "excluded_dependency", namedSchema.Name)
		}

		target := catalogRelationByName(t, after.Inventory, "managed_dependency", "target")
		incomingDependency, found := catalogIncomingDependency(
			after.Inventory, target.OID, "excluded_dependency",
		)
		require.True(t, found, "an excluded-schema object must retain its incoming dependency edge")
		assert.NotZero(t, incomingDependency.Dependent.ClassOID)
		assert.NotZero(t, incomingDependency.Dependent.ObjectOID)
		assert.NotZero(t, incomingDependency.Referenced.ClassOID)
		assert.Equal(t, target.OID, incomingDependency.Referenced.ObjectOID)
		assert.NotEmpty(t, incomingDependency.Type)
		assert.True(t, hasIncomingCatalogDependency(
			after.Inventory, target.RowTypeOID, "excluded_dependency",
		), "row-type consumers must be discoverable by raw type OID")
		assert.True(t, hasSharedCatalogDependency(after.Inventory),
			"shared-catalog owner dependencies must remain available as raw edges")

		view := catalogViewByName(t, after.Inventory, "excluded_dependency", "target_view")
		assert.Equal(t, RelKindView, view.Kind)
		assert.Contains(t, view.Definition, "managed_dependency.target")
		materialized := catalogViewByName(t, after.Inventory, "excluded_dependency", "target_materialized")
		assert.Equal(t, RelKindMaterializedView, materialized.Kind)
		assert.False(t, materialized.IsPopulated)
		catalogRuleByName(t, after.Inventory,
			catalogRelationByName(t, after.Inventory, "excluded_dependency", "rule_source").OID,
			"target_rule")
		catalogRuleByName(t, after.Inventory, view.RelationOID, "_RETURN")

		wrapper := catalogTypeByName(t, after.Inventory, "excluded_dependency", "wrapper")
		assert.Equal(t, CatalogTypeKindComposite, wrapper.Kind)
		attributes := catalogCompositeAttributes(after.Inventory, wrapper.OID)
		require.Len(t, attributes, 2)
		assert.Equal(t, target.RowTypeOID, attributes[0].AttributeTypeOID)

		atomic := catalogRoutineByName(t, after.Inventory, "excluded_dependency", "atomic_reference")
		assert.Equal(t, CatalogRoutineBodyFormSQLStandard, atomic.BodyForm)
		assert.Equal(t, CatalogRoutineReferenceTrackabilityCatalogTrackable, atomic.ReferenceTrackability)
		assert.True(t, atomic.HasSQLBody)
		assert.NotEmpty(t, atomic.SQLBody)
		assert.Contains(t, atomic.Definition, "BEGIN ATOMIC")
		assert.Equal(t, []uint32{target.RowTypeOID}, atomic.InputArgumentTypeOIDs)
		assert.NotEmpty(t, atomic.Configuration)
		assert.True(t, hasCatalogDependency(after.Inventory, atomic.OID, target.RowTypeOID),
			"catalog-trackable routine signatures must retain raw type dependencies")

		procedure := catalogRoutineByName(t, after.Inventory, "excluded_dependency", "atomic_procedure")
		assert.Equal(t, "p", procedure.Kind)
		assert.Equal(t, CatalogRoutineReferenceTrackabilityCatalogTrackable, procedure.ReferenceTrackability)

		stringBody := catalogRoutineByName(t, after.Inventory, "excluded_dependency", "string_reference")
		assert.Equal(t, CatalogRoutineBodyFormSQLString, stringBody.BodyForm)
		assert.Equal(t, CatalogRoutineReferenceTrackabilityUntrackable, stringBody.ReferenceTrackability)
		assert.False(t, stringBody.HasSQLBody)
		plpgsql := catalogRoutineByName(t, after.Inventory, "excluded_dependency", "plpgsql_reference")
		assert.Equal(t, CatalogRoutineBodyFormPLPGSQL, plpgsql.BodyForm)
		assert.Equal(t, CatalogRoutineReferenceTrackabilityUntrackable, plpgsql.ReferenceTrackability)
		dynamic := catalogRoutineByName(t, after.Inventory, "excluded_dependency", "dynamic_reference")
		assert.Equal(t, CatalogRoutineBodyFormPLPGSQL, dynamic.BodyForm)
		assert.Equal(t, CatalogRoutineReferenceTrackabilityUntrackable, dynamic.ReferenceTrackability)
	})

	t.Run("complete foreign keys and partition parents", func(t *testing.T) {
		db := factory.CreateDatabase(t)
		_, err := db.ConnPool.Exec(t.Context(), `
			CREATE SCHEMA foreign_key_inventory;
			CREATE TABLE foreign_key_inventory.referenced (
				first_id INTEGER,
				second_id INTEGER,
				PRIMARY KEY (first_id, second_id)
			);
			CREATE TABLE foreign_key_inventory.partitioned_owner (
				partition_id INTEGER,
				first_id INTEGER,
				second_id INTEGER
			) PARTITION BY RANGE (partition_id);
			CREATE TABLE foreign_key_inventory.owner_child
				PARTITION OF foreign_key_inventory.partitioned_owner
				FOR VALUES FROM (0) TO (100);
			ALTER TABLE foreign_key_inventory.partitioned_owner
				ADD CONSTRAINT partitioned_fk
				FOREIGN KEY (second_id, first_id)
				REFERENCES foreign_key_inventory.referenced (second_id, first_id)
				MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL
				DEFERRABLE INITIALLY DEFERRED;

			CREATE TABLE foreign_key_inventory.invalid_owner (
				first_id INTEGER DEFAULT 0,
				second_id INTEGER DEFAULT 0
			);
			ALTER TABLE foreign_key_inventory.invalid_owner
				ADD CONSTRAINT invalid_fk
				FOREIGN KEY (first_id, second_id)
				REFERENCES foreign_key_inventory.referenced (first_id, second_id)
				MATCH SIMPLE ON UPDATE SET DEFAULT ON DELETE RESTRICT NOT VALID;
		`)
		require.NoError(t, err)

		snapshot, err := GetSchemaSnapshot(t.Context(), db.ConnPool)
		require.NoError(t, err)
		parentRelation := catalogRelationByName(t, snapshot.Inventory,
			"foreign_key_inventory", "partitioned_owner")
		parent := catalogForeignKey(t, snapshot.Inventory, parentRelation.OID, "partitioned_fk")
		assert.Equal(t, []CatalogForeignKeyColumn{
			{OwningNumber: 3, OwningName: "second_id", ReferencedNumber: 2, ReferencedName: "second_id"},
			{OwningNumber: 2, OwningName: "first_id", ReferencedNumber: 1, ReferencedName: "first_id"},
		}, parent.Columns)
		assert.Equal(t, "f", parent.MatchType)
		assert.Equal(t, "c", parent.UpdateAction)
		assert.Equal(t, "n", parent.DeleteAction)
		assert.True(t, parent.IsDeferrable)
		assert.True(t, parent.IsDeferred)
		assert.True(t, parent.IsValidated)
		assert.Zero(t, parent.ParentConstraintOID)
		assert.Contains(t, parent.Definition, "MATCH FULL")

		childRelation := catalogRelationByName(t, snapshot.Inventory,
			"foreign_key_inventory", "owner_child")
		child := catalogForeignKey(t, snapshot.Inventory, childRelation.OID, "partitioned_fk")
		assert.Equal(t, parent.OID, child.ParentConstraintOID)
		assert.Equal(t, parent.Columns, child.Columns)

		invalidRelation := catalogRelationByName(t, snapshot.Inventory,
			"foreign_key_inventory", "invalid_owner")
		invalid := catalogForeignKey(t, snapshot.Inventory, invalidRelation.OID, "invalid_fk")
		assert.Equal(t, "s", invalid.MatchType)
		assert.Equal(t, "d", invalid.UpdateAction)
		assert.Equal(t, "r", invalid.DeleteAction)
		assert.False(t, invalid.IsDeferrable)
		assert.False(t, invalid.IsDeferred)
		assert.False(t, invalid.IsValidated)
	})

	t.Run("types, collations, operators, and standalone sequences", func(t *testing.T) {
		db := factory.CreateDatabase(t)
		_, err := db.ConnPool.Exec(t.Context(), `
			CREATE SCHEMA type_inventory;
			CREATE DOMAIN type_inventory.positive_integer AS INTEGER
				DEFAULT 1 NOT NULL CONSTRAINT positive CHECK (VALUE > 0);
			CREATE TYPE type_inventory.mood AS ENUM ('sad', 'ok', 'happy');
			CREATE TYPE type_inventory.pair AS (
				amount type_inventory.positive_integer,
				label TEXT COLLATE "C"
			);
			CREATE TYPE type_inventory.float_range AS RANGE (
				SUBTYPE = FLOAT8,
				SUBTYPE_DIFF = float8mi
			);
			CREATE COLLATION type_inventory.c_copy FROM "C";
			CREATE FUNCTION type_inventory.integer_equal(left_value INTEGER, right_value INTEGER)
				RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE
				RETURN left_value = right_value;
			CREATE OPERATOR type_inventory.=== (
				LEFTARG = INTEGER,
				RIGHTARG = INTEGER,
				FUNCTION = type_inventory.integer_equal
			);
			CREATE SEQUENCE type_inventory.standalone_sequence
				AS BIGINT START WITH 12 INCREMENT BY 3 CACHE 7 CYCLE;
		`)
		require.NoError(t, err)

		snapshot, err := GetSchemaSnapshot(t.Context(), db.ConnPool)
		require.NoError(t, err)
		inventory := snapshot.Inventory
		domain := catalogTypeByName(t, inventory, "type_inventory", "positive_integer")
		assert.Equal(t, CatalogTypeKindDomain, domain.Kind)
		assert.True(t, domain.IsNotNull)
		assert.Equal(t, "1", domain.DefaultValue)
		domainConstraint := catalogDomainConstraint(t, inventory, domain.OID, "positive")
		assert.Contains(t, domainConstraint.Definition, "VALUE > 0")

		enumType := catalogTypeByName(t, inventory, "type_inventory", "mood")
		assert.Equal(t, CatalogTypeKindEnum, enumType.Kind)
		assert.Equal(t, []string{"sad", "ok", "happy"},
			catalogEnumLabelNames(inventory, enumType.OID))
		assertCatalogTypeSupportRole(t, inventory, enumType.OID, "input", "enum_in")
		assertCatalogTypeSupportRole(t, inventory, enumType.OID, "output", "enum_out")

		composite := catalogTypeByName(t, inventory, "type_inventory", "pair")
		assert.Equal(t, CatalogTypeKindComposite, composite.Kind)
		attributes := catalogCompositeAttributes(inventory, composite.OID)
		require.Len(t, attributes, 2)
		assert.Equal(t, domain.OID, attributes[0].AttributeTypeOID)
		assert.NotZero(t, attributes[1].CollationOID)

		rangeType := catalogTypeByName(t, inventory, "type_inventory", "float_range")
		assert.Equal(t, CatalogTypeKindRange, rangeType.Kind)
		rangeData := catalogRange(t, inventory, rangeType.OID)
		assert.NotZero(t, rangeData.MultirangeTypeOID)
		assert.Equal(t, CatalogTypeKindMultirange,
			catalogTypeByOID(t, inventory, rangeData.MultirangeTypeOID).Kind)
		assert.NotZero(t, rangeData.SubtypeDiffFunctionOID)
		assertCatalogTypeSupportRole(t, inventory, rangeType.OID,
			"range_subtype_diff", "float8mi")

		collation := catalogCollationByName(t, inventory, "type_inventory", "c_copy")
		assert.Equal(t, "c", collation.Provider)
		assert.True(t, collation.IsDeterministic)
		operator := catalogOperatorByName(t, inventory, "type_inventory", "===")
		assert.NotZero(t, operator.FunctionOID)
		assert.Equal(t, "integer", operator.LeftType)
		assert.Equal(t, "integer", operator.RightType)
		assert.Equal(t, "boolean", operator.ResultType)

		sequence := catalogSequenceByName(t, inventory, "type_inventory", "standalone_sequence")
		assert.Equal(t, int64(12), sequence.StartValue)
		assert.Equal(t, int64(3), sequence.IncrementValue)
		assert.Equal(t, int64(7), sequence.CacheSize)
		assert.True(t, sequence.IsCycle)
		for _, ownership := range inventory.OwnedSequences {
			assert.NotEqual(t, sequence.OID, ownership.SequenceOID)
		}
	})

	t.Run("extensions, event triggers, and publications", func(t *testing.T) {
		db := factory.CreateDatabase(t)
		_, err := db.ConnPool.Exec(t.Context(), `
			CREATE EXTENSION pg_trgm;
			CREATE SCHEMA extension_inventory;
			CREATE TABLE extension_inventory.hidden_root (id INTEGER)
				PARTITION BY RANGE (id);
			CREATE TABLE extension_inventory.hidden_child
				PARTITION OF extension_inventory.hidden_root
				FOR VALUES FROM (0) TO (100);
			CREATE FUNCTION extension_inventory.member_function(value INTEGER)
				RETURNS INTEGER LANGUAGE SQL IMMUTABLE RETURN value;
			ALTER EXTENSION pg_trgm ADD TABLE extension_inventory.hidden_root;
			ALTER EXTENSION pg_trgm ADD TABLE extension_inventory.hidden_child;
			ALTER EXTENSION pg_trgm ADD FUNCTION extension_inventory.member_function(INTEGER);

			CREATE SCHEMA publication_inventory;
			CREATE TABLE publication_inventory.events (id INTEGER, payload TEXT);
			CREATE PUBLICATION explicit_publication
				FOR TABLE publication_inventory.events (id) WHERE (id > 0)
				WITH (publish_via_partition_root = true);
			CREATE PUBLICATION schema_publication
				FOR TABLES IN SCHEMA publication_inventory;
			CREATE PUBLICATION all_tables_publication FOR ALL TABLES;

			CREATE FUNCTION public.inventory_event_trigger()
				RETURNS event_trigger LANGUAGE plpgsql AS $function$
			BEGIN
			END
			$function$;
			CREATE EVENT TRIGGER enabled_inventory_trigger ON ddl_command_end
				WHEN TAG IN ('CREATE TABLE')
				EXECUTE FUNCTION public.inventory_event_trigger();
			CREATE EVENT TRIGGER disabled_inventory_trigger ON ddl_command_end
				EXECUTE FUNCTION public.inventory_event_trigger();
			ALTER EVENT TRIGGER disabled_inventory_trigger DISABLE;
		`)
		require.NoError(t, err)

		snapshot, err := GetSchemaSnapshot(t.Context(), db.ConnPool,
			WithIncludeSchemaPatterns("extension_inventory", "publication_inventory"))
		require.NoError(t, err)
		inventory := snapshot.Inventory
		extension := catalogExtensionIdentityByName(t, inventory, "pg_trgm")
		assert.NotZero(t, extension.OID)
		assert.NotEmpty(t, extension.Version)
		root := catalogRelationByName(t, inventory, "extension_inventory", "hidden_root")
		child := catalogRelationByName(t, inventory, "extension_inventory", "hidden_child")
		require.NotNil(t, root.Extension)
		require.NotNil(t, child.Extension)
		assert.Equal(t, extension.OID, root.Extension.OID)
		assert.Equal(t, extension.OID, child.Extension.OID)
		assertCatalogExtensionMember(t, inventory, extension.OID, root.OID, "table")
		assertCatalogExtensionMember(t, inventory, extension.OID, child.OID, "table")
		memberRoutine := catalogRoutineByName(t, inventory, "extension_inventory", "member_function")
		assertCatalogExtensionMember(t, inventory, extension.OID, memberRoutine.OID, "function")
		for _, table := range snapshot.Schema.Tables {
			assert.NotEqual(t, "hidden_root", table.EscapedName)
			assert.NotEqual(t, "hidden_child", table.EscapedName)
		}

		explicit := catalogPublicationByName(t, inventory, "explicit_publication")
		assert.False(t, explicit.PublishesAllTables)
		assert.True(t, explicit.PublishesViaPartitionRoot)
		relationMembership := catalogPublicationRelation(t, inventory, explicit.OID,
			catalogRelationByName(t, inventory, "publication_inventory", "events").OID)
		assert.Equal(t, []int16{1}, relationMembership.ColumnNumbers)
		assert.Equal(t, []string{"id"}, relationMembership.ColumnNames)
		assert.Contains(t, relationMembership.RowFilter, "id > 0")
		schemaPublication := catalogPublicationByName(t, inventory, "schema_publication")
		assert.Equal(t, "publication_inventory",
			catalogPublicationSchema(t, inventory, schemaPublication.OID).SchemaName)
		allTables := catalogPublicationByName(t, inventory, "all_tables_publication")
		assert.True(t, allTables.PublishesAllTables)

		enabled := catalogEventTriggerByName(t, inventory, "enabled_inventory_trigger")
		assert.Equal(t, "O", enabled.EnabledMode)
		assert.Equal(t, []string{"CREATE TABLE"}, enabled.Tags)
		disabled := catalogEventTriggerByName(t, inventory, "disabled_inventory_trigger")
		assert.Equal(t, "D", disabled.EnabledMode)
	})
}

func hasIncomingCatalogDependency(inventory CatalogInventory, objectOID uint32, dependentSchema string) bool {
	_, found := catalogIncomingDependency(inventory, objectOID, dependentSchema)
	return found
}

func catalogIncomingDependency(
	inventory CatalogInventory,
	objectOID uint32,
	dependentSchema string,
) (CatalogDependency, bool) {
	for _, dependency := range inventory.Dependencies {
		if dependency.Referenced.ObjectOID == objectOID &&
			dependency.Dependent.SchemaName == dependentSchema {
			return dependency, true
		}
	}
	return CatalogDependency{}, false
}

func hasSharedCatalogDependency(inventory CatalogInventory) bool {
	for _, dependency := range inventory.Dependencies {
		if dependency.IsShared && dependency.Dependent.ClassOID != 0 &&
			dependency.Referenced.ClassOID != 0 && dependency.Type != "" {
			return true
		}
	}
	return false
}

func hasCatalogDependency(inventory CatalogInventory, dependentOID, referencedOID uint32) bool {
	for _, dependency := range inventory.Dependencies {
		if dependency.Dependent.ObjectOID == dependentOID &&
			dependency.Referenced.ObjectOID == referencedOID {
			return true
		}
	}
	return false
}

func catalogViewByName(t *testing.T, inventory CatalogInventory, schemaName, name string) CatalogView {
	t.Helper()
	for _, view := range inventory.Views {
		if view.SchemaName == schemaName && view.Name == name {
			return view
		}
	}
	require.FailNow(t, "catalog view not found", "view: %s.%s", schemaName, name)
	return CatalogView{}
}

func catalogRoutineByName(t *testing.T, inventory CatalogInventory, schemaName, name string) CatalogRoutine {
	t.Helper()
	for _, routine := range inventory.Routines {
		if routine.SchemaName == schemaName && routine.Name == name {
			return routine
		}
	}
	require.FailNow(t, "catalog routine not found", "routine: %s.%s", schemaName, name)
	return CatalogRoutine{}
}

func catalogTypeByName(t *testing.T, inventory CatalogInventory, schemaName, name string) CatalogType {
	t.Helper()
	for _, catalogType := range inventory.Types {
		if catalogType.SchemaName == schemaName && catalogType.Name == name {
			return catalogType
		}
	}
	require.FailNow(t, "catalog type not found", "type: %s.%s", schemaName, name)
	return CatalogType{}
}

func catalogCompositeAttributes(inventory CatalogInventory, typeOID uint32) []CatalogCompositeAttribute {
	var attributes []CatalogCompositeAttribute
	for _, attribute := range inventory.CompositeAttributes {
		if attribute.TypeOID == typeOID {
			attributes = append(attributes, attribute)
		}
	}
	return attributes
}

func catalogForeignKey(t *testing.T, inventory CatalogInventory, relationOID uint32, name string) CatalogForeignKey {
	t.Helper()
	for _, foreignKey := range inventory.ForeignKeys {
		if foreignKey.OwningRelationOID == relationOID && foreignKey.Name == name {
			return foreignKey
		}
	}
	require.FailNow(t, "catalog foreign key not found", "relation OID: %d, name: %s", relationOID, name)
	return CatalogForeignKey{}
}

func catalogDomainConstraint(t *testing.T, inventory CatalogInventory, typeOID uint32, name string) CatalogDomainConstraint {
	t.Helper()
	for _, constraint := range inventory.DomainConstraints {
		if constraint.TypeOID == typeOID && constraint.Name == name {
			return constraint
		}
	}
	require.FailNow(t, "catalog domain constraint not found", "type OID: %d, name: %s", typeOID, name)
	return CatalogDomainConstraint{}
}

func catalogEnumLabelNames(inventory CatalogInventory, typeOID uint32) []string {
	var labels []string
	for _, label := range inventory.EnumLabels {
		if label.TypeOID == typeOID {
			labels = append(labels, label.Label)
		}
	}
	return labels
}

func assertCatalogTypeSupportRole(
	t *testing.T,
	inventory CatalogInventory,
	typeOID uint32,
	role string,
	functionName string,
) {
	t.Helper()
	for _, function := range inventory.TypeSupportFunctions {
		if function.TypeOID == typeOID && function.Role == role {
			assert.Equal(t, functionName, function.FunctionName)
			assert.NotZero(t, function.FunctionOID)
			return
		}
	}
	require.FailNow(t, "catalog type support function not found", "type OID: %d, role: %s", typeOID, role)
}

func catalogRange(t *testing.T, inventory CatalogInventory, typeOID uint32) CatalogRange {
	t.Helper()
	for _, rangeData := range inventory.Ranges {
		if rangeData.TypeOID == typeOID {
			return rangeData
		}
	}
	require.FailNow(t, "catalog range not found", "type OID: %d", typeOID)
	return CatalogRange{}
}

func catalogCollationByName(t *testing.T, inventory CatalogInventory, schemaName, name string) CatalogCollation {
	t.Helper()
	for _, collation := range inventory.Collations {
		if collation.SchemaName == schemaName && collation.Name == name {
			return collation
		}
	}
	require.FailNow(t, "catalog collation not found", "collation: %s.%s", schemaName, name)
	return CatalogCollation{}
}

func catalogOperatorByName(t *testing.T, inventory CatalogInventory, schemaName, name string) CatalogOperator {
	t.Helper()
	for _, operator := range inventory.Operators {
		if operator.SchemaName == schemaName && operator.Name == name {
			return operator
		}
	}
	require.FailNow(t, "catalog operator not found", "operator: %s.%s", schemaName, name)
	return CatalogOperator{}
}

func catalogExtensionIdentityByName(t *testing.T, inventory CatalogInventory, name string) CatalogExtensionIdentity {
	t.Helper()
	for _, extension := range inventory.Extensions {
		if extension.Name == name {
			return extension
		}
	}
	require.FailNow(t, "catalog extension not found", "extension: %s", name)
	return CatalogExtensionIdentity{}
}

func assertCatalogExtensionMember(
	t *testing.T,
	inventory CatalogInventory,
	extensionOID uint32,
	objectOID uint32,
	objectType string,
) {
	t.Helper()
	for _, member := range inventory.ExtensionMembers {
		if member.ExtensionOID == extensionOID && member.Object.ObjectOID == objectOID {
			assert.Equal(t, objectType, member.Object.ObjectType)
			return
		}
	}
	require.FailNow(t, "catalog extension member not found",
		"extension OID: %d, object OID: %d", extensionOID, objectOID)
}

func catalogPublicationByName(t *testing.T, inventory CatalogInventory, name string) CatalogPublication {
	t.Helper()
	for _, publication := range inventory.Publications {
		if publication.Name == name {
			return publication
		}
	}
	require.FailNow(t, "catalog publication not found", "publication: %s", name)
	return CatalogPublication{}
}

func catalogPublicationRelation(
	t *testing.T,
	inventory CatalogInventory,
	publicationOID uint32,
	relationOID uint32,
) CatalogPublicationRelation {
	t.Helper()
	for _, membership := range inventory.PublicationRelations {
		if membership.PublicationOID == publicationOID && membership.RelationOID == relationOID {
			return membership
		}
	}
	require.FailNow(t, "catalog publication relation not found",
		"publication OID: %d, relation OID: %d", publicationOID, relationOID)
	return CatalogPublicationRelation{}
}

func catalogPublicationSchema(t *testing.T, inventory CatalogInventory, publicationOID uint32) CatalogPublicationSchema {
	t.Helper()
	for _, membership := range inventory.PublicationSchemas {
		if membership.PublicationOID == publicationOID {
			return membership
		}
	}
	require.FailNow(t, "catalog publication schema not found", "publication OID: %d", publicationOID)
	return CatalogPublicationSchema{}
}

func catalogEventTriggerByName(t *testing.T, inventory CatalogInventory, name string) CatalogEventTrigger {
	t.Helper()
	for _, eventTrigger := range inventory.EventTriggers {
		if eventTrigger.Name == name {
			return eventTrigger
		}
	}
	require.FailNow(t, "catalog event trigger not found", "event trigger: %s", name)
	return CatalogEventTrigger{}
}
