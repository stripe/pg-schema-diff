package diff

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

func TestSourceSafetyPreflightManagedDependentIntent(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		configure      func(*schema.SchemaSnapshot, *schema.SchemaSnapshot)
		expectedKind   sourceSafetyIncomingDependencyKind
		expectedScope  sourceSafetyManagedScope
		expectedIntent sourceSafetyTargetIntent
		expectError    string
	}{
		{
			name: "managed view explicitly deleted",
			configure: func(current, _ *schema.SchemaSnapshot) {
				addSourceSafetyView(current, schema.RelKindView, true)
			},
			expectedKind:  sourceSafetyIncomingDependencyKindView,
			expectedScope: sourceSafetyManagedScopeManaged, expectedIntent: sourceSafetyTargetIntentExplicitlyAbsent,
		},
		{
			name: "managed view persists",
			configure: func(current, target *schema.SchemaSnapshot) {
				addSourceSafetyView(current, schema.RelKindView, true)
				addSourceSafetyModeledView(target, schema.RelKindView)
			},
			expectError: "persistent view",
		},
		{
			name: "excluded view persists",
			configure: func(current, _ *schema.SchemaSnapshot) {
				addSourceSafetyView(current, schema.RelKindView, false)
			},
			expectError: "persistent view",
		},
		{
			name: "excluded materialized view persists",
			configure: func(current, _ *schema.SchemaSnapshot) {
				addSourceSafetyView(current, schema.RelKindMaterializedView, false)
			},
			expectError: "persistent materialized_view",
		},
		{
			name: "managed rule deleted with owning table",
			configure: func(current, _ *schema.SchemaSnapshot) {
				addSourceSafetyRule(current, true)
			},
			expectedKind:  sourceSafetyIncomingDependencyKindRule,
			expectedScope: sourceSafetyManagedScopeManaged, expectedIntent: sourceSafetyTargetIntentExplicitlyAbsent,
		},
		{
			name: "excluded rule persists",
			configure: func(current, _ *schema.SchemaSnapshot) {
				addSourceSafetyRule(current, false)
			},
			expectError: "persistent rule",
		},
		{
			name: "managed row type consumer explicitly deleted",
			configure: func(current, _ *schema.SchemaSnapshot) {
				addSourceSafetyRowConsumer(current, true)
			},
			expectedKind:  sourceSafetyIncomingDependencyKindRowTypeConsumer,
			expectedScope: sourceSafetyManagedScopeManaged, expectedIntent: sourceSafetyTargetIntentExplicitlyAbsent,
		},
		{
			name: "excluded row type consumer persists",
			configure: func(current, _ *schema.SchemaSnapshot) {
				addSourceSafetyRowConsumer(current, false)
			},
			expectError: "persistent row_type_consumer",
		},
		{
			name: "managed catalog trackable routine explicitly deleted",
			configure: func(current, _ *schema.SchemaSnapshot) {
				addSourceSafetyRoutine(current, true,
					schema.CatalogRoutineReferenceTrackabilityCatalogTrackable)
			},
			expectedKind:  sourceSafetyIncomingDependencyKindRoutine,
			expectedScope: sourceSafetyManagedScopeManaged, expectedIntent: sourceSafetyTargetIntentExplicitlyAbsent,
		},
		{
			name: "catalog trackable routine persists",
			configure: func(current, target *schema.SchemaSnapshot) {
				addSourceSafetyRoutine(current, true,
					schema.CatalogRoutineReferenceTrackabilityCatalogTrackable)
				addSourceSafetyModeledRoutine(target)
			},
			expectError: "persistent routine",
		},
		{
			name: "excluded catalog trackable routine persists",
			configure: func(current, _ *schema.SchemaSnapshot) {
				addSourceSafetyRoutine(current, false,
					schema.CatalogRoutineReferenceTrackabilityCatalogTrackable)
			},
			expectError: "persistent routine",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			current, target := sourceSafetyBaseSnapshots()
			tc.configure(&current, &target)

			result, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
				CurrentSnapshot: current, TargetSnapshot: target,
				ProposedTableRelationOIDs: []uint32{10},
			})
			if tc.expectError != "" {
				require.ErrorContains(t, err, tc.expectError)
				assert.ErrorContains(t, err, "managed.archived")
				return
			}
			require.NoError(t, err)
			require.Len(t, result.IncomingDependencies, 1)
			assert.Equal(t, tc.expectedKind, result.IncomingDependencies[0].Dependent.Kind)
			assert.Equal(t, tc.expectedScope, result.IncomingDependencies[0].ManagedScope)
			assert.Equal(t, tc.expectedIntent, result.IncomingDependencies[0].TargetIntent)
			assert.Equal(t, sourceSafetyDependencyDispositionNormalDeletion,
				result.IncomingDependencies[0].Disposition)
			if tc.expectedKind == sourceSafetyIncomingDependencyKindRoutine {
				assert.Equal(t, schema.CatalogRoutineBodyFormSQLStandard,
					result.IncomingDependencies[0].Dependent.RoutineBodyForm)
				assert.Equal(t, schema.CatalogRoutineReferenceTrackabilityCatalogTrackable,
					result.IncomingDependencies[0].Dependent.RoutineReferenceTrackability)
			}
		})
	}
}

func TestSourceSafetyPreflightUntrackableRoutinesFailClosed(t *testing.T) {
	t.Parallel()

	for _, bodyForm := range []schema.CatalogRoutineBodyForm{
		schema.CatalogRoutineBodyFormSQLString,
		schema.CatalogRoutineBodyFormPLPGSQL,
		schema.CatalogRoutineBodyFormOther,
	} {
		t.Run(string(bodyForm), func(t *testing.T) {
			t.Parallel()
			current, target := sourceSafetyBaseSnapshots()
			addSourceSafetyRoutine(&current, false,
				schema.CatalogRoutineReferenceTrackabilityUntrackable)
			current.Inventory.Routines[0].BodyForm = bodyForm
			current.Inventory.Dependencies = nil

			_, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
				CurrentSnapshot: current, TargetSnapshot: target,
				ProposedTableRelationOIDs: []uint32{10},
			})
			require.ErrorContains(t, err, "persistent routine")
			require.ErrorContains(t, err, "excluded.routine(integer)")
		})
	}

	t.Run("managed routine explicitly deleted", func(t *testing.T) {
		t.Parallel()
		current, target := sourceSafetyBaseSnapshots()
		addSourceSafetyRoutine(&current, true,
			schema.CatalogRoutineReferenceTrackabilityUntrackable)
		current.Inventory.Dependencies = nil

		result, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
			CurrentSnapshot: current, TargetSnapshot: target,
			ProposedTableRelationOIDs: []uint32{10},
		})
		require.NoError(t, err)
		require.Len(t, result.IncomingDependencies, 1)
		assert.Equal(t, sourceSafetyTargetIntentExplicitlyAbsent,
			result.IncomingDependencies[0].TargetIntent)
		assert.Equal(t, schema.CatalogRoutineReferenceTrackabilityUntrackable,
			result.IncomingDependencies[0].Dependent.RoutineReferenceTrackability)
	})
}

func TestSourceSafetyPreflightRejectsUnknownDependencyClass(t *testing.T) {
	t.Parallel()

	current, target := sourceSafetyBaseSnapshots()
	current.Inventory.Dependencies = []schema.CatalogDependency{{
		Dependent: schema.CatalogDependencyObject{
			ClassOID: 99999, ObjectOID: 77, ObjectType: "future object", Identity: "future identity",
		},
		Referenced: schema.CatalogDependencyObject{
			ClassOID: pgClassCatalogOID, ObjectOID: 10, ObjectType: "table", Identity: "managed.archived",
		},
		Type: "n",
	}}

	_, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		ProposedTableRelationOIDs: []uint32{10},
	})
	require.ErrorContains(t, err, "unsupported catalog dependency")
	assert.ErrorContains(t, err, "class OID 99999")
}

func TestSourceSafetyPreflightForeignKeyInventory(t *testing.T) {
	t.Parallel()

	current, target := sourceSafetyBaseSnapshots()
	current.Inventory.Relations = append(
		current.Inventory.Relations,
		schema.CatalogRelation{OID: 20, SchemaName: "managed", Name: "owner", Kind: schema.RelKindOrdinaryTable},
		schema.CatalogRelation{OID: 30, SchemaName: "managed", Name: "referenced", Kind: schema.RelKindOrdinaryTable},
	)
	current.Schema.Tables = append(
		current.Schema.Tables,
		sourceSafetyModeledTable("managed", "owner"),
		sourceSafetyModeledTable("managed", "referenced"),
	)
	current.Inventory.ForeignKeys = []schema.CatalogForeignKey{
		{
			OID: 102, Name: "self_fk", OwningRelationOID: 10, OwningSchemaName: "managed",
			OwningRelationName: "archived", ReferencedRelationOID: 10,
		},
		{
			OID: 100, Name: "incoming_fk", OwningRelationOID: 20, OwningSchemaName: "managed",
			OwningRelationName: "owner", ReferencedRelationOID: 10,
		},
		{
			OID: 101, Name: "outgoing_fk", OwningRelationOID: 10, OwningSchemaName: "managed",
			OwningRelationName: "archived", ReferencedRelationOID: 30,
		},
	}
	for _, foreignKey := range current.Inventory.ForeignKeys {
		current.Schema.ForeignKeyConstraints = append(current.Schema.ForeignKeyConstraints,
			sourceSafetyModeledForeignKey(foreignKey))
	}

	result, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		ProposedTableRelationOIDs: []uint32{10},
	})
	require.NoError(t, err)
	require.Len(t, result.ForeignKeys, 3)
	assert.Equal(t, []sourceSafetyForeignKeyDirection{
		sourceSafetyForeignKeyDirectionOutgoing,
		sourceSafetyForeignKeyDirectionSelf,
		sourceSafetyForeignKeyDirectionIncoming,
	}, []sourceSafetyForeignKeyDirection{
		result.ForeignKeys[0].Direction,
		result.ForeignKeys[1].Direction,
		result.ForeignKeys[2].Direction,
	})
	for _, foreignKey := range result.ForeignKeys {
		assert.Equal(t, sourceSafetyDependencyDispositionForeignKey, foreignKey.Disposition)
		assert.Equal(t, sourceSafetyManagedScopeManaged, foreignKey.ManagedScope)
		assert.Equal(t, sourceSafetyTargetIntentExplicitlyAbsent, foreignKey.TargetIntent)
	}
}

func TestSourceSafetyPreflightPlatformBlockers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		configure func(*schema.SchemaSnapshot, *schema.SchemaSnapshot, *sourceSafetyPreflightRequest)
		errorText string
	}{
		{
			name: "extension created",
			configure: func(_, target *schema.SchemaSnapshot, _ *sourceSafetyPreflightRequest) {
				target.Inventory.Extensions = []schema.CatalogExtensionIdentity{{Name: "new_extension", Version: "1"}}
			},
			errorText: "extension \"new_extension\" is created",
		},
		{
			name: "extension dropped",
			configure: func(current, _ *schema.SchemaSnapshot, _ *sourceSafetyPreflightRequest) {
				current.Inventory.Extensions = []schema.CatalogExtensionIdentity{{Name: "old_extension", Version: "1"}}
			},
			errorText: "extension \"old_extension\" is dropped",
		},
		{
			name: "extension version updated",
			configure: func(current, target *schema.SchemaSnapshot, _ *sourceSafetyPreflightRequest) {
				current.Inventory.Extensions = []schema.CatalogExtensionIdentity{{Name: "changed", Version: "1"}}
				target.Inventory.Extensions = []schema.CatalogExtensionIdentity{{Name: "changed", Version: "2"}}
			},
			errorText: "extension \"changed\" is updated",
		},
		{
			name: "extension schema updated",
			configure: func(current, target *schema.SchemaSnapshot, _ *sourceSafetyPreflightRequest) {
				current.Inventory.Extensions = []schema.CatalogExtensionIdentity{
					{Name: "changed", Version: "1", SchemaName: "one"},
				}
				target.Inventory.Extensions = []schema.CatalogExtensionIdentity{
					{Name: "changed", Version: "1", SchemaName: "two"},
				}
			},
			errorText: "extension \"changed\" is updated",
		},
		{
			name: "origin event trigger",
			configure: func(current, _ *schema.SchemaSnapshot, _ *sourceSafetyPreflightRequest) {
				current.Inventory.EventTriggers = []schema.CatalogEventTrigger{{Name: "trigger", EnabledMode: "O"}}
			},
			errorText: "event trigger \"trigger\" is enabled in mode \"O\"",
		},
		{
			name: "replica event trigger",
			configure: func(current, _ *schema.SchemaSnapshot, _ *sourceSafetyPreflightRequest) {
				current.Inventory.EventTriggers = []schema.CatalogEventTrigger{{Name: "trigger", EnabledMode: "R"}}
			},
			errorText: "event trigger \"trigger\" is enabled in mode \"R\"",
		},
		{
			name: "always event trigger",
			configure: func(current, _ *schema.SchemaSnapshot, _ *sourceSafetyPreflightRequest) {
				current.Inventory.EventTriggers = []schema.CatalogEventTrigger{{Name: "trigger", EnabledMode: "A"}}
			},
			errorText: "event trigger \"trigger\" is enabled in mode \"A\"",
		},
		{
			name: "extension member proposed table",
			configure: func(current, _ *schema.SchemaSnapshot, _ *sourceSafetyPreflightRequest) {
				current.Inventory.Relations[0].Extension = &schema.CatalogExtension{
					Name: "member_extension", OID: 5,
				}
			},
			errorText: "extension member table managed.archived",
		},
		{
			name: "extension member retained dependency",
			configure: func(current, _ *schema.SchemaSnapshot, request *sourceSafetyPreflightRequest) {
				address := schema.CatalogDependencyObject{
					ClassOID: pgProcCatalogOID, ObjectOID: 88, ObjectType: "function", Identity: "managed.helper()",
				}
				request.ExpectedRetainedObjects = []sourceSafetyExpectedRetainedObject{{
					TableRelationOID: 10, Address: address,
				}}
				current.Inventory.ExtensionMembers = []schema.CatalogExtensionMember{{
					ExtensionName: "member_extension", Object: address,
				}}
			},
			errorText: "retained object function \"managed.helper()\"",
		},
		{
			name: "all tables publication",
			configure: func(current, _ *schema.SchemaSnapshot, _ *sourceSafetyPreflightRequest) {
				current.Inventory.Publications = []schema.CatalogPublication{
					{Name: "everything", PublishesAllTables: true},
				}
			},
			errorText: "FOR ALL TABLES publication \"everything\"",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			current, target := sourceSafetyBaseSnapshots()
			request := sourceSafetyPreflightRequest{
				CurrentSnapshot: current, TargetSnapshot: target,
				ProposedTableRelationOIDs: []uint32{10},
			}
			tc.configure(&request.CurrentSnapshot, &request.TargetSnapshot, &request)

			_, err := runSourceSafetyPreflight(request)
			require.ErrorContains(t, err, tc.errorText)
			assert.ErrorContains(t, err, "managed.archived")
		})
	}

	t.Run("disabled event trigger is allowed", func(t *testing.T) {
		t.Parallel()
		current, target := sourceSafetyBaseSnapshots()
		current.Inventory.EventTriggers = []schema.CatalogEventTrigger{{Name: "trigger", EnabledMode: "D"}}
		_, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
			CurrentSnapshot: current, TargetSnapshot: target,
			ProposedTableRelationOIDs: []uint32{10},
		})
		require.NoError(t, err)
	})
}

func TestSourceSafetyPreflightRecordsPublicationMemberships(t *testing.T) {
	t.Parallel()

	current, target := sourceSafetyBaseSnapshots()
	current.Inventory.Publications = []schema.CatalogPublication{
		{OID: 1, Name: "explicit"},
		{OID: 2, Name: "schema"},
	}
	current.Inventory.PublicationRelations = []schema.CatalogPublicationRelation{
		{
			OID: 11, PublicationOID: 1, PublicationName: "explicit", RelationOID: 10,
			RelationSchemaName: "managed", RelationName: "archived",
		},
		{
			OID: 12, PublicationOID: 1, PublicationName: "explicit", RelationOID: 99,
			RelationSchemaName: "other", RelationName: "other",
		},
	}
	current.Inventory.PublicationSchemas = []schema.CatalogPublicationSchema{
		{OID: 21, PublicationOID: 2, PublicationName: "schema", SchemaName: "managed"},
		{OID: 22, PublicationOID: 2, PublicationName: "schema", SchemaName: "other"},
	}

	result, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		ProposedTableRelationOIDs: []uint32{10},
	})
	require.NoError(t, err)
	require.Len(t, result.PublicationRelations, 1)
	assert.Equal(t, "explicit", result.PublicationRelations[0].PublicationName)
	require.Len(t, result.PublicationSchemas, 1)
	assert.Equal(t, "schema", result.PublicationSchemas[0].PublicationName)
}

func TestSourceSafetyPreflightRejectsAmbiguousTargetIdentity(t *testing.T) {
	t.Parallel()

	current, target := sourceSafetyBaseSnapshots()
	addSourceSafetyRoutine(&current, true,
		schema.CatalogRoutineReferenceTrackabilityCatalogTrackable)
	addSourceSafetyModeledRoutine(&target)
	addSourceSafetyModeledRoutine(&target)

	_, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		ProposedTableRelationOIDs: []uint32{10},
	})
	require.ErrorContains(t, err, "ambiguous modeled match for routine excluded.routine(integer)")
}

func TestSourceSafetyPreflightIsDeterministicAndDoesNotMutateSnapshots(t *testing.T) {
	t.Parallel()

	current, target := sourceSafetyBaseSnapshots()
	addSourceSafetyView(&current, schema.RelKindView, true)
	addSourceSafetyRoutine(&current, true,
		schema.CatalogRoutineReferenceTrackabilityCatalogTrackable)
	current.Inventory.PublicationRelations = []schema.CatalogPublicationRelation{
		{OID: 2, PublicationName: "z", RelationOID: 10},
		{OID: 1, PublicationName: "a", RelationOID: 10},
	}
	before := current

	first, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		ProposedTableRelationOIDs: []uint32{10},
	})
	require.NoError(t, err)
	assert.Equal(t, before, current)

	slices.Reverse(current.Inventory.Dependencies)
	slices.Reverse(current.Inventory.Views)
	slices.Reverse(current.Inventory.Routines)
	slices.Reverse(current.Inventory.PublicationRelations)
	second, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		ProposedTableRelationOIDs: []uint32{10},
	})
	require.NoError(t, err)
	assert.Equal(t, first, second)
}

func TestValidateNoChangedExtensionOwnsTable(t *testing.T) {
	t.Parallel()

	extension := schema.CatalogExtensionIdentity{Name: "table_owner", Version: "1", SchemaName: "public"}
	ownedRelation := schema.CatalogRelation{
		OID: 10, SchemaName: "hidden", Name: "member", Kind: schema.RelKindOrdinaryTable,
		Extension: &schema.CatalogExtension{Name: extension.Name, OID: 1},
	}
	current := schema.SchemaSnapshot{Inventory: schema.CatalogInventory{
		Extensions: []schema.CatalogExtensionIdentity{extension},
		Relations:  []schema.CatalogRelation{ownedRelation},
	}}

	for _, tc := range []struct {
		name        string
		target      schema.SchemaSnapshot
		expectError bool
	}{
		{name: "drop", target: schema.SchemaSnapshot{}, expectError: true},
		{
			name: "version update",
			target: schema.SchemaSnapshot{Inventory: schema.CatalogInventory{
				Extensions: []schema.CatalogExtensionIdentity{{
					Name: extension.Name, Version: "2", SchemaName: extension.SchemaName,
				}},
			}},
			expectError: true,
		},
		{
			name: "schema update",
			target: schema.SchemaSnapshot{Inventory: schema.CatalogInventory{
				Extensions: []schema.CatalogExtensionIdentity{{
					Name: extension.Name, Version: extension.Version, SchemaName: "other",
				}},
			}},
			expectError: true,
		},
		{
			name: "unchanged",
			target: schema.SchemaSnapshot{Inventory: schema.CatalogInventory{
				Extensions: []schema.CatalogExtensionIdentity{extension},
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateNoChangedExtensionOwnsTable(current, tc.target)
			if tc.expectError {
				require.ErrorContains(t, err, "extension owns table-like relation hidden.member")
			} else {
				require.NoError(t, err)
			}
		})
	}

	t.Run("no table extension change remains allowed", func(t *testing.T) {
		t.Parallel()
		currentWithoutTable := current
		currentWithoutTable.Inventory.Relations = nil
		require.NoError(t, validateNoChangedExtensionOwnsTable(
			currentWithoutTable, schema.SchemaSnapshot{},
		))
	})

	t.Run("partition and foreign relation kinds are protected", func(t *testing.T) {
		t.Parallel()
		for _, kind := range []schema.RelKind{schema.RelKindPartitionedTable, schema.RelKindForeignTable} {
			kindCurrent := current
			kindCurrent.Inventory.Relations = slices.Clone(current.Inventory.Relations)
			kindCurrent.Inventory.Relations[0].Kind = kind
			require.Error(t, validateNoChangedExtensionOwnsTable(kindCurrent, schema.SchemaSnapshot{}))
		}
	})
}

func sourceSafetyBaseSnapshots() (schema.SchemaSnapshot, schema.SchemaSnapshot) {
	current := schema.SchemaSnapshot{
		Schema: schema.Schema{
			NamedSchemas: []schema.NamedSchema{{Name: "managed"}},
			Tables:       []schema.Table{sourceSafetyModeledTable("managed", "archived")},
		},
		Inventory: schema.CatalogInventory{
			Schemas: []schema.CatalogSchema{{OID: 1, Name: "managed"}},
			Relations: []schema.CatalogRelation{{
				OID: 10, SchemaOID: 1, SchemaName: "managed", Name: "archived",
				Kind: schema.RelKindOrdinaryTable,
			}},
			Tables: []schema.CatalogTable{{RelationOID: 10}},
		},
	}
	target := schema.SchemaSnapshot{Schema: schema.Schema{
		NamedSchemas: []schema.NamedSchema{{Name: "managed"}},
	}}
	return current, target
}

func addSourceSafetyView(snapshot *schema.SchemaSnapshot, kind schema.RelKind, managed bool) {
	view := schema.CatalogView{
		RelationOID: 20, SchemaOID: 2, SchemaName: "excluded", Name: "dependent",
		Kind: kind, Definition: "SELECT id FROM managed.archived",
	}
	snapshot.Inventory.Relations = append(snapshot.Inventory.Relations, schema.CatalogRelation{
		OID: 20, SchemaOID: 2, SchemaName: view.SchemaName, Name: view.Name, Kind: kind,
	})
	snapshot.Inventory.Views = append(snapshot.Inventory.Views, view)
	snapshot.Inventory.Rules = append(snapshot.Inventory.Rules, schema.CatalogRule{
		OID: 21, RelationOID: 20, Name: "_RETURN", Definition: view.Definition,
	})
	snapshot.Inventory.Dependencies = append(snapshot.Inventory.Dependencies, schema.CatalogDependency{
		Dependent: schema.CatalogDependencyObject{
			ClassOID: pgRewriteCatalogOID, ObjectOID: 21, ObjectType: "rule",
			SchemaName: view.SchemaName, Name: "_RETURN", Identity: "_RETURN on excluded.dependent",
		},
		Referenced: schema.CatalogDependencyObject{
			ClassOID: pgClassCatalogOID, ObjectOID: 10, SubObjectID: 1,
			ObjectType: "table column", SchemaName: "managed", Name: "archived",
		},
		Type: "n",
	})
	if managed {
		addSourceSafetyModeledView(snapshot, kind)
	}
}

func addSourceSafetyModeledView(snapshot *schema.SchemaSnapshot, kind schema.RelKind) {
	name := schema.SchemaQualifiedName{SchemaName: "excluded", EscapedName: `"dependent"`}
	if kind == schema.RelKindMaterializedView {
		snapshot.Schema.MaterializedViews = append(snapshot.Schema.MaterializedViews, schema.MaterializedView{
			SchemaQualifiedName: name, ViewDefinition: "SELECT id FROM managed.archived",
		})
		return
	}
	snapshot.Schema.Views = append(snapshot.Schema.Views, schema.View{
		SchemaQualifiedName: name, ViewDefinition: "SELECT id FROM managed.archived",
	})
}

func addSourceSafetyRule(snapshot *schema.SchemaSnapshot, managedOwner bool) {
	snapshot.Inventory.Relations = append(snapshot.Inventory.Relations, schema.CatalogRelation{
		OID: 20, SchemaName: "excluded", Name: "rule_owner", Kind: schema.RelKindOrdinaryTable,
	})
	snapshot.Inventory.Rules = append(snapshot.Inventory.Rules, schema.CatalogRule{
		OID: 21, RelationOID: 20, Name: "dependent_rule", Definition: "CREATE RULE dependent_rule ...",
	})
	snapshot.Inventory.Dependencies = append(snapshot.Inventory.Dependencies, schema.CatalogDependency{
		Dependent: schema.CatalogDependencyObject{
			ClassOID: pgRewriteCatalogOID, ObjectOID: 21, ObjectType: "rule",
			SchemaName: "excluded", Name: "dependent_rule", Identity: "dependent_rule on excluded.rule_owner",
		},
		Referenced: schema.CatalogDependencyObject{
			ClassOID: pgClassCatalogOID, ObjectOID: 10, ObjectType: "table", Identity: "managed.archived",
		},
		Type: "n",
	})
	if managedOwner {
		snapshot.Schema.Tables = append(snapshot.Schema.Tables,
			sourceSafetyModeledTable("excluded", "rule_owner"))
	}
}

func addSourceSafetyRowConsumer(snapshot *schema.SchemaSnapshot, managed bool) {
	snapshot.Inventory.Relations = append(snapshot.Inventory.Relations, schema.CatalogRelation{
		OID: 20, SchemaName: "excluded", Name: "row_consumer", Kind: schema.RelKindOrdinaryTable,
	})
	snapshot.Inventory.Dependencies = append(snapshot.Inventory.Dependencies, schema.CatalogDependency{
		Dependent: schema.CatalogDependencyObject{
			ClassOID: pgClassCatalogOID, ObjectOID: 20, SubObjectID: 1,
			ObjectType: "table column", SchemaName: "excluded", Name: "row_consumer",
			Identity: "excluded.row_consumer.archived_row",
		},
		Referenced: schema.CatalogDependencyObject{
			ClassOID: pgClassCatalogOID, ObjectOID: 10, ObjectType: "table", Identity: "managed.archived",
		},
		Type: "n",
	})
	if managed {
		snapshot.Schema.Tables = append(snapshot.Schema.Tables,
			sourceSafetyModeledTable("excluded", "row_consumer"))
	}
}

func addSourceSafetyRoutine(
	snapshot *schema.SchemaSnapshot,
	managed bool,
	trackability schema.CatalogRoutineReferenceTrackability,
) {
	routine := schema.CatalogRoutine{
		OID: 20, SchemaName: "excluded", Name: "routine", Kind: "f", IdentityArguments: "integer",
		Definition: "CREATE FUNCTION excluded.routine(integer) RETURNS integer LANGUAGE SQL RETURN $1",
		BodyForm:   schema.CatalogRoutineBodyFormSQLStandard, ReferenceTrackability: trackability,
	}
	snapshot.Inventory.Routines = append(snapshot.Inventory.Routines, routine)
	snapshot.Inventory.Dependencies = append(snapshot.Inventory.Dependencies, schema.CatalogDependency{
		Dependent: schema.CatalogDependencyObject{
			ClassOID: pgProcCatalogOID, ObjectOID: 20, ObjectType: "function",
			SchemaName: "excluded", Name: "routine", Identity: "excluded.routine(integer)",
		},
		Referenced: schema.CatalogDependencyObject{
			ClassOID: pgClassCatalogOID, ObjectOID: 10, ObjectType: "table", Identity: "managed.archived",
		},
		Type: "n",
	})
	if managed {
		addSourceSafetyModeledRoutine(snapshot)
	}
}

func addSourceSafetyModeledRoutine(snapshot *schema.SchemaSnapshot) {
	snapshot.Schema.Functions = append(snapshot.Schema.Functions, schema.Function{
		SchemaQualifiedName: schema.SchemaQualifiedName{
			SchemaName: "excluded", EscapedName: `"routine"(integer)`,
		},
		FunctionDef: "CREATE FUNCTION excluded.routine(integer) RETURNS integer LANGUAGE SQL RETURN $1",
		Language:    "sql",
	})
}

func sourceSafetyModeledTable(schemaName, name string) schema.Table {
	return schema.Table{SchemaQualifiedName: schema.SchemaQualifiedName{
		SchemaName: schemaName, EscapedName: schema.EscapeIdentifier(name),
	}}
}

func sourceSafetyModeledForeignKey(foreignKey schema.CatalogForeignKey) schema.ForeignKeyConstraint {
	return schema.ForeignKeyConstraint{
		EscapedName: schema.EscapeIdentifier(foreignKey.Name),
		OwningTable: schema.SchemaQualifiedName{
			SchemaName:  foreignKey.OwningSchemaName,
			EscapedName: schema.EscapeIdentifier(foreignKey.OwningRelationName),
		},
	}
}
