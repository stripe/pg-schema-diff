package schema

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildSchemaSnapshotHashV1FixedVector(t *testing.T) {
	t.Parallel()

	snapshot := SchemaSnapshot{
		Schema: Schema{
			NamedSchemas: []NamedSchema{{Name: "public"}},
			Tables: []Table{{
				SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: `"accounts"`},
				Columns:             []Column{{Name: "id", Type: "bigint", IsNullable: false}},
				ReplicaIdentity:     ReplicaIdentityDefault,
			}},
		},
		Inventory: CatalogInventory{
			Schemas: []CatalogSchema{{OID: 2200, Name: "public", OwnerOID: 10, OwnerName: "owner"}},
			Relations: []CatalogRelation{{
				OID: 16384, SchemaOID: 2200, SchemaName: "public", Name: "accounts",
				OwnerOID: 10, OwnerName: "owner", Kind: RelKindOrdinaryTable,
				Persistence: RelationPersistencePermanent, RowTypeOID: 16386, ArrayTypeOID: 16385,
			}},
			Types: []CatalogType{{
				OID: 16386, SchemaOID: 2200, SchemaName: "public", Name: "accounts",
				OwnerOID: 10, OwnerName: "owner", Kind: CatalogTypeKindRow, RawKind: "c",
				Category: "C", IsDefined: true, RelationOID: 16384, ArrayTypeOID: 16385,
			}},
			Roles: []CatalogRole{{OID: 10, Name: "owner", InheritsPrivileges: true, CanLogin: true}},
		},
	}

	hash, err := BuildSchemaSnapshotHashV1(snapshot, nil)
	require.NoError(t, err)
	assert.Equal(t, "pg-schema-diff:snapshot:v1:sha256:"+
		"3af1b7820b9fd2b3181e47cfb3cd539cbe313e28b46b16f9be6af2d3e7406858", hash)
	require.NoError(t, ValidateSchemaSnapshotHashV1(hash))
}

func TestValidateSchemaSnapshotHashV1StrictFormat(t *testing.T) {
	t.Parallel()

	valid := SchemaSnapshotHashV1Prefix + strings.Repeat("a", 64)
	require.NoError(t, ValidateSchemaSnapshotHashV1(valid))
	for _, malformed := range []string{
		strings.Repeat("a", 64),
		"pg-schema-diff:snapshot:v2:sha256:" + strings.Repeat("a", 64),
		"pg-schema-diff:snapshot:v1:sha512:" + strings.Repeat("a", 64),
		SchemaSnapshotHashV1Prefix + strings.Repeat("a", 63),
		SchemaSnapshotHashV1Prefix + strings.Repeat("a", 65),
		SchemaSnapshotHashV1Prefix + strings.Repeat("A", 64),
		SchemaSnapshotHashV1Prefix + strings.Repeat("g", 64),
	} {
		assert.Error(t, ValidateSchemaSnapshotHashV1(malformed), malformed)
	}
}

func TestSchemaSnapshotHashV1NormalizesOrderingAndNilSlices(t *testing.T) {
	t.Parallel()

	snapshot, trusted := snapshotHashTestFixture()
	expected, err := BuildSchemaSnapshotHashV1(snapshot, trusted)
	require.NoError(t, err)

	permuted, permutedTrusted := snapshotHashTestFixture()
	reverseTopLevelSlices(reflect.ValueOf(&permuted.Schema).Elem())
	reverseTopLevelSlices(reflect.ValueOf(&permuted.Inventory).Elem())
	slices.Reverse(permutedTrusted)
	for idx := range permutedTrusted {
		slices.Reverse(permutedTrusted[idx].SchemaNames)
	}
	permuted.Schema.Tables[0].CheckConstraints = slices.Clone(
		permuted.Schema.Tables[0].CheckConstraints,
	)
	slices.Reverse(permuted.Schema.Tables[0].CheckConstraints)
	permuted.Inventory.Tables[0].Options = []string{"z", "a"}
	permuted.Inventory.EventTriggers[0].Tags = []string{"table_rewrite", "ddl_command_end"}
	permuted.Inventory.ExtendedStatistics[0].Kinds = []string{"m", "d"}
	permuted.Inventory.ExtendedStatistics[0].ColumnNumbers = []int16{2, 1}
	permuted.Inventory.Extensions[0].ConfigurationRelationOIDs = []uint32{11, 10}
	permuted.Inventory.Extensions[0].ConfigurationConditions = []string{"second", "first"}
	permuted.Inventory.PublicationRelations[0].ColumnNumbers = []int16{2, 1}
	permuted.Inventory.PublicationRelations[0].ColumnNames = []string{"payload", "id"}

	actual, err := BuildSchemaSnapshotHashV1(permuted, permutedTrusted)
	require.NoError(t, err)
	assert.Equal(t, expected, actual)

	empty := SchemaSnapshot{}
	normalizedEmpty := SchemaSnapshot{}
	initializeNilSlicesAndMaps(reflect.ValueOf(&normalizedEmpty.Schema).Elem())
	initializeNilSlicesAndMaps(reflect.ValueOf(&normalizedEmpty.Inventory).Elem())
	nilHash, err := BuildSchemaSnapshotHashV1(empty, nil)
	require.NoError(t, err)
	emptyHash, err := BuildSchemaSnapshotHashV1(normalizedEmpty,
		[]SnapshotHashTrustedArchivalGroup{})
	require.NoError(t, err)
	assert.Equal(t, nilHash, emptyHash)
}

func TestSchemaSnapshotHashV1ChangesForSourceSafetyMaterial(t *testing.T) {
	t.Parallel()

	baseSnapshot, baseTrusted := snapshotHashTestFixture()
	baseHash, err := BuildSchemaSnapshotHashV1(baseSnapshot, baseTrusted)
	require.NoError(t, err)

	testCases := []struct {
		name   string
		mutate func(*SchemaSnapshot, *[]SnapshotHashTrustedArchivalGroup)
	}{
		{name: "modeled object", mutate: func(snapshot *SchemaSnapshot,
			_ *[]SnapshotHashTrustedArchivalGroup,
		) {
			snapshot.Schema.Tables[0].Columns[0].Type = "integer"
		}},
		{name: "relation namespace identity", mutate: func(snapshot *SchemaSnapshot,
			_ *[]SnapshotHashTrustedArchivalGroup,
		) {
			snapshot.Inventory.Relations[0].OID++
		}},
		{name: "type namespace identity", mutate: func(snapshot *SchemaSnapshot,
			_ *[]SnapshotHashTrustedArchivalGroup,
		) {
			snapshot.Inventory.Types[0].SchemaName = "renamed"
		}},
		{name: "archival marker comment", mutate: func(snapshot *SchemaSnapshot,
			_ *[]SnapshotHashTrustedArchivalGroup,
		) {
			snapshot.Inventory.Schemas[2].Comment = "marker-v2"
		}},
		{name: "trusted archival group identity", mutate: func(_ *SchemaSnapshot,
			trusted *[]SnapshotHashTrustedArchivalGroup,
		) {
			(*trusted)[0].GroupID = "group-v2"
		}},
		{name: "excluded schema dependency", mutate: func(snapshot *SchemaSnapshot,
			_ *[]SnapshotHashTrustedArchivalGroup,
		) {
			snapshot.Inventory.Dependencies[0].Type = "i"
		}},
		{name: "extension version", mutate: func(snapshot *SchemaSnapshot,
			_ *[]SnapshotHashTrustedArchivalGroup,
		) {
			snapshot.Inventory.Extensions[0].Version = "2.0"
		}},
		{name: "extension membership", mutate: func(snapshot *SchemaSnapshot,
			_ *[]SnapshotHashTrustedArchivalGroup,
		) {
			snapshot.Inventory.ExtensionMembers[0].Object.ObjectOID++
		}},
		{name: "event trigger enabled mode", mutate: func(snapshot *SchemaSnapshot,
			_ *[]SnapshotHashTrustedArchivalGroup,
		) {
			snapshot.Inventory.EventTriggers[0].EnabledMode = "O"
		}},
		{name: "publication definition", mutate: func(snapshot *SchemaSnapshot,
			_ *[]SnapshotHashTrustedArchivalGroup,
		) {
			snapshot.Inventory.Publications[0].PublishesDeletes = false
		}},
		{name: "publication membership", mutate: func(snapshot *SchemaSnapshot,
			_ *[]SnapshotHashTrustedArchivalGroup,
		) {
			snapshot.Inventory.PublicationRelations[0].RowFilter = "id > 10"
		}},
		{name: "acl grant", mutate: func(snapshot *SchemaSnapshot,
			_ *[]SnapshotHashTrustedArchivalGroup,
		) {
			snapshot.Inventory.ACLGrants[0].Privilege = "UPDATE"
		}},
		{name: "default acl", mutate: func(snapshot *SchemaSnapshot,
			_ *[]SnapshotHashTrustedArchivalGroup,
		) {
			snapshot.Inventory.DefaultACLs[0].IsGrantable = true
		}},
		{name: "role", mutate: func(snapshot *SchemaSnapshot,
			_ *[]SnapshotHashTrustedArchivalGroup,
		) {
			snapshot.Inventory.Roles[0].CanLogin = true
		}},
		{name: "role membership", mutate: func(snapshot *SchemaSnapshot,
			_ *[]SnapshotHashTrustedArchivalGroup,
		) {
			snapshot.Inventory.RoleMemberships[0].AdminOption = true
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot, trusted := snapshotHashTestFixture()
			tc.mutate(&snapshot, &trusted)
			actual, err := BuildSchemaSnapshotHashV1(snapshot, trusted)
			require.NoError(t, err)
			assert.NotEqual(t, baseHash, actual)
		})
	}
}

func TestSchemaSnapshotHashV1ExcludesNonSourceState(t *testing.T) {
	t.Parallel()

	snapshot, trusted := snapshotHashTestFixture()
	expected, err := BuildSchemaSnapshotHashV1(snapshot, trusted)
	require.NoError(t, err)
	snapshot.Hash = "legacy-rendering-independent-hash"
	snapshot.UnfilteredSchema = Schema{NamedSchemas: []NamedSchema{{Name: "not-hash-material"}}}
	actual, err := BuildSchemaSnapshotHashV1(snapshot, trusted)
	require.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestSchemaSnapshotHashV1RejectsUnboundTrustMaterial(t *testing.T) {
	t.Parallel()

	snapshot, trusted := snapshotHashTestFixture()
	trusted[0].SchemaNames = []string{"missing"}
	_, err := BuildSchemaSnapshotHashV1(snapshot, trusted)
	require.ErrorContains(t, err, "absent from inventory")

	snapshot, trusted = snapshotHashTestFixture()
	snapshot.Inventory.Schemas[2].Comment = ""
	_, err = BuildSchemaSnapshotHashV1(snapshot, trusted)
	require.ErrorContains(t, err, "has no marker comment")
}

func snapshotHashTestFixture() (SchemaSnapshot, []SnapshotHashTrustedArchivalGroup) {
	tableAddress := CatalogDependencyObject{
		ClassOID: 1259, ObjectOID: 10, ObjectType: "table", SchemaName: "public", Name: "accounts",
		Identity: "public.accounts",
	}
	return SchemaSnapshot{
			Schema: Schema{
				NamedSchemas: []NamedSchema{{Name: "public"}, {Name: "managed"}},
				Tables: []Table{{
					SchemaQualifiedName: SchemaQualifiedName{SchemaName: "public", EscapedName: `"accounts"`},
					Columns:             []Column{{Name: "id", Type: "bigint"}},
					CheckConstraints:    []CheckConstraint{{Name: "z", Expression: "id > 0"}, {Name: "a", Expression: "id < 100"}},
				}},
			},
			Inventory: CatalogInventory{
				Schemas: []CatalogSchema{
					{OID: 1, Name: "public", OwnerOID: 100, OwnerName: "owner"},
					{OID: 2, Name: "excluded", OwnerOID: 100, OwnerName: "owner"},
					{OID: 3, Name: "archive_accounts", OwnerOID: 100, OwnerName: "owner", Comment: "marker-v1"},
				},
				Relations: []CatalogRelation{
					{OID: 10, SchemaOID: 1, SchemaName: "public", Name: "accounts", Kind: RelKindOrdinaryTable},
					{OID: 20, SchemaOID: 2, SchemaName: "excluded", Name: "dependent", Kind: RelKindView},
				},
				Types:  []CatalogType{{OID: 30, SchemaOID: 1, SchemaName: "public", Name: "status", Kind: CatalogTypeKindEnum}},
				Tables: []CatalogTable{{RelationOID: 10, Options: []string{"a", "z"}}},
				ExtendedStatistics: []CatalogExtendedStatistic{{
					OID: 31, SchemaOID: 1, SchemaName: "public", Name: "stats", RelationOID: 10,
					ColumnNumbers: []int16{1, 2}, Kinds: []string{"d", "m"},
				}},
				Dependencies: []CatalogDependency{{
					Dependent: CatalogDependencyObject{
						ClassOID: 2618, ObjectOID: 21, ObjectType: "rule", SchemaName: "excluded",
						Name: "_RETURN", Identity: "_RETURN on excluded.dependent",
					},
					Referenced: tableAddress, Type: "n",
				}},
				Extensions: []CatalogExtensionIdentity{{
					OID: 40, Name: "example", SchemaOID: 1, SchemaName: "public", Version: "1.0",
					ConfigurationRelationOIDs: []uint32{10, 11}, ConfigurationConditions: []string{"first", "second"},
				}},
				ExtensionMembers: []CatalogExtensionMember{{ExtensionOID: 40, ExtensionName: "example", Object: tableAddress}},
				EventTriggers: []CatalogEventTrigger{{
					OID: 50, Name: "audit", Event: "ddl_command_end", EnabledMode: "D",
					Tags: []string{"ddl_command_end", "table_rewrite"},
				}},
				Publications: []CatalogPublication{{
					OID: 60, Name: "pub", PublishesInserts: true, PublishesUpdates: true,
					PublishesDeletes: true, PublishesTruncates: true,
				}},
				PublicationRelations: []CatalogPublicationRelation{{
					OID: 61, PublicationOID: 60, PublicationName: "pub", RelationOID: 10,
					RelationSchemaName: "public", RelationName: "accounts",
					ColumnNumbers: []int16{1, 2}, ColumnNames: []string{"id", "payload"},
				}},
				ACLGrants: []CatalogACLGrant{{
					ObjectClass: CatalogACLObjectClassTable, Object: tableAddress, OwnerOID: 100, OwnerName: "owner",
					GrantorOID: 100, GrantorName: "owner", GranteeOID: 101, GranteeName: "reader", Privilege: "SELECT",
				}},
				DefaultACLs: []CatalogDefaultACL{{
					OID: 70, OwnerOID: 100, OwnerName: "owner", SchemaOID: 1, SchemaName: "public",
					ObjectType: CatalogDefaultACLObjectTypeTable, GrantorOID: 100, GrantorName: "owner",
					GranteeOID: 101, GranteeName: "reader", Privilege: "SELECT",
				}},
				Roles: []CatalogRole{{OID: 100, Name: "owner"}, {OID: 101, Name: "reader", InheritsPrivileges: true}},
				RoleMemberships: []CatalogRoleMembership{{
					RoleOID: 101, RoleName: "reader", MemberOID: 100, MemberName: "owner",
					GrantorOID: 100, GrantorName: "owner", InheritOption: true, SetOption: true,
				}},
			},
		}, []SnapshotHashTrustedArchivalGroup{{
			GroupID:     "group-v1",
			SchemaNames: []string{"archive_accounts"},
		}}
}

func reverseTopLevelSlices(value reflect.Value) {
	for idx := 0; idx < value.NumField(); idx++ {
		field := value.Field(idx)
		if field.Kind() == reflect.Slice {
			swap := reflect.Swapper(field.Interface())
			for left, right := 0, field.Len()-1; left < right; left, right = left+1, right-1 {
				swap(left, right)
			}
		}
	}
}

func initializeNilSlicesAndMaps(value reflect.Value) {
	for idx := 0; idx < value.NumField(); idx++ {
		field := value.Field(idx)
		switch field.Kind() {
		case reflect.Slice:
			if field.IsNil() {
				field.Set(reflect.MakeSlice(field.Type(), 0, 0))
			}
		case reflect.Map:
			if field.IsNil() {
				field.Set(reflect.MakeMap(field.Type()))
			}
		}
	}
}
