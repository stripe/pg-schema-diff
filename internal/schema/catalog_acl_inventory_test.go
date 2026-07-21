package schema

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/testdb"
)

func TestCatalogACLInventoryNormalize(t *testing.T) {
	object := catalogACLTestObject(0)
	inventory := CatalogInventory{
		ACLGrants: []CatalogACLGrant{
			catalogACLTestGrant(object, 1, 2, 3, false),
			catalogACLTestGrant(object, 1, 1, 2, true),
		},
		DefaultACLs: []CatalogDefaultACL{
			{OID: 2, OwnerOID: 2, ObjectType: CatalogDefaultACLObjectTypeTable, Privilege: "SELECT"},
			{OID: 1, OwnerOID: 1, ObjectType: CatalogDefaultACLObjectTypeSchema, Privilege: "USAGE"},
		},
		Roles: []CatalogRole{{OID: 2, Name: "b"}, {OID: 1, Name: "a"}},
		RoleMemberships: []CatalogRoleMembership{
			{RoleOID: 3, MemberOID: 2, GrantorOID: 1},
			{RoleOID: 2, MemberOID: 1, GrantorOID: 1},
		},
	}

	normalized := inventory.Normalize()
	assert.Equal(t, uint32(1), normalized.ACLGrants[0].GrantorOID)
	assert.Equal(t, uint32(1), normalized.DefaultACLs[0].OID)
	assert.Equal(t, uint32(1), normalized.Roles[0].OID)
	assert.Equal(t, uint32(2), normalized.RoleMemberships[0].RoleOID)
	assert.Equal(t, normalized, normalized.Normalize())
	assert.Equal(t, uint32(2), inventory.ACLGrants[0].GrantorOID,
		"normalization must not mutate the input")
}

func TestCatalogACLInventoryDatabaseFixtures(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	roleNames := []string{
		"acl_stage6_owner",
		"acl_stage6_first",
		"acl_stage6_second",
		"acl_stage6_final",
		"acl_stage6_default_grantee",
		"acl_stage6_membership_role",
		"acl_stage6_member",
		"acl_stage6_flags",
	}
	roleGuard := factory.LockRoles(t, roleNames...)
	roleGuard.CreateRoles()

	db := factory.CreateDatabase(t)
	_, err := db.ConnPool.Exec(t.Context(), `
		ALTER ROLE acl_stage6_flags
			NOINHERIT CREATEROLE LOGIN REPLICATION BYPASSRLS;
		GRANT acl_stage6_membership_role TO acl_stage6_member
			WITH ADMIN OPTION, INHERIT FALSE, SET TRUE;

		CREATE SCHEMA acl_stage6_inventory AUTHORIZATION acl_stage6_owner;
		SET ROLE acl_stage6_owner;
		CREATE TABLE acl_stage6_inventory.table_acl (
			id INTEGER,
			payload TEXT
		);
		CREATE TABLE acl_stage6_inventory.partitioned_acl (id INTEGER)
			PARTITION BY RANGE (id);
		CREATE SEQUENCE acl_stage6_inventory.sequence_acl;
		CREATE FUNCTION acl_stage6_inventory.routine_acl()
		RETURNS INTEGER LANGUAGE SQL RETURN 1;
		CREATE TYPE acl_stage6_inventory.type_acl AS ENUM ('one', 'two');

		GRANT USAGE ON SCHEMA acl_stage6_inventory TO acl_stage6_first;
		GRANT USAGE ON SCHEMA acl_stage6_inventory TO acl_stage6_second;
		GRANT SELECT ON acl_stage6_inventory.table_acl TO PUBLIC;
		GRANT SELECT ON acl_stage6_inventory.table_acl
			TO acl_stage6_first WITH GRANT OPTION;
		GRANT SELECT (payload) ON acl_stage6_inventory.table_acl
			TO acl_stage6_first;
		GRANT SELECT ON acl_stage6_inventory.partitioned_acl TO acl_stage6_first;
		GRANT USAGE ON SEQUENCE acl_stage6_inventory.sequence_acl TO acl_stage6_first;
		GRANT EXECUTE ON FUNCTION acl_stage6_inventory.routine_acl()
			TO acl_stage6_first;
		GRANT USAGE ON TYPE acl_stage6_inventory.type_acl TO acl_stage6_first;

		ALTER DEFAULT PRIVILEGES
			GRANT SELECT ON TABLES TO acl_stage6_default_grantee WITH GRANT OPTION;
		ALTER DEFAULT PRIVILEGES IN SCHEMA acl_stage6_inventory
			GRANT INSERT ON TABLES TO acl_stage6_default_grantee;
		ALTER DEFAULT PRIVILEGES IN SCHEMA acl_stage6_inventory
			GRANT USAGE ON SEQUENCES TO acl_stage6_default_grantee;
		ALTER DEFAULT PRIVILEGES IN SCHEMA acl_stage6_inventory
			GRANT EXECUTE ON FUNCTIONS TO acl_stage6_default_grantee;
		ALTER DEFAULT PRIVILEGES IN SCHEMA acl_stage6_inventory
			GRANT USAGE ON TYPES TO acl_stage6_default_grantee;
		ALTER DEFAULT PRIVILEGES
			GRANT USAGE ON SCHEMAS TO acl_stage6_default_grantee;

		SET ROLE acl_stage6_first;
		GRANT SELECT ON acl_stage6_inventory.table_acl
			TO acl_stage6_second WITH GRANT OPTION;
		SET ROLE acl_stage6_second;
		GRANT SELECT ON acl_stage6_inventory.table_acl TO acl_stage6_final;
		RESET ROLE;
	`)
	require.NoError(t, err)

	snapshot, err := GetSchemaSnapshot(t.Context(), db.ConnPool,
		WithIncludeSchemaPatterns("public"))
	require.NoError(t, err)
	assert.Equal(t, snapshot.Inventory, snapshot.Inventory.Normalize())
	for _, namedSchema := range snapshot.Schema.NamedSchemas {
		assert.NotEqual(t, "acl_stage6_inventory", namedSchema.Name)
	}

	roles := make(map[string]CatalogRole)
	for _, roleName := range roleNames {
		roles[roleName] = catalogRoleByName(t, snapshot.Inventory, roleName)
	}
	flags := roles["acl_stage6_flags"]
	assert.False(t, flags.IsSuperuser)
	assert.False(t, flags.InheritsPrivileges)
	assert.True(t, flags.CanCreateRoles)
	assert.True(t, flags.CanLogin)
	assert.True(t, flags.CanReplicate)
	assert.True(t, flags.BypassesRLS)
	databaseRole := catalogRoleByName(t, snapshot.Inventory,
		db.ConnPool.Config().ConnConfig.User)
	assert.True(t, databaseRole.IsSuperuser)
	assert.True(t, databaseRole.InheritsPrivileges)

	membership := catalogRoleMembership(t, snapshot.Inventory,
		roles["acl_stage6_membership_role"].OID, roles["acl_stage6_member"].OID)
	assert.True(t, membership.AdminOption)
	assert.False(t, membership.InheritOption)
	assert.True(t, membership.SetOption)
	assert.NotZero(t, membership.GrantorOID)
	assert.NotEmpty(t, membership.GrantorName)

	schema := catalogSchemaByName(t, snapshot.Inventory, "acl_stage6_inventory")
	table := catalogRelationByName(t, snapshot.Inventory, "acl_stage6_inventory", "table_acl")
	partitioned := catalogRelationByName(t, snapshot.Inventory,
		"acl_stage6_inventory", "partitioned_acl")
	sequence := catalogSequenceByName(t, snapshot.Inventory, "acl_stage6_inventory", "sequence_acl")
	routine := catalogRoutineByName(t, snapshot.Inventory, "acl_stage6_inventory", "routine_acl")
	typeData := catalogTypeByName(t, snapshot.Inventory, "acl_stage6_inventory", "type_acl")

	catalogACLGrant(t, snapshot.Inventory, CatalogACLObjectClassSchema, schema.OID, 0,
		roles["acl_stage6_first"].OID, "USAGE")
	publicGrant := catalogACLGrant(t, snapshot.Inventory, CatalogACLObjectClassTable, table.OID, 0,
		0, "SELECT")
	assert.True(t, publicGrant.GranteeIsPublic)
	assert.Equal(t, CatalogPublicRoleName, publicGrant.GranteeName)
	assert.Equal(t, table.OwnerOID, publicGrant.OwnerOID)

	firstGrant := catalogACLGrant(t, snapshot.Inventory, CatalogACLObjectClassTable, table.OID, 0,
		roles["acl_stage6_first"].OID, "SELECT")
	assert.Equal(t, roles["acl_stage6_owner"].OID, firstGrant.GrantorOID)
	assert.True(t, firstGrant.IsGrantable)
	secondGrant := catalogACLGrant(t, snapshot.Inventory, CatalogACLObjectClassTable, table.OID, 0,
		roles["acl_stage6_second"].OID, "SELECT")
	assert.Equal(t, roles["acl_stage6_first"].OID, secondGrant.GrantorOID)
	assert.True(t, secondGrant.IsGrantable)
	finalGrant := catalogACLGrant(t, snapshot.Inventory, CatalogACLObjectClassTable, table.OID, 0,
		roles["acl_stage6_final"].OID, "SELECT")
	assert.Equal(t, roles["acl_stage6_second"].OID, finalGrant.GrantorOID)
	assert.False(t, finalGrant.IsGrantable)

	columnGrant := catalogACLGrant(t, snapshot.Inventory, CatalogACLObjectClassTable, table.OID, 2,
		roles["acl_stage6_first"].OID, "SELECT")
	assert.Equal(t, "acl_stage6_inventory", columnGrant.Object.SchemaName)
	assert.Contains(t, columnGrant.Object.Identity, "payload")
	catalogACLGrant(t, snapshot.Inventory, CatalogACLObjectClassTable, partitioned.OID, 0,
		roles["acl_stage6_first"].OID, "SELECT")
	catalogACLGrant(t, snapshot.Inventory, CatalogACLObjectClassSequence, sequence.OID, 0,
		roles["acl_stage6_first"].OID, "USAGE")
	catalogACLGrant(t, snapshot.Inventory, CatalogACLObjectClassRoutine, routine.OID, 0,
		roles["acl_stage6_first"].OID, "EXECUTE")
	catalogACLGrant(t, snapshot.Inventory, CatalogACLObjectClassType, typeData.OID, 0,
		roles["acl_stage6_first"].OID, "USAGE")

	globalTableDefault := catalogDefaultACL(t, snapshot.Inventory,
		roles["acl_stage6_owner"].OID, 0, CatalogDefaultACLObjectTypeTable,
		roles["acl_stage6_default_grantee"].OID, "SELECT")
	assert.True(t, globalTableDefault.IsGlobal)
	assert.True(t, globalTableDefault.IsGrantable)
	assert.Equal(t, roles["acl_stage6_owner"].OID, globalTableDefault.GrantorOID)
	assert.Equal(t, "acl_stage6_default_grantee", globalTableDefault.GranteeName)

	for _, expected := range []struct {
		objectType CatalogDefaultACLObjectType
		privilege  string
	}{
		{CatalogDefaultACLObjectTypeTable, "INSERT"},
		{CatalogDefaultACLObjectTypeSequence, "USAGE"},
		{CatalogDefaultACLObjectTypeRoutine, "EXECUTE"},
		{CatalogDefaultACLObjectTypeType, "USAGE"},
	} {
		defaultACL := catalogDefaultACL(t, snapshot.Inventory,
			roles["acl_stage6_owner"].OID, schema.OID, expected.objectType,
			roles["acl_stage6_default_grantee"].OID, expected.privilege)
		assert.False(t, defaultACL.IsGlobal)
		assert.Equal(t, "acl_stage6_inventory", defaultACL.SchemaName)
	}
	globalSchemaDefault := catalogDefaultACL(t, snapshot.Inventory,
		roles["acl_stage6_owner"].OID, 0, CatalogDefaultACLObjectTypeSchema,
		roles["acl_stage6_default_grantee"].OID, "USAGE")
	assert.True(t, globalSchemaDefault.IsGlobal)

	revokePlan, err := snapshot.Inventory.PlanACLRevokes([]CatalogDependencyObject{{
		ClassOID: firstGrant.Object.ClassOID, ObjectOID: table.OID,
	}})
	require.NoError(t, err)
	finalPrivilege := catalogACLRevokeIndex(t, revokePlan, CatalogACLRevokeKindPrivilege,
		roles["acl_stage6_second"].OID, roles["acl_stage6_final"].OID)
	secondGrantOption := catalogACLRevokeIndex(t, revokePlan, CatalogACLRevokeKindGrantOption,
		roles["acl_stage6_first"].OID, roles["acl_stage6_second"].OID)
	firstGrantOption := catalogACLRevokeIndex(t, revokePlan, CatalogACLRevokeKindGrantOption,
		roles["acl_stage6_owner"].OID, roles["acl_stage6_first"].OID)
	assert.Less(t, finalPrivilege, secondGrantOption)
	assert.Less(t, secondGrantOption, firstGrantOption)

	_, err = db.ConnPool.Exec(t.Context(), `
		SET ROLE acl_stage6_owner;
		GRANT UPDATE ON acl_stage6_inventory.table_acl TO acl_stage6_default_grantee;
		RESET ROLE;
	`)
	require.NoError(t, err)
	afterACLChange, err := GetSchemaSnapshot(t.Context(), db.ConnPool,
		WithIncludeSchemaPatterns("public"))
	require.NoError(t, err)
	assert.Equal(t, snapshot.Schema, afterACLChange.Schema)
	assert.Equal(t, snapshot.Hash, afterACLChange.Hash,
		"excluded-schema ACL inventory must not expand the legacy hash")
	assert.NotEqual(t, snapshot.Inventory.ACLGrants, afterACLChange.Inventory.ACLGrants)
	catalogACLGrant(t, afterACLChange.Inventory, CatalogACLObjectClassTable, table.OID, 0,
		roles["acl_stage6_default_grantee"].OID, "UPDATE")
}

func catalogACLGrant(
	t *testing.T,
	inventory CatalogInventory,
	objectClass CatalogACLObjectClass,
	objectOID uint32,
	subObjectID int32,
	granteeOID uint32,
	privilege string,
) CatalogACLGrant {
	t.Helper()
	for _, grant := range inventory.ACLGrants {
		if grant.ObjectClass == objectClass && grant.Object.ObjectOID == objectOID &&
			grant.Object.SubObjectID == subObjectID && grant.GranteeOID == granteeOID &&
			grant.Privilege == privilege {
			return grant
		}
	}
	require.FailNow(t, "catalog ACL grant not found",
		"class: %s, object OID: %d, subobject: %d, grantee OID: %d, privilege: %s",
		objectClass, objectOID, subObjectID, granteeOID, privilege)
	return CatalogACLGrant{}
}

func catalogDefaultACL(
	t *testing.T,
	inventory CatalogInventory,
	ownerOID, schemaOID uint32,
	objectType CatalogDefaultACLObjectType,
	granteeOID uint32,
	privilege string,
) CatalogDefaultACL {
	t.Helper()
	for _, defaultACL := range inventory.DefaultACLs {
		if defaultACL.OwnerOID == ownerOID && defaultACL.SchemaOID == schemaOID &&
			defaultACL.ObjectType == objectType && defaultACL.GranteeOID == granteeOID &&
			defaultACL.Privilege == privilege {
			return defaultACL
		}
	}
	require.FailNow(t, "catalog default ACL not found",
		"owner OID: %d, schema OID: %d, type: %s, grantee OID: %d, privilege: %s",
		ownerOID, schemaOID, objectType, granteeOID, privilege)
	return CatalogDefaultACL{}
}

func catalogRoleByName(t *testing.T, inventory CatalogInventory, name string) CatalogRole {
	t.Helper()
	for _, role := range inventory.Roles {
		if role.Name == name {
			return role
		}
	}
	require.FailNow(t, "catalog role not found", "role: %s", name)
	return CatalogRole{}
}

func catalogRoleMembership(
	t *testing.T,
	inventory CatalogInventory,
	roleOID, memberOID uint32,
) CatalogRoleMembership {
	t.Helper()
	for _, membership := range inventory.RoleMemberships {
		if membership.RoleOID == roleOID && membership.MemberOID == memberOID {
			return membership
		}
	}
	require.FailNow(t, "catalog role membership not found",
		"role OID: %d, member OID: %d", roleOID, memberOID)
	return CatalogRoleMembership{}
}

func catalogACLRevokeIndex(
	t *testing.T,
	plan CatalogACLRevokePlan,
	kind CatalogACLRevokeKind,
	grantorOID, granteeOID uint32,
) int {
	t.Helper()
	idx := slices.IndexFunc(plan.Revokes, func(revoke CatalogACLRevoke) bool {
		return revoke.Kind == kind && revoke.Grant.GrantorOID == grantorOID &&
			revoke.Grant.GranteeOID == granteeOID
	})
	require.NotEqual(t, -1, idx, "revoke kind %s from OID %d to OID %d", kind, grantorOID, granteeOID)
	return idx
}
