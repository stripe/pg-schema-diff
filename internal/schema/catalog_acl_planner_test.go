package schema

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCatalogACLGrantGraphPlansDirectAndPublicRevokes(t *testing.T) {
	object := catalogACLTestObject(0)
	grants := []CatalogACLGrant{
		catalogACLTestGrant(object, 1, 1, 1, false),
		catalogACLTestGrant(object, 1, 1, 2, false),
		catalogACLTestPublicGrant(object, 1, 1),
	}
	graph, err := NewCatalogACLGrantGraph(grants, catalogACLTestRoles(1, 2), nil)
	require.NoError(t, err)

	plan, err := graph.PlanRevokes([]CatalogDependencyObject{object})
	require.NoError(t, err)
	require.Len(t, plan.Revokes, 2)
	assert.Equal(t, CatalogACLRevokeKindPrivilege, plan.Revokes[0].Kind)
	assert.True(t, plan.Revokes[0].Grant.GranteeIsPublic)
	assert.Equal(t, CatalogACLRevokeKindPrivilege, plan.Revokes[1].Kind)
	assert.Equal(t, uint32(2), plan.Revokes[1].Grant.GranteeOID)

	repeated, err := graph.PlanRevokes([]CatalogDependencyObject{object})
	require.NoError(t, err)
	assert.Equal(t, plan, repeated)
}

func TestCatalogACLGrantGraphPlansReverseGrantChain(t *testing.T) {
	object := catalogACLTestObject(0)
	grants := []CatalogACLGrant{
		catalogACLTestGrant(object, 1, 1, 2, true),
		catalogACLTestGrant(object, 1, 2, 3, true),
		catalogACLTestGrant(object, 1, 3, 4, false),
	}
	graph, err := NewCatalogACLGrantGraph(grants, catalogACLTestRoles(1, 2, 3, 4), nil)
	require.NoError(t, err)

	plan, err := graph.PlanRevokes([]CatalogDependencyObject{object})
	require.NoError(t, err)
	require.Len(t, plan.Revokes, 5)
	assert.Equal(t, []struct {
		kind    CatalogACLRevokeKind
		grantor uint32
		grantee uint32
	}{
		{CatalogACLRevokeKindPrivilege, 3, 4},
		{CatalogACLRevokeKindGrantOption, 2, 3},
		{CatalogACLRevokeKindPrivilege, 2, 3},
		{CatalogACLRevokeKindGrantOption, 1, 2},
		{CatalogACLRevokeKindPrivilege, 1, 2},
	}, catalogACLRevokeIdentities(plan))
}

func TestCatalogACLGrantGraphIncludesColumnsAndTableAuthorization(t *testing.T) {
	table := catalogACLTestObject(0)
	column := catalogACLTestObject(2)
	grants := []CatalogACLGrant{
		catalogACLTestGrant(table, 1, 1, 2, true),
		catalogACLTestGrant(column, 1, 2, 3, false),
	}
	graph, err := NewCatalogACLGrantGraph(grants, catalogACLTestRoles(1, 2, 3), nil)
	require.NoError(t, err)

	plan, err := graph.PlanRevokes([]CatalogDependencyObject{table})
	require.NoError(t, err)
	require.Len(t, plan.Revokes, 3)
	assert.Equal(t, int32(2), plan.Revokes[0].Grant.Object.SubObjectID)
	assert.Equal(t, CatalogACLRevokeKindGrantOption, plan.Revokes[1].Kind)
	assert.Zero(t, plan.Revokes[1].Grant.Object.SubObjectID)
}

func TestCatalogACLGrantGraphUsesOnlyInheritedMembership(t *testing.T) {
	object := catalogACLTestObject(0)
	grants := []CatalogACLGrant{
		catalogACLTestGrant(object, 1, 1, 2, true),
		catalogACLTestGrant(object, 1, 3, 4, false),
	}
	roles := catalogACLTestRoles(1, 2, 3, 4)
	membership := CatalogRoleMembership{
		RoleOID: 2, MemberOID: 3, GrantorOID: 1, InheritOption: true, SetOption: true,
	}
	graph, err := NewCatalogACLGrantGraph(grants, roles, []CatalogRoleMembership{membership})
	require.NoError(t, err)
	_, err = graph.PlanRevokes([]CatalogDependencyObject{object})
	require.NoError(t, err)

	membership.InheritOption = false
	graph, err = NewCatalogACLGrantGraph(grants, roles, []CatalogRoleMembership{membership})
	require.NoError(t, err)
	_, err = graph.PlanRevokes([]CatalogDependencyObject{object})
	require.ErrorContains(t, err, "no traceable grant-option chain")
}

func TestCatalogACLGrantGraphFailsClosed(t *testing.T) {
	object := catalogACLTestObject(0)
	testCases := []struct {
		name          string
		grants        []CatalogACLGrant
		roles         []CatalogRole
		memberships   []CatalogRoleMembership
		expectedError string
	}{
		{
			name: "missing owner",
			grants: []CatalogACLGrant{
				catalogACLTestGrant(object, 1, 2, 3, false),
			},
			roles: catalogACLTestRoles(2, 3), expectedError: "missing owner role OID 1",
		},
		{
			name: "missing grantor",
			grants: []CatalogACLGrant{
				catalogACLTestGrant(object, 1, 2, 3, false),
			},
			roles: catalogACLTestRoles(1, 3), expectedError: "missing grantor role OID 2",
		},
		{
			name: "missing grantee",
			grants: []CatalogACLGrant{
				catalogACLTestGrant(object, 1, 1, 3, false),
			},
			roles: catalogACLTestRoles(1), expectedError: "missing grantee role OID 3",
		},
		{
			name: "unsupported privilege",
			grants: []CatalogACLGrant{
				func() CatalogACLGrant {
					grant := catalogACLTestGrant(object, 1, 1, 2, false)
					grant.Privilege = "CONNECT"
					return grant
				}(),
			},
			roles: catalogACLTestRoles(1, 2), expectedError: "unsupported privilege",
		},
		{
			name: "unsupported object class",
			grants: []CatalogACLGrant{
				func() CatalogACLGrant {
					grant := catalogACLTestGrant(object, 1, 1, 2, false)
					grant.ObjectClass = "database"
					return grant
				}(),
			},
			roles: catalogACLTestRoles(1, 2), expectedError: "unsupported object class",
		},
		{
			name: "PUBLIC grant option",
			grants: []CatalogACLGrant{
				func() CatalogACLGrant {
					grant := catalogACLTestPublicGrant(object, 1, 1)
					grant.IsGrantable = true
					return grant
				}(),
			},
			roles: catalogACLTestRoles(1), expectedError: "grant option to PUBLIC",
		},
		{
			name: "untraceable chain",
			grants: []CatalogACLGrant{
				catalogACLTestGrant(object, 1, 2, 3, false),
			},
			roles: catalogACLTestRoles(1, 2, 3), expectedError: "no traceable grant-option chain",
		},
		{
			name: "grant cycle",
			grants: []CatalogACLGrant{
				catalogACLTestGrant(object, 1, 2, 3, true),
				catalogACLTestGrant(object, 1, 3, 2, true),
			},
			roles: catalogACLTestRoles(1, 2, 3), expectedError: "authorization cycle",
		},
		{
			name: "ambiguous grant chain",
			grants: []CatalogACLGrant{
				catalogACLTestGrant(object, 1, 1, 2, true),
				catalogACLTestGrant(object, 1, 1, 3, true),
				catalogACLTestGrant(object, 1, 2, 3, true),
				catalogACLTestGrant(object, 1, 3, 4, false),
			},
			roles: catalogACLTestRoles(1, 2, 3, 4), expectedError: "ambiguous authorization chain",
		},
		{
			name:   "membership cycle",
			roles:  catalogACLTestRoles(1, 2, 3),
			grants: []CatalogACLGrant{catalogACLTestGrant(object, 1, 1, 2, false)},
			memberships: []CatalogRoleMembership{
				{RoleOID: 2, MemberOID: 3, GrantorOID: 1, InheritOption: true},
				{RoleOID: 3, MemberOID: 2, GrantorOID: 1, InheritOption: true},
			},
			expectedError: "membership cycle",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			graph, err := NewCatalogACLGrantGraph(testCase.grants, testCase.roles, testCase.memberships)
			if err == nil {
				_, err = graph.PlanRevokes([]CatalogDependencyObject{object})
			}
			require.ErrorContains(t, err, testCase.expectedError)
		})
	}
}

func catalogACLTestObject(subObjectID int32) CatalogDependencyObject {
	return CatalogDependencyObject{
		ClassOID: 10, ObjectOID: 20, SubObjectID: subObjectID,
		ObjectType: "table", SchemaName: "public", Name: "accounts", Identity: "public.accounts",
	}
}

func catalogACLTestGrant(
	object CatalogDependencyObject,
	ownerOID, grantorOID, granteeOID uint32,
	isGrantable bool,
) CatalogACLGrant {
	return CatalogACLGrant{
		ObjectClass: CatalogACLObjectClassTable, Object: object,
		OwnerOID: ownerOID, OwnerName: catalogACLTestRoleName(ownerOID),
		GrantorOID: grantorOID, GrantorName: catalogACLTestRoleName(grantorOID),
		GranteeOID: granteeOID, GranteeName: catalogACLTestRoleName(granteeOID),
		Privilege: "SELECT", IsGrantable: isGrantable,
	}
}

func catalogACLTestPublicGrant(
	object CatalogDependencyObject,
	ownerOID, grantorOID uint32,
) CatalogACLGrant {
	grant := catalogACLTestGrant(object, ownerOID, grantorOID, 0, false)
	grant.GranteeName = CatalogPublicRoleName
	grant.GranteeIsPublic = true
	return grant
}

func catalogACLTestRoles(oids ...uint32) []CatalogRole {
	roles := make([]CatalogRole, len(oids))
	for idx, oid := range oids {
		roles[idx] = CatalogRole{OID: oid, Name: catalogACLTestRoleName(oid), InheritsPrivileges: true}
	}
	return roles
}

func catalogACLTestRoleName(oid uint32) string {
	return "role_" + strconv.FormatUint(uint64(oid), 10)
}

func catalogACLRevokeIdentities(plan CatalogACLRevokePlan) []struct {
	kind    CatalogACLRevokeKind
	grantor uint32
	grantee uint32
} {
	result := make([]struct {
		kind    CatalogACLRevokeKind
		grantor uint32
		grantee uint32
	}, len(plan.Revokes))
	for idx, revoke := range plan.Revokes {
		result[idx] = struct {
			kind    CatalogACLRevokeKind
			grantor uint32
			grantee uint32
		}{revoke.Kind, revoke.Grant.GrantorOID, revoke.Grant.GranteeOID}
	}
	return result
}
