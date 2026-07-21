package schema

import (
	"cmp"
	"context"
	"fmt"

	dbsqlc "github.com/stripe/pg-schema-diff/internal/queries"
)

const CatalogPublicRoleName = "PUBLIC"

type CatalogACLObjectClass string

const (
	CatalogACLObjectClassSchema   CatalogACLObjectClass = "schema"
	CatalogACLObjectClassTable    CatalogACLObjectClass = "table"
	CatalogACLObjectClassSequence CatalogACLObjectClass = "sequence"
	CatalogACLObjectClassRoutine  CatalogACLObjectClass = "routine"
	CatalogACLObjectClassType     CatalogACLObjectClass = "type"
)

// CatalogACLGrant preserves one expanded ACL item. Object uses the same catalog
// address representation as dependency inventory; a positive SubObjectID denotes
// a column grant.
type CatalogACLGrant struct {
	ObjectClass     CatalogACLObjectClass
	Object          CatalogDependencyObject
	OwnerOID        uint32
	OwnerName       string
	GrantorOID      uint32
	GrantorName     string
	GranteeOID      uint32
	GranteeName     string
	GranteeIsPublic bool
	Privilege       string
	IsGrantable     bool
}

type CatalogDefaultACLObjectType string

const (
	CatalogDefaultACLObjectTypeSchema   CatalogDefaultACLObjectType = "schema"
	CatalogDefaultACLObjectTypeTable    CatalogDefaultACLObjectType = "table"
	CatalogDefaultACLObjectTypeSequence CatalogDefaultACLObjectType = "sequence"
	CatalogDefaultACLObjectTypeRoutine  CatalogDefaultACLObjectType = "routine"
	CatalogDefaultACLObjectTypeType     CatalogDefaultACLObjectType = "type"
)

type CatalogDefaultACL struct {
	OID             uint32
	OwnerOID        uint32
	OwnerName       string
	SchemaOID       uint32
	SchemaName      string
	IsGlobal        bool
	ObjectType      CatalogDefaultACLObjectType
	GrantorOID      uint32
	GrantorName     string
	GranteeOID      uint32
	GranteeName     string
	GranteeIsPublic bool
	Privilege       string
	IsGrantable     bool
}

type CatalogRole struct {
	OID                uint32
	Name               string
	IsSuperuser        bool
	InheritsPrivileges bool
	CanCreateRoles     bool
	CanLogin           bool
	CanReplicate       bool
	BypassesRLS        bool
}

type CatalogRoleMembership struct {
	RoleOID       uint32
	RoleName      string
	MemberOID     uint32
	MemberName    string
	GrantorOID    uint32
	GrantorName   string
	AdminOption   bool
	InheritOption bool
	SetOption     bool
}

func normalizeCatalogACLInventory(i CatalogInventory) CatalogInventory {
	i.ACLGrants = sortedCatalogRecords(i.ACLGrants, compareCatalogACLGrant)
	i.DefaultACLs = sortedCatalogRecords(i.DefaultACLs, func(a, b CatalogDefaultACL) int {
		return cmp.Or(
			cmp.Compare(a.OwnerOID, b.OwnerOID),
			cmp.Compare(a.SchemaOID, b.SchemaOID),
			cmp.Compare(a.ObjectType, b.ObjectType),
			cmp.Compare(a.Privilege, b.Privilege),
			cmp.Compare(a.GrantorOID, b.GrantorOID),
			cmp.Compare(a.GranteeOID, b.GranteeOID),
			cmp.Compare(a.OID, b.OID),
		)
	})
	i.Roles = sortedCatalogRecords(i.Roles, func(a, b CatalogRole) int {
		return cmp.Or(cmp.Compare(a.OID, b.OID), cmp.Compare(a.Name, b.Name))
	})
	i.RoleMemberships = sortedCatalogRecords(i.RoleMemberships, func(a, b CatalogRoleMembership) int {
		return cmp.Or(
			cmp.Compare(a.RoleOID, b.RoleOID),
			cmp.Compare(a.MemberOID, b.MemberOID),
			cmp.Compare(a.GrantorOID, b.GrantorOID),
			cmp.Compare(boolAsInt(a.AdminOption), boolAsInt(b.AdminOption)),
			cmp.Compare(boolAsInt(a.InheritOption), boolAsInt(b.InheritOption)),
			cmp.Compare(boolAsInt(a.SetOption), boolAsInt(b.SetOption)),
		)
	})
	return i
}

func compareCatalogACLGrant(a, b CatalogACLGrant) int {
	return cmp.Or(
		cmp.Compare(a.ObjectClass, b.ObjectClass),
		compareCatalogDependencyObject(a.Object, b.Object),
		cmp.Compare(a.Privilege, b.Privilege),
		cmp.Compare(a.GrantorOID, b.GrantorOID),
		cmp.Compare(a.GranteeOID, b.GranteeOID),
		cmp.Compare(boolAsInt(a.IsGrantable), boolAsInt(b.IsGrantable)),
		cmp.Compare(a.OwnerOID, b.OwnerOID),
		cmp.Compare(boolAsInt(a.GranteeIsPublic), boolAsInt(b.GranteeIsPublic)),
		cmp.Compare(a.OwnerName, b.OwnerName),
		cmp.Compare(a.GrantorName, b.GrantorName),
		cmp.Compare(a.GranteeName, b.GranteeName),
		cmp.Compare(a.Object.ObjectType, b.Object.ObjectType),
		cmp.Compare(a.Object.SchemaName, b.Object.SchemaName),
		cmp.Compare(a.Object.Name, b.Object.Name),
		cmp.Compare(a.Object.Identity, b.Object.Identity),
	)
}

func fetchCatalogACLInventory(
	ctx context.Context,
	db dbsqlc.DBTX,
	inventory *CatalogInventory,
) error {
	queries := dbsqlc.New()

	rawGrants, err := queries.GetCatalogACLGrants(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogACLGrants: %w", err)
	}
	for _, raw := range rawGrants {
		inventory.ACLGrants = append(inventory.ACLGrants, CatalogACLGrant{
			ObjectClass: CatalogACLObjectClass(raw.ObjectClass),
			Object: CatalogDependencyObject{
				ClassOID: catalogOID(raw.ClassID), ObjectOID: catalogOID(raw.ObjectID),
				SubObjectID: raw.SubObjectID, ObjectType: raw.ObjectType,
				SchemaName: raw.ObjectSchemaName, Name: raw.ObjectName, Identity: raw.ObjectIdentity,
			},
			OwnerOID: catalogOID(raw.OwnerOid), OwnerName: raw.OwnerName,
			GrantorOID: catalogOID(raw.GrantorOid), GrantorName: raw.GrantorName,
			GranteeOID: catalogOID(raw.GranteeOid), GranteeName: raw.GranteeName,
			GranteeIsPublic: raw.GranteeIsPublic, Privilege: raw.Privilege,
			IsGrantable: raw.IsGrantable,
		})
	}

	rawDefaults, err := queries.GetCatalogDefaultACLs(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogDefaultACLs: %w", err)
	}
	for _, raw := range rawDefaults {
		inventory.DefaultACLs = append(inventory.DefaultACLs, CatalogDefaultACL{
			OID: catalogOID(raw.DefaultAclOid), OwnerOID: catalogOID(raw.OwnerOid), OwnerName: raw.OwnerName,
			SchemaOID: catalogOID(raw.SchemaOid), SchemaName: raw.SchemaName, IsGlobal: raw.IsGlobal,
			ObjectType: CatalogDefaultACLObjectType(raw.ObjectType),
			GrantorOID: catalogOID(raw.GrantorOid), GrantorName: raw.GrantorName,
			GranteeOID: catalogOID(raw.GranteeOid), GranteeName: raw.GranteeName,
			GranteeIsPublic: raw.GranteeIsPublic, Privilege: raw.Privilege,
			IsGrantable: raw.IsGrantable,
		})
	}

	rawRoles, err := queries.GetCatalogRoles(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogRoles: %w", err)
	}
	for _, raw := range rawRoles {
		inventory.Roles = append(inventory.Roles, CatalogRole{
			OID: catalogOID(raw.RoleOid), Name: raw.RoleName, IsSuperuser: raw.IsSuperuser,
			InheritsPrivileges: raw.InheritsPrivileges, CanCreateRoles: raw.CanCreateRoles,
			CanLogin: raw.CanLogin, CanReplicate: raw.CanReplicate, BypassesRLS: raw.BypassesRls,
		})
	}

	rawMemberships, err := queries.GetCatalogRoleMemberships(ctx, db)
	if err != nil {
		return fmt.Errorf("GetCatalogRoleMemberships: %w", err)
	}
	for _, raw := range rawMemberships {
		inventory.RoleMemberships = append(inventory.RoleMemberships, CatalogRoleMembership{
			RoleOID: catalogOID(raw.RoleOid), RoleName: raw.RoleName,
			MemberOID: catalogOID(raw.MemberOid), MemberName: raw.MemberName,
			GrantorOID: catalogOID(raw.GrantorOid), GrantorName: raw.GrantorName,
			AdminOption: raw.AdminOption, InheritOption: raw.InheritOption, SetOption: raw.SetOption,
		})
	}

	return nil
}
