package schema

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

type CatalogACLRevokeKind string

const (
	CatalogACLRevokeKindGrantOption CatalogACLRevokeKind = "grant_option"
	CatalogACLRevokeKindPrivilege   CatalogACLRevokeKind = "privilege"
)

// CatalogACLRevoke is a typed revoke operation. Rendering and executing it are
// deliberately outside the ACL inventory stage.
type CatalogACLRevoke struct {
	Kind  CatalogACLRevokeKind
	Grant CatalogACLGrant
}

type CatalogACLRevokePlan struct {
	Revokes []CatalogACLRevoke
}

// CatalogACLGrantGraph contains the role and explicit grant facts needed to
// validate grant ancestry. Its maps are private so callers cannot invalidate a
// graph after construction.
type CatalogACLGrantGraph struct {
	grants      []CatalogACLGrant
	roles       map[uint32]CatalogRole
	memberships map[uint32][]CatalogRoleMembership
}

func NewCatalogACLGrantGraph(
	grants []CatalogACLGrant,
	roles []CatalogRole,
	memberships []CatalogRoleMembership,
) (CatalogACLGrantGraph, error) {
	graph := CatalogACLGrantGraph{
		grants:      sortedCatalogRecords(grants, compareCatalogACLGrant),
		roles:       make(map[uint32]CatalogRole, len(roles)),
		memberships: make(map[uint32][]CatalogRoleMembership),
	}
	roleNames := make(map[string]uint32, len(roles))
	for _, role := range roles {
		if role.OID == 0 || role.Name == "" {
			return CatalogACLGrantGraph{}, fmt.Errorf(
				"ACL grant graph has invalid role identity OID %d name %q", role.OID, role.Name,
			)
		}
		if _, exists := graph.roles[role.OID]; exists {
			return CatalogACLGrantGraph{}, fmt.Errorf(
				"ACL grant graph has duplicate role OID %d", role.OID,
			)
		}
		if existingOID, exists := roleNames[role.Name]; exists {
			return CatalogACLGrantGraph{}, fmt.Errorf(
				"ACL grant graph has duplicate role name %q for OIDs %d and %d",
				role.Name, existingOID, role.OID,
			)
		}
		graph.roles[role.OID] = role
		roleNames[role.Name] = role.OID
	}

	membershipKeys := make(map[[2]uint32]struct{}, len(memberships))
	for _, membership := range memberships {
		for _, identity := range []struct {
			oid   uint32
			label string
		}{
			{membership.RoleOID, "granted role"},
			{membership.MemberOID, "member role"},
			{membership.GrantorOID, "membership grantor"},
		} {
			if _, exists := graph.roles[identity.oid]; !exists {
				return CatalogACLGrantGraph{}, fmt.Errorf(
					"ACL role membership has missing %s OID %d", identity.label, identity.oid,
				)
			}
		}
		key := [2]uint32{membership.RoleOID, membership.MemberOID}
		if _, exists := membershipKeys[key]; exists {
			return CatalogACLGrantGraph{}, fmt.Errorf(
				"ACL grant graph has duplicate membership from role OID %d to member OID %d",
				membership.RoleOID, membership.MemberOID,
			)
		}
		membershipKeys[key] = struct{}{}
		graph.memberships[membership.MemberOID] = append(
			graph.memberships[membership.MemberOID], membership,
		)
	}
	for memberOID := range graph.memberships {
		slices.SortFunc(graph.memberships[memberOID], func(a, b CatalogRoleMembership) int {
			return cmp.Or(cmp.Compare(a.RoleOID, b.RoleOID),
				cmp.Compare(a.GrantorOID, b.GrantorOID))
		})
	}
	if err := graph.validateMembershipCycles(); err != nil {
		return CatalogACLGrantGraph{}, err
	}

	for idx := 1; idx < len(graph.grants); idx++ {
		if graph.grants[idx] == graph.grants[idx-1] {
			return CatalogACLGrantGraph{}, fmt.Errorf(
				"ACL grant graph has duplicate grant %s",
				describeCatalogACLGrant(graph.grants[idx]),
			)
		}
	}
	return graph, nil
}

func (i CatalogInventory) ACLGrantGraph() (CatalogACLGrantGraph, error) {
	return NewCatalogACLGrantGraph(i.ACLGrants, i.Roles, i.RoleMemberships)
}

func (i CatalogInventory) PlanACLRevokes(
	objects []CatalogDependencyObject,
) (CatalogACLRevokePlan, error) {
	graph, err := i.ACLGrantGraph()
	if err != nil {
		return CatalogACLRevokePlan{}, err
	}
	return graph.PlanRevokes(objects)
}

// PlanRevokes validates grant ancestry and orders explicit revokes for the
// selected objects. Selecting a table address also selects its column grants.
// Owner grants are validated but intentionally retained.
func (g CatalogACLGrantGraph) PlanRevokes(
	objects []CatalogDependencyObject,
) (CatalogACLRevokePlan, error) {
	selected := make(map[int]struct{})
	for idx, grant := range g.grants {
		if !catalogACLGrantMatchesAnyObject(grant, objects) {
			continue
		}
		if err := g.validateGrant(grant); err != nil {
			return CatalogACLRevokePlan{}, err
		}
		if grant.GranteeOID != grant.OwnerOID {
			selected[idx] = struct{}{}
		}
	}
	if len(selected) == 0 {
		return CatalogACLRevokePlan{}, nil
	}

	providers := make(map[int]int)
	visitState := make(map[int]uint8)
	var validateAuthorization func(int) error
	validateAuthorization = func(grantIdx int) error {
		switch visitState[grantIdx] {
		case 1:
			return fmt.Errorf(
				"ACL grant authorization cycle at %s",
				describeCatalogACLGrant(g.grants[grantIdx]),
			)
		case 2:
			return nil
		}
		visitState[grantIdx] = 1
		grant := g.grants[grantIdx]
		if err := g.validateGrant(grant); err != nil {
			return err
		}

		grantor := g.roles[grant.GrantorOID]
		if grant.GrantorOID == grant.OwnerOID || grantor.IsSuperuser {
			visitState[grantIdx] = 2
			return nil
		}

		inheritedOwner := g.roleInherits(grant.GrantorOID, grant.OwnerOID)
		var candidates []int
		for candidateIdx, candidate := range g.grants {
			if candidateIdx == grantIdx || !candidate.IsGrantable || candidate.GranteeIsPublic {
				continue
			}
			if !catalogACLGrantCovers(candidate, grant) ||
				!g.roleInherits(grant.GrantorOID, candidate.GranteeOID) {
				continue
			}
			candidates = append(candidates, candidateIdx)
		}

		authorizationCount := len(candidates)
		if inheritedOwner {
			authorizationCount++
		}
		if authorizationCount == 0 {
			return fmt.Errorf(
				"ACL grant %s has no traceable grant-option chain to owner OID %d",
				describeCatalogACLGrant(grant), grant.OwnerOID,
			)
		}
		if authorizationCount > 1 {
			return fmt.Errorf(
				"ACL grant %s has an ambiguous authorization chain with %d possible roots",
				describeCatalogACLGrant(grant), authorizationCount,
			)
		}
		if inheritedOwner {
			visitState[grantIdx] = 2
			return nil
		}

		providerIdx := candidates[0]
		providers[grantIdx] = providerIdx
		if err := validateAuthorization(providerIdx); err != nil {
			return err
		}
		visitState[grantIdx] = 2
		return nil
	}

	for grantIdx := range g.grants {
		if _, isSelected := selected[grantIdx]; !isSelected {
			continue
		}
		if err := validateAuthorization(grantIdx); err != nil {
			return CatalogACLRevokePlan{}, err
		}
	}

	children := make(map[int][]int)
	for childIdx, parentIdx := range providers {
		if _, childSelected := selected[childIdx]; !childSelected {
			continue
		}
		if _, parentSelected := selected[parentIdx]; parentSelected {
			children[parentIdx] = append(children[parentIdx], childIdx)
		}
	}
	for parentIdx := range children {
		slices.SortFunc(children[parentIdx], func(a, b int) int {
			return compareCatalogACLGrant(g.grants[a], g.grants[b])
		})
	}

	selectedIndices := make([]int, 0, len(selected))
	for grantIdx := range selected {
		selectedIndices = append(selectedIndices, grantIdx)
	}
	slices.SortFunc(selectedIndices, func(a, b int) int {
		return compareCatalogACLGrant(g.grants[a], g.grants[b])
	})

	var ordered []int
	orderState := make(map[int]uint8, len(selected))
	var orderGrant func(int) error
	orderGrant = func(grantIdx int) error {
		switch orderState[grantIdx] {
		case 1:
			return fmt.Errorf(
				"ACL revoke dependency cycle at %s", describeCatalogACLGrant(g.grants[grantIdx]),
			)
		case 2:
			return nil
		}
		orderState[grantIdx] = 1
		for _, childIdx := range children[grantIdx] {
			if err := orderGrant(childIdx); err != nil {
				return err
			}
		}
		orderState[grantIdx] = 2
		ordered = append(ordered, grantIdx)
		return nil
	}
	for _, grantIdx := range selectedIndices {
		if err := orderGrant(grantIdx); err != nil {
			return CatalogACLRevokePlan{}, err
		}
	}

	plan := CatalogACLRevokePlan{Revokes: make([]CatalogACLRevoke, 0, len(ordered)*2)}
	for _, grantIdx := range ordered {
		grant := g.grants[grantIdx]
		if grant.IsGrantable {
			plan.Revokes = append(plan.Revokes, CatalogACLRevoke{
				Kind: CatalogACLRevokeKindGrantOption, Grant: grant,
			})
		}
		plan.Revokes = append(plan.Revokes, CatalogACLRevoke{
			Kind: CatalogACLRevokeKindPrivilege, Grant: grant,
		})
	}
	return plan, nil
}

func (g CatalogACLGrantGraph) validateGrant(grant CatalogACLGrant) error {
	if _, exists := g.roles[grant.OwnerOID]; !exists {
		return fmt.Errorf(
			"ACL grant %s references missing owner role OID %d",
			describeCatalogACLGrant(grant), grant.OwnerOID,
		)
	}
	if _, exists := g.roles[grant.GrantorOID]; !exists {
		return fmt.Errorf(
			"ACL grant %s references missing grantor role OID %d",
			describeCatalogACLGrant(grant), grant.GrantorOID,
		)
	}
	if grant.GranteeIsPublic {
		if grant.GranteeOID != 0 || grant.GranteeName != CatalogPublicRoleName {
			return fmt.Errorf("ACL grant %s has an invalid PUBLIC identity", describeCatalogACLGrant(grant))
		}
		if grant.IsGrantable {
			return fmt.Errorf("ACL grant %s gives an unsupported grant option to PUBLIC", describeCatalogACLGrant(grant))
		}
	} else {
		if grant.GranteeOID == 0 {
			return fmt.Errorf("ACL grant %s has an unmarked PUBLIC grantee", describeCatalogACLGrant(grant))
		}
		if _, exists := g.roles[grant.GranteeOID]; !exists {
			return fmt.Errorf(
				"ACL grant %s references missing grantee role OID %d",
				describeCatalogACLGrant(grant), grant.GranteeOID,
			)
		}
	}

	if grant.Object.SubObjectID < 0 {
		return fmt.Errorf("ACL grant %s has an unsupported negative subobject", describeCatalogACLGrant(grant))
	}
	var privileges []string
	switch grant.ObjectClass {
	case CatalogACLObjectClassSchema:
		if grant.Object.SubObjectID != 0 {
			return fmt.Errorf("ACL grant %s has an unsupported schema subobject", describeCatalogACLGrant(grant))
		}
		privileges = []string{"CREATE", "USAGE"}
	case CatalogACLObjectClassTable:
		if grant.Object.SubObjectID == 0 {
			privileges = []string{
				"DELETE", "INSERT", "MAINTAIN", "REFERENCES", "SELECT", "TRIGGER", "TRUNCATE", "UPDATE",
			}
		} else {
			privileges = []string{"INSERT", "REFERENCES", "SELECT", "UPDATE"}
		}
	case CatalogACLObjectClassSequence:
		if grant.Object.SubObjectID != 0 {
			return fmt.Errorf("ACL grant %s has an unsupported sequence subobject", describeCatalogACLGrant(grant))
		}
		privileges = []string{"SELECT", "UPDATE", "USAGE"}
	case CatalogACLObjectClassRoutine:
		if grant.Object.SubObjectID != 0 {
			return fmt.Errorf("ACL grant %s has an unsupported routine subobject", describeCatalogACLGrant(grant))
		}
		privileges = []string{"EXECUTE"}
	case CatalogACLObjectClassType:
		if grant.Object.SubObjectID != 0 {
			return fmt.Errorf("ACL grant %s has an unsupported type subobject", describeCatalogACLGrant(grant))
		}
		privileges = []string{"USAGE"}
	default:
		return fmt.Errorf(
			"ACL grant %s has unsupported object class %q",
			describeCatalogACLGrant(grant), grant.ObjectClass,
		)
	}
	if !slices.Contains(privileges, grant.Privilege) {
		return fmt.Errorf(
			"ACL grant %s has unsupported privilege %q", describeCatalogACLGrant(grant), grant.Privilege,
		)
	}
	return nil
}

func catalogACLGrantMatchesAnyObject(
	grant CatalogACLGrant,
	objects []CatalogDependencyObject,
) bool {
	for _, object := range objects {
		if grant.Object.ClassOID != object.ClassOID ||
			grant.Object.ObjectOID != object.ObjectOID {
			continue
		}
		if grant.Object.SubObjectID == object.SubObjectID ||
			(grant.ObjectClass == CatalogACLObjectClassTable && object.SubObjectID == 0) {
			return true
		}
	}
	return false
}

func catalogACLGrantCovers(provider, grant CatalogACLGrant) bool {
	if provider.ObjectClass != grant.ObjectClass || provider.OwnerOID != grant.OwnerOID ||
		provider.Privilege != grant.Privilege ||
		provider.Object.ClassOID != grant.Object.ClassOID ||
		provider.Object.ObjectOID != grant.Object.ObjectOID {
		return false
	}
	if provider.Object.SubObjectID == grant.Object.SubObjectID {
		return true
	}
	return provider.ObjectClass == CatalogACLObjectClassTable &&
		provider.Object.SubObjectID == 0 && grant.Object.SubObjectID > 0
}

func (g CatalogACLGrantGraph) roleInherits(memberOID, roleOID uint32) bool {
	if memberOID == roleOID {
		return true
	}
	visited := map[uint32]struct{}{memberOID: {}}
	queue := []uint32{memberOID}
	for len(queue) > 0 {
		member := queue[0]
		queue = queue[1:]
		if !g.roles[member].InheritsPrivileges {
			continue
		}
		for _, membership := range g.memberships[member] {
			if !membership.InheritOption {
				continue
			}
			if membership.RoleOID == roleOID {
				return true
			}
			if _, seen := visited[membership.RoleOID]; seen {
				continue
			}
			visited[membership.RoleOID] = struct{}{}
			queue = append(queue, membership.RoleOID)
		}
	}
	return false
}

func (g CatalogACLGrantGraph) validateMembershipCycles() error {
	state := make(map[uint32]uint8, len(g.roles))
	var visit func(uint32) error
	visit = func(memberOID uint32) error {
		switch state[memberOID] {
		case 1:
			return fmt.Errorf("ACL role membership cycle includes role OID %d", memberOID)
		case 2:
			return nil
		}
		state[memberOID] = 1
		for _, membership := range g.memberships[memberOID] {
			if err := visit(membership.RoleOID); err != nil {
				return err
			}
		}
		state[memberOID] = 2
		return nil
	}
	roleOIDs := make([]uint32, 0, len(g.roles))
	for oid := range g.roles {
		roleOIDs = append(roleOIDs, oid)
	}
	slices.Sort(roleOIDs)
	for _, oid := range roleOIDs {
		if err := visit(oid); err != nil {
			return err
		}
	}
	return nil
}

func describeCatalogACLGrant(grant CatalogACLGrant) string {
	identity := grant.Object.Identity
	if identity == "" {
		identity = fmt.Sprintf(
			"address %d/%d/%d", grant.Object.ClassOID, grant.Object.ObjectOID, grant.Object.SubObjectID,
		)
	}
	grantee := grant.GranteeName
	if grantee == "" {
		grantee = fmt.Sprintf("OID %d", grant.GranteeOID)
	}
	return fmt.Sprintf(
		"%s %s on %s from grantor OID %d to %s",
		strings.ToLower(string(grant.ObjectClass)), grant.Privilege, identity, grant.GrantorOID, grantee,
	)
}
