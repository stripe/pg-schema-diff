package diff

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

func TestArchivalIsolationACLPlanUsesTypedDependentFirstRevokes(t *testing.T) {
	t.Parallel()

	request := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_ABCDEFGH", 10, "source", "accounts",
	)
	request.CurrentInventory.Columns = []schema.CatalogColumn{
		{RelationOID: 10, Number: 1, Name: "id"},
		{RelationOID: 10, Number: 2, Name: `quoted"column`},
	}
	request.CurrentInventory.Roles = []schema.CatalogRole{
		{OID: 1, Name: `owner"role`, InheritsPrivileges: true},
		{OID: 2, Name: `grantor"role`, InheritsPrivileges: true},
		{OID: 3, Name: `reader"role`, InheritsPrivileges: true},
	}
	tableAddress := schema.CatalogDependencyObject{
		ClassOID: pgClassCatalogOID, ObjectOID: 10, SchemaName: "source", Name: "accounts",
	}
	columnAddress := tableAddress
	columnAddress.SubObjectID = 2
	request.CurrentInventory.ACLGrants = []schema.CatalogACLGrant{
		archivalACLTestGrant(tableAddress, 1, 1, 2, true),
		archivalACLTestGrant(columnAddress, 1, 2, 3, false),
		{
			ObjectClass: schema.CatalogACLObjectClassTable, Object: tableAddress,
			OwnerOID: 1, OwnerName: `owner"role`, GrantorOID: 1, GrantorName: `owner"role`,
			GranteeName: schema.CatalogPublicRoleName, GranteeIsPublic: true, Privilege: "SELECT",
		},
	}
	request = finalizeArchivalIsolationUnitMarker(t, request)

	statements, err := generatePlainTableArchivalStatements(request)
	require.NoError(t, err)
	ddl := replacementAwareDDL(statements)
	assert.NotContains(t, strings.ToUpper(ddl), "CASCADE")
	publicRevoke := statementIndexContaining(t, statements,
		`REVOKE SELECT ON TABLE "`+request.Groups[0].Allocation.Members[0].CleanupSchemaName+`"."accounts" FROM PUBLIC`)
	columnRevoke := statementIndexContaining(t, statements,
		`REVOKE SELECT ("quoted""column") ON TABLE`)
	grantOptionRevoke := statementIndexContaining(t, statements,
		`REVOKE GRANT OPTION FOR SELECT ON TABLE`)
	fullRevoke := statementIndexContaining(t, statements,
		`SET LOCAL ROLE "owner""role";`+"\n"+`REVOKE SELECT ON TABLE `+
			schema.EscapeIdentifier(request.Groups[0].Allocation.Members[0].CleanupSchemaName)+
			`."accounts" FROM "grantor""role" RESTRICT`)
	assert.Less(t, columnRevoke, grantOptionRevoke)
	assert.Less(t, publicRevoke, grantOptionRevoke)
	assert.Less(t, grantOptionRevoke, fullRevoke)
	for _, index := range []int{publicRevoke, columnRevoke, grantOptionRevoke, fullRevoke} {
		assert.Equal(t, []MigrationHazard{migrationHazardPrivilegeRevoked}, statements[index].Hazards)
	}

	marker, err := parseArchivalMarker(request.Groups[0].FinalizedMarker)
	require.NoError(t, err)
	require.Len(t, marker.OriginalACLs, 3)
	marker.OriginalACLs = marker.OriginalACLs[1:]
	request.Groups[0].FinalizedMarker, err = marshalArchivalMarker(marker)
	require.NoError(t, err)
	_, err = generatePlainTableArchivalStatements(request)
	require.ErrorContains(t, err, "ACL records")
}

func TestArchivalIsolationBoundaryForeignKeysAndPublications(t *testing.T) {
	t.Parallel()

	request := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_ABCDEFGH", 10, "source", "accounts",
	)
	request.CurrentInventory.Relations = append(request.CurrentInventory.Relations,
		schema.CatalogRelation{OID: 20, SchemaName: "source", Name: "orders", Kind: schema.RelKindOrdinaryTable})
	foreignKey := schema.CatalogForeignKey{
		OID: 30, Name: `orders"accounts_fk`, OwningRelationOID: 20,
		OwningSchemaName: "source", OwningRelationName: "orders",
		ReferencedRelationOID: 10, ReferencedSchemaName: "source", ReferencedRelationName: "accounts",
		Columns:   []schema.CatalogForeignKeyColumn{{OwningName: "account_id", ReferencedName: "id"}},
		MatchType: "s", UpdateAction: "a", DeleteAction: "c", IsValidated: true,
		Definition: `FOREIGN KEY (account_id) REFERENCES source.accounts(id) ON DELETE CASCADE`,
	}
	request.SourcePreflight.ForeignKeys = []sourceSafetyForeignKey{{
		ForeignKey: foreignKey, Direction: sourceSafetyForeignKeyDirectionIncoming,
		ManagedScope: sourceSafetyManagedScopeManaged, TargetIntent: sourceSafetyTargetIntentPersistent,
		Disposition: sourceSafetyDependencyDispositionForeignKey,
	}}
	request.SourcePreflight.PublicationRelations = []schema.CatalogPublicationRelation{{
		OID: 40, PublicationName: `explicit"publication`, RelationOID: 10,
		RelationSchemaName: "source", RelationName: "accounts", ColumnNames: []string{"id"},
		RowFilter: "(id > 0)",
	}}
	request.SourcePreflight.PublicationSchemas = []schema.CatalogPublicationSchema{{
		OID: 41, PublicationName: "schema_publication", SchemaName: "source",
	}}
	request.TargetSchema = schema.Schema{ForeignKeyConstraints: []schema.ForeignKeyConstraint{{
		EscapedName: schema.EscapeIdentifier(foreignKey.Name),
		OwningTable: schema.SchemaQualifiedName{
			SchemaName:  "source",
			EscapedName: schema.EscapeIdentifier("orders"),
		},
		ForeignTable: schema.SchemaQualifiedName{
			SchemaName:  "source",
			EscapedName: schema.EscapeIdentifier("accounts"),
		},
		ConstraintDef: foreignKey.Definition, IsValid: true,
	}}}
	request = finalizeArchivalIsolationUnitMarker(t, request)

	statements, err := generatePlainTableArchivalStatements(request)
	require.NoError(t, err)
	drop := statementIndexContaining(t, statements, `DROP CONSTRAINT "orders""accounts_fk"`)
	publication := statementIndexContaining(t, statements,
		`ALTER PUBLICATION "explicit""publication" DROP TABLE "source"."accounts"`)
	move := statementIndexContaining(t, statements, `ALTER TABLE "source"."accounts" SET SCHEMA`)
	readd := statementIndexContaining(t, statements, `ADD CONSTRAINT "orders""accounts_fk"`)
	assert.Less(t, drop, move)
	assert.Less(t, publication, move)
	assert.Less(t, move, readd)
	assert.Equal(t, migrationHazardBoundaryForeignKeyDrop, statements[drop].Hazards)
	assert.Equal(t, []MigrationHazard{migrationHazardIncomingForeignKeyAdd}, statements[readd].Hazards)
	assert.Contains(t, statements[len(statements)-1].DDL, "pg_publication_namespace")

	marker, err := parseArchivalMarker(request.Groups[0].FinalizedMarker)
	require.NoError(t, err)
	require.Len(t, marker.OriginalForeignKeys, 1)
	assert.Equal(t, "c", marker.OriginalForeignKeys[0].DeleteAction)
	assert.Equal(t, foreignKey.Definition, marker.OriginalForeignKeys[0].Definition)
	require.Len(t, marker.OriginalPublicationMemberships, 1)
	assert.Equal(t, []string{"id"}, marker.OriginalPublicationMemberships[0].ColumnNames)
	assert.Equal(t, "(id > 0)", marker.OriginalPublicationMemberships[0].RowFilter)

	publicationMismatch := canonicalizeArchivalMarker(marker)
	publicationMismatch.OriginalPublicationMemberships[0].RowFilter = "(id > 1)"
	request.Groups[0].FinalizedMarker, err = marshalArchivalMarker(publicationMismatch)
	require.NoError(t, err)
	_, err = generatePlainTableArchivalStatements(request)
	require.ErrorContains(t, err, "publication records")

	marker.OriginalForeignKeys[0].DeleteAction = "a"
	request.Groups[0].FinalizedMarker, err = marshalArchivalMarker(marker)
	require.NoError(t, err)
	_, err = generatePlainTableArchivalStatements(request)
	require.ErrorContains(t, err, "foreign-key records")
}

func TestArchivalIsolationPreservesSelfForeignKeyWithoutOrdinaryDelete(t *testing.T) {
	t.Parallel()

	oldSchema := schema.Schema{Tables: []schema.Table{replacementAwareTestTable(false)}}
	self := schema.ForeignKeyConstraint{
		EscapedName:   schema.EscapeIdentifier("accounts_parent_fk"),
		OwningTable:   oldSchema.Tables[0].SchemaQualifiedName,
		ForeignTable:  oldSchema.Tables[0].SchemaQualifiedName,
		ConstraintDef: `FOREIGN KEY (id) REFERENCES public.accounts(id)`, IsValid: true,
	}
	oldSchema.ForeignKeyConstraints = []schema.ForeignKeyConstraint{self}
	diff, _, err := buildSchemaDiff(oldSchema, schema.Schema{})
	require.NoError(t, err)
	request := plainTableArchivalUnitRequest(
		t, "20260721T091011123456Z_ABCDEFGH", 10, "public", "accounts",
	)
	request.SourcePreflight.ForeignKeys = []sourceSafetyForeignKey{{
		ForeignKey: schema.CatalogForeignKey{
			OID: 30, Name: "accounts_parent_fk", OwningRelationOID: 10,
			OwningSchemaName: "public", OwningRelationName: "accounts",
			ReferencedRelationOID: 10, ReferencedSchemaName: "public", ReferencedRelationName: "accounts",
			Columns:   []schema.CatalogForeignKeyColumn{{OwningName: "id", ReferencedName: "id"}},
			MatchType: "s", UpdateAction: "a", DeleteAction: "a", IsValidated: true,
			Definition: self.ConstraintDef,
		},
		Direction:   sourceSafetyForeignKeyDirectionSelf,
		Disposition: sourceSafetyDependencyDispositionForeignKey,
	}}
	statements, err := generateReplacementAwareSchemaSQL(
		newSchemaSQLGenerator(&deterministicRandReader{}, &planOptions{}), diff,
		tableDispositions{oldSchema.Tables[0].GetName(): {
			Kind: tableDispositionKindArchivalMove, GroupID: request.DependencyClosure.ValidatedGroupIDs[0],
		}}, request,
	)
	require.NoError(t, err)
	assert.NotContains(t, replacementAwareDDL(statements), "DROP CONSTRAINT")
	assert.Contains(t, statements[len(statements)-1].DDL,
		"preserved archival foreign key mismatch")
}

func TestRenderArchivalDependencyMovesUsesObjectIdentitySyntax(t *testing.T) {
	t.Parallel()

	inventory := schema.CatalogInventory{
		Routines: []schema.CatalogRoutine{{
			OID: 10, Kind: "f", SchemaName: `source"schema`, Name: `function"name`,
			IdentityArguments: `integer, text`,
		}},
		Operators: []schema.CatalogOperator{{
			OID: 20, SchemaName: `source"schema`, Name: "===",
			LeftTypeOID: 23, RightTypeOID: 25, LeftType: "integer", RightType: "text",
		}},
	}
	function, err := renderArchivalDependencyMove(inventory, archivalObjectMoveOperation{
		Source: archivalMarkerObjectIdentity{
			Kind: archivalMarkerObjectKindFunction, OID: 10,
			SchemaName: `source"schema`, Name: `function"name`,
			IdentityArguments: []string{`integer, text`},
		},
		Destination: archivalMarkerObjectIdentity{SchemaName: `archive"schema`},
	})
	require.NoError(t, err)
	assert.Equal(t, `ALTER FUNCTION "source""schema"."function""name"(integer, text) SET SCHEMA "archive""schema"`,
		function.DDL)
	operator, err := renderArchivalDependencyMove(inventory, archivalObjectMoveOperation{
		Source: archivalMarkerObjectIdentity{
			Kind: archivalMarkerObjectKindOperator, OID: 20,
			SchemaName: `source"schema`, Name: "===", IdentityArguments: []string{"integer", "text"},
		},
		Destination: archivalMarkerObjectIdentity{SchemaName: `archive"schema`},
	})
	require.NoError(t, err)
	assert.Equal(t, `ALTER OPERATOR "source""schema".=== (integer, text) SET SCHEMA "archive""schema"`,
		operator.DDL)
}

func finalizeArchivalIsolationUnitMarker(
	t *testing.T,
	request plainTableArchivalRequest,
) plainTableArchivalRequest {
	t.Helper()
	marker, err := parseArchivalMarker(request.Groups[0].FinalizedMarker)
	require.NoError(t, err)
	groupID := marker.GroupID
	move, err := request.CurrentInventory.ExpectedTableMove(marker.Members[0].SourceTable.OID)
	require.NoError(t, err)
	move = filterPlainTableIsolationObjects(move, request.SourcePreflight,
		map[uint32]preparedArchivalGroup{marker.Members[0].SourceTable.OID: {id: groupID}})
	marker.Members[0].AutomaticallyMovedObjects = markerObjectsFromCatalog(
		move.CleanupSchemaObjects, marker.Members[0].CleanupTable.SchemaName,
	)
	marker.Members[0].AttachedObjects = markerObjectsFromCatalog(
		move.AttachedObjects, marker.Members[0].CleanupTable.SchemaName,
	)
	marker.Members[0].ExplicitlyMovedObjects = markerObjectsFromCatalog(
		move.ExplicitMoveObjects, marker.Members[0].CleanupTable.SchemaName,
	)
	marker.Members[0].InternalToastObjects = markerObjectsFromCatalog(move.InternalObjects, "")

	assignments := make([]archivalMarkerObjectIdentity, 0)
	for _, assignment := range request.DependencyClosure.Assignments {
		if assignment.GroupID == groupID {
			assignments = append(assignments, assignment.Destination)
		}
	}
	marker.ExclusiveDependencyObjects = canonicalMarkerObjects(assignments)
	marker.SharedCleanupComponentGroupEdges = incidentArchivedDependencyEdges(
		groupID, request.DependencyClosure.SharedGroupEdges,
	)

	objects := []schema.CatalogDependencyObject{{
		ClassOID:  pgClassCatalogOID,
		ObjectOID: marker.Members[0].SourceTable.OID,
	}}
	for _, object := range marker.Members[0].AutomaticallyMovedObjects {
		if object.Kind == archivalMarkerObjectKindOwnedSequence {
			objects = append(objects, schema.CatalogDependencyObject{
				ClassOID: pgClassCatalogOID, ObjectOID: object.OID,
			})
		}
	}
	for _, assignment := range request.DependencyClosure.Assignments {
		if assignment.GroupID == groupID && assignment.Source.Kind == archivalMarkerObjectKindSequence {
			objects = append(objects, schema.CatalogDependencyObject{
				ClassOID: pgClassCatalogOID, ObjectOID: assignment.Source.OID,
			})
		}
	}
	revokes, err := request.CurrentInventory.PlanACLRevokes(objects)
	require.NoError(t, err)
	for _, revoke := range revokes.Revokes {
		record, recordErr := archivalMarkerACLRecord(request.CurrentInventory, revoke.Grant)
		require.NoError(t, recordErr)
		if !containsMarkerACL(marker.OriginalACLs, record) {
			marker.OriginalACLs = append(marker.OriginalACLs, record)
		}
	}

	for _, classified := range request.SourcePreflight.ForeignKeys {
		foreignKey := classified.ForeignKey
		owning := foreignKey.OwningRelationOID == marker.Members[0].SourceTable.OID
		referenced := foreignKey.ReferencedRelationOID == marker.Members[0].SourceTable.OID
		if owning != referenced {
			marker.OriginalForeignKeys = append(marker.OriginalForeignKeys,
				archivalMarkerForeignKey(foreignKey))
		}
	}
	for _, publication := range request.SourcePreflight.PublicationRelations {
		if publication.RelationOID == marker.Members[0].SourceTable.OID {
			marker.OriginalPublicationMemberships = append(marker.OriginalPublicationMemberships,
				archivalMarkerPublicationMembership(publication))
		}
	}
	marker = canonicalizeArchivalMarker(marker)
	request.Groups[0].FinalizedMarker, err = marshalArchivalMarker(marker)
	require.NoError(t, err)
	return request
}

func archivalACLTestGrant(
	object schema.CatalogDependencyObject,
	ownerOID, grantorOID, granteeOID uint32,
	grantable bool,
) schema.CatalogACLGrant {
	roleName := func(oid uint32) string {
		return map[uint32]string{1: `owner"role`, 2: `grantor"role`, 3: `reader"role`}[oid]
	}
	return schema.CatalogACLGrant{
		ObjectClass: schema.CatalogACLObjectClassTable, Object: object,
		OwnerOID: ownerOID, OwnerName: roleName(ownerOID), GrantorOID: grantorOID,
		GrantorName: roleName(grantorOID), GranteeOID: granteeOID, GranteeName: roleName(granteeOID),
		Privilege: "SELECT", IsGrantable: grantable,
	}
}
