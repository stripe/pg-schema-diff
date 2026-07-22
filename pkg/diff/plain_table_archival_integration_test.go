package diff

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/internal/testdb"
)

func TestPlainTableArchivalPostgresPreservesTableSnapshot(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	_, err := db.ConnPool.Exec(t.Context(), `
		CREATE SCHEMA stage12_source;
		CREATE FUNCTION public.stage12_accounts_trigger() RETURNS trigger
			LANGUAGE plpgsql AS 'BEGIN NEW.payload := NEW.payload || ''-triggered''; RETURN NEW; END';
		CREATE TABLE stage12_source.accounts (
			serial_id BIGSERIAL PRIMARY KEY,
			identity_id BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
			tenant_id BIGINT NOT NULL,
			base_value INTEGER NOT NULL DEFAULT 7,
			generated_value INTEGER GENERATED ALWAYS AS (base_value * 2) STORED,
			payload TEXT NOT NULL,
			CONSTRAINT accounts_positive CHECK (base_value > 0)
		) WITH (fillfactor = 75);
		CREATE INDEX accounts_payload_idx ON stage12_source.accounts (payload);
		CREATE UNIQUE INDEX accounts_tenant_identity_idx
			ON stage12_source.accounts (tenant_id, identity_id);
		ALTER TABLE stage12_source.accounts CLUSTER ON accounts_payload_idx;
		ALTER TABLE stage12_source.accounts REPLICA IDENTITY USING INDEX accounts_tenant_identity_idx;
		ALTER TABLE stage12_source.accounts ALTER COLUMN payload SET STATISTICS 321;
		ALTER TABLE stage12_source.accounts ALTER COLUMN payload SET (n_distinct = 0.25);
		ALTER TABLE stage12_source.accounts ENABLE ROW LEVEL SECURITY;
		ALTER TABLE stage12_source.accounts FORCE ROW LEVEL SECURITY;
		CREATE POLICY accounts_policy ON stage12_source.accounts USING (tenant_id > 0);
		CREATE TRIGGER accounts_trigger BEFORE INSERT OR UPDATE ON stage12_source.accounts
			FOR EACH ROW EXECUTE FUNCTION public.stage12_accounts_trigger();
		CREATE RULE accounts_rule AS ON UPDATE TO stage12_source.accounts DO ALSO NOTHING;
		COMMENT ON TABLE stage12_source.accounts IS 'retained table comment';
		COMMENT ON COLUMN stage12_source.accounts.payload IS 'retained column comment';
		COMMENT ON INDEX stage12_source.accounts_payload_idx IS 'retained index comment';
		COMMENT ON CONSTRAINT accounts_positive ON stage12_source.accounts IS 'retained constraint comment';
		INSERT INTO stage12_source.accounts (tenant_id, payload, base_value)
		VALUES (1, repeat('payload-', 500), 11), (2, 'second', 13);
	`)
	require.NoError(t, err)

	before, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
	require.NoError(t, err)
	original := mustCatalogRelationByName(t, before.Inventory, "stage12_source", "accounts")
	request, marker := plainTableArchivalDatabaseRequest(
		t, before.Inventory, original, "20260721T091011123456Z_ABCDEFGH",
	)
	statements, err := generatePlainTableArchivalStatements(request)
	require.NoError(t, err)
	require.Len(t, statements, 3)

	_, err = db.ConnPool.Exec(t.Context(), statements[0].ToSQL())
	require.NoError(t, err)
	assertPlainTableMarkerComments(t, db.ConnPool, marker, request.Groups[0].FinalizedMarker)
	assertPlainTableSchemasLockedDown(t, db.ConnPool, marker)
	assert.Equal(t, original.OID, postgresRelationOID(t, db.ConnPool, "stage12_source", "accounts"))

	_, err = db.ConnPool.Exec(t.Context(), statements[1].ToSQL())
	require.NoError(t, err)
	_, err = db.ConnPool.Exec(t.Context(), statements[2].ToSQL())
	require.NoError(t, err)

	cleanupSchema := marker.Members[0].CleanupTable.SchemaName
	assert.Zero(t, postgresRelationOID(t, db.ConnPool, "stage12_source", "accounts"))
	assert.Equal(t, original.OID, postgresRelationOID(t, db.ConnPool, cleanupSchema, "accounts"))
	var rows []struct {
		TenantID       int64
		BaseValue      int
		GeneratedValue int
		Payload        string
	}
	rows, err = queryArchivedRows(t, db.ConnPool, cleanupSchema)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(1), rows[0].TenantID)
	assert.Equal(t, 11, rows[0].BaseValue)
	assert.Equal(t, 22, rows[0].GeneratedValue)
	assert.True(t, strings.HasSuffix(rows[0].Payload, "-triggered"))

	after, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
	require.NoError(t, err)
	assertPlainTableMovedIdentities(t, before.Inventory, after.Inventory, marker.Members[0])
	assertPlainTableAttachedMetadata(t, before.Inventory, after.Inventory, original.OID)
	assertPlainTableSourceNamesFreed(t, db.ConnPool, marker.Members[0])

	var inserted struct {
		BaseValue      int
		GeneratedValue int
		Payload        string
	}
	err = db.ConnPool.QueryRow(t.Context(), fmt.Sprintf(
		"INSERT INTO %s.accounts (tenant_id, payload) VALUES (3, 'later') "+
			"RETURNING base_value, generated_value, payload",
		schema.EscapeIdentifier(cleanupSchema),
	)).Scan(&inserted.BaseValue, &inserted.GeneratedValue, &inserted.Payload)
	require.NoError(t, err)
	assert.Equal(t, 7, inserted.BaseValue)
	assert.Equal(t, 14, inserted.GeneratedValue)
	assert.Equal(t, "later-triggered", inserted.Payload)
}

func TestPlainTableArchivalInitializationRollsBackAtomically(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	_, err := db.ConnPool.Exec(t.Context(), `CREATE TABLE public.atomic_accounts (id BIGINT PRIMARY KEY)`)
	require.NoError(t, err)
	snapshot, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
	require.NoError(t, err)
	relation := mustCatalogRelationByName(t, snapshot.Inventory, "public", "atomic_accounts")
	request, marker := plainTableArchivalDatabaseRequest(
		t, snapshot.Inventory, relation, "20260721T091011123456Z_12345678",
	)
	statements, err := generatePlainTableArchivalStatements(request)
	require.NoError(t, err)

	schemaNames := archivedMarkerSchemaNames(marker)
	require.Len(t, schemaNames, 2)
	_, err = db.ConnPool.Exec(t.Context(), "CREATE SCHEMA "+
		schema.EscapeIdentifier(schemaNames[1]))
	require.NoError(t, err)
	_, err = db.ConnPool.Exec(t.Context(), statements[0].ToSQL())
	require.Error(t, err)
	assert.False(t, postgresSchemaExists(t, db.ConnPool, schemaNames[0]),
		"the schema created before the collision must be rolled back")
	assert.True(t, postgresSchemaExists(t, db.ConnPool, schemaNames[1]),
		"the pre-existing colliding schema must remain")
}

func TestPlainTableArchivalPostMoveAssertionRejectsCatalogMismatch(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	for _, tc := range []struct {
		name        string
		mutate      func(*testing.T, *pgxpool.Pool, archivalMarkerV1)
		expectError string
	}{
		{
			name: "marker mismatch",
			mutate: func(t *testing.T, pool *pgxpool.Pool, marker archivalMarkerV1) {
				_, err := pool.Exec(t.Context(), fmt.Sprintf("COMMENT ON SCHEMA %s IS 'tampered'",
					schema.EscapeIdentifier(marker.Members[0].CleanupTable.SchemaName)))
				require.NoError(t, err)
			},
			expectError: "archival marker mismatch",
		},
		{
			name: "catalog mismatch",
			mutate: func(t *testing.T, pool *pgxpool.Pool, marker archivalMarkerV1) {
				_, err := pool.Exec(t.Context(), fmt.Sprintf(
					"ALTER TABLE %s.%s RENAME TO unexpected_name",
					schema.EscapeIdentifier(marker.Members[0].CleanupTable.SchemaName),
					schema.EscapeIdentifier(marker.Members[0].CleanupTable.Name),
				))
				require.NoError(t, err)
			},
			expectError: "identity mismatch",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := factory.CreateDatabase(t)
			_, err := db.ConnPool.Exec(t.Context(),
				`CREATE TABLE public.assertion_accounts (id BIGINT PRIMARY KEY)`)
			require.NoError(t, err)
			snapshot, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
			require.NoError(t, err)
			relation := mustCatalogRelationByName(t, snapshot.Inventory, "public", "assertion_accounts")
			request, marker := plainTableArchivalDatabaseRequest(
				t, snapshot.Inventory, relation, "20260721T091011123456Z_IJKLMNOP",
			)
			statements, err := generatePlainTableArchivalStatements(request)
			require.NoError(t, err)
			require.Len(t, statements, 3)
			for _, statement := range statements[:2] {
				_, err = db.ConnPool.Exec(t.Context(), statement.ToSQL())
				require.NoError(t, err)
			}
			tc.mutate(t, db.ConnPool, marker)
			_, err = db.ConnPool.Exec(t.Context(), statements[2].ToSQL())
			require.ErrorContains(t, err, tc.expectError)
		})
	}
}

func TestPlainTableArchivalPostgresResumeReusesRecordedNames(t *testing.T) {
	factory := testdb.MustNewFactory(t)

	for _, tc := range []struct {
		name             string
		moveBeforeResume bool
	}{
		{name: "empty initialization"},
		{name: "partial marker refresh", moveBeforeResume: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := factory.CreateDatabase(t)
			_, err := db.ConnPool.Exec(t.Context(), `
				CREATE TABLE public.resume_accounts (id BIGSERIAL PRIMARY KEY, payload TEXT);
				INSERT INTO public.resume_accounts (payload) VALUES ('retained');
			`)
			require.NoError(t, err)
			before, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
			require.NoError(t, err)
			relation := mustCatalogRelationByName(t, before.Inventory, "public", "resume_accounts")
			newRequest, oldMarker := plainTableArchivalDatabaseRequest(
				t, before.Inventory, relation, "20260721T091011123456Z_QRSTUVWX",
			)
			newStatements, err := generatePlainTableArchivalStatements(newRequest)
			require.NoError(t, err)
			_, err = db.ConnPool.Exec(t.Context(), newStatements[0].ToSQL())
			require.NoError(t, err)
			if tc.moveBeforeResume {
				_, err = db.ConnPool.Exec(t.Context(), newStatements[1].ToSQL())
				require.NoError(t, err)
			}

			current, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool)
			require.NoError(t, err)
			resolution, err := resolveArchivedState("archive", current, schema.SchemaSnapshot{})
			require.NoError(t, err)
			require.Len(t, resolution.CandidateGroups, 1)
			candidate := resolution.CandidateGroups[0]
			oldMarkerText := newRequest.Groups[0].FinalizedMarker
			if tc.moveBeforeResume {
				candidate.State = archivedCandidateGroupStatePartialResumable
				candidate.Resume.RemainingMarkerUpdates =
					markerUpdatesForSchemas(candidate.SchemaNames, oldMarkerText)
			}
			finalMarker := oldMarker
			finalMarker.CleanupDigest = cleanupOperationDigest("sha256:" + strings.Repeat("2", 64))
			finalMarkerText, err := marshalArchivalMarker(finalMarker)
			require.NoError(t, err)
			resumeRequest := plainTableArchivalRequest{
				CurrentInventory: current.Inventory,
				Groups: []plainTableArchivalGroupRequest{{
					FinalizedMarker: finalMarkerText, Resume: &candidate.Resume,
				}},
				SourcePreflight: sourceSafetyPreflightResult{
					ValidatedTableRelationOIDs: []uint32{relation.OID},
				},
				DependencyClosure: archivedDependencyClosureResult{
					ValidatedGroupIDs: []archivalGroupID{candidate.GroupID},
					DependencyValidatedCandidateGroups: []dependencyValidatedArchivedCandidateGroup{{
						Candidate: candidate,
					}},
				},
			}
			resumeStatements, err := generatePlainTableArchivalStatements(resumeRequest)
			require.NoError(t, err)
			for _, statement := range resumeStatements {
				assert.NotContains(t, statement.DDL, "CREATE SCHEMA")
				_, err = db.ConnPool.Exec(t.Context(), statement.ToSQL())
				require.NoError(t, err)
			}
			assert.Equal(t, relation.OID, postgresRelationOID(t, db.ConnPool,
				oldMarker.Members[0].CleanupTable.SchemaName, "resume_accounts"))
			assertPlainTableMarkerComments(t, db.ConnPool, finalMarker, finalMarkerText)
			for _, schemaName := range archivedMarkerSchemaNames(finalMarker) {
				assert.True(t, postgresSchemaExists(t, db.ConnPool, schemaName))
			}
		})
	}
}

func plainTableArchivalDatabaseRequest(
	t *testing.T,
	inventory schema.CatalogInventory,
	relation schema.CatalogRelation,
	groupID archivalGroupID,
) (plainTableArchivalRequest, archivalMarkerV1) {
	t.Helper()
	timestamp, nonce, err := parseArchivalGroupID(groupID)
	require.NoError(t, err)
	cleanupSchema, err := buildArchivalSchemaName(
		"archive", relation.SchemaName, relation.Name, timestamp, nonce,
	)
	require.NoError(t, err)
	dependencySchema, err := buildArchivalDependencySchemaName("archive", timestamp, nonce)
	require.NoError(t, err)
	move, err := inventory.ExpectedTableMove(relation.OID)
	require.NoError(t, err)
	marker := archivalMarkerV1{
		Version: archivalMarkerVersion, GroupID: groupID,
		Members: []archivalMarkerMemberV1{{
			MemberID: fmt.Sprintf("member-%d", relation.OID),
			SourceTable: markerObject(
				relation.OID, archivalMarkerObjectKindTable, relation.SchemaName, relation.Name,
			),
			CleanupTable: markerObject(
				relation.OID, archivalMarkerObjectKindTable, cleanupSchema, relation.Name,
			),
			AutomaticallyMovedObjects: markerObjectsFromCatalog(move.CleanupSchemaObjects, cleanupSchema),
			AttachedObjects:           markerObjectsFromCatalog(move.AttachedObjects, cleanupSchema),
			InternalToastObjects:      markerObjectsFromCatalog(move.InternalObjects, ""),
		}},
		ExclusiveDependencySchemas: []archivalMarkerSchemaIdentity{{Name: dependencySchema}},
		CleanupDigest:              cleanupOperationDigest("sha256:" + strings.Repeat("0", 64)),
	}
	markerText, err := marshalArchivalMarker(marker)
	require.NoError(t, err)
	allocation := &archivalGroupNameAllocation{
		GroupID: groupID, Timestamp: timestamp, Nonce: nonce,
		DependencySchemaName: dependencySchema, EscapedDependencySchemaName: schema.EscapeIdentifier(dependencySchema),
		Members: []archivalPhysicalMemberNameAllocation{{
			RelationOID: relation.OID, SourceSchemaName: relation.SchemaName, SourceTableName: relation.Name,
			CleanupSchemaName: cleanupSchema, EscapedCleanupSchemaName: schema.EscapeIdentifier(cleanupSchema),
		}},
	}
	return plainTableArchivalRequest{
		CurrentInventory: inventory,
		Groups: []plainTableArchivalGroupRequest{{
			Allocation: allocation, FinalizedMarker: markerText,
		}},
		SourcePreflight: sourceSafetyPreflightResult{ValidatedTableRelationOIDs: []uint32{relation.OID}},
		DependencyClosure: archivedDependencyClosureResult{
			ValidatedGroupIDs: []archivalGroupID{groupID},
		},
	}, marker
}

func assertPlainTableMarkerComments(
	t *testing.T,
	pool *pgxpool.Pool,
	marker archivalMarkerV1,
	expected string,
) {
	t.Helper()
	for _, schemaName := range archivedMarkerSchemaNames(marker) {
		var comment string
		err := pool.QueryRow(t.Context(), `
			SELECT pg_catalog.obj_description(n.oid, 'pg_namespace')
			FROM pg_catalog.pg_namespace AS n
			WHERE n.nspname = $1
		`, schemaName).Scan(&comment)
		require.NoError(t, err)
		assert.Equal(t, expected, comment)
	}
}

func assertPlainTableSchemasLockedDown(t *testing.T, pool *pgxpool.Pool, marker archivalMarkerV1) {
	t.Helper()
	for _, schemaName := range archivedMarkerSchemaNames(marker) {
		var nonOwnerGrants int
		err := pool.QueryRow(t.Context(), `
			SELECT count(*)
			FROM pg_catalog.pg_namespace AS n
			CROSS JOIN LATERAL pg_catalog.aclexplode(
				COALESCE(n.nspacl, pg_catalog.acldefault('n', n.nspowner))
			) AS acl
			WHERE n.nspname = $1
				AND acl.grantee <> n.nspowner
		`, schemaName).Scan(&nonOwnerGrants)
		require.NoError(t, err)
		assert.Zero(t, nonOwnerGrants)
	}
}

func postgresRelationOID(t *testing.T, pool *pgxpool.Pool, schemaName, relationName string) uint32 {
	t.Helper()
	var oid uint32
	err := pool.QueryRow(t.Context(), `
		SELECT COALESCE((
			SELECT c.oid
			FROM pg_catalog.pg_class AS c
			JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2
		), 0)::oid
	`, schemaName, relationName).Scan(&oid)
	require.NoError(t, err)
	return oid
}

func postgresSchemaExists(t *testing.T, pool *pgxpool.Pool, schemaName string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(t.Context(), `
		SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1)
	`, schemaName).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func queryArchivedRows(
	t *testing.T,
	pool *pgxpool.Pool,
	cleanupSchema string,
) ([]struct {
	TenantID       int64
	BaseValue      int
	GeneratedValue int
	Payload        string
}, error,
) {
	t.Helper()
	rows, err := pool.Query(t.Context(), fmt.Sprintf(
		"SELECT tenant_id, base_value, generated_value, payload FROM %s.accounts ORDER BY tenant_id",
		schema.EscapeIdentifier(cleanupSchema),
	))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []struct {
		TenantID       int64
		BaseValue      int
		GeneratedValue int
		Payload        string
	}
	for rows.Next() {
		var row struct {
			TenantID       int64
			BaseValue      int
			GeneratedValue int
			Payload        string
		}
		if err := rows.Scan(&row.TenantID, &row.BaseValue, &row.GeneratedValue, &row.Payload); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func assertPlainTableMovedIdentities(
	t *testing.T,
	before schema.CatalogInventory,
	after schema.CatalogInventory,
	member archivalMarkerMemberV1,
) {
	t.Helper()
	beforeMove, err := before.ExpectedTableMove(member.SourceTable.OID)
	require.NoError(t, err)
	afterMove, err := after.ExpectedTableMove(member.SourceTable.OID)
	require.NoError(t, err)
	assert.NoError(t, validateExactMarkerObjectSet("automatically moved",
		member.AutomaticallyMovedObjects,
		markerObjectsFromCatalog(afterMove.CleanupSchemaObjects, member.CleanupTable.SchemaName)))
	assert.NoError(t, validateExactMarkerObjectSet("attached", member.AttachedObjects,
		markerObjectsFromCatalog(afterMove.AttachedObjects, member.CleanupTable.SchemaName)))
	assert.NoError(t, validateExactMarkerObjectSet("TOAST", member.InternalToastObjects,
		markerObjectsFromCatalog(afterMove.InternalObjects, "")))
	assert.Equal(t, catalogObjectOIDs(beforeMove.CleanupSchemaObjects),
		catalogObjectOIDs(afterMove.CleanupSchemaObjects))
	assert.Equal(t, catalogObjectOIDs(beforeMove.AttachedObjects),
		catalogObjectOIDs(afterMove.AttachedObjects))
	assert.Equal(t, catalogObjectOIDs(beforeMove.InternalObjects),
		catalogObjectOIDs(afterMove.InternalObjects))
}

func assertPlainTableAttachedMetadata(
	t *testing.T,
	before schema.CatalogInventory,
	after schema.CatalogInventory,
	relationOID uint32,
) {
	t.Helper()
	assert.Equal(t, catalogTableByRelationOID(before, relationOID),
		catalogTableByRelationOID(after, relationOID))
	beforeColumns := catalogColumnsByRelationOID(before, relationOID)
	afterColumns := catalogColumnsByRelationOID(after, relationOID)
	assert.Equal(t, columnsWithDefaults(beforeColumns), columnsWithDefaults(afterColumns))
	for idx := range beforeColumns {
		beforeColumns[idx].DefaultExpression = ""
		afterColumns[idx].DefaultExpression = ""
	}
	assert.Equal(t, beforeColumns, afterColumns)
	assert.Equal(t, catalogTriggerOIDs(before, relationOID), catalogTriggerOIDs(after, relationOID))
	assert.Equal(t, catalogRuleOIDs(before, relationOID), catalogRuleOIDs(after, relationOID))
	assert.Equal(t, catalogPolicyOIDs(before, relationOID), catalogPolicyOIDs(after, relationOID))
	assert.Equal(t, catalogConstraintOIDs(before, relationOID),
		catalogConstraintOIDs(after, relationOID))
	assert.Equal(t, catalogOwnedSequences(before, relationOID),
		catalogOwnedSequences(after, relationOID))
	assert.Equal(t, catalogSecurityLabels(before, relationOID),
		catalogSecurityLabels(after, relationOID))
	for _, constraint := range before.Constraints {
		if constraint.RelationOID == relationOID {
			assert.Equal(t, constraint.Comment, catalogConstraintByOID(after, constraint.OID).Comment)
		}
	}
	for _, index := range before.Indexes {
		if index.RelationOID == relationOID {
			assert.Equal(t, mustCatalogRelationByOID(t, before, index.OID).Comment,
				mustCatalogRelationByOID(t, after, index.OID).Comment)
		}
	}
	beforeRelation := mustCatalogRelationByOID(t, before, relationOID)
	afterRelation := mustCatalogRelationByOID(t, after, relationOID)
	assert.Equal(t, beforeRelation.Comment, afterRelation.Comment)
	assert.Equal(t, beforeRelation.RowTypeOID, afterRelation.RowTypeOID)
	assert.Equal(t, beforeRelation.ArrayTypeOID, afterRelation.ArrayTypeOID)
	assert.Equal(t, beforeRelation.ToastRelation, afterRelation.ToastRelation)
}

func assertPlainTableSourceNamesFreed(
	t *testing.T,
	pool *pgxpool.Pool,
	member archivalMarkerMemberV1,
) {
	t.Helper()
	for _, object := range member.AutomaticallyMovedObjects {
		var exists bool
		switch object.Kind {
		case archivalMarkerObjectKindTable, archivalMarkerObjectKindIndex,
			archivalMarkerObjectKindOwnedSequence:
			err := pool.QueryRow(t.Context(), `
				SELECT EXISTS (
					SELECT 1 FROM pg_catalog.pg_class AS c
					JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
					WHERE n.nspname = $1 AND c.relname = $2
				)
			`, member.SourceTable.SchemaName, object.Name).Scan(&exists)
			require.NoError(t, err)
		case archivalMarkerObjectKindRowType, archivalMarkerObjectKindArrayType:
			err := pool.QueryRow(t.Context(), `
				SELECT EXISTS (
					SELECT 1 FROM pg_catalog.pg_type AS ty
					JOIN pg_catalog.pg_namespace AS n ON n.oid = ty.typnamespace
					WHERE n.nspname = $1 AND ty.typname = $2
				)
			`, member.SourceTable.SchemaName, object.Name).Scan(&exists)
			require.NoError(t, err)
		default:
			continue
		}
		assert.False(t, exists, "%s %s remains in the source namespace", object.Kind, object.Name)
	}
}

func markerUpdatesForSchemas(schemaNames []string, marker string) []archivedMarkerUpdateDescriptor {
	updates := make([]archivedMarkerUpdateDescriptor, 0, len(schemaNames))
	for _, schemaName := range schemaNames {
		updates = append(updates, archivedMarkerUpdateDescriptor{SchemaName: schemaName, Marker: marker})
	}
	return updates
}

func catalogObjectOIDs(objects []schema.CatalogObjectIdentity) []uint32 {
	oids := make([]uint32, 0, len(objects))
	for _, object := range objects {
		oids = append(oids, object.OID)
	}
	slices.Sort(oids)
	return oids
}

func catalogTableByRelationOID(inventory schema.CatalogInventory, relationOID uint32) schema.CatalogTable {
	for _, table := range inventory.Tables {
		if table.RelationOID == relationOID {
			return table
		}
	}
	return schema.CatalogTable{}
}

func catalogColumnsByRelationOID(inventory schema.CatalogInventory, relationOID uint32) []schema.CatalogColumn {
	var result []schema.CatalogColumn
	for _, column := range inventory.Columns {
		if column.RelationOID == relationOID {
			result = append(result, column)
		}
	}
	return result
}

func columnsWithDefaults(columns []schema.CatalogColumn) []string {
	var result []string
	for _, column := range columns {
		if column.DefaultExpression != "" || column.IdentityMode != "" {
			result = append(result, column.Name)
		}
	}
	return result
}

func catalogTriggerOIDs(inventory schema.CatalogInventory, relationOID uint32) []uint32 {
	var result []uint32
	for _, trigger := range inventory.Triggers {
		if trigger.RelationOID == relationOID {
			result = append(result, trigger.OID)
		}
	}
	slices.Sort(result)
	return result
}

func catalogRuleOIDs(inventory schema.CatalogInventory, relationOID uint32) []uint32 {
	var result []uint32
	for _, rule := range inventory.Rules {
		if rule.RelationOID == relationOID {
			result = append(result, rule.OID)
		}
	}
	slices.Sort(result)
	return result
}

func catalogPolicyOIDs(inventory schema.CatalogInventory, relationOID uint32) []uint32 {
	var result []uint32
	for _, policy := range inventory.Policies {
		if policy.RelationOID == relationOID {
			result = append(result, policy.OID)
		}
	}
	slices.Sort(result)
	return result
}

func catalogConstraintOIDs(inventory schema.CatalogInventory, relationOID uint32) []uint32 {
	var result []uint32
	for _, constraint := range inventory.Constraints {
		if constraint.RelationOID == relationOID {
			result = append(result, constraint.OID)
		}
	}
	slices.Sort(result)
	return result
}

func catalogConstraintByOID(inventory schema.CatalogInventory, oid uint32) schema.CatalogConstraint {
	for _, constraint := range inventory.Constraints {
		if constraint.OID == oid {
			return constraint
		}
	}
	return schema.CatalogConstraint{}
}

func catalogOwnedSequences(
	inventory schema.CatalogInventory,
	relationOID uint32,
) []schema.CatalogOwnedSequence {
	var result []schema.CatalogOwnedSequence
	for _, owned := range inventory.OwnedSequences {
		if owned.RelationOID == relationOID {
			result = append(result, owned)
		}
	}
	return result
}

func catalogSecurityLabels(
	inventory schema.CatalogInventory,
	relationOID uint32,
) []schema.CatalogSecurityLabel {
	var result []schema.CatalogSecurityLabel
	for _, label := range inventory.SecurityLabels {
		if label.RelationOID == relationOID {
			result = append(result, label)
		}
	}
	return result
}
