package diff

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/internal/testdb"
)

func TestReplacementAwareRegularGraphPostgresOrdinaryRemoval(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	currentDB := factory.CreateDatabase(t)
	targetDB := factory.CreateDatabase(t)
	_, err := currentDB.ConnPool.Exec(t.Context(), `
		CREATE FUNCTION public.stage13_removed_trigger_fn() RETURNS trigger
			LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END';
		CREATE TABLE public.stage13_removed (id BIGSERIAL PRIMARY KEY, payload TEXT NOT NULL);
		CREATE INDEX stage13_removed_payload_idx ON public.stage13_removed (payload);
		CREATE TRIGGER stage13_removed_trigger BEFORE INSERT ON public.stage13_removed
			FOR EACH ROW EXECUTE FUNCTION public.stage13_removed_trigger_fn();
		CREATE RULE stage13_removed_rule AS ON UPDATE TO public.stage13_removed DO ALSO NOTHING;
		ALTER TABLE public.stage13_removed ENABLE ROW LEVEL SECURITY;
		CREATE POLICY stage13_removed_policy ON public.stage13_removed USING (id > 0);
		COMMENT ON TABLE public.stage13_removed IS 'retained ordinary removal';
		INSERT INTO public.stage13_removed (payload) VALUES ('retained');
	`)
	require.NoError(t, err)
	_, err = targetDB.ConnPool.Exec(t.Context(), `
		CREATE FUNCTION public.stage13_removed_trigger_fn() RETURNS trigger
			LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END';
	`)
	require.NoError(t, err)

	current, err := schema.GetSchemaSnapshot(t.Context(), currentDB.ConnPool)
	require.NoError(t, err)
	target, err := schema.GetSchemaSnapshot(t.Context(), targetDB.ConnPool)
	require.NoError(t, err)
	original := mustCatalogRelationByName(t, current.Inventory, "public", "stage13_removed")
	request, marker := plainTableArchivalDatabaseRequest(
		t, current.Inventory, original, "20260721T091011123456Z_12345678",
	)
	schemaDiff, _, err := buildSchemaDiff(current.Schema, target.Schema)
	require.NoError(t, err)
	statements, err := generateReplacementAwareSchemaSQL(
		newSchemaSQLGenerator(&deterministicRandReader{}, &planOptions{}),
		schemaDiff,
		tableDispositions{current.Schema.Tables[0].GetName(): {
			Kind: tableDispositionKindArchivalMove, GroupID: marker.GroupID, RelationOID: original.OID,
		}},
		request,
	)
	require.NoError(t, err)
	assertReplacementAwareStatementsAreNonDestructive(t, statements)
	assert.NotContains(t, replacementAwareDDL(statements), "DROP ")
	assert.Contains(t, statements[len(statements)-1].DDL, "archival marker mismatch")

	for _, statement := range statements {
		_, err = currentDB.ConnPool.Exec(t.Context(), statement.ToSQL())
		require.NoErrorf(t, err, "executing statement:\n%s", statement.DDL)
	}
	cleanupSchema := marker.Members[0].CleanupTable.SchemaName
	assert.Zero(t, postgresRelationOID(t, currentDB.ConnPool, "public", "stage13_removed"))
	assert.Equal(t, original.OID,
		postgresRelationOID(t, currentDB.ConnPool, cleanupSchema, "stage13_removed"))
	after, err := schema.GetSchemaSnapshot(t.Context(), currentDB.ConnPool)
	require.NoError(t, err)
	assertPlainTableMovedIdentities(t, current.Inventory, after.Inventory, marker.Members[0])
	assertPlainTableAttachedMetadata(t, current.Inventory, after.Inventory, original.OID)
}

func TestReplacementAwareRegularGraphPostgresMoveThenRecreate(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	currentDB := factory.CreateDatabase(t)
	targetDB := factory.CreateDatabase(t)

	_, err := currentDB.ConnPool.Exec(t.Context(), `
		CREATE FUNCTION public.stage13_accounts_trigger_fn() RETURNS trigger
			LANGUAGE plpgsql AS 'BEGIN NEW.payload := NEW.payload || ''-triggered''; RETURN NEW; END';
		CREATE TABLE public.stage13_accounts (
			id BIGSERIAL NOT NULL,
			payload TEXT NOT NULL,
			CONSTRAINT stage13_accounts_payload_check CHECK (length(payload) > 0)
		);
		CREATE INDEX stage13_accounts_payload_idx ON public.stage13_accounts (payload);
		ALTER TABLE public.stage13_accounts ENABLE ROW LEVEL SECURITY;
		CREATE POLICY stage13_accounts_policy ON public.stage13_accounts USING (id > 0);
		CREATE TRIGGER stage13_accounts_trigger BEFORE INSERT OR UPDATE ON public.stage13_accounts
			FOR EACH ROW EXECUTE FUNCTION public.stage13_accounts_trigger_fn();
		CREATE RULE stage13_accounts_rule AS ON UPDATE TO public.stage13_accounts DO ALSO NOTHING;
		COMMENT ON TABLE public.stage13_accounts IS 'stage 13 retained table';
		COMMENT ON COLUMN public.stage13_accounts.payload IS 'stage 13 retained column';
		COMMENT ON INDEX public.stage13_accounts_payload_idx IS 'stage 13 retained index';
		COMMENT ON CONSTRAINT stage13_accounts_payload_check ON public.stage13_accounts
			IS 'stage 13 retained constraint';
		INSERT INTO public.stage13_accounts (payload) VALUES ('retained');
	`)
	require.NoError(t, err)

	_, err = targetDB.ConnPool.Exec(t.Context(), `
		CREATE TYPE public.stage13_status AS ENUM ('active', 'disabled');
		CREATE FUNCTION public.stage13_accounts_trigger_fn() RETURNS trigger
			LANGUAGE plpgsql AS 'BEGIN NEW.payload := NEW.payload || ''-triggered''; RETURN NEW; END';
		CREATE TABLE public.stage13_accounts (
			id BIGSERIAL NOT NULL,
			payload TEXT NOT NULL,
			status public.stage13_status NOT NULL DEFAULT 'active',
			CONSTRAINT stage13_accounts_payload_check CHECK (length(payload) > 0)
		) PARTITION BY HASH (id);
		CREATE INDEX stage13_accounts_payload_idx ON public.stage13_accounts (payload);
		ALTER TABLE public.stage13_accounts ENABLE ROW LEVEL SECURITY;
		CREATE POLICY stage13_accounts_policy ON public.stage13_accounts USING (id > 0);
		CREATE TRIGGER stage13_accounts_trigger BEFORE INSERT OR UPDATE ON public.stage13_accounts
			FOR EACH ROW EXECUTE FUNCTION public.stage13_accounts_trigger_fn();
		CREATE SEQUENCE public.stage13_standalone_sequence;
		CREATE VIEW public.stage13_accounts_view AS SELECT id, payload FROM public.stage13_accounts;
		CREATE MATERIALIZED VIEW public.stage13_accounts_materialized AS
			SELECT id, payload FROM public.stage13_accounts;
		CREATE FUNCTION public.stage13_account_id(public.stage13_accounts) RETURNS bigint
			LANGUAGE sql IMMUTABLE RETURN $1.id;
		CREATE PROCEDURE public.stage13_touch_accounts()
			LANGUAGE sql
			BEGIN ATOMIC
				INSERT INTO public.stage13_accounts (payload)
				SELECT 'not-run' WHERE false;
			END;
	`)
	require.NoError(t, err)

	current, err := schema.GetSchemaSnapshot(t.Context(), currentDB.ConnPool)
	require.NoError(t, err)
	target, err := schema.GetSchemaSnapshot(t.Context(), targetDB.ConnPool)
	require.NoError(t, err)
	original := mustCatalogRelationByName(t, current.Inventory, "public", "stage13_accounts")
	request, marker := plainTableArchivalDatabaseRequest(
		t, current.Inventory, original, "20260721T091011123456Z_ABCDEFGH",
	)
	schemaDiff, _, err := buildSchemaDiff(current.Schema, target.Schema)
	require.NoError(t, err)
	statements, err := generateReplacementAwareSchemaSQL(
		newSchemaSQLGenerator(&deterministicRandReader{}, &planOptions{}),
		schemaDiff,
		tableDispositions{current.Schema.Tables[0].GetName(): {
			Kind: tableDispositionKindArchivalMove, GroupID: marker.GroupID, RelationOID: original.OID,
		}},
		request,
	)
	require.NoError(t, err)
	assertReplacementAwareStatementsAreNonDestructive(t, statements)
	assert.Contains(t, statements[len(statements)-1].DDL, "archival marker mismatch")

	moveIndex := statementIndexContaining(t, statements,
		`ALTER TABLE "public"."stage13_accounts" SET SCHEMA`)
	createTableIndex := statementIndexContaining(t, statements,
		`CREATE TABLE "public"."stage13_accounts"`)
	assert.Less(t, moveIndex, createTableIndex)
	assert.Less(t, moveIndex, statementIndexContaining(t, statements,
		`CREATE TYPE "public"."stage13_status"`))
	assert.Less(t, moveIndex, statementIndexContaining(t, statements,
		`CREATE SEQUENCE "public"."stage13_accounts_id_seq"`))
	assert.Less(t, createTableIndex, statementIndexContaining(t, statements,
		"FUNCTION public.stage13_account_id"))
	assert.Less(t, createTableIndex, statementIndexContaining(t, statements,
		"PROCEDURE public.stage13_touch_accounts"))

	for _, statement := range statements {
		_, err = currentDB.ConnPool.Exec(t.Context(), statement.ToSQL())
		require.NoErrorf(t, err, "executing statement:\n%s", statement.DDL)
	}

	cleanupSchema := marker.Members[0].CleanupTable.SchemaName
	assert.Equal(t, original.OID,
		postgresRelationOID(t, currentDB.ConnPool, cleanupSchema, "stage13_accounts"))
	targetOID := postgresRelationOID(t, currentDB.ConnPool, "public", "stage13_accounts")
	assert.NotZero(t, targetOID)
	assert.NotEqual(t, original.OID, targetOID)

	var retainedPayload string
	err = currentDB.ConnPool.QueryRow(t.Context(), fmt.Sprintf(
		"SELECT payload FROM %s.stage13_accounts WHERE id = 1",
		schema.EscapeIdentifier(cleanupSchema),
	)).Scan(&retainedPayload)
	require.NoError(t, err)
	assert.Equal(t, "retained-triggered", retainedPayload)

	after, err := schema.GetSchemaSnapshot(t.Context(), currentDB.ConnPool)
	require.NoError(t, err)
	assertPlainTableMovedIdentities(t, current.Inventory, after.Inventory, marker.Members[0])
	assertPlainTableAttachedMetadata(t, current.Inventory, after.Inventory, original.OID)

	for _, relationName := range []string{
		"stage13_accounts", "stage13_accounts_id_seq", "stage13_accounts_payload_idx",
		"stage13_accounts_view", "stage13_accounts_materialized", "stage13_standalone_sequence",
	} {
		assert.NotZero(t, postgresRelationOID(t, currentDB.ConnPool, "public", relationName), relationName)
	}
	var targetObjectsPresent bool
	err = currentDB.ConnPool.QueryRow(t.Context(), `
		SELECT
			to_regtype('public.stage13_status') IS NOT NULL
			AND to_regprocedure('public.stage13_account_id(public.stage13_accounts)') IS NOT NULL
			AND to_regprocedure('public.stage13_touch_accounts()') IS NOT NULL
	`).Scan(&targetObjectsPresent)
	require.NoError(t, err)
	assert.True(t, targetObjectsPresent)
}
