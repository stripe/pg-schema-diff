package diff

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/testdb"
)

func TestArchivalComponentInitializationPostgresAtomicNewGroupsAndRetry(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	currentDB := factory.CreateDatabase(t)
	targetDB := factory.CreateDatabase(t)
	_, err := currentDB.ConnPool.Exec(t.Context(), `
		CREATE TYPE public.review_shared_status AS ENUM ('active', 'disabled');
		CREATE TABLE public.review_first (id bigint PRIMARY KEY, status public.review_shared_status);
		CREATE TABLE public.review_second (id bigint PRIMARY KEY, status public.review_shared_status);
	`)
	require.NoError(t, err)

	plan, err := Generate(t.Context(), DBSchemaSource(currentDB.ConnPool),
		DBSchemaSource(targetDB.ConnPool), WithDoNotValidatePlan())
	require.NoError(t, err)
	require.NotEmpty(t, plan.Statements)
	initialization := plan.Statements[0]
	assert.Equal(t, 4, strings.Count(initialization.DDL, "CREATE SCHEMA"))
	assert.Equal(t, 4, strings.Count(initialization.DDL, "COMMENT ON SCHEMA"))

	failedDDL := archivalDOWithFailure(t, initialization.DDL)
	_, err = currentDB.ConnPool.Exec(t.Context(), Statement{DDL: failedDDL}.ToSQL())
	require.ErrorContains(t, err, "review component initialization failure")
	assert.Equal(t, 0, postgresReviewArchivalSchemaCount(t, currentDB.ConnPool))

	_, err = currentDB.ConnPool.Exec(t.Context(), initialization.ToSQL())
	require.NoError(t, err)
	assert.Equal(t, 4, postgresReviewArchivalSchemaCount(t, currentDB.ConnPool))
	assert.NotZero(t, postgresRelationOID(t, currentDB.ConnPool, "public", "review_first"))
	assert.NotZero(t, postgresRelationOID(t, currentDB.ConnPool, "public", "review_second"))

	resumed, err := Generate(t.Context(), DBSchemaSource(currentDB.ConnPool),
		DBSchemaSource(targetDB.ConnPool), WithDoNotValidatePlan())
	require.NoError(t, err)
	require.NotEmpty(t, resumed.Statements)
	assert.NotContains(t, resumed.Statements[0].DDL, "CREATE SCHEMA")
	executeReviewStatements(t, currentDB.ConnPool, resumed.Statements)

	complete, err := Generate(t.Context(), DBSchemaSource(currentDB.ConnPool),
		DBSchemaSource(targetDB.ConnPool), WithDoNotValidatePlan())
	require.NoError(t, err)
	assert.Empty(t, complete.Statements)
}

func TestArchivalComponentInitializationPostgresAtomicNewAndExistingGroups(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	currentDB := factory.CreateDatabase(t)
	firstTargetDB := factory.CreateDatabase(t)
	emptyTargetDB := factory.CreateDatabase(t)
	_, err := currentDB.ConnPool.Exec(t.Context(), `
		CREATE TYPE public.review_existing_status AS ENUM ('active', 'disabled');
		CREATE TABLE public.review_existing_old (id bigint PRIMARY KEY, status public.review_existing_status);
		CREATE TABLE public.review_existing_new (id bigint PRIMARY KEY, status public.review_existing_status);
	`)
	require.NoError(t, err)
	_, err = firstTargetDB.ConnPool.Exec(t.Context(), `
		CREATE TYPE public.review_existing_status AS ENUM ('active', 'disabled');
		CREATE TABLE public.review_existing_new (id bigint PRIMARY KEY, status public.review_existing_status);
	`)
	require.NoError(t, err)

	firstPlan, err := Generate(t.Context(), DBSchemaSource(currentDB.ConnPool),
		DBSchemaSource(firstTargetDB.ConnPool), WithDoNotValidatePlan())
	require.NoError(t, err)
	executeReviewStatements(t, currentDB.ConnPool, firstPlan.Statements)
	assert.Equal(t, 2, postgresReviewArchivalSchemaCount(t, currentDB.ConnPool))
	oldComments := postgresReviewArchivalSchemaComments(t, currentDB.ConnPool)

	sharedPlan, err := Generate(t.Context(), DBSchemaSource(currentDB.ConnPool),
		DBSchemaSource(emptyTargetDB.ConnPool), WithDoNotValidatePlan())
	require.NoError(t, err)
	require.NotEmpty(t, sharedPlan.Statements)
	initialization := sharedPlan.Statements[0]
	assert.Equal(t, 2, strings.Count(initialization.DDL, "CREATE SCHEMA"))
	assert.Equal(t, 4, strings.Count(initialization.DDL, "COMMENT ON SCHEMA"))

	_, err = currentDB.ConnPool.Exec(t.Context(), Statement{
		DDL: archivalDOWithFailure(t, initialization.DDL),
	}.ToSQL())
	require.ErrorContains(t, err, "review component initialization failure")
	assert.Equal(t, 2, postgresReviewArchivalSchemaCount(t, currentDB.ConnPool))
	assert.Equal(t, oldComments, postgresReviewArchivalSchemaComments(t, currentDB.ConnPool))

	retryAfterFailure, err := Generate(t.Context(), DBSchemaSource(currentDB.ConnPool),
		DBSchemaSource(emptyTargetDB.ConnPool), WithDoNotValidatePlan())
	require.NoError(t, err)
	require.NotEmpty(t, retryAfterFailure.Statements)

	_, err = currentDB.ConnPool.Exec(t.Context(), initialization.ToSQL())
	require.NoError(t, err)
	assert.Equal(t, 4, postgresReviewArchivalSchemaCount(t, currentDB.ConnPool))

	resumed, err := Generate(t.Context(), DBSchemaSource(currentDB.ConnPool),
		DBSchemaSource(emptyTargetDB.ConnPool), WithDoNotValidatePlan())
	require.NoError(t, err)
	require.NotEmpty(t, resumed.Statements)
	assert.NotContains(t, resumed.Statements[0].DDL, "CREATE SCHEMA")
	executeReviewStatements(t, currentDB.ConnPool, resumed.Statements)

	complete, err := Generate(t.Context(), DBSchemaSource(currentDB.ConnPool),
		DBSchemaSource(emptyTargetDB.ConnPool), WithDoNotValidatePlan())
	require.NoError(t, err)
	assert.Empty(t, complete.Statements)
}

func TestArchivalACLIsolationPostgresResumesAfterMove(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	roleNames := []string{"review_acl_grantor", "review_acl_reader"}
	roleGuard := factory.LockRoles(t, roleNames...)
	roleGuard.CreateRoles()
	currentDB := factory.CreateDatabase(t)
	targetDB := factory.CreateDatabase(t)
	_, err := currentDB.ConnPool.Exec(t.Context(), `
		CREATE TABLE public.review_acl_resume (id bigint PRIMARY KEY, payload text);
		GRANT SELECT ON public.review_acl_resume TO review_acl_grantor WITH GRANT OPTION;
		SET ROLE review_acl_grantor;
		GRANT SELECT ON public.review_acl_resume TO review_acl_reader;
		RESET ROLE;
	`)
	require.NoError(t, err)

	initial, err := Generate(t.Context(), DBSchemaSource(currentDB.ConnPool),
		DBSchemaSource(targetDB.ConnPool), WithDoNotValidatePlan())
	require.NoError(t, err)
	moveIndex := statementIndexContaining(t, initial.Statements,
		`ALTER TABLE "public"."review_acl_resume" SET SCHEMA`)
	executeReviewStatements(t, currentDB.ConnPool, initial.Statements[:moveIndex+1])

	resumed, err := Generate(t.Context(), DBSchemaSource(currentDB.ConnPool),
		DBSchemaSource(targetDB.ConnPool), WithDoNotValidatePlan())
	require.NoError(t, err)
	readerRevoke := statementIndexContaining(t, resumed.Statements,
		`FROM "review_acl_reader" RESTRICT`)
	grantOptionRevoke := statementIndexContaining(t, resumed.Statements,
		`REVOKE GRANT OPTION FOR SELECT ON TABLE`)
	grantorRevoke := lastStatementIndexContaining(t, resumed.Statements,
		`FROM "review_acl_grantor" RESTRICT`)
	assert.Less(t, readerRevoke, grantOptionRevoke)
	assert.Less(t, grantOptionRevoke, grantorRevoke)

	_, err = currentDB.ConnPool.Exec(t.Context(), resumed.Statements[readerRevoke].ToSQL())
	require.NoError(t, err)
	partiallyIsolated, err := Generate(t.Context(), DBSchemaSource(currentDB.ConnPool),
		DBSchemaSource(targetDB.ConnPool), WithDoNotValidatePlan())
	require.NoError(t, err)
	assert.NotContains(t, replacementAwareDDL(partiallyIsolated.Statements),
		`FROM "review_acl_reader" RESTRICT`)
	assert.Contains(t, replacementAwareDDL(partiallyIsolated.Statements),
		`FROM "review_acl_grantor" RESTRICT`)
	executeReviewStatements(t, currentDB.ConnPool, partiallyIsolated.Statements)

	complete, err := Generate(t.Context(), DBSchemaSource(currentDB.ConnPool),
		DBSchemaSource(targetDB.ConnPool), WithDoNotValidatePlan())
	require.NoError(t, err)
	assert.Empty(t, complete.Statements)
}

func TestArchivalDispositionPostgresSameNameReplacement(t *testing.T) {
	factory := testdb.MustNewFactory(t)
	for _, testCase := range []struct {
		name       string
		targetDDL  string
		assertPlan func(*testing.T, []Statement)
	}{
		{
			name: "unchanged",
			targetDDL: reviewReplacementTableDDL + reviewReplacementIndexDDL +
				reviewReplacementTriggerDDL,
			assertPlan: func(t *testing.T, statements []Statement) {
				ddl := replacementAwareDDL(statements)
				assert.NotContains(t, ddl, "DROP INDEX")
				assert.NotContains(t, ddl, "DROP TRIGGER")
			},
		},
		{
			name: "modified",
			targetDDL: reviewReplacementTableDDL +
				`CREATE INDEX review_replacement_payload_idx ON public.review_replacement (payload, id);` +
				`CREATE TRIGGER review_replacement_trigger BEFORE INSERT OR UPDATE ON public.review_replacement ` +
				`FOR EACH ROW EXECUTE FUNCTION pg_catalog.tsvector_update_trigger(search, 'pg_catalog.english', payload);`,
			assertPlan: func(t *testing.T, statements []Statement) {
				ddl := replacementAwareDDL(statements)
				assert.Contains(t, ddl, "DROP INDEX")
				assert.Contains(t, ddl, "CREATE INDEX CONCURRENTLY review_replacement_payload_idx")
				assert.NotContains(t, ddl, "DROP TRIGGER")
				assert.Contains(t, ddl,
					"CREATE OR REPLACE TRIGGER review_replacement_trigger BEFORE INSERT OR UPDATE")
			},
		},
		{
			name:      "deleted and rearchived",
			targetDDL: "",
			assertPlan: func(t *testing.T, statements []Statement) {
				ddl := replacementAwareDDL(statements)
				assert.Contains(t, ddl, `ALTER TABLE "public"."review_replacement" SET SCHEMA`)
				assert.NotContains(t, ddl, "DROP TABLE")
				assert.NotContains(t, ddl, "DROP INDEX")
				assert.NotContains(t, ddl, "DROP TRIGGER")
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			roleName := "review_disposition_" + strings.ReplaceAll(testCase.name, " ", "_")
			roleGuard := factory.LockRoles(t, roleName)
			roleGuard.CreateRoles()
			currentDB := factory.CreateDatabase(t)
			archiveTargetDB := factory.CreateDatabase(t)
			targetDB := factory.CreateDatabase(t)
			_, err := currentDB.ConnPool.Exec(t.Context(), reviewReplacementTableDDL+
				reviewReplacementIndexDDL+reviewReplacementTriggerDDL+
				fmt.Sprintf(`GRANT SELECT ON public.review_replacement TO %s;`, roleName))
			require.NoError(t, err)
			_, err = targetDB.ConnPool.Exec(t.Context(), testCase.targetDDL)
			require.NoError(t, err)

			originalOID := postgresRelationOID(t, currentDB.ConnPool, "public", "review_replacement")
			initial, err := Generate(t.Context(), DBSchemaSource(currentDB.ConnPool),
				DBSchemaSource(archiveTargetDB.ConnPool), WithDoNotValidatePlan())
			require.NoError(t, err)
			moveIndex := statementIndexContaining(t, initial.Statements,
				`ALTER TABLE "public"."review_replacement" SET SCHEMA`)
			executeReviewStatements(t, currentDB.ConnPool, initial.Statements[:moveIndex+1])
			_, err = currentDB.ConnPool.Exec(t.Context(), reviewReplacementTableDDL+
				reviewReplacementIndexDDL+reviewReplacementTriggerDDL)
			require.NoError(t, err)
			replacementOID := postgresRelationOID(t, currentDB.ConnPool, "public", "review_replacement")
			require.NotEqual(t, originalOID, replacementOID)

			plan, err := Generate(t.Context(), DBSchemaSource(currentDB.ConnPool),
				DBSchemaSource(targetDB.ConnPool), WithDoNotValidatePlan())
			require.NoError(t, err)
			testCase.assertPlan(t, plan.Statements)
			assertReplacementAwareStatementsAreNonDestructive(t, plan.Statements)
			executeReviewStatements(t, currentDB.ConnPool, plan.Statements)

			if testCase.name == "deleted and rearchived" {
				assert.Zero(t, postgresRelationOID(t, currentDB.ConnPool, "public", "review_replacement"))
				var archivedCount int
				err = currentDB.ConnPool.QueryRow(t.Context(), `
					SELECT count(*)
					FROM pg_catalog.pg_class AS c
					JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
					WHERE n.nspname LIKE 'pgschemadiff_archive_%'
					  AND c.relname = 'review_replacement'
					  AND c.relkind IN ('r', 'p')
				`).Scan(&archivedCount)
				require.NoError(t, err)
				assert.Equal(t, 2, archivedCount)
			} else {
				assert.Equal(t, replacementOID,
					postgresRelationOID(t, currentDB.ConnPool, "public", "review_replacement"))
				assert.NotZero(t, postgresRelationOID(t, currentDB.ConnPool, "public",
					"review_replacement_payload_idx"))
			}
		})
	}
}

const (
	reviewReplacementTableDDL = `
		CREATE TABLE public.review_replacement (
			id bigint PRIMARY KEY,
			payload text,
			search tsvector
		);
	`
	reviewReplacementIndexDDL = `
		CREATE INDEX review_replacement_payload_idx ON public.review_replacement (payload);
	`
	reviewReplacementTriggerDDL = `
		CREATE TRIGGER review_replacement_trigger BEFORE INSERT ON public.review_replacement
			FOR EACH ROW EXECUTE FUNCTION
				pg_catalog.tsvector_update_trigger(search, 'pg_catalog.english', payload);
	`
)

func archivalDOWithFailure(t *testing.T, ddl string) string {
	t.Helper()
	end := strings.LastIndex(ddl, "\nEND\n$")
	require.NotEqual(t, -1, end)
	return ddl[:end] + "\n    RAISE EXCEPTION 'review component initialization failure';" + ddl[end:]
}

func postgresReviewArchivalSchemaCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	err := pool.QueryRow(t.Context(), `
		SELECT count(*) FROM pg_catalog.pg_namespace
		WHERE nspname LIKE 'pgschemadiff_archive_%'
	`).Scan(&count)
	require.NoError(t, err)
	return count
}

func postgresReviewArchivalSchemaComments(t *testing.T, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	rows, err := pool.Query(t.Context(), `
		SELECT nspname, pg_catalog.obj_description(oid, 'pg_namespace')
		FROM pg_catalog.pg_namespace
		WHERE nspname LIKE 'pgschemadiff_archive_%'
		ORDER BY nspname
	`)
	require.NoError(t, err)
	defer rows.Close()
	result := make(map[string]string)
	for rows.Next() {
		var name, comment string
		require.NoError(t, rows.Scan(&name, &comment))
		result[name] = comment
	}
	require.NoError(t, rows.Err())
	return result
}

func executeReviewStatements(t *testing.T, pool *pgxpool.Pool, statements []Statement) {
	t.Helper()
	for _, statement := range statements {
		_, err := pool.Exec(t.Context(), statement.ToSQL())
		require.NoErrorf(t, err, "executing statement:\n%s", statement.DDL)
	}
}
