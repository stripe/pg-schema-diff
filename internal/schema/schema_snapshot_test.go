package schema

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dbsqlc "github.com/stripe/pg-schema-diff/internal/queries"
	"github.com/stripe/pg-schema-diff/internal/testdb"
)

type tracedQuery struct {
	conn *pgx.Conn
	sql  string
}

type snapshotQueryTracer struct {
	mu      sync.Mutex
	queries []tracedQuery
	onQuery func(string)
}

func (t *snapshotQueryTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	t.mu.Lock()
	t.queries = append(t.queries, tracedQuery{conn: conn, sql: data.SQL})
	onQuery := t.onQuery
	t.mu.Unlock()
	if onQuery != nil {
		onQuery(data.SQL)
	}
	return ctx
}

func (*snapshotQueryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (t *snapshotQueryTracer) snapshot() []tracedQuery {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]tracedQuery(nil), t.queries...)
}

func TestGetSchemaSnapshotRollsBackOnCancellation(t *testing.T) {
	t.Parallel()

	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	ctx, cancel := context.WithCancel(t.Context())
	var cancelOnce sync.Once
	tracer := &snapshotQueryTracer{
		onQuery: func(sql string) {
			if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(sql)), "begin") {
				cancelOnce.Do(cancel)
			}
		},
	}
	config := db.ConnPool.Config().Copy()
	config.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	_, err = GetSchemaSnapshot(ctx, pool)
	require.ErrorIs(t, err, context.Canceled)

	queries := tracer.snapshot()
	require.GreaterOrEqual(t, len(queries), 3)
	assert.Equal(t, "begin isolation level repeatable read read only",
		strings.ToLower(strings.TrimSpace(queries[0].sql)))
	assert.Equal(t, "rollback", strings.ToLower(strings.TrimSpace(queries[len(queries)-1].sql)))
}

func TestGetSchemaSnapshotUsesOneReadOnlyRepeatableReadTransaction(t *testing.T) {
	t.Parallel()

	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	_, err := db.ConnPool.Exec(t.Context(), `
		CREATE FUNCTION checked_value(value integer) RETURNS boolean
			LANGUAGE SQL IMMUTABLE RETURN value > 0;
		CREATE TABLE snapshot_test (
			id integer CHECK (checked_value(id))
		);
	`)
	require.NoError(t, err)

	tracer := &snapshotQueryTracer{}
	config := db.ConnPool.Config().Copy()
	config.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	snapshot, err := GetSchemaSnapshot(t.Context(), pool)
	require.NoError(t, err)
	expectedHash, err := snapshot.Schema.Hash()
	require.NoError(t, err)
	assert.Equal(t, snapshot.Schema.Normalize(), snapshot.Schema)
	assert.Equal(t, expectedHash, snapshot.Hash)

	queries := tracer.snapshot()
	require.GreaterOrEqual(t, len(queries), 20)
	assert.Equal(t, "begin isolation level repeatable read read only",
		strings.ToLower(strings.TrimSpace(queries[0].sql)))
	assert.Equal(t, "commit", strings.ToLower(strings.TrimSpace(queries[len(queries)-1].sql)))
	for _, query := range queries[1:] {
		assert.Same(t, queries[0].conn, query.conn,
			"query used a connection outside the snapshot transaction: %s", query.sql)
	}
}

func TestGetSchemaSnapshotRetainsUnfilteredModelForValidationReconstruction(t *testing.T) {
	t.Parallel()

	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	_, err := db.ConnPool.Exec(t.Context(), `
		CREATE SCHEMA managed_snapshot;
		CREATE SCHEMA hidden_snapshot;
		CREATE TABLE managed_snapshot.kept (id bigint);
		CREATE TABLE hidden_snapshot.reconstructed (id bigint);
	`)
	require.NoError(t, err)

	snapshot, err := GetSchemaSnapshot(t.Context(), db.ConnPool,
		WithIncludeSchemaPatterns("managed_snapshot"))
	require.NoError(t, err)
	assert.Len(t, snapshot.Schema.Tables, 1)
	assert.Equal(t, "managed_snapshot", snapshot.Schema.Tables[0].SchemaName)
	assert.Len(t, snapshot.UnfilteredSchema.Tables, 2)
	assert.Contains(t, snapshot.UnfilteredSchema.NamedSchemas, NamedSchema{Name: "hidden_snapshot"})
	expectedHash, err := snapshot.Schema.Hash()
	require.NoError(t, err)
	assert.Equal(t, expectedHash, snapshot.Hash,
		"the dormant reconstruction model must not expand the public schema hash")
}

type catalogQueryGate struct {
	tx pgx.Tx

	firstQueryComplete chan struct{}
	continueQueries    chan struct{}
	once               sync.Once
}

var _ dbsqlc.DBTX = (*catalogQueryGate)(nil)

func (g *catalogQueryGate) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return g.tx.Exec(ctx, sql, arguments...)
}

func (g *catalogQueryGate) Query(ctx context.Context, sql string, arguments ...any) (pgx.Rows, error) {
	rows, err := g.tx.Query(ctx, sql, arguments...)
	if err != nil {
		return nil, err
	}

	wait := false
	g.once.Do(func() {
		wait = true
		close(g.firstQueryComplete)
	})
	if wait {
		select {
		case <-g.continueQueries:
		case <-ctx.Done():
			rows.Close()
			return nil, ctx.Err()
		}
	}
	return rows, nil
}

func (g *catalogQueryGate) QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row {
	return g.tx.QueryRow(ctx, sql, arguments...)
}

func TestSchemaSnapshotIsCoherentDuringConcurrentDDL(t *testing.T) {
	t.Parallel()

	factory := testdb.MustNewFactory(t)
	db := factory.CreateDatabase(t)
	conn, err := db.ConnPool.Acquire(t.Context())
	require.NoError(t, err)
	defer conn.Release()

	tx, err := conn.BeginTx(t.Context(), pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	require.NoError(t, err)
	defer func() {
		_ = tx.Rollback(context.WithoutCancel(t.Context()))
	}()

	gate := &catalogQueryGate{
		tx:                 tx,
		firstQueryComplete: make(chan struct{}),
		continueQueries:    make(chan struct{}),
	}
	nameFilter, err := getSchemaNameFilter()
	require.NoError(t, err)

	type snapshotResult struct {
		snapshot SchemaSnapshot
		err      error
	}
	resultCh := make(chan snapshotResult, 1)
	go func() {
		snapshot, err := getSchemaSnapshot(t.Context(), gate, nameFilter)
		resultCh <- snapshotResult{snapshot: snapshot, err: err}
	}()

	select {
	case <-gate.firstQueryComplete:
	case result := <-resultCh:
		require.NoError(t, result.err)
		require.Fail(t, "schema fetch completed before the catalog query gate")
	}

	_, ddlErr := db.ConnPool.Exec(t.Context(), `
		CREATE SCHEMA concurrent_ddl;
		CREATE TABLE concurrent_ddl.new_table (id bigint);
	`)
	close(gate.continueQueries)
	result := <-resultCh
	require.NoError(t, ddlErr)
	require.NoError(t, result.err)
	for _, namedSchema := range result.snapshot.Schema.NamedSchemas {
		assert.NotEqual(t, "concurrent_ddl", namedSchema.Name)
	}
	for _, table := range result.snapshot.Schema.Tables {
		assert.NotEqual(t, "concurrent_ddl", table.SchemaName)
	}
	require.NoError(t, tx.Commit(t.Context()))

	afterDDL, err := GetSchemaSnapshot(t.Context(), db.ConnPool)
	require.NoError(t, err)
	assert.Contains(t, afterDDL.Schema.NamedSchemas, NamedSchema{Name: "concurrent_ddl"})
	var foundNewTable bool
	for _, table := range afterDDL.Schema.Tables {
		if table.SchemaName == "concurrent_ddl" && table.EscapedName == `"new_table"` {
			foundNewTable = true
			break
		}
	}
	assert.True(t, foundNewTable)
}
