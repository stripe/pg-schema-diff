package testdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const roleLockID int64 = 0x7067736368656d61 // "pgschema"

type RoleGuard struct {
	t       testing.TB
	factory *Factory
	pool    *pgxpool.Pool
	conn    *pgxpool.Conn
	roles   []testRole
}

type testRole struct {
	sqlIdentifier string
	name          string
}

func (f *Factory) LockRoles(t testing.TB, roles ...string) *RoleGuard {
	t.Helper()

	pool, err := f.NewPool(t.Context(), f.RootDatabaseName())
	if err != nil {
		t.Fatalf("connecting to root test database: %v", err)
	}
	conn, err := pool.Acquire(t.Context())
	if err != nil {
		pool.Close()
		t.Fatalf("acquiring root test database connection: %v", err)
	}
	if _, err := conn.Exec(t.Context(), "SELECT pg_advisory_lock($1)", roleLockID); err != nil {
		conn.Release()
		pool.Close()
		t.Fatalf("locking test roles: %v", err)
	}

	testRoles := make([]testRole, 0, len(roles))
	for _, role := range roles {
		name, err := parseRoleIdentifier(role)
		if err != nil {
			conn.Release()
			pool.Close()
			t.Fatalf("parsing test role %q: %v", role, err)
		}
		testRoles = append(testRoles, testRole{sqlIdentifier: role, name: name})
	}

	guard := &RoleGuard{t: t, factory: f, pool: pool, conn: conn, roles: testRoles}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := guard.close(ctx); err != nil {
			t.Errorf("cleaning up test roles: %v", err)
		}
	})
	if err := guard.dropRoles(t.Context()); err != nil {
		t.Fatalf("removing stale test roles: %v", err)
	}
	return guard
}

func (g *RoleGuard) CreateRoles() {
	g.t.Helper()
	for _, role := range g.roles {
		if _, err := g.conn.Exec(g.t.Context(), "CREATE ROLE "+role.sqlIdentifier); err != nil {
			g.t.Fatalf("creating test role %q: %v", role.name, err)
		}
	}
}

func (g *RoleGuard) close(ctx context.Context) error {
	dropErr := g.dropRoles(ctx)
	_, unlockErr := g.conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", roleLockID)
	g.conn.Release()
	g.pool.Close()
	return errors.Join(dropErr, unlockErr)
}

func (g *RoleGuard) dropRoles(ctx context.Context) error {
	for _, role := range g.roles {
		var exists bool
		if err := g.conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", role.name).Scan(&exists); err != nil {
			return fmt.Errorf("checking test role %q: %w", role.name, err)
		}
		if !exists {
			continue
		}

		if err := g.dropOwnedInAllDatabases(ctx, role.name); err != nil {
			return err
		}
		if _, err := g.conn.Exec(ctx, "DROP ROLE "+pgx.Identifier{role.name}.Sanitize()); err != nil {
			return fmt.Errorf("dropping test role %q: %w", role.name, err)
		}
	}
	return nil
}

func (g *RoleGuard) dropOwnedInAllDatabases(ctx context.Context, role string) error {
	rows, err := g.conn.Query(ctx, "SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate")
	if err != nil {
		return fmt.Errorf("listing databases: %w", err)
	}
	var dbNames []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			rows.Close()
			return fmt.Errorf("scanning database name: %w", err)
		}
		dbNames = append(dbNames, dbName)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("listing databases: %w", err)
	}
	rows.Close()

	for _, dbName := range dbNames {
		pool, err := g.factory.NewPool(ctx, dbName)
		if err != nil {
			return fmt.Errorf("connecting to database %q: %w", dbName, err)
		}
		_, err = pool.Exec(ctx, "DROP OWNED BY "+pgx.Identifier{role}.Sanitize())
		pool.Close()
		if err == nil || isDatabaseUnavailable(err) {
			continue
		}
		return fmt.Errorf("dropping objects owned by role %q in database %q: %w", role, dbName, err)
	}
	return nil
}

func isDatabaseUnavailable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "3D000" || pgErr.Code == "57P01")
}

func parseRoleIdentifier(identifier string) (string, error) {
	if !strings.HasPrefix(identifier, `"`) {
		return identifier, nil
	}
	if len(identifier) < 2 || !strings.HasSuffix(identifier, `"`) {
		return "", fmt.Errorf("unterminated quoted identifier")
	}
	return strings.ReplaceAll(identifier[1:len(identifier)-1], `""`, `"`), nil
}
