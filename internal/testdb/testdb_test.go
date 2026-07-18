package testdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewFactoryRequiresDatabaseURL(t *testing.T) {
	t.Setenv(testDatabaseURLEnv, "")
	_, err := NewFactory(t.Context())
	require.ErrorContains(t, err, testDatabaseURLEnv+" must be set")
}

func TestFactoryCreatesAndDropsDatabase(t *testing.T) {
	factory := MustNewFactory(t)
	db, err := factory.Create(t.Context())
	require.NoError(t, err)

	var dbName string
	require.NoError(t, db.ConnPool.QueryRow(t.Context(), "SELECT current_database()").Scan(&dbName))
	require.NotEqual(t, factory.RootDatabaseName(), dbName)
	require.NoError(t, db.Close(t.Context()))

	pool, err := factory.NewPool(t.Context(), dbName)
	require.NoError(t, err)
	defer pool.Close()
	var one int
	require.ErrorContains(t, pool.QueryRow(t.Context(), "SELECT 1").Scan(&one), "SQLSTATE 3D000")
}

func TestRoleGuardCreatesAndDropsRoles(t *testing.T) {
	factory := MustNewFactory(t)
	const (
		roleIdentifier = `"schema_diff_test_role""quoted"`
		roleName       = `schema_diff_test_role"quoted`
	)

	t.Run("guarded", func(t *testing.T) {
		guard := factory.LockRoles(t, roleIdentifier)
		guard.CreateRoles()

		var exists bool
		require.NoError(t, guard.conn.QueryRow(t.Context(),
			"SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", roleName).Scan(&exists))
		require.True(t, exists)
	})

	pool, err := factory.NewPool(t.Context(), factory.RootDatabaseName())
	require.NoError(t, err)
	defer pool.Close()
	var exists bool
	require.NoError(t, pool.QueryRow(t.Context(),
		"SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", roleName).Scan(&exists))
	require.False(t, exists)
}
