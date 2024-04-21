package pgengine

import (
	"context"
	"database/sql"
	"fmt"
)

// ResetInstance attempts to reset the cluster to a clean state.
// It deletes all cluster level objects, i.e., roles, which are not deleted
// by dropping database(s). This can be useful for re-using a cluster for multiple tests.
func ResetInstance(ctx context.Context, db *sql.DB) error {
	// Drop all roles except the current user and postgres internal roles
	if err := dropRoles(ctx, db); err != nil {
		return fmt.Errorf("dropping roles: %w", err)
	}

	return nil
}

// DropRoles drops all roles except the current user and postgres internal roles
func dropRoles(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `
		SELECT rolname 
		FROM pg_catalog.pg_roles 
		WHERE rolname NOT LIKE 'pg_%'
			AND rolname != current_user;
	`,
	)
	if err != nil {
		return fmt.Errorf("querying roles: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var roleName string
		if err := rows.Scan(&roleName); err != nil {
			return fmt.Errorf("scanning role: %w", err)
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP ROLE %s", roleName)); err != nil {
			return fmt.Errorf("dropping role %q: %w", roleName, err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating over rows: %w", err)
	}

	return nil
}
