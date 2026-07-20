package sqldb

import (
	"context"
	"fmt"
)

// pinSearchPathSQL pins search_path to pg_catalog for the remainder of the session.
// This prevents CVE-2018-1058 style shadowing of built-in functions and operators
// by objects in user schemas when executing generated DDL.
const pinSearchPathSQL = `SELECT pg_catalog.set_config('search_path', 'pg_catalog', false)`

// PinSearchPath pins search_path on conn before executing generated or reconstructed DDL.
func PinSearchPath(ctx context.Context, conn Queryable) error {
	if _, err := conn.ExecContext(ctx, pinSearchPathSQL); err != nil {
		return fmt.Errorf("pinning search_path: %w", err)
	}
	return nil
}
