package diff

import (
	"context"
	"fmt"

	"github.com/stripe/pg-schema-diff/pkg/sqldb"
)

// ExecuteStatement executes a migration statement, optionally pinning search_path when required.
func ExecuteStatement(ctx context.Context, conn sqldb.Queryable, stmt Statement) error {
	if stmt.pinSearchPathBeforeExec {
		if err := sqldb.PinSearchPath(ctx, conn); err != nil {
			return err
		}
		defer func() {
			_ = sqldb.ResetSearchPath(ctx, conn)
		}()
	}

	if _, err := conn.ExecContext(ctx, stmt.ToSQL()); err != nil {
		return fmt.Errorf("executing migration statement: %s: %w", stmt.DDL, err)
	}
	return nil
}
