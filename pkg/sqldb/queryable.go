package sqldb

import (
	"context"
	"database/sql"
)

// Queryable represents a queryable database. It is recommended to use *sql.DB or *sql.Conn.
// In a future major version update, we will probably deprecate *sql.Conn support and only support *sql.DB.
// Alternatively, we might only support *sql.Conn or *sql.DB.
type Queryable interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}
