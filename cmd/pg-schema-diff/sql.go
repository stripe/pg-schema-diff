package main

import (
	"database/sql"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
)

// openDbWithPgxConfig opens a database connection using the provided pgx.ConnConfig and pings it
func openDbWithPgxConfig(config *pgx.ConnConfig) (*sql.DB, error) {
	connPool := stdlib.OpenDB(*config)
	if err := connPool.Ping(); err != nil {
		connPool.Close()
		return nil, err
	}
	return connPool, nil
}
