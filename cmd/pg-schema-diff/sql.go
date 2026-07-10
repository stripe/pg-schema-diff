package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// openPoolWithPgxConfig opens a database pool using the provided pgxpool.Config and pings it.
func openPoolWithPgxConfig(ctx context.Context, config *pgxpool.Config) (*pgxpool.Pool, error) {
	connPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	if err := connPool.Ping(ctx); err != nil {
		connPool.Close()
		return nil, err
	}
	return connPool, nil
}

type pgxPoolCloser struct {
	pool *pgxpool.Pool
}

func (p pgxPoolCloser) Close() error {
	p.pool.Close()
	return nil
}
