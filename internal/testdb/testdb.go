package testdb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

const testDatabaseURLEnv = "TEST_DATABASE_URL"

type Factory struct {
	tempdb.Factory
	config *pgxpool.Config
}

func NewFactory(ctx context.Context, opts ...tempdb.FactoryOption) (*Factory, error) {
	connString := os.Getenv(testDatabaseURLEnv)
	if connString == "" {
		return nil, fmt.Errorf("%s must be set", testDatabaseURLEnv)
	}

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", testDatabaseURLEnv, err)
	}

	factory := &Factory{config: config}
	factory.Factory, err = tempdb.NewFactory(ctx, config, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating temporary database factory: %w", err)
	}

	return factory, nil
}

func MustNewFactory(t testing.TB, opts ...tempdb.FactoryOption) *Factory {
	t.Helper()

	factory, err := NewFactory(context.Background(), opts...)
	if err != nil {
		t.Fatalf("creating test database factory: %v", err)
	}
	t.Cleanup(func() {
		if err := factory.Close(); err != nil {
			t.Errorf("closing test database factory: %v", err)
		}
	})
	return factory
}

func (f *Factory) NewPool(ctx context.Context, dbName string) (*pgxpool.Pool, error) {
	config := f.config.Copy()
	config.ConnConfig.Database = dbName
	return pgxpool.NewWithConfig(ctx, config)
}

func (f *Factory) RootDatabaseName() string {
	return f.config.ConnConfig.Database
}

func (f *Factory) CreateDatabase(t testing.TB) *tempdb.Database {
	t.Helper()

	db, err := f.Create(context.Background())
	if err != nil {
		t.Fatalf("creating test database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := db.Close(ctx); err != nil {
			t.Errorf("dropping test database: %v", err)
		}
	})
	return db
}
