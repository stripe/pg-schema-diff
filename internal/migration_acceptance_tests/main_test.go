package migration_acceptance_tests

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
	"go.segfaultmedaddy.com/pgxephemeraltest"
)

var poolFactory *pgxephemeraltest.PoolFactory

type noopMigrator struct{}

func (noopMigrator) Migrate(context.Context, *pgx.Conn) error { return nil }
func (noopMigrator) Hash() string                             { return "empty" }

type testTempDBFactory struct {
	t *testing.T
}

func (f testTempDBFactory) Create(context.Context) (*tempdb.Database, error) {
	pool := poolFactory.Pool(f.t)
	return &tempdb.Database{
		ConnPool:               pool,
		ExcludeMetadataOptions: nil,
		ContextualCloser:       poolCloser(pool.Close),
	}, nil
}

func (testTempDBFactory) Close() error { return nil }

type poolCloser func()

func (f poolCloser) Close(context.Context) error {
	f()
	return nil
}

func TestMain(m *testing.M) {
	connString := os.Getenv("TEST_DATABASE_URL")
	if connString == "" {
		log.Fatal("TEST_DATABASE_URL must be set")
	}

	var err error
	poolFactory, err = pgxephemeraltest.NewPoolFactoryFromConnString(context.Background(), connString, noopMigrator{})
	if err != nil {
		log.Fatalf("create test database pool factory: %v", err)
	}

	os.Exit(m.Run())
}
