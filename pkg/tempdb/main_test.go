package tempdb

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"go.segfaultmedaddy.com/pgxephemeraltest"
)

var poolFactory *pgxephemeraltest.PoolFactory

type noopMigrator struct{}

func (noopMigrator) Migrate(context.Context, *pgx.Conn) error { return nil }
func (noopMigrator) Hash() string                             { return "empty" }

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
