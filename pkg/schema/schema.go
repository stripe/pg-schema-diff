package schema

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	internalschema "github.com/stripe/pg-schema-diff/internal/schema"
)

type GetSchemaOpt = internalschema.GetSchemaOpt

const DefaultCleanupSchemaPrefix = "pgschemadiff_archive"

var (
	WithIncludeSchemaPatterns = internalschema.WithIncludeSchemaPatterns
	WithExcludeSchemaPatterns = internalschema.WithExcludeSchemaPatterns
)

// GetSchemaHash hash gets the hash of the target schema. It can be used to compare against the hash in the migration
// plan to determine if the plan is still valid.
//
// We do not expose the Schema struct yet because it is subject to change, and we do not want folks depending on its API.
func GetSchemaHash(ctx context.Context, connPool *pgxpool.Pool, opts ...GetSchemaOpt) (string, error) {
	opts = append([]GetSchemaOpt{WithExcludeSchemaPatterns(DefaultCleanupSchemaPrefix + ".*")}, opts...)
	snapshot, err := internalschema.GetSchemaSnapshot(ctx, connPool, opts...)
	if err != nil {
		return "", fmt.Errorf("getting public schema: %w", err)
	}

	return snapshot.Hash, nil
}
