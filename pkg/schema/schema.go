package schema

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	internalschema "github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/pkg/diff"
)

type GetSchemaOpt = internalschema.GetSchemaOpt

const DefaultCleanupSchemaPrefix = internalschema.DefaultCleanupSchemaPrefix

var (
	WithIncludeSchemaPatterns = internalschema.WithIncludeSchemaPatterns
	WithExcludeSchemaPatterns = internalschema.WithExcludeSchemaPatterns
)

// GetSchemaHash returns the versioned source snapshot hash using the default
// archival prefix. It can be compared with diff.Plan.CurrentSchemaHash to
// determine whether a serialized plan still applies to the database.
//
// We do not expose the Schema struct yet because it is subject to change, and we do not want folks depending on its API.
func GetSchemaHash(ctx context.Context, connPool *pgxpool.Pool, opts ...GetSchemaOpt) (string, error) {
	return GetSchemaHashWithArchivalPrefix(ctx, connPool, DefaultCleanupSchemaPrefix, opts...)
}

// GetSchemaHashWithArchivalPrefix returns the versioned source snapshot hash
// using the same archival prefix and strict marker trust contract as
// diff.Generate. A custom prefix replaces the default; include and exclude
// schema options continue to accumulate.
func GetSchemaHashWithArchivalPrefix(
	ctx context.Context,
	connPool *pgxpool.Pool,
	cleanupPrefix string,
	opts ...GetSchemaOpt,
) (string, error) {
	hash, err := diff.GetSchemaHash(ctx, connPool, cleanupPrefix, opts...)
	if err != nil {
		return "", fmt.Errorf("getting public schema hash: %w", err)
	}
	return hash, nil
}
