package schema

import (
	"context"
	"fmt"

	internalschema "github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/pkg/sqldb"
)

type GetSchemaOpt = internalschema.GetSchemaOpt

var (
	WithIncludeSchemas = internalschema.WithIncludeSchemas
	WithExcludeSchemas = internalschema.WithExcludeSchemas
)

// GetPublicSchemaHash hash gets the hash of the "public" schema. It can be used to compare against the hash in the migration
// plan to determine if the plan is still valid
// We do not expose the Schema struct yet because it is subject to change, and we do not want folks depending on its API
func GetPublicSchemaHash(ctx context.Context, queryable sqldb.Queryable) (string, error) {
	schema, err := internalschema.GetSchema(ctx, queryable, internalschema.WithIncludeSchemas("public"))
	if err != nil {
		return "", fmt.Errorf("getting public schema: %w", err)
	}
	hash, err := schema.Hash()
	if err != nil {
		return "", fmt.Errorf("hashing schema: %w", err)
	}

	return hash, nil
}
