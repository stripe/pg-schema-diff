package schema

import (
	"context"
	"fmt"
	"regexp"

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

// getCandidateSchemaHash is the dormant public-package Stage 18 seam. Marker
// trust must come from the strict archival orchestrator; this package does not
// duplicate its private marker codec. Stage 19 will activate an orchestrated
// call while preserving the prefix and accumulated caller filters used here.
func getCandidateSchemaHash(
	ctx context.Context,
	connPool *pgxpool.Pool,
	cleanupPrefix string,
	trustedGroups []internalschema.SnapshotHashTrustedArchivalGroup,
	opts ...GetSchemaOpt,
) (string, error) {
	snapshot, err := getCandidateSchemaSnapshot(ctx, connPool, cleanupPrefix, opts...)
	if err != nil {
		return "", err
	}
	return buildCandidateSchemaHash(snapshot, trustedGroups)
}

func getCandidateSchemaSnapshot(
	ctx context.Context,
	connPool *pgxpool.Pool,
	cleanupPrefix string,
	opts ...GetSchemaOpt,
) (internalschema.SchemaSnapshot, error) {
	opts = append([]GetSchemaOpt{
		WithExcludeSchemaPatterns(regexp.QuoteMeta(cleanupPrefix) + ".*"),
	}, opts...)
	snapshot, err := internalschema.GetSchemaSnapshot(ctx, connPool, opts...)
	if err != nil {
		return internalschema.SchemaSnapshot{},
			fmt.Errorf("getting candidate public schema snapshot: %w", err)
	}
	return snapshot, nil
}

func buildCandidateSchemaHash(
	snapshot internalschema.SchemaSnapshot,
	trustedGroups []internalschema.SnapshotHashTrustedArchivalGroup,
) (string, error) {
	return internalschema.BuildSchemaSnapshotHashV1(snapshot, trustedGroups)
}
