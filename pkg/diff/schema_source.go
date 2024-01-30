package diff

import (
	"context"
	"fmt"

	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/pkg/log"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

type schemaSourcePlanDeps struct {
	factory tempdb.Factory
	logger  log.Logger
}

type SchemaSource interface {
	GetSchema(ctx context.Context, deps schemaSourcePlanDeps) (schema.Schema, error)
}

type ddlSchemaSource struct {
	ddl []string
}

func DDLSchemaSource(ddl []string) SchemaSource {
	return ddlSchemaSource{ddl: ddl}
}

func (s ddlSchemaSource) GetSchema(ctx context.Context, deps schemaSourcePlanDeps) (schema.Schema, error) {
	return deriveSchemaFromDDLOnTempDb(ctx, deps.logger, deps.factory, s.ddl)
}

func deriveSchemaFromDDLOnTempDb(ctx context.Context, logger log.Logger, tempDbFactory tempdb.Factory, ddl []string) (schema.Schema, error) {
	tempDb, dropTempDb, err := tempDbFactory.Create(ctx)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("creating temp database: %w", err)
	}
	defer func(drop tempdb.Dropper) {
		if err := drop(ctx); err != nil {
			logger.Errorf("an error occurred while dropping the temp database: %s", err)
		}
	}(dropTempDb)

	for _, stmt := range ddl {
		if _, err := tempDb.ExecContext(ctx, stmt); err != nil {
			return schema.Schema{}, fmt.Errorf("running DDL: %w", err)
		}
	}

	return schema.GetSchema(ctx, tempDb, schema.WithSchemas("public"))
}
