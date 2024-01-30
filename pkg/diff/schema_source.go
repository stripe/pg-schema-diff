package diff

import (
	"context"
	"fmt"

	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/pkg/log"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

type schemaSourcePlanDeps struct {
	tempDBFactory tempdb.Factory
	logger        log.Logger
	getSchemaOpts []schema.GetSchemaOpt
}

type SchemaSource interface {
	GetSchema(ctx context.Context, deps schemaSourcePlanDeps) (schema.Schema, error)
}

type ddlSchemaSource struct {
	ddl []string
}

func DDLSchemaSource(ddl []string) SchemaSource {
	return &ddlSchemaSource{ddl: ddl}
}

func (s *ddlSchemaSource) GetSchema(ctx context.Context, deps schemaSourcePlanDeps) (schema.Schema, error) {
	if deps.tempDBFactory == nil {
		return schema.Schema{}, errTempDbFactoryRequired
	}

	tempDb, dropTempDb, err := deps.tempDBFactory.Create(ctx)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("creating temp database: %w", err)
	}
	defer func(drop tempdb.Dropper) {
		if err := drop(ctx); err != nil {
			deps.logger.Errorf("an error occurred while dropping the temp database: %s", err)
		}
	}(dropTempDb)

	for _, stmt := range s.ddl {
		if _, err := tempDb.ExecContext(ctx, stmt); err != nil {
			return schema.Schema{}, fmt.Errorf("running DDL: %w", err)
		}
	}

	return schema.GetSchema(ctx, tempDb, deps.getSchemaOpts...)
}