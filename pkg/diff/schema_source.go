package diff

import (
	"context"
	"fmt"

	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/pkg/log"
	"github.com/stripe/pg-schema-diff/pkg/sqldb"
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

// DDLSchemaSource returns a SchemaSource that returns a schema based on the provided DDL. You must provide a tempDBFactory
// via the WithTempDbFactory option.
func DDLSchemaSource(ddl []string) SchemaSource {
	return &ddlSchemaSource{ddl: ddl}
}

func (s *ddlSchemaSource) GetSchema(ctx context.Context, deps schemaSourcePlanDeps) (schema.Schema, error) {
	if deps.tempDBFactory == nil {
		return schema.Schema{}, errTempDbFactoryRequired
	}

	tempDb, err := deps.tempDBFactory.Create(ctx)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("creating temp database: %w", err)
	}
	defer func(closer tempdb.ContextualCloser) {
		if err := closer.Close(ctx); err != nil {
			deps.logger.Errorf("an error occurred while dropping the temp database: %s", err)
		}
	}(tempDb.ContextualCloser)

	for _, stmt := range s.ddl {
		if _, err := tempDb.ConnPool.ExecContext(ctx, stmt); err != nil {
			return schema.Schema{}, fmt.Errorf("running DDL: %w", err)
		}
	}

	return schema.GetSchema(ctx, tempDb.ConnPool, append(deps.getSchemaOpts, tempDb.ExcludeMetadataOptions...)...)
}

type dbSchemaSource struct {
	queryable sqldb.Queryable
}

// DBSchemaSource returns a SchemaSource that returns a schema based on the provided queryable. It is recommended
// that the sqldb.Queryable is a *sql.DB with a max # of connections set.
func DBSchemaSource(queryable sqldb.Queryable) SchemaSource {
	return &dbSchemaSource{queryable: queryable}
}

func (s *dbSchemaSource) GetSchema(ctx context.Context, deps schemaSourcePlanDeps) (schema.Schema, error) {
	return schema.GetSchema(ctx, s.queryable, deps.getSchemaOpts...)
}
