package diff

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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

type (
	ddlStatement struct {
		// stmt is the DDL statement to run.
		stmt string
		// file is an optional field that can be used to store the file name from which the DDL was read.
		file string
	}

	ddlSchemaSource struct {
		ddl []ddlStatement
	}
)

// DirSchemaSource returns a SchemaSource that returns a schema based on the provided directories. You must provide a tempDBFactory
// via the WithTempDbFactory option.
func DirSchemaSource(dirs []string) (SchemaSource, error) {
	var ddl []ddlStatement
	for _, dir := range dirs {
		stmts, err := getDDLFromPath(dir)
		if err != nil {
			return &ddlSchemaSource{}, err
		}
		ddl = append(ddl, stmts...)

	}
	return &ddlSchemaSource{
		ddl: ddl,
	}, nil
}

// getDDLFromPath reads all .sql files under the given path (including sub-directories) and returns the DDL
// in lexical order.
func getDDLFromPath(path string) ([]ddlStatement, error) {
	var ddl []ddlStatement
	if err := filepath.Walk(path, func(path string, entry os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("walking path %q: %w", path, err)
		}
		if strings.ToLower(filepath.Ext(entry.Name())) != ".sql" {
			return nil
		}

		fileContents, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("reading file %q: %w", entry.Name(), err)
		}

		// In the future, it would make sense to split the file contents into individual DDL statements; however,
		// that would require fully parsing the SQL. Naively splitting on `;` would not work because `;` can be
		// used in comments, strings, and escaped identifiers.
		ddl = append(ddl, ddlStatement{
			stmt: string(fileContents),
			file: path,
		})
		return nil
	}); err != nil {
		return nil, err
	}
	return ddl, nil
}

// DDLSchemaSource returns a SchemaSource that returns a schema based on the provided DDL. You must provide a tempDBFactory
// via the WithTempDbFactory option.
func DDLSchemaSource(stmts []string) SchemaSource {
	var ddl []ddlStatement
	for _, stmt := range stmts {
		ddl = append(ddl, ddlStatement{
			stmt: stmt,
			// There is no file name associated with the DDL statement.
			file: ""},
		)
	}

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

	for _, ddlStmt := range s.ddl {
		if _, err := tempDb.ConnPool.ExecContext(ctx, ddlStmt.stmt); err != nil {
			debugInfo := ""
			if ddlStmt.file != "" {
				debugInfo = fmt.Sprintf(" (from %s)", ddlStmt.file)
			}
			return schema.Schema{}, fmt.Errorf("running DDL%s: %w", debugInfo, err)
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
