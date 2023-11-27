package tempdb

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stripe/pg-schema-diff/internal/pgidentifier"
	"github.com/stripe/pg-schema-diff/pkg/log"
)

const (
	DefaultOnInstanceDbPrefix       = "pgschemadifftmp_"
	DefaultOnInstanceMetadataSchema = "pgschemadifftmp_metadata"
	DefaultOnInstanceMetadataTable  = "metadata"
)

// Factory is used to create temp databases These databases do not have to be in-memory. They might be, for example,
// be created on the target Postgres server
type (
	Dropper func(ctx context.Context) error

	Factory interface {
		// Create creates a temporary database. Be sure to always call the Dropper to ensure the database and
		// connections are cleaned up
		Create(ctx context.Context) (db *sql.DB, dropper Dropper, err error)

		io.Closer
	}
)

type (
	onInstanceFactoryOptions struct {
		dbPrefix       string
		metadataSchema string
		metadataTable  string
		logger         log.Logger
		rootDatabase   string
	}

	OnInstanceFactoryOpt func(*onInstanceFactoryOptions)
)

// WithLogger sets the logger for the factory. If not set, a SimpleLogger will be used
func WithLogger(logger log.Logger) OnInstanceFactoryOpt {
	return func(opts *onInstanceFactoryOptions) {
		opts.logger = logger
	}
}

// WithDbPrefix sets the prefix for the temp database name
func WithDbPrefix(prefix string) OnInstanceFactoryOpt {
	return func(opts *onInstanceFactoryOptions) {
		opts.dbPrefix = prefix
	}
}

// WithMetadataSchema sets the prefix for the schema name containing the metadata
func WithMetadataSchema(schema string) OnInstanceFactoryOpt {
	return func(opts *onInstanceFactoryOptions) {
		opts.metadataSchema = schema
	}
}

// WithMetadataTable sets the metadata table name
func WithMetadataTable(table string) OnInstanceFactoryOpt {
	return func(opts *onInstanceFactoryOptions) {
		opts.metadataTable = table
	}
}

// WithRootDatabase sets the database to connect to when creating temporary databases
func WithRootDatabase(db string) OnInstanceFactoryOpt {
	return func(opts *onInstanceFactoryOptions) {
		opts.rootDatabase = db
	}
}

type (
	CreateConnForDbFn func(ctx context.Context, dbName string) (*sql.DB, error)

	// onInstanceFactory creates temporary databases on the provided Postgres server
	onInstanceFactory struct {
		rootDb          *sql.DB
		createConnForDb CreateConnForDbFn
		options         onInstanceFactoryOptions
	}
)

// NewOnInstanceFactory provides an implementation to easily create temporary databases on the Postgres instance
// connected to via CreateConnForDbFn. The Postgres instance is connected to via the "postgres" database, and then
// temporary databases are created using that connection. These temporary databases are also connected to via the
// CreateConnForDbFn.
// Make sure to always call Close() on the returned Factory to ensure the root connection is closed
//
// WARNING:
// It is possible this will lead to orphaned temporary databases. These orphaned databases should be pretty small if
// they're only being used by the pg-schema-diff library, but it's recommended to clean them up when possible. This can
// be done by deleting all old databases with the provided temp db prefix. The metadata table can be inspected to find
// when the temporary database was created, e.g., to create a TTL
func NewOnInstanceFactory(ctx context.Context, createConnForDb CreateConnForDbFn, opts ...OnInstanceFactoryOpt) (Factory, error) {
	options := onInstanceFactoryOptions{
		dbPrefix:       DefaultOnInstanceDbPrefix,
		metadataSchema: DefaultOnInstanceMetadataSchema,
		metadataTable:  DefaultOnInstanceMetadataTable,
		rootDatabase:   "postgres",
		logger:         log.SimpleLogger(),
	}
	for _, opt := range opts {
		opt(&options)
	}
	if !pgidentifier.IsSimpleIdentifier(options.dbPrefix) {
		return nil, fmt.Errorf("dbPrefix (%s) must be a simple Postgres identifier matching the following regex: %s", options.dbPrefix, pgidentifier.SimpleIdentifierRegex)
	}

	rootDb, err := createConnForDb(ctx, options.rootDatabase)
	if err != nil {
		return &onInstanceFactory{}, err
	}
	if err := assertConnPoolIsOnExpectedDatabase(ctx, rootDb, options.rootDatabase); err != nil {
		rootDb.Close()
		return &onInstanceFactory{}, fmt.Errorf("assertConnPoolIsOnExpectedDatabase: %w", err)
	}

	return &onInstanceFactory{
		rootDb:          rootDb,
		createConnForDb: createConnForDb,
		options:         options,
	}, nil
}

func (o *onInstanceFactory) Close() error {
	return o.rootDb.Close()
}

func (o *onInstanceFactory) Create(ctx context.Context) (sql *sql.DB, dropper Dropper, retErr error) {
	dbUUID, err := uuid.NewUUID()
	if err != nil {
		return nil, nil, err
	}
	tempDbName := o.options.dbPrefix + strings.ReplaceAll(dbUUID.String(), "-", "_")
	if _, err = o.rootDb.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s;", tempDbName)); err != nil {
		return nil, nil, err
	}
	defer func() {
		// Only drop the temp database if an error occurred during creation
		if retErr != nil {
			if err := o.dropTempDatabase(ctx, tempDbName); err != nil {
				o.options.logger.Errorf("Failed to drop temporary database %s because of error %s. This drop was automatically triggered by error %s", tempDbName, err.Error(), retErr.Error())
			}
		}
	}()

	tempDbConn, err := o.createConnForDb(ctx, tempDbName)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		// Only close the connection pool if an error occurred during creation.
		// We should close the connection pool on the off-chance that the drop database fails
		if retErr != nil {
			_ = tempDbConn.Close()
		}
	}()
	if err := assertConnPoolIsOnExpectedDatabase(ctx, tempDbConn, tempDbName); err != nil {
		return nil, nil, fmt.Errorf("assertConnPoolIsOnExpectedDatabase: %w", err)
	}

	// There's no easy way to keep track of when a database was created, so
	// create a row with the temp database's time of creation. This will be used
	// by a cleanup process to drop any temporary databases that were not cleaned up
	// successfully by the "dropper"
	sanitizedSchemaName := pgx.Identifier{o.options.metadataSchema}.Sanitize()
	sanitizedTableName := pgx.Identifier{o.options.metadataSchema, o.options.metadataTable}.Sanitize()
	createMetadataStmts := fmt.Sprintf(`
		CREATE SCHEMA %s
			CREATE TABLE %s(
				db_created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
			);
		INSERT INTO %s DEFAULT VALUES;
	`, sanitizedSchemaName, sanitizedTableName, sanitizedTableName)
	if _, err := tempDbConn.ExecContext(ctx, createMetadataStmts); err != nil {
		return nil, nil, err
	}

	return tempDbConn, func(ctx context.Context) error {
		_ = tempDbConn.Close()
		return o.dropTempDatabase(ctx, tempDbName)
	}, nil

}

// assertConnPoolIsOnExpectedDatabase provides validation that a user properly passed in a proper CreateConnForDbFn
func assertConnPoolIsOnExpectedDatabase(ctx context.Context, connPool *sql.DB, expectedDatabase string) error {
	var dbName string
	if err := connPool.QueryRowContext(ctx, "SELECT current_database();").Scan(&dbName); err != nil {
		return err
	}
	if dbName != expectedDatabase {
		return fmt.Errorf("connection pool is on database %s, expected %s", dbName, expectedDatabase)
	}

	return nil
}

func (o *onInstanceFactory) dropTempDatabase(ctx context.Context, dbName string) error {
	if !strings.HasPrefix(dbName, o.options.dbPrefix) {
		return fmt.Errorf("drop non-temporary database: %s", dbName)
	}
	_, err := o.rootDb.ExecContext(ctx, fmt.Sprintf("DROP DATABASE %s;", dbName))
	return err
}
