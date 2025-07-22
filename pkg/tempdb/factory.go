package tempdb

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stripe/pg-schema-diff/internal/pgidentifier"
	"github.com/stripe/pg-schema-diff/internal/util"
	"github.com/stripe/pg-schema-diff/pkg/log"
	"github.com/stripe/pg-schema-diff/pkg/schema"
)

const (
	DefaultOnInstanceDbPrefix       = "pgschemadiff_tmp_"
	DefaultOnInstanceMetadataSchema = "pgschemadiff_tmp_metadata"
	DefaultOnInstanceMetadataTable  = "metadata"

	DefaultStatementTimeout = 3 * time.Second
)

type ContextualCloser interface {
	Close(context.Context) error
}

type fnContextualCloser func(context.Context) error

func (f fnContextualCloser) Close(ctx context.Context) error {
	return f(ctx)
}

type (
	// Database represents a temporary database. It should be closed when it is no longer needed.
	Database struct {
		// ConnPool is the connection pool to the temporary database
		ConnPool *sql.DB
		// ExcludeMetadataOptions are the options used to exclude any internal metadata from plan generation
		ExcludeMetadataOptions []schema.GetSchemaOpt
		// ContextualCloser should be called to clean up the temporary database
		ContextualCloser
	}

	// Factory is used to create temp databases These databases do not have to be in-memory. They might be, for example,
	// be created on the target Postgres server
	Factory interface {
		// Create creates a temporary database. Be sure to always call the ContextualCloser to ensure the database and
		// connections are cleaned up
		Create(ctx context.Context) (*Database, error)
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
		dropTimeout    time.Duration
		randReader     io.Reader
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

// WithDropTimeout sets the timeout used when dropping database
func WithDropTimeout(d time.Duration) OnInstanceFactoryOpt {
	return func(opts *onInstanceFactoryOptions) {
		opts.dropTimeout = d
	}
}

// WithRandReader seeds the random used to generate random SQL identifiers.
func WithRandReader(randReader io.Reader) OnInstanceFactoryOpt {
	return func(options *onInstanceFactoryOptions) {
		options.randReader = randReader
	}
}

type (
	CreateConnPoolForDbFn func(ctx context.Context, dbName string) (*sql.DB, error)

	// onInstanceFactory creates temporary databases on the provided Postgres server
	onInstanceFactory struct {
		rootDb              *sql.DB
		createConnPoolForDb CreateConnPoolForDbFn
		options             onInstanceFactoryOptions
	}
)

// NewOnInstanceFactory provides an implementation to easily create temporary databases on the Postgres instance
// connected to via CreateConnPoolForDbFn. The Postgres instance is connected to via the "postgres" database, and then
// temporary databases are created using that connection. These temporary databases are also connected to via the
// CreateConnPoolForDbFn.
// Make sure to always call Close() on the returned Factory to ensure the root connection is closed
//
// WARNING:
// It is possible this will lead to orphaned temporary databases. These orphaned databases should be pretty small if
// they're only being used by the pg-schema-diff library, but it's recommended to clean them up when possible. This can
// be done by deleting all old databases with the provided temp db prefix. The metadata table can be inspected to find
// when the temporary database was created, e.g., to create a TTL
func NewOnInstanceFactory(ctx context.Context, createConnPoolForDb CreateConnPoolForDbFn, opts ...OnInstanceFactoryOpt) (_ Factory, _retErr error) {
	options := onInstanceFactoryOptions{
		dbPrefix:       DefaultOnInstanceDbPrefix,
		metadataSchema: DefaultOnInstanceMetadataSchema,
		metadataTable:  DefaultOnInstanceMetadataTable,
		dropTimeout:    DefaultStatementTimeout,
		rootDatabase:   "postgres",
		logger:         log.SimpleLogger(),
		randReader:     rand.Reader,
	}
	for _, opt := range opts {
		opt(&options)
	}
	if !pgidentifier.IsSimpleIdentifier(options.dbPrefix) {
		return nil, fmt.Errorf("dbPrefix (%s) must be a simple Postgres identifier matching the following regex: %s", options.dbPrefix, pgidentifier.SimpleIdentifierRegex)
	}

	rootDb, err := createConnPoolForDb(ctx, options.rootDatabase)
	if err != nil {
		return &onInstanceFactory{}, fmt.Errorf("createConnForDb: %w", err)
	}
	defer util.DoOnErrOrPanic(&_retErr, func() {
		_ = rootDb.Close()
	})

	if err := assertConnPoolIsOnExpectedDatabase(ctx, rootDb, options.rootDatabase); err != nil {
		return &onInstanceFactory{}, fmt.Errorf("assertConnPoolIsOnExpectedDatabase: %w", err)
	}

	return &onInstanceFactory{
		rootDb:              rootDb,
		createConnPoolForDb: createConnPoolForDb,
		options:             options,
	}, nil
}

func (o *onInstanceFactory) Close() error {
	return o.rootDb.Close()
}

func (o *onInstanceFactory) Create(ctx context.Context) (_ *Database, _retErr error) {
	dbUUID, err := uuid.NewUUID()
	if err != nil {
		return nil, fmt.Errorf("creating uuid: %w", err)
	}

	rootConn, err := openConnectionWithDefaults(ctx, o.rootDb)
	if err != nil {
		return nil, fmt.Errorf("openConnectionWithDefaults: %w", err)
	}
	defer rootConn.Close()

	tempDbName := o.options.dbPrefix + strings.ReplaceAll(dbUUID.String(), "-", "_")
	// Create the temporary database using template0, the default Postgres template with no user-defined objects.
	if _, err = rootConn.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s TEMPLATE template0;", tempDbName)); err != nil {
		return nil, fmt.Errorf("creating temporary database: %w", err)
	}
	defer util.DoOnErrOrPanic(&_retErr, func() {
		// Only drop the temp database if an error occurred during creation
		if err := o.dropTempDatabase(ctx, tempDbName); err != nil {
			o.options.logger.Errorf("Failed to drop temporary database %s because %q. This drop was automatically triggered by %q", tempDbName, err.Error(), _retErr.Error())
		}
	})

	tempDbConnPool, err := o.createConnPoolForDb(ctx, tempDbName)
	if err != nil {
		return nil, fmt.Errorf("connecting to temporary database: %w", err)
	}
	defer util.DoOnErrOrPanic(&_retErr, func() {
		// Only close the connection pool if an error occurred during creation.
		// We should close the connection pool on the off-chance that the drop database fails
		_ = tempDbConnPool.Close()
	})
	if err := assertConnPoolIsOnExpectedDatabase(ctx, tempDbConnPool, tempDbName); err != nil {
		return nil, fmt.Errorf("assertConnPoolIsOnExpectedDatabase: %w", err)
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
	if _, err := tempDbConnPool.ExecContext(ctx, createMetadataStmts); err != nil {
		return nil, fmt.Errorf("creating metadata in temporary database: %w", err)
	}

	return &Database{
		ConnPool: tempDbConnPool,
		ExcludeMetadataOptions: []schema.GetSchemaOpt{
			schema.WithExcludeSchemas(o.options.metadataSchema),
		},
		ContextualCloser: fnContextualCloser(func(ctx context.Context) error {
			_ = tempDbConnPool.Close()
			return o.dropTempDatabase(ctx, tempDbName)
		}),
	}, nil

}

// assertConnPoolIsOnExpectedDatabase provides validation that a user properly passed in a proper CreateConnPoolForDbFn
func assertConnPoolIsOnExpectedDatabase(ctx context.Context, connPool *sql.DB, expectedDatabase string) (retErr error) {
	conn, err := openConnectionWithDefaults(ctx, connPool)
	if err != nil {
		return fmt.Errorf("openConnectionWithDefaults: %w", err)
	}
	defer conn.Close()

	var dbName string
	if err := connPool.QueryRowContext(ctx, "SELECT current_database();").Scan(&dbName); err != nil {
		return fmt.Errorf("query current database name: %w", err)
	}
	if dbName != expectedDatabase {
		return fmt.Errorf("connection pool is on database %s, expected %s", dbName, expectedDatabase)
	}

	return nil
}

func (o *onInstanceFactory) dropTempDatabase(ctx context.Context, dbName string) (retErr error) {
	if !strings.HasPrefix(dbName, o.options.dbPrefix) {
		return fmt.Errorf("drop non-temporary database: %s", dbName)
	}

	rootConn, err := openConnectionWithDefaults(ctx, o.rootDb)
	if err != nil {
		return fmt.Errorf("openConnectionWithDefaults: %w", err)
	}
	defer rootConn.Close()

	if _, err := rootConn.ExecContext(ctx, fmt.Sprintf("SET SESSION statement_timeout = %d;", o.options.dropTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("setting statement timeout: %w", err)
	}

	_, err = rootConn.ExecContext(ctx, fmt.Sprintf("DROP DATABASE %s;", dbName))
	if err != nil {
		return fmt.Errorf("dropping temporary database: %w", err)
	}

	return nil
}

// openConnectionWithDefaults uses the provided connection pool to open a connection to the database and sets safe
// defaults, such as statement_timeout
func openConnectionWithDefaults(ctx context.Context, connPool *sql.DB) (_ *sql.Conn, retErr error) {
	conn, err := connPool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting connection: %w", err)
	}
	defer util.DoOnErrOrPanic(&retErr, func() {
		_ = conn.Close()
	})

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION statement_timeout = %d;", DefaultStatementTimeout.Milliseconds())); err != nil {
		return nil, fmt.Errorf("setting statement timeout: %w", err)
	}
	return conn, nil
}
