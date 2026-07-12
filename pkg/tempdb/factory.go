package tempdb

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stripe/pg-schema-diff/internal/namegenerator"
	"github.com/stripe/pg-schema-diff/internal/pgidentifier"
)

const (
	DefaultOnInstanceDbPrefix = "pgschemadiff_tmp_"
	DefaultStatementTimeout   = 3 * time.Second
)

type (
	// Database represents a temporary database. It should be closed when it is no longer needed.
	Database struct {
		// ConnPool is the connection pool to the temporary database.
		ConnPool *pgxpool.Pool

		mu      sync.Mutex
		closed  bool
		closeFn func(context.Context) error
	}

	// Factory creates temporary databases on a Postgres server.
	Factory interface {
		// Create creates a temporary database. Call Database.Close when it is no longer needed.
		Create(ctx context.Context) (*Database, error)
		io.Closer
	}
)

// Close closes the connection pool and drops the temporary database. A failed drop can be retried.
func (d *Database) Close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}

	d.ConnPool.Close()
	if err := d.closeFn(ctx); err != nil {
		return err
	}

	d.closed = true
	return nil
}

type (
	factoryConfig struct {
		dbPrefix    string
		logger      *slog.Logger
		dropTimeout time.Duration
	}

	FactoryOption func(*factoryConfig)
)

// WithLogger sets the logger for the factory. If not set, slog.Default will be used.
func WithLogger(logger *slog.Logger) FactoryOption {
	return func(opts *factoryConfig) {
		opts.logger = logger
	}
}

// WithDbPrefix sets the prefix for temporary database names.
func WithDbPrefix(prefix string) FactoryOption {
	return func(opts *factoryConfig) {
		opts.dbPrefix = prefix
	}
}

// WithDropTimeout sets the timeout used when dropping a database.
func WithDropTimeout(d time.Duration) FactoryOption {
	return func(opts *factoryConfig) {
		opts.dropTimeout = d
	}
}

// onInstanceFactory creates temporary databases on the provided Postgres server.
type onInstanceFactory struct {
	rootDb     *pgxpool.Pool
	poolConfig *pgxpool.Config
	config     factoryConfig
}

// NewFactory creates temporary databases on the Postgres instance described by rootConfig. The database in
// rootConfig is used for administrative connections; temporary database pools copy every other setting from it.
// Call Close on the returned factory to close its root connection pool.
//
// WARNING: Failed cleanup can leave orphaned databases. They can be identified by the configured database prefix.
func NewFactory(ctx context.Context, rootConfig *pgxpool.Config, opts ...FactoryOption) (Factory, error) {
	if rootConfig == nil {
		return nil, fmt.Errorf("rootConfig must not be nil")
	}

	options := factoryConfig{
		dbPrefix:    DefaultOnInstanceDbPrefix,
		dropTimeout: DefaultStatementTimeout,
		logger:      slog.Default(),
	}
	for _, opt := range opts {
		opt(&options)
	}
	if options.logger == nil {
		options.logger = slog.Default()
	}
	if !pgidentifier.IsSimpleIdentifier(options.dbPrefix) {
		return nil, fmt.Errorf("dbPrefix (%s) must be a simple Postgres identifier matching the following regex: %s",
			options.dbPrefix, pgidentifier.SimpleIdentifierRegex)
	}

	config := rootConfig.Copy()
	rootDb, err := pgxpool.NewWithConfig(ctx, config.Copy())
	if err != nil {
		return nil, fmt.Errorf("creating root connection pool: %w", err)
	}
	if err := rootDb.Ping(ctx); err != nil {
		rootDb.Close()
		return nil, fmt.Errorf("connecting to root database %q: %w", config.ConnConfig.Database, err)
	}

	return &onInstanceFactory{
		rootDb:     rootDb,
		poolConfig: config,
		config:     options,
	}, nil
}

func (o *onInstanceFactory) Close() error {
	o.rootDb.Close()
	return nil
}

func (o *onInstanceFactory) Create(ctx context.Context) (_ *Database, retErr error) {
	generatedName, err := namegenerator.Generate(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating temporary database name: %w", err)
	}
	tempDbName := o.config.dbPrefix + generatedName
	escapedTempDbName := pgx.Identifier{tempDbName}.Sanitize()
	if _, err = o.rootDb.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s TEMPLATE template0", escapedTempDbName)); err != nil {
		return nil, fmt.Errorf("creating temporary database: %w", err)
	}

	var tempDbConnPool *pgxpool.Pool
	defer func() {
		if retErr == nil {
			return
		}
		if tempDbConnPool != nil {
			tempDbConnPool.Close()
		}

		cleanupCtx, cancel := context.WithTimeout(context.Background(), o.config.dropTimeout)
		defer cancel()
		if err := o.dropTempDatabase(cleanupCtx, tempDbName); err != nil {
			o.config.logger.ErrorContext(
				cleanupCtx, "failed to drop temporary database after creation error",
				slog.String("database", tempDbName),
				slog.Any("error", err),
				slog.Any("triggering_error", retErr),
			)
		}
	}()

	tempDbConfig := o.poolConfig.Copy()
	tempDbConfig.ConnConfig.Database = tempDbName

	tempDbConnPool, err = pgxpool.NewWithConfig(ctx, tempDbConfig)
	if err != nil {
		return nil, fmt.Errorf("creating temporary database pool: %w", err)
	}
	if err := tempDbConnPool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("connecting to temporary database: %w", err)
	}

	return &Database{
		ConnPool: tempDbConnPool,
		closeFn: func(ctx context.Context) error {
			return o.dropTempDatabase(ctx, tempDbName)
		},
	}, nil
}

func (o *onInstanceFactory) dropTempDatabase(ctx context.Context, dbName string) error {
	if !strings.HasPrefix(dbName, o.config.dbPrefix) {
		return fmt.Errorf("drop non-temporary database: %s", dbName)
	}

	dropCtx, cancel := context.WithTimeout(ctx, o.config.dropTimeout)
	defer cancel()
	if _, err := o.rootDb.Exec(dropCtx,
		fmt.Sprintf("DROP DATABASE %s WITH (FORCE)", pgx.Identifier{dbName}.Sanitize())); err != nil {
		return fmt.Errorf("dropping temporary database: %w", err)
	}
	return nil
}
