package pgengine

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"
)

type ConnectionOption string

const (
	ConnectionOptionDatabase ConnectionOption = "dbname"
)

type ConnectionOptions map[ConnectionOption]string

func (c ConnectionOptions) With(option ConnectionOption, value string) ConnectionOptions {
	clone := make(ConnectionOptions)
	for k, v := range c {
		clone[k] = v
	}
	clone[option] = value
	return clone
}

func (c ConnectionOptions) ToDSN() string {
	var pairs []string
	for k, v := range c {
		pairs = append(pairs, fmt.Sprintf("%s%s%s", k, "=", v))
	}

	return strings.Join(pairs, " ")
}

type Engine struct {
	user *user.User

	// for cleanup purposes
	process  *os.Process
	dbPath   string
	sockPath string
}

const (
	port = 5432

	maxConnAttemptsAtStartup      = 10
	waitBetweenStartupConnAttempt = time.Second
)

var (
	defaultServerConfiguration = map[string]string{
		"log_checkpoints": "false",
	}
)

// StartEngine starts a postgres instance. This is useful for testing, where Postgres databases need to be spun up.
// "postgres" must be on the system's PATH, and the binary must be located in a directory containing "initdb"
func StartEngine() (*Engine, error) {
	postgresPath, err := exec.LookPath("postgres")
	if err != nil {
		return nil, errors.New("postgres executable not found in path")
	}
	return StartEngineUsingPgDir(path.Dir(postgresPath))
}

func StartEngineUsingPgDir(pgDir string) (pgEngine *Engine, retErr error) {
	currentUser, err := user.Current()
	if err != nil {
		return nil, err
	}

	dbPath, err := os.MkdirTemp("", "postgresql-")
	if err != nil {
		return nil, err
	}

	sockPath, err := os.MkdirTemp("", "pgsock-")
	if err != nil {
		return nil, err
	}

	if err := initDB(currentUser, path.Join(pgDir, "initdb"), dbPath); err != nil {
		return nil, err
	}

	process, err := startServer(path.Join(pgDir, "postgres"), dbPath, sockPath, defaultServerConfiguration)
	if err != nil {
		// Cleanup temporary directories that were created
		os.RemoveAll(dbPath)
		os.RemoveAll(sockPath)
		return nil, err
	}

	pgEngine = &Engine{
		dbPath:   dbPath,
		sockPath: sockPath,
		user:     currentUser,
		process:  process,
	}
	defer func() {
		if retErr != nil {
			pgEngine.Close()
		}
	}()
	if err := pgEngine.waitTillServingTraffic(maxConnAttemptsAtStartup, waitBetweenStartupConnAttempt); err != nil {
		return nil, fmt.Errorf("waiting till server can serve traffic: %w", err)
	}

	return pgEngine, nil
}

func initDB(currentUser *user.User, initDbPath, dbPath string) error {
	cmd := exec.Command(initDbPath, []string{
		"-U", currentUser.Username,
		"-D", dbPath,
		"-A", "trust",
	}...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		outputStr := string(output)

		var tip string
		line := strings.Repeat("=", 95)
		if strings.Contains(outputStr, "request for a shared memory segment exceeded your kernel's SHMALL parameter") {
			tip = line + "\n   Run 'sudo sysctl -w kern.sysv.shmall=16777216' to solve this issue   \n" + line + "\n"
		} else if strings.Contains(outputStr, "could not create shared memory segment: No space left on device") {
			tip = line + "\n   Use the ipcs and ipcrm commands to clear the shared memory \n" + line + "\n"
		}

		return fmt.Errorf("error running initdb: %w\n%s\n%s", err, outputStr, tip)
	}
	return nil
}

func startServer(pgBinaryPath, dbPath, sockPath string, configuration map[string]string) (*os.Process, error) {
	opts := []string{
		"-D", dbPath,
		"-k", sockPath,
		"-p", strconv.Itoa(port),
		"-h", "",
	}
	for k, v := range configuration {
		opts = append(opts, "-c", fmt.Sprintf("%s=%s", k, v))
	}
	cmd := exec.Command(pgBinaryPath, opts...)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting postgres server instance: %w", err)
	}

	return cmd.Process, nil
}

func (e *Engine) waitTillServingTraffic(maxAttempts int, timeBetweenAttempts time.Duration) error {
	var mostRecentErr error
	for i := 0; i < maxAttempts; i++ {
		mostRecentErr = e.testIfInstanceServingTraffic()
		if mostRecentErr == nil {
			return nil
		}
		time.Sleep(timeBetweenAttempts)
	}
	return fmt.Errorf("unable to establish connection to postgres instance. most recent error: %w", mostRecentErr)
}

func (e *Engine) testIfInstanceServingTraffic() error {
	db, err := sql.Open("pgx", e.GetPostgresDatabaseDSN())
	if err != nil {
		return err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return err
	}
	return db.Close()
}

func (e *Engine) GetPostgresDatabaseConnOpts() ConnectionOptions {
	result := make(map[ConnectionOption]string)
	result[ConnectionOptionDatabase] = "postgres"
	result["host"] = e.sockPath
	result["port"] = strconv.Itoa(port)
	result["sslmode"] = "disable"

	return result
}

func (e *Engine) GetPostgresDatabaseDSN() string {
	return e.GetPostgresDatabaseConnOpts().ToDSN()
}

func (e *Engine) Close() error {
	// Make best effort attempt to clean up everything
	e.process.Signal(os.Interrupt)
	e.process.Wait()
	os.RemoveAll(e.dbPath)
	os.RemoveAll(e.dbPath)

	return nil
}

func (e *Engine) CreateDatabase() (*DB, error) {
	uuid, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("generating uuid: %w", err)
	}
	testDBName := fmt.Sprintf("pgtestdb_%v", uuid.String())

	testDb, err := e.CreateDatabaseWithName(testDBName)
	if err != nil {
		return nil, err
	}

	return testDb, err
}

func (e *Engine) CreateDatabaseWithName(name string) (*DB, error) {
	dsn := e.GetPostgresDatabaseConnOpts().With(ConnectionOptionDatabase, "postgres").ToDSN()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE \"%s\"", name))
	if err != nil {
		return nil, err
	}

	return &DB{
		connOpts: e.GetPostgresDatabaseConnOpts().With(ConnectionOptionDatabase, name),
	}, nil
}
