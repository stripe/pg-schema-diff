package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/stripe/pg-schema-diff/internal/pgengine"
)

type cmdTestSuite struct {
	suite.Suite
	pgEngine *pgengine.Engine
}

func (suite *cmdTestSuite) SetupSuite() {
	pgEngine, err := pgengine.StartEngine()
	suite.Require().NoError(err)
	suite.pgEngine = pgEngine
}

func (suite *cmdTestSuite) TearDownSuite() {
	suite.Require().NoError(suite.pgEngine.Close())
}

type runCmdWithAssertionsParams struct {
	args []string
	// dynamicArgs is function that can be used to build args that are dynamic, i.e.,
	// saving schemas to a randomly generated temporary directory.
	dynamicArgs []dArgGenerator

	// outputEquals is the exact string that stdout should equal.
	outputEquals string
	// outputContains is a list of substrings that are expected to be contained in the stdout output of the command.
	outputContains []string
	// expectErrContains is a list of substrings that are expected to be contained in the error returned by
	// cmd.RunE. This is DISTINCT from stdErr.
	expectErrContains []string
}

func (suite *cmdTestSuite) runCmdWithAssertions(tc runCmdWithAssertionsParams) {
	args := tc.args
	for _, da := range tc.dynamicArgs {
		args = append(args, da(suite.T())...)
	}

	rootCmd := buildRootCmd()
	rootCmd.SetArgs(args)
	stdOut := &bytes.Buffer{}
	rootCmd.SetOut(stdOut)
	stdErr := &bytes.Buffer{}
	rootCmd.SetErr(stdErr)

	err := rootCmd.Execute()
	if len(tc.expectErrContains) > 0 {
		for _, e := range tc.expectErrContains {
			suite.ErrorContains(err, e)
		}
	} else {
		stdErrStr := stdErr.String()
		suite.Require().NoError(err)
		// Only assert the std error is empty if we don't expect an error
		suite.Empty(stdErrStr, "expected no stderr")
	}

	stdOutStr := stdOut.String()
	if len(tc.outputContains) > 0 {
		for _, o := range tc.outputContains {
			suite.Contains(stdOutStr, o)
		}
	}
	if len(tc.outputEquals) > 0 {
		suite.Equal(tc.outputEquals, stdOutStr)
	}
}

// dArgGenerator generates argument at the run-time of the test case...
// intended for resources that are not known at test start and potentially need
// to be cleaned up.
type dArgGenerator func(*testing.T) []string

func tempSchemaDirDArg(argName string, ddl []string) dArgGenerator {
	return func(t *testing.T) []string {
		t.Helper()
		return []string{"--" + argName, tempSchemaDir(t, ddl)}
	}
}

func tempSchemaDir(t *testing.T, ddl []string) string {
	t.Helper()
	dir := t.TempDir()
	for i, d := range ddl {
		require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("ddl_%d.sql", i)), []byte(d), 0644))
	}
	return dir
}

func tempDsnDArg(pgEngine *pgengine.Engine, argName string, ddl []string) dArgGenerator {
	return func(t *testing.T) []string {
		t.Helper()
		db := tempDbWithSchema(t, pgEngine, ddl)
		return []string{"--" + argName, db.GetDSN()}
	}
}

func tempDbWithSchema(t *testing.T, pgEngine *pgengine.Engine, ddl []string) *pgengine.DB {
	t.Helper()
	db, err := pgEngine.CreateDatabase()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.DropDB())
	})
	dbPool, err := sql.Open("pgx", db.GetDSN())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, dbPool.Close())
	}()
	for _, d := range ddl {
		_, err := dbPool.Exec(d)
		require.NoError(t, err)
	}
	return db
}

func tempSetPqEnvVarsForDb(t *testing.T, db *pgengine.DB) {
	t.Helper()
	tempSetEnvVar(t, "PGHOST", db.GetConnOpts()[pgengine.ConnectionOptionHost])
	tempSetEnvVar(t, "PGPORT", db.GetConnOpts()[pgengine.ConnectionOptionPort])
	tempSetEnvVar(t, "PGUSER", db.GetConnOpts()[pgengine.ConnectionOptionUser])
	tempSetEnvVar(t, "PGDATABASE", db.GetName())
}

func tempSetEnvVar(t *testing.T, k, v string) {
	t.Helper()
	require.NoError(t, os.Setenv(k, v))
	t.Cleanup(func() {
		require.NoError(t, os.Unsetenv(k))
	})
}

func TestCmdTestSuite(t *testing.T) {
	suite.Run(t, new(cmdTestSuite))
}
