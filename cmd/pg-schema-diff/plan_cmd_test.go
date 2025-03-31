package main

import (
	"regexp"
	"testing"
	"time"
)

func (suite *cmdTestSuite) TestPlanCmd() {
	type testCase struct {
		name        string
		args        []string
		dynamicArgs []dArgGenerator

		// outputContains is a list of substrings that are expected to be contained in the stdout output of the command.
		outputContains []string
		// expectErrContains is a list of substrings that are expected to be contained in the error returned by
		// cmd.RunE. This is DISTINCT from stdErr.
		expectErrContains []string
	}
	// Non-comprehensive set of tests for the plan command. Not totally comprehensive to avoid needing to avoid
	// hindering developer velocity when updating the command.
	for _, tc := range []testCase{
		{
			name: "from dsn to dsn",
			dynamicArgs: []dArgGenerator{
				tempDsnDArg(suite.pgEngine, "from-dsn", nil),
				tempDsnDArg(suite.pgEngine, "to-dsn", []string{"CREATE TABLE foobar()"}),
			},
			outputContains: []string{"CREATE TABLE"},
		},
		{
			name: "from dsn to dir",
			dynamicArgs: []dArgGenerator{
				tempDsnDArg(suite.pgEngine, "from-dsn", []string{""}),
				tempSchemaDirDArg("to-dir", []string{"CREATE TABLE foobar()"}),
			},
			outputContains: []string{"CREATE TABLE"},
		},
		{
			name: "from dir to dsn",
			dynamicArgs: []dArgGenerator{
				tempSchemaDirDArg("from-dir", nil),
				tempDsnDArg(suite.pgEngine, "to-dsn", []string{"CREATE TABLE foobar()"}),
			},
			outputContains: []string{"CREATE TABLE"},
		},
		{
			name: "from dir to dir",
			dynamicArgs: []dArgGenerator{
				tempSchemaDirDArg("from-dir", nil),
				tempSchemaDirDArg("to-dir", []string{"CREATE TABLE foobar()"}),
				tempDsnDArg(suite.pgEngine, "temp-db-dsn", []string{""}),
			},
			outputContains: []string{"CREATE TABLE"},
		},
		{
			name: "from empty dsn to dir",
			dynamicArgs: []dArgGenerator{
				func(t *testing.T) []string {
					db := tempDbWithSchema(t, suite.pgEngine, []string{""})
					tempSetPqEnvVarsForDb(t, db)
					return []string{"--from-empty-dsn"}
				},
				tempSchemaDirDArg("to-dir", []string{"CREATE TABLE foobar()"}),
			},
			outputContains: []string{"CREATE TABLE"},
		},
		{
			name:              "no from schema provided",
			args:              []string{"--to-dir", "some-other-dir"},
			expectErrContains: []string{"must be set"},
		},
		{
			name:              "no to schema provided",
			args:              []string{"--from-dir", "some-other-dir"},
			expectErrContains: []string{"must be set"},
		},
		{
			name:              "two from schemas provided",
			args:              []string{"--from-dir", "some-dir", "--from-dsn", "some-dsn", "--to-dir", "some-other-dir"},
			expectErrContains: []string{"only one of"},
		},
		{
			name:              "two to schemas provided",
			args:              []string{"--from-dir", "some-dir", "--to-dir", "some-other-dir", "--to-dsn", "some-dsn"},
			expectErrContains: []string{"only one of"},
		},
		{
			name:              "no postgres server provided",
			args:              []string{"--from-dir", "some-dir", "--to-dir", "some-other-dir"},
			expectErrContains: []string{"at least one Postgres server"},
		},
	} {
		suite.Run(tc.name, func() {
			suite.runCmdWithAssertions(runCmdWithAssertionsParams{
				args:              append([]string{"plan"}, tc.args...),
				dynamicArgs:       tc.dynamicArgs,
				outputContains:    tc.outputContains,
				expectErrContains: tc.expectErrContains,
			})
		})
	}
}

func (suite *cmdTestSuite) TestParseTimeoutModifierStr() {
	for _, tc := range []struct {
		opt                 string `explicit:"always"`
		expected            timeoutModifier
		expectedErrContains string
	}{
		{
			opt: `pattern="normal \"pattern\"" timeout=5m`,
			expected: timeoutModifier{
				regex:   regexp.MustCompile(`normal "pattern"`),
				timeout: 5 * time.Minute,
			},
		},
		{
			opt: `pattern=unquoted-no-space-pattern timeout=5m`,
			expected: timeoutModifier{
				regex:   regexp.MustCompile("unquoted-no-space-pattern"),
				timeout: 5 * time.Minute,
			},
		},
		{
			opt:                 "timeout=15m",
			expectedErrContains: "could not find key",
		},
		{
			opt:                 `pattern="some pattern"`,
			expectedErrContains: "could not find key",
		},
		{
			opt:                 `pattern="normal" timeout=5m some-unknown-key=5m`,
			expectedErrContains: "unknown keys",
		},
		{
			opt:                 `pattern="some-pattern" timeout=invalid-duration`,
			expectedErrContains: "duration could not be parsed",
		},
		{
			opt:                 `pattern="some-invalid-pattern-[" timeout=5m`,
			expectedErrContains: "pattern regex could not be compiled",
		},
	} {
		suite.Run(tc.opt, func() {
			modifier, err := parseTimeoutModifier(tc.opt)
			if len(tc.expectedErrContains) > 0 {
				suite.ErrorContains(err, tc.expectedErrContains)
				return
			}
			suite.Require().NoError(err)
			suite.Equal(tc.expected, modifier)
		})
	}
}

func (suite *cmdTestSuite) TestParseInsertStatementStr() {
	for _, tc := range []struct {
		opt                 string `explicit:"always"`
		expectedInsertStmt  insertStatement
		expectedErrContains string
	}{
		{
			opt: `index=1 statement="SELECT * FROM \"foobar\"" timeout=5m6s lock_timeout=1m11s`,
			expectedInsertStmt: insertStatement{
				index:       1,
				ddl:         `SELECT * FROM "foobar"`,
				timeout:     5*time.Minute + 6*time.Second,
				lockTimeout: 1*time.Minute + 11*time.Second,
			},
		},
		{
			opt:                 "statement=no-index timeout=5m6s lock_timeout=1m11s",
			expectedErrContains: "could not find key",
		},
		{
			opt:                 "index=0 timeout=5m6s lock_timeout=1m11s",
			expectedErrContains: "could not find key",
		},
		{
			opt:                 "index=0 statement=no-timeout lock_timeout=1m11s",
			expectedErrContains: "could not find key",
		},
		{
			opt:                 "index=0 statement=no-lock-timeout-timeout timeout=5m6s",
			expectedErrContains: "could not find key",
		},
		{
			opt:                 "index=not-an-int statement=some-statement timeout=5m6s lock_timeout=1m11s",
			expectedErrContains: "index could not be parsed",
		},
		{
			opt:                 "index=0 statement=some-statement timeout=invalid-duration lock_timeout=1m11s",
			expectedErrContains: "statement timeout duration could not be parsed",
		},
		{
			opt:                 "index=0 statement=some-statement timeout=5m6s lock_timeout=invalid-duration",
			expectedErrContains: "lock timeout duration could not be parsed",
		},
	} {
		suite.Run(tc.opt, func() {
			insertStatement, err := parseInsertStatementStr(tc.opt)
			if len(tc.expectedErrContains) > 0 {
				suite.ErrorContains(err, tc.expectedErrContains)
				return
			}
			suite.Require().NoError(err)
			suite.Equal(tc.expectedInsertStmt, insertStatement)
		})
	}
}
