package main

import (
	"github.com/stripe/pg-schema-diff/internal/pgdump"
	"github.com/stripe/pg-schema-diff/internal/pgengine"
)

func (suite *cmdTestSuite) TestApplyCmd() {
	// Non-comprehensive set of tests for the plan command. Not totally comprehensive to avoid needing to avoid
	// hindering developer velocity when updating the command.
	type testCase struct {
		name string
		// fromDbArg is an optional argument to override the default "--from-dsn" arg.
		fromDbArg func(db *pgengine.DB) []string
		args      []string
		// dynamicArgs is function that can be used to build args that are dynamic, i.e.,
		// saving schemas to a randomly generated temporary directory.
		dynamicArgs []dArgGenerator

		outputContains []string
		// expectedSchema is the schema that is expected to be in the database after the migration.
		// If nil, the expected schema will be the fromDDL.
		expectedSchemaDDL []string
		// expectErrContains is a list of substrings that are expected to be contained in the error returned by
		// cmd.RunE. This is DISTINCT from stdErr.
		expectErrContains []string
	}
	for _, tc := range []testCase{
		{
			name:        "to dir",
			dynamicArgs: []dArgGenerator{tempSchemaDirDArg("to-dir", []string{"CREATE TABLE foobar();"})},

			expectedSchemaDDL: []string{"CREATE TABLE foobar();"},
		},
		{
			name:        "to dsn",
			dynamicArgs: []dArgGenerator{tempDsnDArg(suite.pgEngine, "to-dsn", []string{"CREATE TABLE foobar();"})},

			expectedSchemaDDL: []string{"CREATE TABLE foobar();"},
		},
		{
			name: "from empty dsn",
			fromDbArg: func(db *pgengine.DB) []string {
				tempSetPqEnvVarsForDb(suite.T(), db)
				return []string{"--from-empty-dsn"}
			},
			dynamicArgs: []dArgGenerator{tempSchemaDirDArg("to-dir", []string{"CREATE TABLE foobar();"})},

			expectedSchemaDDL: []string{"CREATE TABLE foobar();"},
		},
		{
			name:              "no to schema provided",
			expectErrContains: []string{"must be set"},
		},
		{
			name:              "two to schemas provided",
			args:              []string{"--to-dir", "some-other-dir", "--to-dsn", "some-dsn"},
			expectErrContains: []string{"only one of"},
		},
	} {
		suite.Run(tc.name, func() {
			fromDb := tempDbWithSchema(suite.T(), suite.pgEngine, nil)
			if tc.fromDbArg == nil {
				tc.fromDbArg = func(db *pgengine.DB) []string {
					return []string{"--from-dsn", db.GetDSN()}
				}
			}
			args := append([]string{
				"apply",
				"--skip-confirm-prompt",
			}, tc.fromDbArg(fromDb)...)
			args = append(args, tc.args...)
			suite.runCmdWithAssertions(runCmdWithAssertionsParams{
				args:              args,
				dynamicArgs:       tc.dynamicArgs,
				outputContains:    tc.outputContains,
				expectErrContains: tc.expectErrContains,
			})
			// The migration should have been successful. Assert it was.
			expectedDb := tempDbWithSchema(suite.T(), suite.pgEngine, tc.expectedSchemaDDL)
			expectedDbDump, err := pgdump.GetDump(expectedDb, pgdump.WithSchemaOnly(), pgdump.WithRestrictKey(pgdump.FixedRestrictKey))
			suite.Require().NoError(err)
			fromDbDump, err := pgdump.GetDump(fromDb, pgdump.WithSchemaOnly(), pgdump.WithRestrictKey(pgdump.FixedRestrictKey))
			suite.Require().NoError(err)

			suite.Equal(expectedDbDump, fromDbDump)
		})
	}
}
