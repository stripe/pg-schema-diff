package main

func (suite *cmdTestSuite) TestDumpCmd() {
	type testCase struct {
		name        string
		args        []string
		dynamicArgs []dArgGenerator

		// outputContains is a list of substrings that are expected to be contained in the stdout output of the command.
		outputContains []string
		// outputNotContains is a list of substrings that are expected to NOT be contained in the stdout output.
		outputNotContains []string
		// expectErrContains is a list of substrings that are expected to be contained in the error returned by
		// cmd.RunE. This is DISTINCT from stdErr.
		expectErrContains []string
	}

	for _, tc := range []testCase{
		{
			name: "dump database with table",
			dynamicArgs: []dArgGenerator{
				tempDsnDArg(suite.pgEngine, "dsn", []string{
					"CREATE TABLE foobar(id INT PRIMARY KEY, name TEXT NOT NULL)",
				}),
			},
			outputContains: []string{
				"CREATE TABLE",
				"foobar",
				"id",
				"name",
			},
		},
		{
			name: "dump with exclude-table",
			args: []string{"--exclude-table", "tmp_.*"},
			dynamicArgs: []dArgGenerator{
				tempDsnDArg(suite.pgEngine, "dsn", []string{
					"CREATE TABLE foobar(id INT PRIMARY KEY)",
					"CREATE TABLE tmp_foo(id INT PRIMARY KEY)",
				}),
			},
			outputContains:    []string{"foobar"},
			outputNotContains: []string{"tmp_foo"},
		},
		{
			name: "dump with invalid exclude-table pattern",
			args: []string{"--exclude-table", "["},
			dynamicArgs: []dArgGenerator{
				tempDsnDArg(suite.pgEngine, "dsn", nil),
			},
			expectErrContains: []string{"invalid --exclude-table pattern"},
		},
	} {
		suite.Run(tc.name, func() {
			suite.runCmdWithAssertions(runCmdWithAssertionsParams{
				args:              append([]string{"dump"}, tc.args...),
				dynamicArgs:       tc.dynamicArgs,
				outputContains:    tc.outputContains,
				outputNotContains: tc.outputNotContains,
				expectErrContains: tc.expectErrContains,
			})
		})
	}
}
