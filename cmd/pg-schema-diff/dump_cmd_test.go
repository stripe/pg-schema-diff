package main

func (suite *cmdTestSuite) TestDumpCmd() {
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
	} {
		suite.Run(tc.name, func() {
			suite.runCmdWithAssertions(runCmdWithAssertionsParams{
				args:              append([]string{"dump"}, tc.args...),
				dynamicArgs:       tc.dynamicArgs,
				outputContains:    tc.outputContains,
				expectErrContains: tc.expectErrContains,
			})
		})
	}
}
