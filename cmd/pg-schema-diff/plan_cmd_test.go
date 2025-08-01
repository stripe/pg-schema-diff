package main

import (
	"database/sql"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stripe/pg-schema-diff/pkg/diff"
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
		{
			name: "sql output format - from dsn to dsn",
			args: []string{"--output-format", "sql"},
			dynamicArgs: []dArgGenerator{
				tempDsnDArg(suite.pgEngine, "from-dsn", nil),
				tempDsnDArg(suite.pgEngine, "to-dsn", []string{"CREATE TABLE foobar()"}),
			},
			outputContains: []string{"CREATE TABLE \"public\".\"foobar\"", ";"},
		},
		{
			name: "sql output format - from dsn to dir", 
			args: []string{"--output-format", "sql"},
			dynamicArgs: []dArgGenerator{
				tempDsnDArg(suite.pgEngine, "from-dsn", []string{""}),
				tempSchemaDirDArg("to-dir", []string{"CREATE TABLE foobar()"}),
			},
			outputContains: []string{"CREATE TABLE \"public\".\"foobar\"", ";"},
		},
		{
			name: "sql output format - multiple statements",
			args: []string{"--output-format", "sql"},
			dynamicArgs: []dArgGenerator{
				tempDsnDArg(suite.pgEngine, "from-dsn", nil),
				tempDsnDArg(suite.pgEngine, "to-dsn", []string{
					"CREATE TABLE table1()",
					"CREATE TABLE table2()",
				}),
			},
			outputContains: []string{"CREATE TABLE \"public\".\"table1\"", "CREATE TABLE \"public\".\"table2\"", ";"},
		},
		{
			name:              "invalid output format",
			args:              []string{"--output-format", "invalid"},
			dynamicArgs: []dArgGenerator{
				tempDsnDArg(suite.pgEngine, "from-dsn", nil),
				tempDsnDArg(suite.pgEngine, "to-dsn", []string{"CREATE TABLE foobar()"}),
			},
			expectErrContains: []string{"invalid output format"},
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

func TestPlanToSqlS(t *testing.T) {
	testCases := []struct {
		name     string
		plan     diff.Plan
		expected string
	}{
		{
			name: "empty plan",
			plan: diff.Plan{Statements: []diff.Statement{}},
			expected: "",
		},
		{
			name: "single statement",
			plan: diff.Plan{
				Statements: []diff.Statement{
					{DDL: "CREATE TABLE test ()"},
				},
			},
			expected: "CREATE TABLE test ();\n\t-- Statement Timeout: 0s",
		},
		{
			name: "multiple statements",
			plan: diff.Plan{
				Statements: []diff.Statement{
					{DDL: "CREATE TABLE test1 ()"},
					{DDL: "CREATE TABLE test2 ()"},
				},
			},
			expected: "CREATE TABLE test1 ();\n\t-- Statement Timeout: 0s\n\nCREATE TABLE test2 ();\n\t-- Statement Timeout: 0s",
		},
		{
			name: "statements with comments and timeouts should be included",
			plan: diff.Plan{
				Statements: []diff.Statement{
					{
						DDL:     "CREATE INDEX CONCURRENTLY idx_test ON test (col)",
						Timeout: time.Minute * 5,
						Hazards: []diff.MigrationHazard{
							{Type: "SOME_HAZARD", Message: "This is dangerous"},
						},
					},
				},
			},
			expected: "CREATE INDEX CONCURRENTLY idx_test ON test (col);\n\t-- Statement Timeout: 5m0s\n\t-- Hazard SOME_HAZARD: This is dangerous",
		},
		{
			name: "statements already ending with semicolon",
			plan: diff.Plan{
				Statements: []diff.Statement{
					{DDL: "CREATE TABLE test1 ();"},
					{DDL: "ALTER TABLE test SET DATA TYPE integer;"},
				},
			},
			expected: "CREATE TABLE test1 ();;\n\t-- Statement Timeout: 0s\n\nALTER TABLE test SET DATA TYPE integer;;\n\t-- Statement Timeout: 0s",
		},
		{
			name: "mixed statements with and without semicolons",
			plan: diff.Plan{
				Statements: []diff.Statement{
					{DDL: "CREATE TABLE test1 ()"},
					{DDL: "ALTER TABLE test SET DATA TYPE integer;"},
					{DDL: "DROP TABLE test2"},
				},
			},
			expected: "CREATE TABLE test1 ();\n\t-- Statement Timeout: 0s\n\nALTER TABLE test SET DATA TYPE integer;;\n\t-- Statement Timeout: 0s\n\nDROP TABLE test2;\n\t-- Statement Timeout: 0s",
		},
		{
			name: "statements with trailing whitespace",
			plan: diff.Plan{
				Statements: []diff.Statement{
					{DDL: "CREATE TABLE test ()  "},
					{DDL: "ALTER TABLE test ADD COLUMN id int;  \n"},
				},
			},
			expected: "CREATE TABLE test ()  ;\n\t-- Statement Timeout: 0s\n\nALTER TABLE test ADD COLUMN id int;  \n;\n\t-- Statement Timeout: 0s",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := planToSqlS(tc.plan)
			if result != tc.expected {
				t.Errorf("Expected:\n%s\nGot:\n%s", tc.expected, result)
			}
		})
	}
}

func TestOutputFormatValidation(t *testing.T) {
	testCases := []struct {
		name          string
		formatStr     string
		expectError   bool
		expectedValue outputFormat
	}{
		{
			name:          "valid pretty format",
			formatStr:     "pretty",
			expectError:   false,
			expectedValue: outputFormatPretty,
		},
		{
			name:          "valid json format",
			formatStr:     "json",
			expectError:   false,
			expectedValue: outputFormatJson,
		},
		{
			name:          "valid sql format",
			formatStr:     "sql",
			expectError:   false,
			expectedValue: outputFormatSql,
		},
		{
			name:        "invalid format",
			formatStr:   "invalid",
			expectError: true,
		},
		{
			name:        "empty format",
			formatStr:   "",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var format outputFormat
			err := format.Set(tc.formatStr)
			
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error for format '%s', but got none", tc.formatStr)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for format '%s': %v", tc.formatStr, err)
				}
				if format.identifier != tc.expectedValue.identifier {
					t.Errorf("Expected identifier '%s', got '%s'", tc.expectedValue.identifier, format.identifier)
				}
			}
		})
	}
}

func TestSqlFormatDoesNotContainHeaders(t *testing.T) {
	// Test that SQL format doesn't contain formatting headers like "####" or "1."
	plan := diff.Plan{
		Statements: []diff.Statement{
			{DDL: "CREATE TABLE test_table (id int)"},
		},
	}
	
	result := planToSqlS(plan)
	
	// Check that the result doesn't contain pretty format markers
	forbiddenStrings := []string{"####", "Generated plan", "1.", "2.", "3."}
	for _, forbidden := range forbiddenStrings {
		if strings.Contains(result, forbidden) {
			t.Errorf("SQL format output should not contain '%s', but found it in: %s", forbidden, result)
		}
	}
	
	// Check that it contains proper SQL
	if !strings.Contains(result, "CREATE TABLE test_table") {
		t.Errorf("SQL format should contain the actual SQL statement")
	}
	if !strings.Contains(result, ";") {
		t.Errorf("SQL format should end statements with semicolon")
	}
}

func (suite *cmdTestSuite) TestSqlOutputExecutable() {
	// End-to-end test to verify that generated SQL can actually be executed
	// Create source and target databases
	sourceDb := tempDbWithSchema(suite.T(), suite.pgEngine, []string{
		"CREATE TABLE users (id int PRIMARY KEY)",
	})
	targetDb := tempDbWithSchema(suite.T(), suite.pgEngine, []string{
		"CREATE TABLE users (id int PRIMARY KEY)",
		"CREATE TABLE posts (id int PRIMARY KEY, user_id int REFERENCES users(id))",
	})
	
	// Create a third database to test the generated SQL
	testDb := tempDbWithSchema(suite.T(), suite.pgEngine, []string{
		"CREATE TABLE users (id int PRIMARY KEY)",
	})
	
	// Generate SQL using our new format
	args := []string{
		"plan",
		"--output-format", "sql",
		"--from-dsn", sourceDb.GetDSN(),
		"--to-dsn", targetDb.GetDSN(),
	}
	
	rootCmd := buildRootCmd()
	rootCmd.SetArgs(args)
	var sqlOutput strings.Builder
	rootCmd.SetOut(&sqlOutput)
	rootCmd.SetErr(&strings.Builder{})
	
	err := rootCmd.Execute()
	suite.Require().NoError(err)
	
	generatedSQL := sqlOutput.String()
	suite.T().Logf("Generated SQL: %s", generatedSQL)
	
	// Verify the SQL is not empty and contains expected content
	suite.Assert().NotEmpty(generatedSQL)
	suite.Assert().Contains(generatedSQL, "CREATE TABLE")
	suite.Assert().Contains(generatedSQL, "posts")
	
	// Now try to execute the generated SQL against the test database
	conn, err := sql.Open("pgx", testDb.GetDSN())
	suite.Require().NoError(err)
	defer conn.Close()
	
	// Split SQL by semicolons and execute each statement
	sqlStatements := strings.Split(strings.TrimSpace(generatedSQL), ";")
	for _, stmt := range sqlStatements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		// Add semicolon back for execution
		stmt += ";"
		suite.T().Logf("Executing SQL: %s", stmt)
		_, err := conn.Exec(stmt)
		suite.Require().NoError(err, "Failed to execute generated SQL statement: %s", stmt)
	}
	
	// Verify that the posts table was created successfully
	var tableName string
	err = conn.QueryRow("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'posts'").Scan(&tableName)
	suite.Require().NoError(err)
	suite.Assert().Equal("posts", tableName)
}
