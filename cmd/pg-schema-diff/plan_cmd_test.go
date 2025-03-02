package main

import (
	"regexp"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTimeoutModifierStr(t *testing.T) {
	for _, tc := range []struct {
		opt string `explicit:"always"`

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
			expectedErrContains: "could not find key", // "pattern" missing
		},
		{
			opt:                 `pattern="some pattern"`,
			expectedErrContains: "could not find key", // "timeout" missing
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
		t.Run(tc.opt, func(t *testing.T) {
			modifier, err := parseTimeoutModifier(tc.opt)
			if len(tc.expectedErrContains) > 0 {
				assert.ErrorContains(t, err, tc.expectedErrContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, modifier)
		})
	}
}

func TestParseInsertStatementStr(t *testing.T) {
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
			expectedErrContains: "could not find key", // "index" missing
		},
		{
			opt:                 "index=0 timeout=5m6s lock_timeout=1m11s",
			expectedErrContains: "could not find key", // "statement" missing
		},
		{
			opt:                 "index=0 statement=no-timeout lock_timeout=1m11s",
			expectedErrContains: "could not find key", // "timeout" missing
		},
		{
			opt:                 "index=0 statement=no-lock-timeout-timeout timeout=5m6s",
			expectedErrContains: "could not find key", // "lock_timeout" missing
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
		t.Run(tc.opt, func(t *testing.T) {
			insertStatement, err := parseInsertStatementStr(tc.opt)
			if len(tc.expectedErrContains) > 0 {
				assert.ErrorContains(t, err, tc.expectedErrContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expectedInsertStmt, insertStatement)
		})
	}
}

func TestPlanFlagsTemplateDBDefault(t *testing.T) {
	cmd := &cobra.Command{}
	flags := createPlanFlags(cmd)

	err := cmd.ParseFlags([]string{
		"--schema-dir=/no/such/dir",
	})
	require.NoError(t, err)

	planCfg, err := parsePlanConfig(*flags)
	require.NoError(t, err, "parsePlanConfig should not fail with a dummy --schema-dir")

	assert.Equal(t, "template0", planCfg.templateDb)
}

func TestPlanFlagsTemplateDBOverride(t *testing.T) {
	cmd := &cobra.Command{}
	flags := createPlanFlags(cmd)

	err := cmd.ParseFlags([]string{
		"--template-db=template1",
		"--schema-dir=/no/such/dir",
	})
	require.NoError(t, err)

	planCfg, err := parsePlanConfig(*flags)
	require.NoError(t, err, "parsePlanConfig should not fail with dummy --schema-dir")

	assert.Equal(t, "template1", planCfg.templateDb)
}
