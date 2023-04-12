package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseStatementTimeoutModifierStr(t *testing.T) {
	for _, tc := range []struct {
		opt string `explicit:"always"`

		expectedRegexStr    string
		expectedTimeout     time.Duration
		expectedErrContains string
	}{
		{
			opt:              "normal duration=5m",
			expectedRegexStr: "normal duration",
			expectedTimeout:  5 * time.Minute,
		},
		{
			opt:              "some regex with a duration ending in a period=5.h",
			expectedRegexStr: "some regex with a duration ending in a period",
			expectedTimeout:  5 * time.Hour,
		},
		{
			opt:              " starts with spaces than has a *=5.5m",
			expectedRegexStr: " starts with spaces than has a *",
			expectedTimeout:  time.Minute*5 + 30*time.Second,
		},
		{
			opt:              "has a valid opt in the regex something=5.5m in the regex =15s",
			expectedRegexStr: "has a valid opt in the regex something=5.5m in the regex ",
			expectedTimeout:  15 * time.Second,
		},
		{
			opt:              "has multiple valid opts opt=15m5s in the regex something=5.5m in the regex and has compound duration=15m1ms2us10ns",
			expectedRegexStr: "has multiple valid opts opt=15m5s in the regex something=5.5m in the regex and has compound duration",
			expectedTimeout:  15*time.Minute + 1*time.Millisecond + 2*time.Microsecond + 10*time.Nanosecond,
		},
		{
			opt:                 "=5m",
			expectedErrContains: "could not parse regex and duration from arg",
		},
		{
			opt:                 "15m",
			expectedErrContains: "could not parse regex and duration from arg",
		},
		{
			opt:                 "someregex;15m",
			expectedErrContains: "could not parse regex and duration from arg",
		},
		{
			opt:                 "someregex=invalid duration5s",
			expectedErrContains: "duration could not be parsed",
		},
	} {
		t.Run(tc.opt, func(t *testing.T) {
			modifier, err := parseStatementTimeoutModifierStr(tc.opt)
			if len(tc.expectedErrContains) > 0 {
				assert.ErrorContains(t, err, tc.expectedErrContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expectedRegexStr, modifier.regex.String())
			assert.Equal(t, tc.expectedTimeout, modifier.timeout)
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
			opt: "1 0h5.1m:SELECT * FROM :TABLE:0_5m:something",
			expectedInsertStmt: insertStatement{
				index:   1,
				ddl:     "SELECT * FROM :TABLE:0_5m:something",
				timeout: 5*time.Minute + 6*time.Second,
			},
		},
		{
			opt: "0 100ms:SELECT 1; SELECT * FROM something;",
			expectedInsertStmt: insertStatement{
				index:   0,
				ddl:     "SELECT 1; SELECT * FROM something",
				timeout: 100 * time.Millisecond,
			},
		},
		{
			opt:                 " 5s:No index",
			expectedErrContains: "could not parse",
		},
		{
			opt:                 "0 5g:Invalid duration",
			expectedErrContains: "duration could not be parsed",
		},
		{
			opt:                 "0 5s:",
			expectedErrContains: "could not parse",
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
