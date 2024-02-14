package main

import (
	"fmt"
	"strings"

	"github.com/go-logfmt/logfmt"
	"github.com/jackc/pgx/v4"
	"github.com/spf13/cobra"
	"github.com/stripe/pg-schema-diff/pkg/log"
)

type connFlags struct {
	dsn string
}

func createConnFlags(cmd *cobra.Command) *connFlags {
	flags := &connFlags{}

	cmd.Flags().StringVar(&flags.dsn, "dsn", "", "Connection string for the database (DB password can be specified through PGPASSWORD environment variable)")
	// Don't mark dsn as a required flag.
	// Allow users to use the "PGHOST" etc environment variables like `psql`.

	return flags
}

func parseConnConfig(c connFlags, logger log.Logger) (*pgx.ConnConfig, error) {
	if c.dsn == "" {
		logger.Warnf("DSN flag not set. Using libpq environment variables and default values.")
	}

	return pgx.ParseConfig(c.dsn)
}

// LogFmtToMap parses all LogFmt key/value pairs from the provided string into a
// map.
//
// All records are scanned. If a duplicate key is found, an error is returned.
func LogFmtToMap(logFmt string) (map[string]string, error) {
	logMap := make(map[string]string)
	decoder := logfmt.NewDecoder(strings.NewReader(logFmt))
	for decoder.ScanRecord() {
		for decoder.ScanKeyval() {
			if _, ok := logMap[string(decoder.Key())]; ok {
				return nil, fmt.Errorf("duplicate key %q in logfmt", string(decoder.Key()))
			}
			logMap[string(decoder.Key())] = string(decoder.Value())
		}
	}
	if decoder.Err() != nil {
		return nil, decoder.Err()
	}
	return logMap, nil
}
