package main

import (
	"fmt"
	"strings"

	"github.com/go-logfmt/logfmt"
	"github.com/jackc/pgx/v4"
	"github.com/spf13/cobra"
)

type connectionFlags struct {
	dsn         string
	dsnFlagName string
}

func createConnectionFlags(cmd *cobra.Command, prefix string, additionalHelp string) *connectionFlags {
	var c connectionFlags

	c.dsnFlagName = prefix + "dsn"
	dsnFlagHelp := "Connection string for the database (DB password can be specified through PGPASSWORD environment variable)."
	if additionalHelp != "" {
		dsnFlagHelp += " " + additionalHelp
	}
	cmd.Flags().StringVar(&c.dsn, c.dsnFlagName, "", dsnFlagHelp)

	return &c
}

func parseConnectionFlags(flags *connectionFlags) (*pgx.ConnConfig, error) {
	connConfig, err := pgx.ParseConfig(flags.dsn)
	if err != nil {
		return nil, fmt.Errorf("could not parse connection string %q: %w", flags.dsn, err)
	}
	return connConfig, nil
}

// logFmtToMap parses all LogFmt key/value pairs from the provided string into a
// map.
//
// All records are scanned. If a duplicate key is found, an error is returned.
func logFmtToMap(logFmt string) (map[string]string, error) {
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
