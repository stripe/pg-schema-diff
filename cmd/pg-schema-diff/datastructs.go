package main

import (
	"fmt"
	"sort"

	"github.com/stripe/pg-schema-diff/internal/util"
)

func mustGetAndDeleteKey(m map[string]string, key string) (string, error) {
	val, ok := m[key]
	if !ok {
		return "", fmt.Errorf("could not find key %q", key)
	}
	delete(m, key)
	return val, nil
}

func keys(m map[string]string) []string {
	vals := util.Keys(m)
	sort.Strings(vals)
	return vals
}
