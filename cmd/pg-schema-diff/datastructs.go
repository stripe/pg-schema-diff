package main

import (
	"fmt"
	"sort"
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
	var vals []string
	for k := range m {
		vals = append(vals, k)
	}
	sort.Strings(vals)
	return vals
}
