package main

import (
	"fmt"
)

func mustGetAndDeleteKey(m map[string]string, key string) (string, error) {
	val, ok := m[key]
	if !ok {
		return "", fmt.Errorf("could not find key %q", key)
	}
	delete(m, key)
	return val, nil
}
