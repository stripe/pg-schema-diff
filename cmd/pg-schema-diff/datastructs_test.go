package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeys(t *testing.T) {
	for _, tt := range []struct {
		name string
		m    map[string]string

		want []string
	}{
		{
			name: "nil map",

			want: nil,
		},
		{
			name: "empty map",

			want: nil,
		},
		{
			name: "filled map",
			m: map[string]string{
				// Use an arbitrary order
				"key2": "value2",
				"key3": "value3",
				"key1": "value1",
			},

			want: []string{"key1", "key2", "key3"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, keys(tt.m))
		})
	}
}
