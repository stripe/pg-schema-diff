package set_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stripe/pg-schema-diff/internal/set"
)

func TestSet_Add(t *testing.T) {
	for _, tt := range []struct {
		name           string
		adds           [][]int
		expectedValues []int
	}{
		{
			name: "start empty",
			adds: [][]int{
				{},
				{1, 1, 1},
				{2, 3, 4},
				{},
				{4, 4, 4, 5},
			},
			expectedValues: []int{1, 2, 3, 4, 5},
		},
		{
			name: "start with values (and add out of order)",
			adds: [][]int{
				{1, 1, 1},
				{4, 4, 4, 5},
				{2, 3, 4},
			},
			expectedValues: []int{1, 2, 3, 4, 5},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := set.NewSet(tt.adds[0]...)
			for _, adds := range tt.adds {
				for _, add := range adds {
					s.Add(add)
					assert.True(t, s.Has(add))
				}
			}
			assert.Equal(t, tt.expectedValues, s.Values())
		})
	}
}

func TestDifference(t *testing.T) {
	for _, tc := range []struct {
		name       string
		a          *set.Set[int, int]
		b          *set.Set[int, int]
		difference []int
	}{
		{
			name: "empty sets",
			a:    set.NewSet[int](),
			b:    set.NewSet[int](),
		},
		{
			name: "a is empty",
			a:    set.NewSet[int](),
			b:    set.NewSet[int](1, 2, 3),
		},
		{
			name:       "b is empty",
			a:          set.NewSet[int](1, 2, 3),
			b:          set.NewSet[int](),
			difference: []int{1, 2, 3},
		},
		{
			name: "a and b are the same",
			a:    set.NewSet[int](1, 2, 3),
			b:    set.NewSet[int](1, 2, 3),
		},
		{
			name:       "a has elements b does not",
			a:          set.NewSet[int](5, 4, 1, 2, 3),
			b:          set.NewSet[int](1, 2, 3, 6),
			difference: []int{4, 5},
		},
		{
			name: "a is a subset of b",
			a:    set.NewSet[int](1, 2, 3),
			b:    set.NewSet[int](1, 2, 3, 4, 5),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			diff := set.Difference(tc.a, tc.b)
			assert.Equal(t, tc.difference, diff)
		})
	}
}
