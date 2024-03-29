package set

import "sort"

type ordered interface {
	~int | ~string
}

// Set is a set of values of type V, indexed by keys of type K. We require an orderable type, so that we can
// provide a deterministic output. There are other ways to return a deterministic output (i.e., maintain the insert order),
// but this is the simplest.
type Set[K ordered, V any] struct {
	valuesByKey map[K]V
	getKey      func(V) K
}

func NewSet[T ordered](vals ...T) *Set[T, T] {
	return NewSetWithCustomKey(func(v T) T {
		return v
	}, vals...)
}

func NewSetWithCustomKey[K ordered, V any](getKey func(V) K, vals ...V) *Set[K, V] {
	set := &Set[K, V]{
		valuesByKey: make(map[K]V),
		getKey:      getKey,
	}
	set.Add(vals...)
	return set
}

func (s *Set[K, V]) Add(vals ...V) {
	for _, val := range vals {
		s.valuesByKey[s.getKey(val)] = val
	}
}

func (s *Set[K, V]) Has(val V) bool {
	_, ok := s.valuesByKey[s.getKey(val)]
	return ok
}

func (s *Set[K, V]) Values() []V {
	values := make([]V, 0, len(s.valuesByKey))
	for _, val := range s.valuesByKey {
		values = append(values, val)
	}
	sort.Slice(values, func(i, j int) bool {
		return s.getKey(values[i]) < s.getKey(values[j])
	})
	return values
}

func Difference[K ordered, V any](a, b *Set[K, V]) []V {
	var vals []V
	for _, val := range a.Values() {
		if !b.Has(val) {
			vals = append(vals, val)
		}
	}
	return vals
}
