package util

// Keys returns the keys for a map. An analog be added in golang 1.23.
// TODO(https://github.com/stripe/pg-schema-diff/issues/227) - Remove this
func Keys[K comparable, V any](val map[K]V) []K {
	out := make([]K, 0, len(val))
	for k := range val {
		out = append(out, k)
	}
	return out
}
