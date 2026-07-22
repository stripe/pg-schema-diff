// Package schema exposes schema snapshot hashing without exposing the internal
// schema model. GetSchemaHash uses the default archival prefix, while
// GetSchemaHashWithArchivalPrefix must be used with plans generated using a
// custom prefix. Both return the same versioned source hash contract used by
// diff.Plan.CurrentSchemaHash.
package schema
