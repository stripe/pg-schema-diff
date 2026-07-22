// Package diff compares PostgreSQL schemas and generates ordered migration plans.
//
// Removed and recreated tables are retained by moving them into marked archival
// schemas. Plan.Statements performs the ordinary migration without deleting the
// retained rows. Plan.CleanupStatements contains separately ordered, destructive
// cleanup SQL that is never applied automatically and must not be concatenated
// with Statements.
package diff
