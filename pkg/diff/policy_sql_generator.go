package diff

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

var (
	migrationHazardRLSEnabled = MigrationHazard{
		Type:    MigrationHazardTypeAuthzUpdate,
		Message: "Enabling RLS on a table could cause queries to fail if not correctly configured.",
	}
	migrationHazardRLSDisabled = MigrationHazard{
		Type:    MigrationHazardTypeAuthzUpdate,
		Message: "Disabling RLS on a table could allow unauthorized access to data.",
	}
	migrationHazardRLSForced = MigrationHazard{
		Type:    MigrationHazardTypeAuthzUpdate,
		Message: "Forcing RLS on a table could cause queries to fail if not correctly configured.",
	}
	migrationHazardRLSUnforced = MigrationHazard{
		Type:    MigrationHazardTypeAuthzUpdate,
		Message: "Disabling forcing RLS on a table could allow unauthorized access to data.",
	}

	migrationHazardPermissivePolicyAdded = MigrationHazard{
		Type:    MigrationHazardTypeAuthzUpdate,
		Message: "Adding a permissive policy could allow unauthorized access to data.",
	}
	migrationHazardPermissivePolicyRemoved = MigrationHazard{
		Type:    MigrationHazardTypeAuthzUpdate,
		Message: "Removing a permissive policy could cause queries to fail if not correctly configured.",
	}

	migrationHazardRestrictivePolicyAdded = MigrationHazard{
		Type:    MigrationHazardTypeAuthzUpdate,
		Message: "Adding a restrictive policy could cause queries to fail if not correctly configured.",
	}
	migrationHazardRestrictivePolicyRemoved = MigrationHazard{
		Type:    MigrationHazardTypeAuthzUpdate,
		Message: "Removing a restrictive policy could allow unauthorized access to data.",
	}

	migrationHazardPolicyAltered = MigrationHazard{
		Type:    MigrationHazardTypeAuthzUpdate,
		Message: "Altering a policy could cause queries to fail if not correctly configured or allow unauthorized access to data.",
	}
)

// When building/altering RLS policies, we must maintain the following order:
// 1. Create/alter table such that all necessary columns exist
// 2. Create/alter policies
// 3. Enable RLS -- This MUST be done last
//
// If not done in this order, we may create an outtage for a user's queries where RLS rejects their queries because
// the policy allowing them hasn't been created yet. The same is true for disabling RLS, but in the reverse order. RLS
// must be disabled before policies are dropped.
//
// Another quirk of policies: Policies on partitions must be dropped before the base table is altered, otherwise
// the SQL could fail because, e.g., the policy references a column that no longer exists.

func enableRLSForTable(t schema.Table) Statement {
	return Statement{
		DDL:         fmt.Sprintf("%s ENABLE ROW LEVEL SECURITY", alterTablePrefix(t.SchemaQualifiedName)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     []MigrationHazard{migrationHazardRLSEnabled},
	}
}

func disableRLSForTable(t schema.Table) Statement {
	return Statement{
		DDL:         fmt.Sprintf("%s DISABLE ROW LEVEL SECURITY", alterTablePrefix(t.SchemaQualifiedName)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     []MigrationHazard{migrationHazardRLSDisabled},
	}
}

func forceRLSForTable(t schema.Table) Statement {
	return Statement{
		DDL:         fmt.Sprintf("%s FORCE ROW LEVEL SECURITY", alterTablePrefix(t.SchemaQualifiedName)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     []MigrationHazard{migrationHazardRLSForced},
	}

}

func unforceRLSForTable(t schema.Table) Statement {
	return Statement{
		DDL:         fmt.Sprintf("%s NO FORCE ROW LEVEL SECURITY", alterTablePrefix(t.SchemaQualifiedName)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     []MigrationHazard{migrationHazardRLSUnforced},
	}
}

// escapeRoleNames escapes role names for use in SQL policy statements.
// PUBLIC is a SQL keyword (not an identifier) and must not be quoted.
func escapeRoleNames(roles []string) []string {
	escaped := make([]string, len(roles))
	for i, role := range roles {
		if role == "PUBLIC" {
			escaped[i] = role
		} else {
			escaped[i] = schema.EscapeIdentifier(role)
		}
	}
	return escaped
}

type policyDiff struct {
	oldAndNew[schema.Policy]
}

func buildPolicyDiffs(psg sqlVertexGenerator[schema.Policy, policyDiff], old, new []schema.Policy) (listDiff[schema.Policy, policyDiff], error) {
	return diffLists(old, new, func(old, new schema.Policy, _, _ int) (_ policyDiff, requiresRecreate bool, _ error) {
		diff := policyDiff{
			oldAndNew: oldAndNew[schema.Policy]{
				old: old, new: new,
			},
		}

		if _, err := psg.Alter(diff); err != nil {
			if errors.Is(err, ErrNotImplemented) {
				// If we can't generate the alter SQL, we'll have to recreate the policy.
				return diff, true, nil
			}
			return policyDiff{}, false, fmt.Errorf("generating alter SQL: %w", err)
		}

		return diff, false, nil
	})
}

type policySQLVertexGenerator struct {
	table                  schema.Table
	oldTable               *schema.Table
	newSchemaColumnsByName map[string]schema.Column
	oldSchemaColumnsByName map[string]schema.Column
}

func newPolicySQLVertexGenerator(oldTable *schema.Table, table schema.Table) (sqlVertexGenerator[schema.Policy, policyDiff], error) {
	var oldSchemaColumnsByName map[string]schema.Column
	if oldTable != nil {
		if oldTable.SchemaQualifiedName != table.SchemaQualifiedName {
			return nil, fmt.Errorf("old and new tables must have the same schema-qualified name. new=%s, old=%s", table.GetFQEscapedName(), oldTable.GetFQEscapedName())
		}
		oldSchemaColumnsByName = buildSchemaObjByNameMap(oldTable.Columns)
	}

	return legacyToNewSqlVertexGenerator[schema.Policy, policyDiff](&policySQLVertexGenerator{
		table:                  table,
		newSchemaColumnsByName: buildSchemaObjByNameMap(table.Columns),
		oldTable:               oldTable,
		oldSchemaColumnsByName: oldSchemaColumnsByName,
	}), nil
}

func (psg *policySQLVertexGenerator) Add(p schema.Policy) ([]Statement, error) {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("CREATE POLICY %s ON %s", p.EscapedName, psg.table.GetFQEscapedName()))

	typeModifier := "RESTRICTIVE"
	if p.IsPermissive {
		typeModifier = "PERMISSIVE"
	}
	sb.WriteString(fmt.Sprintf("\n\tAS %s", typeModifier))

	cmdSQL, err := policyCharToSQL(p.Cmd)
	if err != nil {
		return nil, err
	}
	sb.WriteString(fmt.Sprintf("\n\tFOR %s", cmdSQL))

	sb.WriteString(fmt.Sprintf("\n\tTO %s", strings.Join(escapeRoleNames(p.AppliesTo), ", ")))

	if p.UsingExpression != "" {
		sb.WriteString(fmt.Sprintf("\n\tUSING (%s)", p.UsingExpression))
	}
	if p.CheckExpression != "" {
		sb.WriteString(fmt.Sprintf("\n\tWITH CHECK (%s)", p.CheckExpression))
	}

	hazard := migrationHazardRestrictivePolicyAdded
	if p.IsPermissive {
		hazard = migrationHazardPermissivePolicyAdded
	}

	return []Statement{{
		DDL:         sb.String(),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     []MigrationHazard{hazard},
	}}, nil
}

func policyCharToSQL(c schema.PolicyCmd) (string, error) {
	switch c {
	case schema.SelectPolicyCmd:
		return "SELECT", nil
	case schema.InsertPolicyCmd:
		return "INSERT", nil
	case schema.UpdatePolicyCmd:
		return "UPDATE", nil
	case schema.DeletePolicyCmd:
		return "DELETE", nil
	case schema.AllPolicyCmd:
		return "ALL", nil
	default:
		return "", fmt.Errorf("unknown policy command: %v", c)
	}
}

func (psg *policySQLVertexGenerator) Delete(p schema.Policy) ([]Statement, error) {
	hazard := migrationHazardRestrictivePolicyRemoved
	if p.IsPermissive {
		hazard = migrationHazardPermissivePolicyRemoved
	}
	return []Statement{{
		DDL:         fmt.Sprintf("DROP POLICY %s ON %s", p.EscapedName, psg.table.GetFQEscapedName()),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     []MigrationHazard{hazard},
	}}, nil
}

func (psg *policySQLVertexGenerator) Alter(diff policyDiff) ([]Statement, error) {
	oldCopy := diff.old

	// alterPolicyParts represents the set of strings to include in the ALTER POLICY ... ON TABLE ... statement
	var alterPolicyParts []string

	if !cmp.Equal(oldCopy.AppliesTo, diff.new.AppliesTo) {
		alterPolicyParts = append(alterPolicyParts, fmt.Sprintf("TO %s", strings.Join(escapeRoleNames(diff.new.AppliesTo), ", ")))
		oldCopy.AppliesTo = diff.new.AppliesTo
	}

	if oldCopy.UsingExpression != diff.new.UsingExpression && diff.new.UsingExpression != "" {
		// Weirdly, you can't actually drop a "USING EXPRESSION" clause from an ALL policy even though you
		// can have an ALL policy with only a check expression.
		alterPolicyParts = append(alterPolicyParts, fmt.Sprintf("USING (%s)", diff.new.UsingExpression))
		oldCopy.UsingExpression = diff.new.UsingExpression
	}

	if oldCopy.CheckExpression != diff.new.CheckExpression && diff.new.CheckExpression != "" {
		// Same quirk as above with ALL policies.
		alterPolicyParts = append(alterPolicyParts, fmt.Sprintf("WITH CHECK (%s)", diff.new.CheckExpression))
		oldCopy.CheckExpression = diff.new.CheckExpression
	}
	oldCopy.Columns = diff.new.Columns
	// Dependencies follow the expressions, which ALTER POLICY already covers.
	oldCopy.TableDependencies = diff.new.TableDependencies
	oldCopy.DependsOnFunctions = diff.new.DependsOnFunctions

	if diff := cmp.Diff(oldCopy, diff.new); diff != "" {
		return nil, fmt.Errorf("unsupported diff %s: %w", diff, ErrNotImplemented)
	}

	if len(alterPolicyParts) == 0 {
		// There is no diff
		return nil, nil
	}

	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("ALTER POLICY %s ON %s\n\t", diff.new.EscapedName, psg.table.GetFQEscapedName()))
	sb.WriteString(strings.Join(alterPolicyParts, "\n\t"))

	return []Statement{{
		DDL:         sb.String(),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     []MigrationHazard{migrationHazardPolicyAltered},
	}}, nil
}

func (psg *policySQLVertexGenerator) GetSQLVertexId(p schema.Policy, diffType diffType) sqlVertexId {
	return buildPolicyVertexId(psg.table.SchemaQualifiedName, p.EscapedName, diffType)
}

func buildPolicyVertexId(owningTable schema.SchemaQualifiedName, policyEscapedName string, diffType diffType) sqlVertexId {
	return buildSchemaObjVertexId("policy", fmt.Sprintf("%s.%s", owningTable.GetFQEscapedName(), policyEscapedName), diffType)
}

func (psg *policySQLVertexGenerator) GetAddAlterDependencies(newPolicy, oldPolicy schema.Policy) ([]dependency, error) {
	deps := []dependency{
		mustRun(psg.GetSQLVertexId(newPolicy, diffTypeDelete)).before(psg.GetSQLVertexId(newPolicy, diffTypeAddAlter)),
	}

	newTargetColumns, err := getTargetColumns(newPolicy.Columns, psg.newSchemaColumnsByName)
	if err != nil {
		return nil, fmt.Errorf("getting target columns: %w", err)
	}

	// Run after the new columns are added/altered
	for _, tc := range newTargetColumns {
		deps = append(deps, mustRun(psg.GetSQLVertexId(newPolicy, diffTypeAddAlter)).after(buildColumnVertexId(tc.Name, diffTypeAddAlter)))
	}

	// Run after the other relations and the functions the expressions reference exist
	deps = append(deps, policyReferenceAddDependencies(psg.GetSQLVertexId(newPolicy, diffTypeAddAlter), newPolicy)...)

	if !cmp.Equal(oldPolicy, schema.Policy{}) {
		// Run before the old columns are deleted (if they are deleted)
		oldTargetColumns, err := getTargetColumns(oldPolicy.Columns, psg.oldSchemaColumnsByName)
		if err != nil {
			return nil, fmt.Errorf("getting target columns: %w", err)
		}
		for _, tc := range oldTargetColumns {
			// It only needs to run before the delete if the column is actually being deleted
			if _, stillExists := psg.newSchemaColumnsByName[tc.GetName()]; !stillExists {
				deps = append(deps, mustRun(psg.GetSQLVertexId(newPolicy, diffTypeAddAlter)).before(buildColumnVertexId(tc.Name, diffTypeDelete)))
			}
		}
	}

	return deps, nil
}

func (psg *policySQLVertexGenerator) GetDeleteDependencies(pol schema.Policy) ([]dependency, error) {
	var deps []dependency

	columns, err := getTargetColumns(pol.Columns, psg.oldSchemaColumnsByName)
	if err != nil {
		return nil, fmt.Errorf("getting target columns: %w", err)
	}
	// The policy needs to be deleted before all the columns it references are deleted or add/altered
	for _, c := range columns {
		deps = append(deps, mustRun(psg.GetSQLVertexId(pol, diffTypeDelete)).before(buildColumnVertexId(c.Name, diffTypeDelete)))
		deps = append(deps, mustRun(psg.GetSQLVertexId(pol, diffTypeDelete)).before(buildColumnVertexId(c.Name, diffTypeAddAlter)))
	}

	// ... and before the other relations and functions its expressions reference are dropped or recreated
	deps = append(deps, policyReferenceDeleteDependencies(psg.GetSQLVertexId(pol, diffTypeDelete), pol)...)

	return deps, nil
}

// policyHasExternalDependencies reports whether the policy expressions reference relations other than the
// owning table, or call functions.
func policyHasExternalDependencies(p schema.Policy) bool {
	return len(p.TableDependencies) > 0 || len(p.DependsOnFunctions) > 0
}

// generateDeferredPolicyPartialGraph creates the policies of brand-new tables that reference other relations or
// functions as their own vertices, instead of inline with the table (see tableSQLVertexGenerator.Add): they must
// run after the table and after what they reference, and what they reference may in turn depend on the table —
// e.g., a function that reads the table it guards — so an edge on the table itself would form a cycle.
func generateDeferredPolicyPartialGraph(addedTables []schema.Table) (partialSQLGraph, error) {
	var parts []partialSQLGraph
	for _, table := range addedTables {
		if table.IsPartition() {
			// Policies on partitions are not supported (see tableSQLVertexGenerator.Add)
			continue
		}
		generator, err := newPolicySQLVertexGenerator(nil, table)
		if err != nil {
			return partialSQLGraph{}, fmt.Errorf("creating policy sql vertex generator for %s: %w", table.GetName(), err)
		}
		for _, policy := range table.Policies {
			if !policyHasExternalDependencies(policy) {
				continue
			}
			part, err := generator.Add(policy)
			if err != nil {
				return partialSQLGraph{}, fmt.Errorf("generating add policy statements for policy %s: %w", policy.EscapedName, err)
			}
			// Remove hazards from statements since the table is brand new, as for the inlined policies
			for i := range part.vertices {
				part.vertices[i].statements = stripMigrationHazards(part.vertices[i].statements...)
			}
			part.dependencies = append(part.dependencies,
				mustRun(buildPolicyVertexId(table.SchemaQualifiedName, policy.EscapedName, diffTypeAddAlter)).after(buildTableVertexId(table.SchemaQualifiedName, diffTypeAddAlter)),
			)
			parts = append(parts, part)
		}
	}
	return concatPartialGraphs(parts...), nil
}

// policyReferenceAddDependencies makes vertex run after the relations and functions the policy expressions
// reference exist, e.g., a subquery on another table created in the same migration. A referenced relation may
// be a table, a view or a materialized view, so edges to all three vertex ids are added; edges to vertices
// absent from the graph are no-ops. The vertex is the policy's own when the policy is added or altered on an
// existing table, and the table's when the table is new, because its policies are created inline with it.
func policyReferenceAddDependencies(vertex sqlVertexId, p schema.Policy) []dependency {
	var deps []dependency
	for _, t := range p.TableDependencies {
		deps = append(deps,
			mustRun(vertex).after(buildTableVertexId(t.SchemaQualifiedName, diffTypeDelete)),
			mustRun(vertex).after(buildTableVertexId(t.SchemaQualifiedName, diffTypeAddAlter)),
			mustRun(vertex).after(buildViewVertexId(t.SchemaQualifiedName, diffTypeAddAlter)),
			mustRun(vertex).after(buildMaterializedViewVertexId(t.SchemaQualifiedName, diffTypeAddAlter)),
		)
	}
	for _, f := range p.DependsOnFunctions {
		deps = append(deps, mustRun(vertex).after(buildFunctionVertexId(f, diffTypeAddAlter)))
	}
	return deps
}

// policyReferenceDeleteDependencies makes vertex run before the relations and functions the policy expressions
// reference are dropped or recreated: Postgres refuses to drop them while the policy depends on them. The vertex
// is the policy's own, or the table's when the whole table is dropped with its policies.
func policyReferenceDeleteDependencies(vertex sqlVertexId, p schema.Policy) []dependency {
	var deps []dependency
	for _, t := range p.TableDependencies {
		deps = append(deps,
			mustRun(vertex).before(buildTableVertexId(t.SchemaQualifiedName, diffTypeDelete)),
			mustRun(vertex).before(buildTableVertexId(t.SchemaQualifiedName, diffTypeAddAlter)),
			mustRun(vertex).before(buildViewVertexId(t.SchemaQualifiedName, diffTypeDelete)),
			mustRun(vertex).before(buildMaterializedViewVertexId(t.SchemaQualifiedName, diffTypeDelete)),
		)
	}
	for _, f := range p.DependsOnFunctions {
		deps = append(deps, mustRun(vertex).before(buildFunctionVertexId(f, diffTypeDelete)))
	}
	return deps
}
