package diff

import (
	"fmt"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

// domainSQLVertexGenerator handles `CREATE DOMAIN`, `DROP DOMAIN` and `ALTER DOMAIN` for
// user-defined domains. It is a sqlVertexGenerator (rather than a plain sqlGenerator) because a
// domain sits in the middle of the dependency graph: it must be created after the functions its
// CHECK expressions and default value call, and before every object typed with it — table columns,
// function/procedure signatures, and other domains.
type domainSQLVertexGenerator struct {
	oldSchema schema.Schema
	newSchema schema.Schema

	// recreatedDomains is the set of domains whose base type or collation is changing, i.e. the
	// ones resolved as a DROP followed by a CREATE rather than an in-place ALTER.
	recreatedDomains map[string]bool
}

func newDomainSQLVertexGenerator(oldSchema, newSchema schema.Schema, recreatedDomains map[string]bool) sqlVertexGenerator[schema.Domain, domainDiff] {
	return &domainSQLVertexGenerator{
		oldSchema:        oldSchema,
		newSchema:        newSchema,
		recreatedDomains: recreatedDomains,
	}
}

func (d *domainSQLVertexGenerator) Add(domain schema.Domain) (partialSQLGraph, error) {
	addVertexId := buildDomainVertexId(domain.SchemaQualifiedName, diffTypeAddAlter)

	deps := []dependency{
		// If the domain is being re-created, the CREATE must follow the DROP.
		mustRun(addVertexId).after(buildDomainVertexId(domain.SchemaQualifiedName, diffTypeDelete)),
	}
	deps = append(deps, d.dependsOnDepsForAddAlter(domain)...)
	deps = append(deps, d.consumerDepsForAddAlter(domain)...)

	return partialSQLGraph{
		vertices: []sqlVertex{{
			id:       addVertexId,
			priority: sqlPrioritySooner,
			statements: []Statement{{
				DDL:         buildCreateDomainDDL(domain),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
			}},
		}},
		dependencies: deps,
	}, nil
}

func (d *domainSQLVertexGenerator) Delete(domain schema.Domain) (partialSQLGraph, error) {
	deleteVertexId := buildDomainVertexId(domain.SchemaQualifiedName, diffTypeDelete)

	var hazards []MigrationHazard
	if d.recreatedDomains[domain.GetName()] {
		hazards = append(hazards, MigrationHazard{
			Type: MigrationHazardTypeHasUntrackableDependencies,
			Message: "The domain's base type or collation changed, which cannot be altered in place. The domain is " +
				"dropped and re-created. Any object depending on the domain that is not tracked by this tool (e.g., a " +
				"view or a check constraint expression) will block the drop or must be re-created manually.",
		})
	}

	deps := d.dependsOnDepsForDelete(domain)
	deps = append(deps, d.consumerDepsForDelete(domain)...)

	return partialSQLGraph{
		vertices: []sqlVertex{{
			id:       deleteVertexId,
			priority: sqlPriorityLater,
			statements: []Statement{{
				DDL:         fmt.Sprintf("DROP DOMAIN %s", domain.GetFQEscapedName()),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
				Hazards:     hazards,
			}},
		}},
		dependencies: deps,
	}, nil
}

func (d *domainSQLVertexGenerator) Alter(diff domainDiff) (partialSQLGraph, error) {
	if cmp.Equal(diff.old, diff.new) {
		return partialSQLGraph{}, nil
	}

	// A base type or collation change is resolved as a DROP + CREATE by
	// identifyDomainsToRecreate, so Alter never sees one.
	if diff.old.BaseType != diff.new.BaseType || !cmp.Equal(diff.old.Collation, diff.new.Collation) {
		return partialSQLGraph{}, fmt.Errorf("altering the base type or collation of domain %s: %w", diff.new.GetFQEscapedName(), ErrNotImplemented)
	}

	statements, err := buildAlterDomainStatements(diff.old, diff.new)
	if err != nil {
		return partialSQLGraph{}, err
	}
	if len(statements) == 0 {
		return partialSQLGraph{}, nil
	}

	alterVertexId := buildDomainVertexId(diff.new.SchemaQualifiedName, diffTypeAddAlter)
	deps := d.dependsOnDepsForAddAlter(diff.new)
	deps = append(deps, d.consumerDepsForAddAlter(diff.new)...)

	return partialSQLGraph{
		vertices: []sqlVertex{{
			id:         alterVertexId,
			priority:   sqlPrioritySooner,
			statements: statements,
		}},
		dependencies: deps,
	}, nil
}

func buildAlterDomainStatements(old, new schema.Domain) ([]Statement, error) {
	alterPrefix := fmt.Sprintf("ALTER DOMAIN %s", new.GetFQEscapedName())

	oldConstraintsByName := buildMap(old.Constraints, func(c schema.DomainConstraint) string { return c.Name })
	newConstraintsByName := buildMap(new.Constraints, func(c schema.DomainConstraint) string { return c.Name })

	var statements []Statement
	// Drop the constraints that are gone, and the ones whose expression changed: a domain
	// constraint's expression cannot be altered in place.
	for _, oldConstraint := range old.Constraints {
		if newConstraint, ok := newConstraintsByName[oldConstraint.Name]; ok && newConstraint.Def == oldConstraint.Def {
			continue
		}
		statements = append(statements, Statement{
			DDL:         fmt.Sprintf("%s DROP CONSTRAINT %s", alterPrefix, schema.EscapeIdentifier(oldConstraint.Name)),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		})
	}

	if old.Default != new.Default {
		if new.Default == "" {
			statements = append(statements, Statement{
				DDL:         fmt.Sprintf("%s DROP DEFAULT", alterPrefix),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
			})
		} else {
			statements = append(statements, Statement{
				DDL:         fmt.Sprintf("%s SET DEFAULT %s", alterPrefix, new.Default),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
			})
		}
	}

	if old.IsNotNull != new.IsNotNull {
		if new.IsNotNull {
			statements = append(statements, Statement{
				DDL:         fmt.Sprintf("%s SET NOT NULL", alterPrefix),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
				Hazards: []MigrationHazard{{
					Type: MigrationHazardTypeAcquiresAccessExclusiveLock,
					Message: "Marking a domain as not null requires a full scan of every table with a column of " +
						"that domain, which will lock out writes on those tables",
				}},
			})
		} else {
			statements = append(statements, Statement{
				DDL:         fmt.Sprintf("%s DROP NOT NULL", alterPrefix),
				Timeout:     statementTimeoutDefault,
				LockTimeout: lockTimeoutDefault,
			})
		}
	}

	// Add the new constraints and re-add the ones whose expression changed.
	for _, newConstraint := range new.Constraints {
		if oldConstraint, ok := oldConstraintsByName[newConstraint.Name]; ok && oldConstraint.Def == newConstraint.Def {
			continue
		}
		statements = append(statements, Statement{
			DDL: fmt.Sprintf("%s ADD CONSTRAINT %s %s", alterPrefix,
				schema.EscapeIdentifier(newConstraint.Name), newConstraint.Def),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
			Hazards: []MigrationHazard{{
				Type: MigrationHazardTypeAcquiresAccessExclusiveLock,
				Message: "Adding a constraint to a domain requires a full scan of every table with a column of " +
					"that domain, which will lock out writes on those tables",
			}},
		})
	}

	// Assert the diff is fully resolved: copy over everything the statements above handle and
	// require the result to equal the new domain.
	oldCopy := old
	oldCopy.Constraints = new.Constraints
	oldCopy.Default = new.Default
	oldCopy.IsNotNull = new.IsNotNull
	oldCopy.DependsOnFunctions = new.DependsOnFunctions
	oldCopy.DependsOnDomains = new.DependsOnDomains
	if diff := cmp.Diff(oldCopy, new); diff != "" {
		return nil, fmt.Errorf("unable to resolve the diff %s: %w", diff, ErrNotImplemented)
	}

	return statements, nil
}

func buildCreateDomainDDL(domain schema.Domain) string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("CREATE DOMAIN %s AS %s", domain.GetFQEscapedName(), domain.BaseType))
	if !domain.Collation.IsEmpty() {
		sb.WriteString(fmt.Sprintf(" COLLATE %s", domain.Collation.GetFQEscapedName()))
	}
	if domain.Default != "" {
		sb.WriteString(fmt.Sprintf(" DEFAULT %s", domain.Default))
	}
	if domain.IsNotNull {
		sb.WriteString(" NOT NULL")
	}
	for _, constraint := range domain.Constraints {
		// constraint.Def comes verbatim from pg_get_constraintdef, e.g. `CHECK ((VALUE > 0))`.
		sb.WriteString(fmt.Sprintf("\n\tCONSTRAINT %s %s", schema.EscapeIdentifier(constraint.Name), constraint.Def))
	}
	return sb.String()
}

func buildDomainVertexId(name schema.SchemaQualifiedName, d diffType) sqlVertexId {
	return buildSchemaObjVertexId("domain", name.GetFQEscapedName(), d)
}

// dependsOnDepsForAddAlter orders the domain's CREATE/ALTER after the objects its CHECK
// expressions, default value, and base type refer to.
func (d *domainSQLVertexGenerator) dependsOnDepsForAddAlter(domain schema.Domain) []dependency {
	addVertexId := buildDomainVertexId(domain.SchemaQualifiedName, diffTypeAddAlter)

	var deps []dependency
	for _, function := range domain.DependsOnFunctions {
		deps = append(deps, mustRun(addVertexId).after(buildFunctionVertexId(function, diffTypeAddAlter)))
	}
	for _, base := range domain.DependsOnDomains {
		deps = append(deps, mustRun(addVertexId).after(buildDomainVertexId(base, diffTypeAddAlter)))
	}
	return deps
}

// dependsOnDepsForDelete orders the domain's DROP before the DROP of everything it depends on:
// the functions called by its CHECK expressions and default value, and the domain it is built on.
func (d *domainSQLVertexGenerator) dependsOnDepsForDelete(domain schema.Domain) []dependency {
	deleteVertexId := buildDomainVertexId(domain.SchemaQualifiedName, diffTypeDelete)

	var deps []dependency
	for _, function := range domain.DependsOnFunctions {
		deps = append(deps, mustRun(deleteVertexId).before(buildFunctionVertexId(function, diffTypeDelete)))
	}
	for _, base := range domain.DependsOnDomains {
		deps = append(deps, mustRun(deleteVertexId).before(buildDomainVertexId(base, diffTypeDelete)))
	}
	return deps
}

// consumerDepsForAddAlter orders the domain's CREATE/ALTER before every object in the new schema
// that is typed with it.
func (d *domainSQLVertexGenerator) consumerDepsForAddAlter(domain schema.Domain) []dependency {
	addVertexId := buildDomainVertexId(domain.SchemaQualifiedName, diffTypeAddAlter)
	domainName := domain.GetName()

	var deps []dependency
	for _, table := range d.newSchema.Tables {
		if !dependsOnDomain(table.DependsOnDomains, domainName) {
			continue
		}
		deps = append(deps, mustRun(addVertexId).before(buildTableVertexId(table.SchemaQualifiedName, diffTypeAddAlter)))
		// A column added to an existing table gets its own vertex, so the table vertex alone is
		// not enough to order the domain before the column that uses it.
		for _, column := range table.Columns {
			deps = append(deps, mustRun(addVertexId).before(buildColumnVertexId(column.Name, diffTypeAddAlter)))
		}
	}
	for _, function := range d.newSchema.Functions {
		if dependsOnDomain(function.DependsOnDomains, domainName) {
			deps = append(deps, mustRun(addVertexId).before(buildFunctionVertexId(function.SchemaQualifiedName, diffTypeAddAlter)))
		}
	}
	for _, procedure := range d.newSchema.Procedures {
		if dependsOnDomain(procedure.DependsOnDomains, domainName) {
			deps = append(deps, mustRun(addVertexId).before(buildProcedureVertexId(procedure.SchemaQualifiedName, diffTypeAddAlter)))
		}
	}
	for _, other := range d.newSchema.Domains {
		if dependsOnDomain(other.DependsOnDomains, domainName) {
			deps = append(deps, mustRun(addVertexId).before(buildDomainVertexId(other.SchemaQualifiedName, diffTypeAddAlter)))
		}
	}
	return deps
}

// consumerDepsForDelete orders the domain's DROP after every object in the old schema that was
// typed with it has been dropped or altered to no longer reference it.
//
// A consumer that still references the domain in the new schema (because the domain is being
// re-created and the consumer is force re-created alongside it) must NOT get the
// `domainDelete > consumerAddAlter` edge: that would contradict the required order
//
//	consumerDelete < domainDelete < domainAdd < consumerAddAlter
func (d *domainSQLVertexGenerator) consumerDepsForDelete(domain schema.Domain) []dependency {
	deleteVertexId := buildDomainVertexId(domain.SchemaQualifiedName, diffTypeDelete)
	domainName := domain.GetName()

	newFunctionsByName := buildSchemaObjByNameMap(d.newSchema.Functions)
	newProceduresByName := buildSchemaObjByNameMap(d.newSchema.Procedures)
	newTablesByName := buildSchemaObjByNameMap(d.newSchema.Tables)
	newDomainsByName := buildSchemaObjByNameMap(d.newSchema.Domains)

	var deps []dependency
	for _, table := range d.oldSchema.Tables {
		if !dependsOnDomain(table.DependsOnDomains, domainName) {
			continue
		}
		deps = append(deps, mustRun(deleteVertexId).after(buildTableVertexId(table.SchemaQualifiedName, diffTypeDelete)))
		for _, column := range table.Columns {
			deps = append(deps, mustRun(deleteVertexId).after(buildColumnVertexId(column.Name, diffTypeDelete)))
		}
		// A table column typed with a re-created domain is refused up-front by
		// identifyDomainsToRecreate, so the domain is genuinely gone from the new table.
		if !dependsOnDomain(newTablesByName[table.GetName()].DependsOnDomains, domainName) {
			deps = append(deps, mustRun(deleteVertexId).after(buildTableVertexId(table.SchemaQualifiedName, diffTypeAddAlter)))
		}
	}
	for _, function := range d.oldSchema.Functions {
		if !dependsOnDomain(function.DependsOnDomains, domainName) {
			continue
		}
		deps = append(deps, mustRun(deleteVertexId).after(buildFunctionVertexId(function.SchemaQualifiedName, diffTypeDelete)))
		if !dependsOnDomain(newFunctionsByName[function.GetName()].DependsOnDomains, domainName) {
			deps = append(deps, mustRun(deleteVertexId).after(buildFunctionVertexId(function.SchemaQualifiedName, diffTypeAddAlter)))
		}
	}
	for _, procedure := range d.oldSchema.Procedures {
		if !dependsOnDomain(procedure.DependsOnDomains, domainName) {
			continue
		}
		deps = append(deps, mustRun(deleteVertexId).after(buildProcedureVertexId(procedure.SchemaQualifiedName, diffTypeDelete)))
		if !dependsOnDomain(newProceduresByName[procedure.GetName()].DependsOnDomains, domainName) {
			deps = append(deps, mustRun(deleteVertexId).after(buildProcedureVertexId(procedure.SchemaQualifiedName, diffTypeAddAlter)))
		}
	}
	for _, other := range d.oldSchema.Domains {
		if !dependsOnDomain(other.DependsOnDomains, domainName) {
			continue
		}
		deps = append(deps, mustRun(deleteVertexId).after(buildDomainVertexId(other.SchemaQualifiedName, diffTypeDelete)))
		if !dependsOnDomain(newDomainsByName[other.GetName()].DependsOnDomains, domainName) {
			deps = append(deps, mustRun(deleteVertexId).after(buildDomainVertexId(other.SchemaQualifiedName, diffTypeAddAlter)))
		}
	}
	return deps
}

func dependsOnDomain(deps []schema.SchemaQualifiedName, domainName string) bool {
	for _, dep := range deps {
		if dep.GetName() == domainName {
			return true
		}
	}
	return false
}
