package diff

import (
	"fmt"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type columnSQLVertexGenerator struct {
	tableName schema.SchemaQualifiedName
}

func newColumnSQLVertexGenerator(tableName schema.SchemaQualifiedName) sqlVertexGenerator[schema.Column, columnDiff] {
	return legacyToNewSqlVertexGenerator[schema.Column, columnDiff](&columnSQLVertexGenerator{tableName: tableName})
}

func (csg *columnSQLVertexGenerator) Add(column schema.Column) ([]Statement, error) {
	columnDef, err := buildColumnDefinition(column)
	if err != nil {
		return nil, fmt.Errorf("building column definition: %w", err)
	}
	return []Statement{{
		DDL:         fmt.Sprintf("%s ADD COLUMN %s", alterTablePrefix(csg.tableName), columnDef),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (csg *columnSQLVertexGenerator) Delete(column schema.Column) ([]Statement, error) {
	return []Statement{{
		DDL:         fmt.Sprintf("%s DROP COLUMN %s", alterTablePrefix(csg.tableName), schema.EscapeIdentifier(column.Name)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards: []MigrationHazard{
			{
				Type:    MigrationHazardTypeDeletesData,
				Message: "Deletes all values in the column",
			},
		},
	}}, nil
}

func (csg *columnSQLVertexGenerator) Alter(diff columnDiff) ([]Statement, error) {
	if diff.oldOrdering != diff.newOrdering {
		return nil, fmt.Errorf("old=%d; new=%d: %w", diff.oldOrdering, diff.newOrdering, ErrColumnOrderingChanged)
	}
	oldColumn, newColumn := diff.old, diff.new
	var stmts []Statement
	alterColumnPrefix := fmt.Sprintf("%s ALTER COLUMN %s", alterTablePrefix(csg.tableName), schema.EscapeIdentifier(newColumn.Name))

	// Adding a "NOT NULL" constraint must come before updating a column to be an identity column, otherwise
	// the add statement will fail because a column must be non-nullable to become an identity column.
	if oldColumn.IsNullable != newColumn.IsNullable && !newColumn.IsNullable {
		stmts = append(stmts, Statement{
			DDL:         fmt.Sprintf("%s SET NOT NULL", alterColumnPrefix),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		})
	}

	// Drop the default before type conversion. This will allow type conversions
	// between incompatible types if the previous column has a default and the new column is dropping its default.
	// It also must be dropped before an identity is added to the column, otherwise adding the identity errors
	// with "a default already exists."
	//
	// To keep the code simpler, put dropping the default before updating the column identity AND dropping the default.
	// There is an argument to drop the default after removing the not null constraint, since writes will continue to succeed
	// on columns migrating from not-null and a default to nullable with no default; however, this would not work
	// for the case where an identity is being added to the column.
	if len(oldColumn.Default) > 0 && len(newColumn.Default) == 0 {
		stmts = append(stmts, Statement{
			DDL:         fmt.Sprintf("%s DROP DEFAULT", alterColumnPrefix),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		})
	}

	updateIdentityStmts, err := csg.buildUpdateIdentityStatements(oldColumn, newColumn)
	if err != nil {
		return nil, fmt.Errorf("building update identity statements: %w", err)
	}
	stmts = append(stmts, updateIdentityStmts...)

	// Removing a "NOT NULL" constraint must come after updating a column to no longer be an identity column, otherwise
	// the "DROP NOT NULL" statement will fail because the column will still be an identity column.
	if oldColumn.IsNullable != newColumn.IsNullable && newColumn.IsNullable {
		stmts = append(stmts, Statement{
			DDL:         fmt.Sprintf("%s DROP NOT NULL", alterColumnPrefix),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		})
	}

	if !strings.EqualFold(oldColumn.Type, newColumn.Type) ||
		!strings.EqualFold(oldColumn.Collation.GetFQEscapedName(), newColumn.Collation.GetFQEscapedName()) {
		stmts = append(stmts,
			[]Statement{
				csg.generateTypeTransformationStatement(
					diff.new,
					oldColumn.Type,
					newColumn.Type,
					newColumn.Collation,
				),
				// When "SET TYPE" is used to alter a column, that column's statistics are removed, which could
				// affect query plans. In order to mitigate the effect on queries, re-generate the statistics for the
				// column before continuing with the migration.
				{
					DDL:         fmt.Sprintf("ANALYZE %s (%s)", csg.tableName.GetFQEscapedName(), schema.EscapeIdentifier(newColumn.Name)),
					Timeout:     statementTimeoutAnalyzeColumn,
					LockTimeout: lockTimeoutDefault,
					Hazards: []MigrationHazard{
						{
							Type: MigrationHazardTypeImpactsDatabasePerformance,
							Message: "Running analyze will read rows from the table, putting increased load " +
								"on the database and consuming database resources. It won't prevent reads/writes to " +
								"the table, but it could affect performance when executing queries.",
						},
					},
				},
			}...)
	}

	if oldColumn.Default != newColumn.Default && len(newColumn.Default) > 0 {
		// Set the default after the type conversion. This will allow type conversions
		// between incompatible types if the previous column has no default and the new column has a default
		stmts = append(stmts, Statement{
			DDL:         fmt.Sprintf("%s SET DEFAULT %s", alterColumnPrefix, newColumn.Default),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		})
	}

	return stmts, nil
}

func (csg *columnSQLVertexGenerator) generateTypeTransformationStatement(
	col schema.Column,
	oldType string,
	newType string,
	newTypeCollation schema.SchemaQualifiedName,
) Statement {
	if strings.EqualFold(oldType, "bigint") &&
		strings.EqualFold(newType, "timestamp without time zone") {
		return Statement{
			DDL: fmt.Sprintf("%s SET DATA TYPE %s using to_timestamp(%s / 1000)",
				csg.alterColumnPrefix(col),
				newType,
				schema.EscapeIdentifier(col.Name),
			),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
			Hazards: []MigrationHazard{{
				Type: MigrationHazardTypeAcquiresAccessExclusiveLock,
				Message: "This will completely lock the table while the data is being " +
					"re-written for a duration of time that scales with the size of your data. " +
					"The values previously stored as BIGINT will be translated into a " +
					"TIMESTAMP value via the PostgreSQL to_timestamp() function. This " +
					"translation will assume that the values stored in BIGINT represent a " +
					"millisecond epoch value.",
			}},
		}
	}

	collationModifier := ""
	if !newTypeCollation.IsEmpty() {
		collationModifier = fmt.Sprintf("COLLATE %s ", newTypeCollation.GetFQEscapedName())
	}

	return Statement{
		DDL: fmt.Sprintf("%s SET DATA TYPE %s %susing %s::%s",
			csg.alterColumnPrefix(col),
			newType,
			collationModifier,
			schema.EscapeIdentifier(col.Name),
			newType,
		),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards: []MigrationHazard{{
			Type: MigrationHazardTypeAcquiresAccessExclusiveLock,
			Message: "This will completely lock the table while the data is being re-written. " +
				"The duration of this conversion depends on if the type conversion is trivial " +
				"or not. A non-trivial conversion will require a table rewrite. A trivial " +
				"conversion is one where the binary values are coercible and the column " +
				"contents are not changing.",
		}},
	}
}

func (csg *columnSQLVertexGenerator) buildUpdateIdentityStatements(old, new schema.Column) ([]Statement, error) {
	if cmp.Equal(old.Identity, new.Identity) {
		return nil, nil
	}

	// Drop the old identity
	if new.Identity == nil {
		// ALTER [ COLUMN ] column_name DROP IDENTITY [ IF EXISTS ]
		return []Statement{{
			DDL:         fmt.Sprintf("%s DROP IDENTITY", csg.alterColumnPrefix(old)),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		}}, nil
	}

	// Add the new identity
	if old.Identity == nil {
		def, err := buildColumnIdentityDefinition(*new.Identity)
		if err != nil {
			return nil, fmt.Errorf("building column identity definition: %w", err)
		}
		// ALTER [ COLUMN ] column_name ADD GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( sequence_options ) ]
		return []Statement{{
			DDL:         fmt.Sprintf("%s ADD %s", csg.alterColumnPrefix(new), def),
			Timeout:     statementTimeoutDefault,
			LockTimeout: lockTimeoutDefault,
		}}, nil
	}

	// Alter the existing identity
	var modifications []string
	if old.Identity.Type != new.Identity.Type {
		typeModifier, err := columnIdentityTypeToModifier(new.Identity.Type)
		if err != nil {
			return nil, fmt.Errorf("column identity type modifier: %w", err)
		}
		modifications = append(modifications, fmt.Sprintf("\tSET GENERATED %s", typeModifier))
	}
	if old.Identity.Increment != new.Identity.Increment {
		modifications = append(modifications, fmt.Sprintf("\tSET INCREMENT BY %d", new.Identity.Increment))
	}
	if old.Identity.MinValue != new.Identity.MinValue {
		modifications = append(modifications, fmt.Sprintf("\tSET MINVALUE %d", new.Identity.MinValue))
	}
	if old.Identity.MaxValue != new.Identity.MaxValue {
		modifications = append(modifications, fmt.Sprintf("\tSET MAXVALUE %d", new.Identity.MaxValue))
	}
	if old.Identity.StartValue != new.Identity.StartValue {
		modifications = append(modifications, fmt.Sprintf("\tSET START %d", new.Identity.StartValue))
	}
	if old.Identity.CacheSize != new.Identity.CacheSize {
		modifications = append(modifications, fmt.Sprintf("\tSET CACHE %d", new.Identity.CacheSize))
	}
	if old.Identity.Cycle != new.Identity.Cycle {
		cycleModifier := ""
		if !new.Identity.Cycle {
			cycleModifier = "NO "
		}
		modifications = append(modifications, fmt.Sprintf("\tSET %sCYCLE", cycleModifier))
	}
	// ALTER [ COLUMN ] column_name { SET GENERATED { ALWAYS | BY DEFAULT } | SET sequence_option | RESTART [ [ WITH ] restart ] } [...]
	return []Statement{{
		DDL:         fmt.Sprintf("%s\n%s", csg.alterColumnPrefix(new), strings.Join(modifications, "\n")),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (csg *columnSQLVertexGenerator) alterColumnPrefix(col schema.Column) string {
	return fmt.Sprintf("%s ALTER COLUMN %s", alterTablePrefix(csg.tableName), schema.EscapeIdentifier(col.Name))
}

func (csg *columnSQLVertexGenerator) GetSQLVertexId(column schema.Column, diffType diffType) sqlVertexId {
	return buildColumnVertexId(column.Name, diffType)
}

func buildColumnVertexId(columnName string, diffType diffType) sqlVertexId {
	return buildSchemaObjVertexId("column", columnName, diffType)
}

func (csg *columnSQLVertexGenerator) GetAddAlterDependencies(col, _ schema.Column) ([]dependency, error) {
	return []dependency{
		mustRun(csg.GetSQLVertexId(col, diffTypeDelete)).before(csg.GetSQLVertexId(col, diffTypeAddAlter)),
	}, nil
}

func (csg *columnSQLVertexGenerator) GetDeleteDependencies(_ schema.Column) ([]dependency, error) {
	return nil, nil
}
