package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/stripe/pg-schema-diff/internal/concurrent"
	dbsqlc "github.com/stripe/pg-schema-diff/internal/queries"
	"go.inout.gg/foundations/pointer"
)

func fetchNamedSchemas(ctx context.Context, db dbsqlc.DBTX) ([]NamedSchema, error) {
	schemaNames, err := dbsqlc.New().GetSchemas(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("GetSchemas(): %w", err)
	}

	var schemas []NamedSchema
	for _, schemaName := range schemaNames {
		schemas = append(schemas, NamedSchema{
			Name: schemaName,
		})
	}

	return schemas, nil
}

func fetchExtensions(ctx context.Context, db dbsqlc.DBTX) ([]Extension, error) {
	rawExtensions, err := dbsqlc.New().GetExtensions(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("GetExtensions(): %w", err)
	}

	var extensions []Extension
	for _, e := range rawExtensions {
		extensions = append(extensions, Extension{
			SchemaQualifiedName: SchemaQualifiedName{
				EscapedName: EscapeIdentifier(e.ExtensionName),
				SchemaName:  e.SchemaName,
			},
			Version: e.ExtensionVersion,
		})
	}

	return extensions, nil
}

func fetchEnums(ctx context.Context, db dbsqlc.DBTX) ([]Enum, error) {
	rawEnums, err := dbsqlc.New().GetEnums(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("GetEnums: %w", err)
	}

	var enums []Enum
	for _, rawEnum := range rawEnums {
		enums = append(enums, Enum{
			SchemaQualifiedName: SchemaQualifiedName{
				SchemaName:  rawEnum.EnumSchemaName,
				EscapedName: EscapeIdentifier(rawEnum.EnumName),
			},
			Labels: rawEnum.EnumLabels,
		})
	}

	return enums, nil
}

func fetchTables(ctx context.Context, db dbsqlc.DBTX, goroutineRunnerFactory func() concurrent.GoroutineRunner) ([]Table, error) {
	rawTables, err := dbsqlc.New().GetTables(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("GetTables(): %w", err)
	}

	checkCons, err := fetchCheckCons(ctx, db, goroutineRunnerFactory)
	if err != nil {
		return nil, fmt.Errorf("fetchCheckCons(): %w", err)
	}
	checkConsByTable := make(map[string][]CheckConstraint)
	for _, cc := range checkCons {
		checkConsByTable[cc.table.GetFQEscapedName()] =
			append(checkConsByTable[cc.table.GetFQEscapedName()], cc.checkConstraint)
	}

	policies, err := fetchPolicies(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("fetchPolicies(): %w", err)
	}
	policiesByTable := make(map[string][]Policy)
	for _, p := range policies {
		policiesByTable[p.table.GetFQEscapedName()] =
			append(policiesByTable[p.table.GetFQEscapedName()], p.policy)
	}

	privileges, err := fetchPrivileges(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("fetchPrivileges(): %w", err)
	}
	privilegesByTable := make(map[string][]TablePrivilege)
	for _, p := range privileges {
		privilegesByTable[p.table.GetFQEscapedName()] =
			append(privilegesByTable[p.table.GetFQEscapedName()], p.privilege)
	}

	goroutineRunner := goroutineRunnerFactory()
	var tableFutures []concurrent.Future[Table]
	for _, _rawTable := range rawTables {
		rawTable := _rawTable // Capture loop variables for go routine
		tableFuture, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() (Table, error) {
			return buildTable(ctx, db, rawTable, checkConsByTable, policiesByTable, privilegesByTable)
		})
		if err != nil {
			return nil, fmt.Errorf("starting table future: %w", err)
		}
		tableFutures = append(tableFutures, tableFuture)
	}
	tables, err := concurrent.GetAll(ctx, tableFutures...)
	if err != nil {
		return nil, fmt.Errorf("getting tables: %w", err)
	}

	return tables, nil
}

func buildTable(
	ctx context.Context,
	db dbsqlc.DBTX,
	table dbsqlc.GetTablesRow,
	checkConsByTable map[string][]CheckConstraint,
	policiesByTable map[string][]Policy,
	privilegesByTable map[string][]TablePrivilege,
) (Table, error) {
	rawColumns, err := dbsqlc.New().GetColumnsForTable(ctx, db, table.Oid)
	if err != nil {
		return Table{}, fmt.Errorf("GetColumnsForTable(%v): %w", table.Oid, err)
	}
	var columns []Column
	for _, column := range rawColumns {
		collation := SchemaQualifiedName{}
		if len(column.CollationName) > 0 {
			collation = SchemaQualifiedName{
				EscapedName: EscapeIdentifier(column.CollationName),
				SchemaName:  column.CollationSchemaName,
			}
		}

		var identity *ColumnIdentity
		if len(column.IdentityType) > 0 && table.ParentTableName == "" {
			// Exclude identity columns from table partitions because they are owned by the parent.
			identity = &ColumnIdentity{
				Type:       ColumnIdentityType(column.IdentityType),
				StartValue: pointer.ToValue(column.StartValue, 0),
				Increment:  pointer.ToValue(column.IncrementValue, 0),
				MaxValue:   pointer.ToValue(column.MaxValue, 0),
				MinValue:   pointer.ToValue(column.MinValue, 0),
				CacheSize:  pointer.ToValue(column.CacheSize, 0),
				Cycle:      pointer.ToValue(column.IsCycle, false),
			}
		}

		columns = append(columns, Column{
			Name:                      column.ColumnName,
			Type:                      column.ColumnType,
			Collation:                 collation,
			IsNullable:                !column.IsNotNull,
			HasMissingValOptimization: column.HasMissingValOptimization,
			// If the column has a default value, this will be a SQL string representing that value.
			// Examples:
			//   ''::text
			//   CURRENT_TIMESTAMP
			// If empty, indicates that there is no default value.
			Default:              column.DefaultValue,
			IsGenerated:          column.IsGenerated,
			GenerationExpression: column.GenerationExpression,
			Size:                 int(column.ColumnSize),
			Identity:             identity,
		})
	}

	var parentTable *SchemaQualifiedName
	if table.ParentTableName != "" {
		parentTable = &SchemaQualifiedName{
			SchemaName:  table.ParentTableSchemaName,
			EscapedName: EscapeIdentifier(table.ParentTableName),
		}
	}
	schemaQualifiedName := SchemaQualifiedName{
		SchemaName:  table.TableSchemaName,
		EscapedName: EscapeIdentifier(table.TableName),
	}
	return Table{
		SchemaQualifiedName: schemaQualifiedName,
		Columns:             columns,
		CheckConstraints:    checkConsByTable[schemaQualifiedName.GetFQEscapedName()],
		Policies:            policiesByTable[schemaQualifiedName.GetFQEscapedName()],
		Privileges:          privilegesByTable[schemaQualifiedName.GetFQEscapedName()],
		ReplicaIdentity:     ReplicaIdentity(table.ReplicaIdentity),
		RLSEnabled:          table.RlsEnabled,
		RLSForced:           table.RlsForced,

		PartitionKeyDef: table.PartitionKeyDef,

		ParentTable: parentTable,
		ForValues:   table.PartitionForValues,
	}, nil
}

type checkConstraintAndTable struct {
	checkConstraint CheckConstraint
	table           SchemaQualifiedName
}

// fetchCheckCons fetches the check constraints
func fetchCheckCons(ctx context.Context, db dbsqlc.DBTX, goroutineRunnerFactory func() concurrent.GoroutineRunner,
) ([]checkConstraintAndTable, error) {
	rawCheckCons, err := dbsqlc.New().GetCheckConstraints(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("GetCheckConstraints: %w", err)
	}

	goroutineRunner := goroutineRunnerFactory()
	var ccFutures []concurrent.Future[checkConstraintAndTable]
	for _, _rawCC := range rawCheckCons {
		rawCC := _rawCC // Capture loop variable for go routine
		f, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() (checkConstraintAndTable, error) {
			cc, err := buildCheckConstraint(ctx, db, rawCC)
			if err != nil {
				return checkConstraintAndTable{}, fmt.Errorf("building check constraint: %w", err)
			}
			return checkConstraintAndTable{
				checkConstraint: cc,
				table:           buildNameFromUnescaped(rawCC.TableName, rawCC.TableSchemaName),
			}, nil
		})
		if err != nil {
			return nil, fmt.Errorf("starting check constraint future: %w", err)
		}

		ccFutures = append(ccFutures, f)
	}

	ccs, err := concurrent.GetAll(ctx, ccFutures...)
	if err != nil {
		return nil, fmt.Errorf("getting check constraints: %w", err)
	}

	return ccs, nil
}

func buildCheckConstraint(ctx context.Context, db dbsqlc.DBTX, cc dbsqlc.GetCheckConstraintsRow) (CheckConstraint, error) {
	dependsOnFunctions, err := fetchDependsOnFunctions(ctx, db, "pg_constraint", cc.Oid)
	if err != nil {
		return CheckConstraint{}, fmt.Errorf("fetchDependsOnFunctions(%v): %w", cc.Oid, err)
	}
	return CheckConstraint{
		Name:               cc.ConstraintName,
		KeyColumns:         cc.ColumnNames,
		Expression:         cc.ConstraintExpression,
		IsValid:            cc.IsValid,
		IsInheritable:      !cc.IsNotInheritable,
		DependsOnFunctions: dependsOnFunctions,
	}, nil
}

// fetchIndexes fetches the indexes. We fetch all the indexes at once to minimize the number of queries.
func fetchIndexes(ctx context.Context, db dbsqlc.DBTX) ([]Index, error) {
	rawIndexes, err := dbsqlc.New().GetIndexes(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("GetIndexes: %w", err)
	}

	var idxs []Index
	for _, idx := range rawIndexes {
		idxs = append(idxs, buildIndex(idx))
	}

	return idxs, nil
}

func buildIndex(rawIndex dbsqlc.GetIndexesRow) Index {
	var indexConstraint *IndexConstraint
	if rawIndex.ConstraintName != "" {
		indexConstraint = &IndexConstraint{
			Type:                  IndexConstraintType(rawIndex.ConstraintType),
			EscapedConstraintName: EscapeIdentifier(rawIndex.ConstraintName),
			ConstraintDef:         rawIndex.ConstraintDef,
			IsLocal:               rawIndex.ConstraintIsLocal,
		}
	}

	var parentIdx *SchemaQualifiedName
	if rawIndex.ParentIndexName != "" {
		parentIdx = &SchemaQualifiedName{
			SchemaName:  rawIndex.ParentIndexSchemaName,
			EscapedName: EscapeIdentifier(rawIndex.ParentIndexName),
		}
	}

	return Index{
		OwningRelName: SchemaQualifiedName{
			SchemaName:  rawIndex.TableSchemaName,
			EscapedName: EscapeIdentifier(rawIndex.TableName),
		},
		OwningRelKind:   RelKind(rawIndex.OwningTableRelkind),
		Name:            rawIndex.IndexName,
		Columns:         rawIndex.ColumnNames,
		GetIndexDefStmt: GetIndexDefStatement(rawIndex.DefStmt),
		IsInvalid:       !rawIndex.IndexIsValid,
		IsUnique:        rawIndex.IndexIsUnique,

		Constraint: indexConstraint,

		ParentIdx: parentIdx,
	}
}

func fetchForeignKeyCons(ctx context.Context, db dbsqlc.DBTX) ([]ForeignKeyConstraint, error) {
	rawFkCons, err := dbsqlc.New().GetForeignKeyConstraints(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("GetForeignKeyConstraints: %w", err)
	}

	var fkCons []ForeignKeyConstraint
	for _, rawFkCon := range rawFkCons {
		fkCons = append(fkCons, ForeignKeyConstraint{
			EscapedName: EscapeIdentifier(rawFkCon.ConstraintName),
			OwningTable: SchemaQualifiedName{
				SchemaName:  rawFkCon.OwningTableSchemaName,
				EscapedName: EscapeIdentifier(rawFkCon.OwningTableName),
			},
			ForeignTable: SchemaQualifiedName{
				SchemaName:  rawFkCon.ForeignTableSchemaName,
				EscapedName: EscapeIdentifier(rawFkCon.ForeignTableName),
			},
			ConstraintDef: rawFkCon.ConstraintDef,
			IsValid:       rawFkCon.IsValid,
		})
	}

	return fkCons, nil
}

func fetchSequences(ctx context.Context, db dbsqlc.DBTX) ([]Sequence, error) {
	rawSeqs, err := dbsqlc.New().GetSequences(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("GetSequences: %w", err)
	}

	var seqs []Sequence
	for _, rawSeq := range rawSeqs {
		var owner *SequenceOwner
		if len(rawSeq.OwnerColumnName) > 0 {
			owner = &SequenceOwner{
				TableName: SchemaQualifiedName{
					SchemaName:  rawSeq.OwnerSchemaName,
					EscapedName: EscapeIdentifier(rawSeq.OwnerTableName),
				},
				ColumnName: rawSeq.OwnerColumnName,
			}
		}
		seqs = append(seqs, Sequence{
			SchemaQualifiedName: SchemaQualifiedName{
				SchemaName:  rawSeq.SequenceSchemaName,
				EscapedName: EscapeIdentifier(rawSeq.SequenceName),
			},
			Owner:      owner,
			Type:       rawSeq.DataType,
			StartValue: rawSeq.StartValue,
			Increment:  rawSeq.IncrementValue,
			MaxValue:   rawSeq.MaxValue,
			MinValue:   rawSeq.MinValue,
			CacheSize:  rawSeq.CacheSize,
			Cycle:      rawSeq.IsCycle,
		})
	}

	return seqs, nil
}

func fetchFunctions(ctx context.Context, db dbsqlc.DBTX, goroutineRunnerFactory func() concurrent.GoroutineRunner) ([]Function, error) {
	rawFunctions, err := dbsqlc.New().GetProcs(ctx, db, 'f')
	if err != nil {
		return nil, fmt.Errorf("GetProcs: %w", err)
	}

	goroutineRunner := goroutineRunnerFactory()
	var functionFutures []concurrent.Future[Function]
	for _, _rawFunction := range rawFunctions {
		rawFunction := _rawFunction // Capture loop variable for go routine
		f, err := concurrent.SubmitFuture(ctx, goroutineRunner, func() (Function, error) {
			return buildFunction(ctx, db, rawFunction)
		})
		if err != nil {
			return nil, fmt.Errorf("starting function future: %w", err)
		}
		functionFutures = append(functionFutures, f)
	}

	functions, err := concurrent.GetAll(ctx, functionFutures...)
	if err != nil {
		return nil, fmt.Errorf("getting functions: %w", err)
	}

	return functions, nil
}

func buildFunction(ctx context.Context, db dbsqlc.DBTX, rawFunction dbsqlc.GetProcsRow) (Function, error) {
	dependsOnFunctions, err := fetchDependsOnFunctions(ctx, db, "pg_proc", rawFunction.Oid)
	if err != nil {
		return Function{}, fmt.Errorf("fetchDependsOnFunctions(%v): %w", rawFunction.Oid, err)
	}

	return Function{
		SchemaQualifiedName: buildProcName(rawFunction.FuncName, rawFunction.FuncIdentityArguments, rawFunction.FuncSchemaName),
		FunctionDef:         rawFunction.FuncDef,
		Language:            rawFunction.FuncLang,
		DependsOnFunctions:  dependsOnFunctions,
	}, nil
}

func fetchDependsOnFunctions(ctx context.Context, db dbsqlc.DBTX, systemCatalog string, oid pgtype.Uint32) ([]SchemaQualifiedName, error) {
	dependsOnFunctions, err := dbsqlc.New().GetDependsOnFunctions(ctx, db, dbsqlc.GetDependsOnFunctionsParams{
		SystemCatalog: systemCatalog,
		ObjectID:      oid,
	})
	if err != nil {
		return nil, err
	}

	var functionNames []SchemaQualifiedName
	for _, rawFunction := range dependsOnFunctions {
		functionNames = append(functionNames, buildProcName(rawFunction.FuncName,
			rawFunction.FuncIdentityArguments, rawFunction.FuncSchemaName))
	}

	return functionNames, nil
}

func fetchProcedures(ctx context.Context, db dbsqlc.DBTX) ([]Procedure, error) {
	rawProcedures, err := dbsqlc.New().GetProcs(ctx, db, 'p')
	if err != nil {
		return nil, fmt.Errorf("GetProcs: %w", err)
	}

	var procedures []Procedure
	for _, rawProcedure := range rawProcedures {
		p := Procedure{
			SchemaQualifiedName: buildProcName(rawProcedure.FuncName,
				rawProcedure.FuncIdentityArguments, rawProcedure.FuncSchemaName),
			Def: rawProcedure.FuncDef,
		}
		procedures = append(procedures, p)
	}

	return procedures, nil
}

type policyAndTable struct {
	policy Policy
	table  SchemaQualifiedName
}

type privilegeAndTable struct {
	privilege TablePrivilege
	table     SchemaQualifiedName
}

func fetchPolicies(ctx context.Context, db dbsqlc.DBTX) ([]policyAndTable, error) {
	rawPolicies, err := dbsqlc.New().GetPolicies(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("GetPolicies: %w", err)
	}

	var policies []policyAndTable
	for _, rp := range rawPolicies {
		policies = append(policies, policyAndTable{
			policy: Policy{
				EscapedName:     EscapeIdentifier(rp.PolicyName),
				IsPermissive:    rp.IsPermissive,
				AppliesTo:       rp.AppliesTo,
				Cmd:             PolicyCmd(rp.Cmd),
				CheckExpression: rp.CheckExpression,
				UsingExpression: rp.UsingExpression,
				Columns:         rp.ColumnNames,
			},
			table: buildNameFromUnescaped(rp.OwningTableName, rp.OwningTableSchemaName),
		})
	}

	return policies, nil
}

func fetchPrivileges(ctx context.Context, db dbsqlc.DBTX) ([]privilegeAndTable, error) {
	rawPrivileges, err := dbsqlc.New().GetTablePrivileges(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("GetTablePrivileges: %w", err)
	}

	var privileges []privilegeAndTable
	for _, rp := range rawPrivileges {
		// Handle the is_grantable field which may be returned as interface{}
		isGrantable := false
		if rp.IsGrantable != nil {
			if b, ok := rp.IsGrantable.(bool); ok {
				isGrantable = b
			}
		}

		privileges = append(privileges, privilegeAndTable{
			privilege: TablePrivilege{
				Grantee:     rp.Grantee,
				Privilege:   rp.Privilege,
				IsGrantable: isGrantable,
			},
			table: buildNameFromUnescaped(rp.PaTableName, rp.PaTableSchemaName),
		})
	}

	return privileges, nil
}

func fetchTriggers(ctx context.Context, db dbsqlc.DBTX) ([]Trigger, error) {
	rawTriggers, err := dbsqlc.New().GetTriggers(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("GetTriggers: %w", err)
	}

	var triggers []Trigger
	for _, rawTrigger := range rawTriggers {
		triggers = append(triggers, Trigger{
			EscapedName: EscapeIdentifier(rawTrigger.TriggerName),
			OwningTable: buildNameFromUnescaped(rawTrigger.OwningTableName, rawTrigger.OwningTableSchemaName),
			Function: buildProcName(rawTrigger.FuncName,
				rawTrigger.FuncIdentityArguments, rawTrigger.FuncSchemaName),
			GetTriggerDefStmt: GetTriggerDefStatement(rawTrigger.TriggerDef),
			IsConstraint:      rawTrigger.IsConstraint,
		})
	}

	return triggers, nil
}

func fetchViews(ctx context.Context, db dbsqlc.DBTX) ([]View, error) {
	rawViews, err := dbsqlc.New().GetViews(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("GetViews: %w", err)
	}

	var views []View
	for _, v := range rawViews {
		options, err := relOptionsToMap(v.RelOptions)
		if err != nil {
			return nil, fmt.Errorf("view (%q): %w", v.ViewName, err)
		}

		tableDependencies, err := parseJSONTableDependencies(v.TableDependencies)
		if err != nil {
			return nil, fmt.Errorf("parsing schema qualified names JSON: %w", err)
		}

		views = append(views, View{
			SchemaQualifiedName: buildNameFromUnescaped(v.ViewName, v.SchemaName),
			ViewDefinition:      v.ViewDefinition,
			Options:             options,

			TableDependencies: tableDependencies,
		})
	}

	return views, nil
}

func fetchMaterializedViews(ctx context.Context, db dbsqlc.DBTX) ([]MaterializedView, error) {
	rawMaterializedViews, err := dbsqlc.New().GetMaterializedViews(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("GetMaterializedViews: %w", err)
	}

	var materializedViews []MaterializedView
	for _, mv := range rawMaterializedViews {
		options, err := relOptionsToMap(mv.RelOptions)
		if err != nil {
			return nil, fmt.Errorf("materialized view (%q): %w", mv.ViewName, err)
		}

		tableDependencies, err := parseJSONTableDependencies(mv.TableDependencies)
		if err != nil {
			return nil, fmt.Errorf("parsing schema qualified names JSON: %w", err)
		}

		materializedViews = append(materializedViews, MaterializedView{
			SchemaQualifiedName: buildNameFromUnescaped(mv.ViewName, mv.SchemaName),
			ViewDefinition:      mv.ViewDefinition,
			Options:             options,
			Tablespace:          mv.TablespaceName,

			TableDependencies: tableDependencies,
		})
	}

	return materializedViews, nil
}

// parseViewJSONTableDependencies takes an slice of JSON values with schema,
// `schema: string; table: string, columns: []string` and unmarshals them into a go struct.
func parseJSONTableDependencies(vals []string) ([]TableDependency, error) {
	var out []TableDependency
	for _, v := range vals {
		var s struct {
			Schema  string   `json:"schema"`
			Name    string   `json:"name"`
			Columns []string `json:"columns"`
		}
		if err := json.Unmarshal([]byte(v), &s); err != nil {
			return nil, fmt.Errorf("json.Unmarshal(%q, SchemaQualifiedName): %w", string(v), err)
		}
		out = append(out, TableDependency{
			SchemaQualifiedName: buildNameFromUnescaped(s.Name, s.Schema),
			Columns:             s.Columns,
		})
	}
	return out, nil
}

// buildProcName is used to build the schema qualified name for a proc (function, procedure), i.e., anything
// identified by a name AND its arguments.
func buildProcName(name, identityArguments, schemaName string) SchemaQualifiedName {
	return SchemaQualifiedName{
		SchemaName:  schemaName,
		EscapedName: fmt.Sprintf("%s(%s)", EscapeIdentifier(name), identityArguments),
	}
}

func buildNameFromUnescaped(unescapedName, schemaName string) SchemaQualifiedName {
	return SchemaQualifiedName{
		EscapedName: EscapeIdentifier(unescapedName),
		SchemaName:  schemaName,
	}
}

// relOptionsToMap converts pg_catalog.pg_class.reloptions to a map.
func relOptionsToMap(vals []string) (map[string]string, error) {
	out := make(map[string]string)
	for i, v := range vals {
		kv := strings.SplitN(v, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("val[%d] (%q): expected 2 values when splitting by \"=\" but found %d", i, v, len(kv))
		}
		out[kv[0]] = kv[1]
	}
	return out, nil
}
