package schema

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

const (
	// SchemaSnapshotHashV1Domain separates the candidate snapshot hash from every
	// other pg-schema-diff digest contract.
	SchemaSnapshotHashV1Domain = "pg-schema-diff:snapshot:v1"
	SchemaSnapshotHashV1Prefix = SchemaSnapshotHashV1Domain + ":sha256:"
)

// SnapshotHashTrustedArchivalGroup identifies marker material that an archival
// orchestrator has already validated. The hasher deliberately does not parse
// markers: it binds these identities to raw OID/name/comment facts in Inventory.
type SnapshotHashTrustedArchivalGroup struct {
	GroupID     string
	SchemaNames []string
}

type snapshotHashMaterialV1 struct {
	ModeledSchema         []snapshotHashSectionV1 `json:"modeled_schema"`
	CatalogInventory      []snapshotHashSectionV1 `json:"catalog_inventory"`
	TrustedArchivalGroups []snapshotHashRecordV1  `json:"trusted_archival_groups"`
}

type snapshotHashSectionV1 struct {
	Name    string                 `json:"name"`
	Records []snapshotHashRecordV1 `json:"records"`
}

type snapshotHashRecordV1 struct {
	Kind   string                `json:"kind"`
	Fields []snapshotHashFieldV1 `json:"fields"`
}

type snapshotHashFieldV1 struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
}

// BuildSchemaSnapshotHashV1 returns the active versioned snapshot hash. The
// modeled schema and every catalog field are projected explicitly below, so a
// future Go struct field cannot silently change the v1 contract.
func BuildSchemaSnapshotHashV1(
	snapshot SchemaSnapshot,
	trustedGroups []SnapshotHashTrustedArchivalGroup,
) (string, error) {
	trustedMaterial, err := snapshotHashTrustedGroupsV1(snapshot.Inventory, trustedGroups)
	if err != nil {
		return "", err
	}
	material := snapshotHashMaterialV1{
		ModeledSchema:         snapshotHashModeledSchemaV1(snapshot.Schema.Normalize()),
		CatalogInventory:      snapshotHashCatalogInventoryV1(snapshot.Inventory.Normalize()),
		TrustedArchivalGroups: trustedMaterial,
	}
	encoded, err := json.Marshal(material)
	if err != nil {
		return "", fmt.Errorf("marshaling canonical snapshot hash material: %w", err)
	}

	digestInput := make([]byte, 0, len(SchemaSnapshotHashV1Domain)+1+len(encoded))
	digestInput = append(digestInput, SchemaSnapshotHashV1Domain...)
	digestInput = append(digestInput, 0)
	digestInput = append(digestInput, encoded...)
	digest := sha256.Sum256(digestInput)
	return SchemaSnapshotHashV1Prefix + hex.EncodeToString(digest[:]), nil
}

// ValidateSchemaSnapshotHashV1 rejects alternate domains, algorithms, casing,
// lengths, and non-hex encodings.
func ValidateSchemaSnapshotHashV1(value string) error {
	if !strings.HasPrefix(value, SchemaSnapshotHashV1Prefix) {
		return fmt.Errorf("snapshot hash must use prefix %q", SchemaSnapshotHashV1Prefix)
	}
	hexDigest := strings.TrimPrefix(value, SchemaSnapshotHashV1Prefix)
	if len(hexDigest) != sha256.Size*2 {
		return fmt.Errorf("snapshot hash digest must contain %d lowercase hexadecimal characters", sha256.Size*2)
	}
	decoded, err := hex.DecodeString(hexDigest)
	if err != nil || hex.EncodeToString(decoded) != hexDigest {
		return fmt.Errorf("snapshot hash digest must contain %d lowercase hexadecimal characters", sha256.Size*2)
	}
	return nil
}

func snapshotHashModeledSchemaV1(modeled Schema) []snapshotHashSectionV1 {
	return []snapshotHashSectionV1{
		hashSectionV1("named_schemas", hashRecordsV1(modeled.NamedSchemas, func(value NamedSchema) snapshotHashRecordV1 {
			return hashRecordV1("named_schema", hashFieldV1("name", value.Name))
		})),
		hashSectionV1("extensions", hashRecordsV1(modeled.Extensions, func(value Extension) snapshotHashRecordV1 {
			return hashRecordV1(
				"extension",
				hashFieldV1("schema_name", value.SchemaName),
				hashFieldV1("escaped_name", value.EscapedName),
				hashFieldV1("version", value.Version),
			)
		})),
		hashSectionV1("enums", hashRecordsV1(modeled.Enums, func(value Enum) snapshotHashRecordV1 {
			return hashRecordV1(
				"enum",
				hashFieldV1("schema_name", value.SchemaName),
				hashFieldV1("escaped_name", value.EscapedName),
				hashFieldV1("labels", hashOrderedSliceV1(value.Labels)),
			)
		})),
		hashSectionV1("tables", hashRecordsV1(modeled.Tables, snapshotHashTableV1)),
		hashSectionV1("indexes", hashRecordsV1(modeled.Indexes, snapshotHashIndexV1)),
		hashSectionV1("foreign_key_constraints", hashRecordsV1(modeled.ForeignKeyConstraints,
			func(value ForeignKeyConstraint) snapshotHashRecordV1 {
				return hashRecordV1(
					"foreign_key_constraint",
					hashFieldV1("escaped_name", value.EscapedName),
					hashFieldV1("owning_table", snapshotHashQualifiedNameV1(value.OwningTable)),
					hashFieldV1("foreign_table", snapshotHashQualifiedNameV1(value.ForeignTable)),
					hashFieldV1("constraint_definition", value.ConstraintDef),
					hashFieldV1("is_valid", value.IsValid),
				)
			})),
		hashSectionV1("sequences", hashRecordsV1(modeled.Sequences, snapshotHashSequenceV1)),
		hashSectionV1("functions", hashRecordsV1(modeled.Functions, func(value Function) snapshotHashRecordV1 {
			return hashRecordV1(
				"function",
				hashFieldV1("schema_name", value.SchemaName),
				hashFieldV1("escaped_name", value.EscapedName),
				hashFieldV1("definition", value.FunctionDef),
				hashFieldV1("language", value.Language),
				hashFieldV1("depends_on_functions", hashRecordsV1(value.DependsOnFunctions, snapshotHashQualifiedNameV1)),
			)
		})),
		hashSectionV1("procedures", hashRecordsV1(modeled.Procedures, func(value Procedure) snapshotHashRecordV1 {
			return hashRecordV1(
				"procedure",
				hashFieldV1("schema_name", value.SchemaName),
				hashFieldV1("escaped_name", value.EscapedName),
				hashFieldV1("definition", value.Def),
			)
		})),
		hashSectionV1("triggers", hashRecordsV1(modeled.Triggers, func(value Trigger) snapshotHashRecordV1 {
			return hashRecordV1(
				"trigger",
				hashFieldV1("escaped_name", value.EscapedName),
				hashFieldV1("owning_table", snapshotHashQualifiedNameV1(value.OwningTable)),
				hashFieldV1("function", snapshotHashQualifiedNameV1(value.Function)),
				hashFieldV1("definition", string(value.GetTriggerDefStmt)),
				hashFieldV1("is_constraint", value.IsConstraint),
			)
		})),
		hashSectionV1("views", hashRecordsV1(modeled.Views, func(value View) snapshotHashRecordV1 {
			return snapshotHashViewV1("view", value.SchemaQualifiedName, value.ViewDefinition,
				value.Options, "", value.TableDependencies)
		})),
		hashSectionV1("materialized_views", hashRecordsV1(modeled.MaterializedViews,
			func(value MaterializedView) snapshotHashRecordV1 {
				return snapshotHashViewV1("materialized_view", value.SchemaQualifiedName,
					value.ViewDefinition, value.Options, value.Tablespace, value.TableDependencies)
			})),
	}
}

func snapshotHashTableV1(value Table) snapshotHashRecordV1 {
	var parent any
	if value.ParentTable != nil {
		parent = snapshotHashQualifiedNameV1(*value.ParentTable)
	}
	return hashRecordV1(
		"table",
		hashFieldV1("schema_name", value.SchemaName),
		hashFieldV1("escaped_name", value.EscapedName),
		hashFieldV1("columns", hashOrderedRecordsV1(value.Columns, func(column Column) snapshotHashRecordV1 {
			var identity any
			if column.Identity != nil {
				identity = hashRecordV1(
					"column_identity",
					hashFieldV1("type", string(column.Identity.Type)),
					hashFieldV1("min_value", column.Identity.MinValue),
					hashFieldV1("max_value", column.Identity.MaxValue),
					hashFieldV1("start_value", column.Identity.StartValue),
					hashFieldV1("increment", column.Identity.Increment),
					hashFieldV1("cache_size", column.Identity.CacheSize),
					hashFieldV1("cycle", column.Identity.Cycle),
				)
			}
			return hashRecordV1(
				"column",
				hashFieldV1("name", column.Name),
				hashFieldV1("type", column.Type),
				hashFieldV1("collation", snapshotHashQualifiedNameV1(column.Collation)),
				hashFieldV1("default", column.Default),
				hashFieldV1("is_generated", column.IsGenerated),
				hashFieldV1("generation_expression", column.GenerationExpression),
				hashFieldV1("is_nullable", column.IsNullable),
				hashFieldV1("has_missing_value_optimization", column.HasMissingValOptimization),
				hashFieldV1("identity", identity),
			)
		})),
		hashFieldV1("check_constraints", hashRecordsV1(value.CheckConstraints,
			func(constraint CheckConstraint) snapshotHashRecordV1 {
				return hashRecordV1(
					"check_constraint",
					hashFieldV1("name", constraint.Name),
					hashFieldV1("key_columns", hashSortedSliceV1(constraint.KeyColumns)),
					hashFieldV1("expression", constraint.Expression),
					hashFieldV1("is_valid", constraint.IsValid),
					hashFieldV1("is_inheritable", constraint.IsInheritable),
					hashFieldV1("depends_on_functions", hashRecordsV1(constraint.DependsOnFunctions,
						snapshotHashQualifiedNameV1)),
				)
			})),
		hashFieldV1("policies", hashRecordsV1(value.Policies, func(policy Policy) snapshotHashRecordV1 {
			return hashRecordV1(
				"policy",
				hashFieldV1("escaped_name", policy.EscapedName),
				hashFieldV1("is_permissive", policy.IsPermissive),
				hashFieldV1("applies_to", hashSortedSliceV1(policy.AppliesTo)),
				hashFieldV1("command", string(policy.Cmd)),
				hashFieldV1("check_expression", policy.CheckExpression),
				hashFieldV1("using_expression", policy.UsingExpression),
				hashFieldV1("columns", hashSortedSliceV1(policy.Columns)),
			)
		})),
		hashFieldV1("privileges", hashRecordsV1(value.Privileges,
			func(privilege TablePrivilege) snapshotHashRecordV1 {
				return hashRecordV1(
					"table_privilege",
					hashFieldV1("grantee", privilege.Grantee),
					hashFieldV1("privilege", privilege.Privilege),
					hashFieldV1("is_grantable", privilege.IsGrantable),
				)
			})),
		hashFieldV1("replica_identity", string(value.ReplicaIdentity)),
		hashFieldV1("rls_enabled", value.RLSEnabled),
		hashFieldV1("rls_forced", value.RLSForced),
		hashFieldV1("partition_key_definition", value.PartitionKeyDef),
		hashFieldV1("parent_table", parent),
		hashFieldV1("for_values", value.ForValues),
	)
}

func snapshotHashIndexV1(value Index) snapshotHashRecordV1 {
	var constraint any
	if value.Constraint != nil {
		constraint = hashRecordV1(
			"index_constraint",
			hashFieldV1("type", string(value.Constraint.Type)),
			hashFieldV1("escaped_name", value.Constraint.EscapedConstraintName),
			hashFieldV1("definition", value.Constraint.ConstraintDef),
			hashFieldV1("is_local", value.Constraint.IsLocal),
		)
	}
	var parent any
	if value.ParentIdx != nil {
		parent = snapshotHashQualifiedNameV1(*value.ParentIdx)
	}
	return hashRecordV1(
		"index",
		hashFieldV1("name", value.Name),
		hashFieldV1("owning_relation", snapshotHashQualifiedNameV1(value.OwningRelName)),
		hashFieldV1("owning_relation_kind", string(value.OwningRelKind)),
		hashFieldV1("columns", hashOrderedSliceV1(value.Columns)),
		hashFieldV1("is_invalid", value.IsInvalid),
		hashFieldV1("is_unique", value.IsUnique),
		hashFieldV1("constraint", constraint),
		hashFieldV1("definition", string(value.GetIndexDefStmt)),
		hashFieldV1("parent_index", parent),
	)
}

func snapshotHashSequenceV1(value Sequence) snapshotHashRecordV1 {
	var owner any
	if value.Owner != nil {
		owner = hashRecordV1(
			"sequence_owner",
			hashFieldV1("table", snapshotHashQualifiedNameV1(value.Owner.TableName)),
			hashFieldV1("column_name", value.Owner.ColumnName),
		)
	}
	return hashRecordV1(
		"sequence",
		hashFieldV1("schema_name", value.SchemaName),
		hashFieldV1("escaped_name", value.EscapedName),
		hashFieldV1("owner", owner),
		hashFieldV1("type", value.Type),
		hashFieldV1("start_value", value.StartValue),
		hashFieldV1("increment", value.Increment),
		hashFieldV1("max_value", value.MaxValue),
		hashFieldV1("min_value", value.MinValue),
		hashFieldV1("cache_size", value.CacheSize),
		hashFieldV1("cycle", value.Cycle),
	)
}

func snapshotHashViewV1(
	kind string,
	name SchemaQualifiedName,
	definition string,
	options map[string]string,
	tablespace string,
	dependencies []TableDependency,
) snapshotHashRecordV1 {
	fields := []snapshotHashFieldV1{
		hashFieldV1("schema_name", name.SchemaName),
		hashFieldV1("escaped_name", name.EscapedName),
		hashFieldV1("definition", definition),
		hashFieldV1("options", snapshotHashStringMapV1(options)),
	}
	if kind == "materialized_view" {
		fields = append(fields, hashFieldV1("tablespace", tablespace))
	}
	fields = append(fields, hashFieldV1("table_dependencies", hashRecordsV1(dependencies,
		func(dependency TableDependency) snapshotHashRecordV1 {
			return hashRecordV1(
				"table_dependency",
				hashFieldV1("schema_name", dependency.SchemaName),
				hashFieldV1("escaped_name", dependency.EscapedName),
				hashFieldV1("columns", hashSortedSliceV1(dependency.Columns)),
			)
		})))
	return hashRecordV1(kind, fields...)
}

func snapshotHashCatalogInventoryV1(inventory CatalogInventory) []snapshotHashSectionV1 {
	return []snapshotHashSectionV1{
		hashSectionV1("schemas", hashRecordsV1(inventory.Schemas, func(value CatalogSchema) snapshotHashRecordV1 {
			return hashRecordV1("schema", hashFieldV1("oid", value.OID), hashFieldV1("name", value.Name),
				hashFieldV1("owner_oid", value.OwnerOID),
				hashFieldV1("owner_name", value.OwnerName),
				hashFieldV1("comment", value.Comment))
		})),
		hashSectionV1("relations", hashRecordsV1(inventory.Relations, snapshotHashCatalogRelationV1)),
		hashSectionV1("types", hashRecordsV1(inventory.Types, snapshotHashCatalogTypeV1)),
		hashSectionV1("tables", hashRecordsV1(inventory.Tables, func(value CatalogTable) snapshotHashRecordV1 {
			return hashRecordV1("table", hashFieldV1("relation_oid", value.RelationOID),
				hashFieldV1("rls_enabled", value.RLSEnabled),
				hashFieldV1("rls_forced", value.RLSForced),
				hashFieldV1("replica_identity", value.ReplicaIdentity),
				hashFieldV1("replica_identity_index_oid", value.ReplicaIdentityIndexOID),
				hashFieldV1("clustered_index_oid", value.ClusteredIndexOID),
				hashFieldV1("options", hashSortedSliceV1(value.Options)),
				hashFieldV1("tablespace_oid", value.TablespaceOID),
				hashFieldV1("tablespace_name", value.TablespaceName),
				hashFieldV1("access_method_oid", value.AccessMethodOID),
				hashFieldV1("access_method_name", value.AccessMethodName))
		})),
		hashSectionV1("columns", hashRecordsV1(inventory.Columns, snapshotHashCatalogColumnV1)),
		hashSectionV1("indexes", hashRecordsV1(inventory.Indexes, snapshotHashCatalogIndexV1)),
		hashSectionV1("constraints", hashRecordsV1(inventory.Constraints, snapshotHashCatalogConstraintV1)),
		hashSectionV1("triggers", hashRecordsV1(inventory.Triggers, snapshotHashCatalogTriggerV1)),
		hashSectionV1("rules", hashRecordsV1(inventory.Rules, snapshotHashCatalogRuleV1)),
		hashSectionV1("policies", hashRecordsV1(inventory.Policies, snapshotHashCatalogPolicyV1)),
		hashSectionV1("sequences", hashRecordsV1(inventory.Sequences, snapshotHashCatalogSequenceV1)),
		hashSectionV1("owned_sequences", hashRecordsV1(inventory.OwnedSequences,
			func(value CatalogOwnedSequence) snapshotHashRecordV1 {
				return hashRecordV1("owned_sequence",
					hashFieldV1("sequence_oid", value.SequenceOID),
					hashFieldV1("relation_oid", value.RelationOID),
					hashFieldV1("column_number", value.ColumnNumber),
					hashFieldV1("column_name", value.ColumnName),
					hashFieldV1("dependency_type", string(value.DependencyType)))
			})),
		hashSectionV1("extended_statistics", hashRecordsV1(inventory.ExtendedStatistics,
			snapshotHashCatalogExtendedStatisticV1)),
		hashSectionV1("inheritance_edges", hashRecordsV1(inventory.InheritanceEdges,
			func(value CatalogInheritanceEdge) snapshotHashRecordV1 {
				return hashRecordV1("inheritance_edge",
					hashFieldV1("child_relation_oid", value.ChildRelationOID),
					hashFieldV1("parent_relation_oid", value.ParentRelationOID),
					hashFieldV1("sequence_number", value.SequenceNumber))
			})),
		hashSectionV1("partition_attachments", hashRecordsV1(inventory.PartitionAttachments,
			func(value CatalogPartitionAttachment) snapshotHashRecordV1 {
				return hashRecordV1("partition_attachment",
					hashFieldV1("relation_oid", value.RelationOID),
					hashFieldV1("parent_relation_oid", value.ParentRelationOID),
					hashFieldV1("bound_expression", value.BoundExpression))
			})),
		hashSectionV1("security_labels", hashRecordsV1(inventory.SecurityLabels,
			func(value CatalogSecurityLabel) snapshotHashRecordV1 {
				return hashRecordV1("security_label",
					hashFieldV1("relation_oid", value.RelationOID),
					hashFieldV1("column_number", value.ColumnNumber),
					hashFieldV1("provider", value.Provider),
					hashFieldV1("label", value.Label))
			})),
		hashSectionV1("dependencies", hashRecordsV1(inventory.Dependencies, snapshotHashCatalogDependencyV1)),
		hashSectionV1("foreign_keys", hashRecordsV1(inventory.ForeignKeys, snapshotHashCatalogForeignKeyV1)),
		hashSectionV1("views", hashRecordsV1(inventory.Views, snapshotHashCatalogViewV1)),
		hashSectionV1("routines", hashRecordsV1(inventory.Routines, snapshotHashCatalogRoutineV1)),
		hashSectionV1("enum_labels", hashRecordsV1(inventory.EnumLabels, func(
			value CatalogEnumLabel,
		) snapshotHashRecordV1 {
			return hashRecordV1("enum_label", hashFieldV1("type_oid", value.TypeOID), hashFieldV1("oid", value.OID),
				hashFieldV1("sort_order", value.SortOrder), hashFieldV1("label", value.Label))
		})),
		hashSectionV1("composite_attributes", hashRecordsV1(inventory.CompositeAttributes,
			func(value CatalogCompositeAttribute) snapshotHashRecordV1 {
				return hashRecordV1("composite_attribute", hashFieldV1("type_oid", value.TypeOID),
					hashFieldV1("relation_oid", value.RelationOID), hashFieldV1("number", value.Number),
					hashFieldV1("name", value.Name), hashFieldV1("attribute_type_oid", value.AttributeTypeOID),
					hashFieldV1("type_modifier", value.TypeModifier),
					hashFieldV1("collation_oid", value.CollationOID))
			})),
		hashSectionV1("domain_constraints", hashRecordsV1(inventory.DomainConstraints,
			func(value CatalogDomainConstraint) snapshotHashRecordV1 {
				return hashRecordV1("domain_constraint", hashFieldV1("oid", value.OID),
					hashFieldV1("type_oid", value.TypeOID), hashFieldV1("name", value.Name),
					hashFieldV1("is_deferrable", value.IsDeferrable),
					hashFieldV1("is_deferred", value.IsDeferred),
					hashFieldV1("is_validated", value.IsValidated),
					hashFieldV1("definition", value.Definition))
			})),
		hashSectionV1("ranges", hashRecordsV1(inventory.Ranges, snapshotHashCatalogRangeV1)),
		hashSectionV1("type_support_functions", hashRecordsV1(inventory.TypeSupportFunctions,
			func(value CatalogTypeSupportFunction) snapshotHashRecordV1 {
				return hashRecordV1("type_support_function", hashFieldV1("type_oid", value.TypeOID),
					hashFieldV1("role", value.Role), hashFieldV1("function_oid", value.FunctionOID),
					hashFieldV1("function_schema_name", value.FunctionSchemaName),
					hashFieldV1("function_name", value.FunctionName),
					hashFieldV1("function_identity_arguments", value.FunctionIdentityArguments))
			})),
		hashSectionV1("collations", hashRecordsV1(inventory.Collations, snapshotHashCatalogCollationV1)),
		hashSectionV1("operators", hashRecordsV1(inventory.Operators, snapshotHashCatalogOperatorV1)),
		hashSectionV1("extensions", hashRecordsV1(inventory.Extensions,
			snapshotHashCatalogExtensionIdentityV1)),
		hashSectionV1("extension_members", hashRecordsV1(inventory.ExtensionMembers,
			func(value CatalogExtensionMember) snapshotHashRecordV1 {
				return hashRecordV1("extension_member",
					hashFieldV1("extension_oid", value.ExtensionOID),
					hashFieldV1("extension_name", value.ExtensionName),
					hashFieldV1("object", snapshotHashCatalogDependencyObjectV1(value.Object)))
			})),
		hashSectionV1("event_triggers", hashRecordsV1(inventory.EventTriggers,
			func(value CatalogEventTrigger) snapshotHashRecordV1 {
				return hashRecordV1("event_trigger", hashFieldV1("oid", value.OID), hashFieldV1("name", value.Name),
					hashFieldV1("owner_oid", value.OwnerOID),
					hashFieldV1("owner_name", value.OwnerName),
					hashFieldV1("event", value.Event), hashFieldV1("function_oid", value.FunctionOID),
					hashFieldV1("enabled_mode", value.EnabledMode),
					hashFieldV1("tags", hashSortedSliceV1(value.Tags)))
			})),
		hashSectionV1("publications", hashRecordsV1(inventory.Publications, snapshotHashCatalogPublicationV1)),
		hashSectionV1("publication_relations", hashRecordsV1(inventory.PublicationRelations,
			snapshotHashCatalogPublicationRelationV1)),
		hashSectionV1("publication_schemas", hashRecordsV1(inventory.PublicationSchemas,
			func(value CatalogPublicationSchema) snapshotHashRecordV1 {
				return hashRecordV1("publication_schema", hashFieldV1("oid", value.OID),
					hashFieldV1("publication_oid", value.PublicationOID),
					hashFieldV1("publication_name", value.PublicationName),
					hashFieldV1("schema_oid", value.SchemaOID),
					hashFieldV1("schema_name", value.SchemaName))
			})),
		hashSectionV1("acl_grants", hashRecordsV1(inventory.ACLGrants, snapshotHashCatalogACLGrantV1)),
		hashSectionV1("default_acls", hashRecordsV1(inventory.DefaultACLs, snapshotHashCatalogDefaultACLV1)),
		hashSectionV1("roles", hashRecordsV1(inventory.Roles, snapshotHashCatalogRoleV1)),
		hashSectionV1("role_memberships", hashRecordsV1(inventory.RoleMemberships,
			snapshotHashCatalogRoleMembershipV1)),
	}
}

func snapshotHashCatalogRelationV1(value CatalogRelation) snapshotHashRecordV1 {
	var toast any
	if value.ToastRelation != nil {
		toast = hashRecordV1("relation_identity",
			hashFieldV1("oid", value.ToastRelation.OID),
			hashFieldV1("schema_oid", value.ToastRelation.SchemaOID),
			hashFieldV1("schema_name", value.ToastRelation.SchemaName),
			hashFieldV1("name", value.ToastRelation.Name))
	}
	return hashRecordV1("relation", hashFieldV1("oid", value.OID),
		hashFieldV1("schema_oid", value.SchemaOID),
		hashFieldV1("schema_name", value.SchemaName), hashFieldV1("name", value.Name),
		hashFieldV1("comment", value.Comment), hashFieldV1("owner_oid", value.OwnerOID),
		hashFieldV1("owner_name", value.OwnerName), hashFieldV1("kind", string(value.Kind)),
		hashFieldV1("persistence", string(value.Persistence)),
		hashFieldV1("is_partition", value.IsPartition),
		hashFieldV1("row_type_oid", value.RowTypeOID),
		hashFieldV1("array_type_oid", value.ArrayTypeOID),
		hashFieldV1("toast_relation", toast), hashFieldV1("extension",
			snapshotHashCatalogExtensionV1(value.Extension)))
}

func snapshotHashCatalogTypeV1(value CatalogType) snapshotHashRecordV1 {
	return hashRecordV1("type", hashFieldV1("oid", value.OID),
		hashFieldV1("schema_oid", value.SchemaOID),
		hashFieldV1("schema_name", value.SchemaName), hashFieldV1("name", value.Name),
		hashFieldV1("owner_oid", value.OwnerOID), hashFieldV1("owner_name", value.OwnerName),
		hashFieldV1("kind", string(value.Kind)), hashFieldV1("raw_kind", value.RawKind),
		hashFieldV1("category", value.Category), hashFieldV1("is_preferred", value.IsPreferred),
		hashFieldV1("is_defined", value.IsDefined), hashFieldV1("internal_length", value.InternalLength),
		hashFieldV1("is_passed_by_value", value.IsPassedByValue),
		hashFieldV1("delimiter", value.Delimiter),
		hashFieldV1("alignment", value.Alignment), hashFieldV1("storage", value.Storage),
		hashFieldV1("relation_oid", value.RelationOID),
		hashFieldV1("element_type_oid", value.ElementTypeOID),
		hashFieldV1("array_type_oid", value.ArrayTypeOID),
		hashFieldV1("base_type_oid", value.BaseTypeOID),
		hashFieldV1("type_modifier", value.TypeModifier),
		hashFieldV1("dimensions", value.Dimensions),
		hashFieldV1("collation_oid", value.CollationOID),
		hashFieldV1("is_not_null", value.IsNotNull),
		hashFieldV1("default_value", value.DefaultValue),
		hashFieldV1("extension", snapshotHashCatalogExtensionV1(value.Extension)))
}

func snapshotHashCatalogColumnV1(value CatalogColumn) snapshotHashRecordV1 {
	return hashRecordV1("column", hashFieldV1("relation_oid", value.RelationOID),
		hashFieldV1("number", value.Number), hashFieldV1("name", value.Name),
		hashFieldV1("is_dropped", value.IsDropped), hashFieldV1("type_oid", value.TypeOID),
		hashFieldV1("type_schema_oid", value.TypeSchemaOID),
		hashFieldV1("type_schema_name", value.TypeSchemaName),
		hashFieldV1("type_name", value.TypeName), hashFieldV1("type_modifier", value.TypeModifier),
		hashFieldV1("formatted_type", value.FormattedType),
		hashFieldV1("collation_oid", value.CollationOID),
		hashFieldV1("collation_schema_oid", value.CollationSchemaOID),
		hashFieldV1("collation_schema_name", value.CollationSchemaName),
		hashFieldV1("collation_name", value.CollationName),
		hashFieldV1("is_not_null", value.IsNotNull),
		hashFieldV1("identity_mode", value.IdentityMode),
		hashFieldV1("generated_mode", value.GeneratedMode),
		hashFieldV1("default_expression", value.DefaultExpression),
		hashFieldV1("generated_expression", value.GeneratedExpression),
		hashFieldV1("has_missing_value", value.HasMissingValue),
		hashFieldV1("missing_value_binary", value.MissingValueBinary),
		hashFieldV1("storage_mode", value.StorageMode),
		hashFieldV1("compression_mode", value.CompressionMode),
		hashFieldV1("options", hashSortedSliceV1(value.Options)),
		hashFieldV1("statistics_target", value.StatisticsTarget), hashFieldV1("is_local", value.IsLocal),
		hashFieldV1("inheritance_count", value.InheritanceCount), hashFieldV1("comment", value.Comment))
}

func snapshotHashCatalogIndexV1(value CatalogIndex) snapshotHashRecordV1 {
	return hashRecordV1("index", hashFieldV1("oid", value.OID),
		hashFieldV1("schema_oid", value.SchemaOID),
		hashFieldV1("schema_name", value.SchemaName), hashFieldV1("name", value.Name),
		hashFieldV1("relation_oid", value.RelationOID), hashFieldV1("kind", string(value.Kind)),
		hashFieldV1("constraint_oid", value.ConstraintOID),
		hashFieldV1("is_clustered", value.IsClustered),
		hashFieldV1("is_replica_identity", value.IsReplicaIdentity),
		hashFieldV1("is_primary", value.IsPrimary),
		hashFieldV1("is_unique", value.IsUnique), hashFieldV1("is_exclusion", value.IsExclusion),
		hashFieldV1("is_valid", value.IsValid), hashFieldV1("is_ready", value.IsReady),
		hashFieldV1("is_live", value.IsLive), hashFieldV1("extension",
			snapshotHashCatalogExtensionV1(value.Extension)))
}

func snapshotHashCatalogConstraintV1(value CatalogConstraint) snapshotHashRecordV1 {
	return hashRecordV1("constraint", hashFieldV1("oid", value.OID),
		hashFieldV1("schema_oid", value.SchemaOID),
		hashFieldV1("schema_name", value.SchemaName), hashFieldV1("name", value.Name),
		hashFieldV1("type", value.Type), hashFieldV1("relation_oid", value.RelationOID),
		hashFieldV1("index_oid", value.IndexOID), hashFieldV1("parent_constraint_oid", value.ParentConstraintOID),
		hashFieldV1("referenced_relation_oid", value.ReferencedRelationOID),
		hashFieldV1("key_column_numbers", hashOrderedSliceV1(value.KeyColumnNumbers)),
		hashFieldV1("is_deferrable", value.IsDeferrable),
		hashFieldV1("is_deferred", value.IsDeferred),
		hashFieldV1("is_validated", value.IsValidated), hashFieldV1("is_local", value.IsLocal),
		hashFieldV1("inheritance_count", value.InheritanceCount),
		hashFieldV1("is_no_inherit", value.IsNoInherit),
		hashFieldV1("check_expression", value.CheckExpression), hashFieldV1("comment", value.Comment),
		hashFieldV1("extension", snapshotHashCatalogExtensionV1(value.Extension)))
}

func snapshotHashCatalogTriggerV1(value CatalogTrigger) snapshotHashRecordV1 {
	return hashRecordV1("trigger", hashFieldV1("oid", value.OID),
		hashFieldV1("relation_oid", value.RelationOID),
		hashFieldV1("name", value.Name), hashFieldV1("function_oid", value.FunctionOID),
		hashFieldV1("type", value.Type), hashFieldV1("enabled_mode", value.EnabledMode),
		hashFieldV1("is_internal", value.IsInternal),
		hashFieldV1("parent_trigger_oid", value.ParentTriggerOID),
		hashFieldV1("constraint_oid", value.ConstraintOID),
		hashFieldV1("definition", value.Definition),
		hashFieldV1("comment", value.Comment))
}

func snapshotHashCatalogRuleV1(value CatalogRule) snapshotHashRecordV1 {
	return hashRecordV1("rule", hashFieldV1("oid", value.OID),
		hashFieldV1("relation_oid", value.RelationOID),
		hashFieldV1("name", value.Name), hashFieldV1("event_type", value.EventType),
		hashFieldV1("enabled_mode", value.EnabledMode), hashFieldV1("is_instead", value.IsInstead),
		hashFieldV1("definition", value.Definition), hashFieldV1("comment", value.Comment))
}

func snapshotHashCatalogPolicyV1(value CatalogPolicy) snapshotHashRecordV1 {
	roles := make([]snapshotHashRecordV1, max(len(value.RoleOIDs), len(value.RoleNames)))
	for idx := range roles {
		var oid uint32
		if idx < len(value.RoleOIDs) {
			oid = value.RoleOIDs[idx]
		}
		name := ""
		if idx < len(value.RoleNames) {
			name = value.RoleNames[idx]
		}
		roles[idx] = hashRecordV1("role", hashFieldV1("oid", oid), hashFieldV1("name", name))
	}
	roles = hashSortRecordsV1(roles)
	return hashRecordV1("policy", hashFieldV1("oid", value.OID),
		hashFieldV1("relation_oid", value.RelationOID),
		hashFieldV1("name", value.Name), hashFieldV1("command", value.Command),
		hashFieldV1("is_permissive", value.IsPermissive), hashFieldV1("roles", roles),
		hashFieldV1("using_expression", value.UsingExpression),
		hashFieldV1("check_expression", value.CheckExpression),
		hashFieldV1("comment", value.Comment))
}

func snapshotHashCatalogSequenceV1(value CatalogSequence) snapshotHashRecordV1 {
	return hashRecordV1("sequence", hashFieldV1("oid", value.OID),
		hashFieldV1("schema_oid", value.SchemaOID),
		hashFieldV1("schema_name", value.SchemaName), hashFieldV1("name", value.Name),
		hashFieldV1("owner_oid", value.OwnerOID), hashFieldV1("owner_name", value.OwnerName),
		hashFieldV1("persistence", string(value.Persistence)),
		hashFieldV1("data_type_oid", value.DataTypeOID),
		hashFieldV1("start_value", value.StartValue),
		hashFieldV1("increment_value", value.IncrementValue),
		hashFieldV1("max_value", value.MaxValue), hashFieldV1("min_value", value.MinValue),
		hashFieldV1("cache_size", value.CacheSize), hashFieldV1("is_cycle", value.IsCycle),
		hashFieldV1("extension", snapshotHashCatalogExtensionV1(value.Extension)))
}

func snapshotHashCatalogExtendedStatisticV1(value CatalogExtendedStatistic) snapshotHashRecordV1 {
	return hashRecordV1("extended_statistic", hashFieldV1("oid", value.OID),
		hashFieldV1("schema_oid", value.SchemaOID), hashFieldV1("schema_name", value.SchemaName),
		hashFieldV1("name", value.Name), hashFieldV1("owner_oid", value.OwnerOID),
		hashFieldV1("owner_name", value.OwnerName), hashFieldV1("relation_oid", value.RelationOID),
		hashFieldV1("column_numbers", hashSortedSliceV1(value.ColumnNumbers)),
		hashFieldV1("expressions", hashSortedSliceV1(value.Expressions)),
		hashFieldV1("kinds", hashSortedSliceV1(value.Kinds)), hashFieldV1("comment", value.Comment))
}

func snapshotHashCatalogDependencyV1(value CatalogDependency) snapshotHashRecordV1 {
	return hashRecordV1("dependency", hashFieldV1("is_shared", value.IsShared),
		hashFieldV1("dependent", snapshotHashCatalogDependencyObjectV1(value.Dependent)),
		hashFieldV1("referenced", snapshotHashCatalogDependencyObjectV1(value.Referenced)),
		hashFieldV1("type", value.Type))
}

func snapshotHashCatalogDependencyObjectV1(value CatalogDependencyObject) snapshotHashRecordV1 {
	return hashRecordV1("dependency_object", hashFieldV1("class_oid", value.ClassOID),
		hashFieldV1("object_oid", value.ObjectOID), hashFieldV1("sub_object_id", value.SubObjectID),
		hashFieldV1("object_type", value.ObjectType), hashFieldV1("schema_name", value.SchemaName),
		hashFieldV1("name", value.Name), hashFieldV1("identity", value.Identity))
}

func snapshotHashCatalogForeignKeyV1(value CatalogForeignKey) snapshotHashRecordV1 {
	return hashRecordV1("foreign_key", hashFieldV1("oid", value.OID),
		hashFieldV1("schema_oid", value.SchemaOID),
		hashFieldV1("schema_name", value.SchemaName), hashFieldV1("name", value.Name),
		hashFieldV1("owning_relation_oid", value.OwningRelationOID),
		hashFieldV1("owning_schema_name", value.OwningSchemaName),
		hashFieldV1("owning_relation_name", value.OwningRelationName),
		hashFieldV1("referenced_relation_oid", value.ReferencedRelationOID),
		hashFieldV1("referenced_schema_name", value.ReferencedSchemaName),
		hashFieldV1("referenced_relation_name", value.ReferencedRelationName),
		hashFieldV1("columns", hashOrderedRecordsV1(value.Columns, func(
			column CatalogForeignKeyColumn,
		) snapshotHashRecordV1 {
			return hashRecordV1("foreign_key_column",
				hashFieldV1("owning_number", column.OwningNumber),
				hashFieldV1("owning_name", column.OwningName),
				hashFieldV1("referenced_number", column.ReferencedNumber),
				hashFieldV1("referenced_name", column.ReferencedName))
		})),
		hashFieldV1("match_type", value.MatchType), hashFieldV1("update_action", value.UpdateAction),
		hashFieldV1("delete_action", value.DeleteAction),
		hashFieldV1("is_deferrable", value.IsDeferrable),
		hashFieldV1("is_deferred", value.IsDeferred),
		hashFieldV1("is_validated", value.IsValidated),
		hashFieldV1("parent_constraint_oid", value.ParentConstraintOID),
		hashFieldV1("definition", value.Definition))
}

func snapshotHashCatalogViewV1(value CatalogView) snapshotHashRecordV1 {
	return hashRecordV1("view", hashFieldV1("relation_oid", value.RelationOID),
		hashFieldV1("schema_oid", value.SchemaOID), hashFieldV1("schema_name", value.SchemaName),
		hashFieldV1("name", value.Name), hashFieldV1("kind", string(value.Kind)),
		hashFieldV1("is_populated", value.IsPopulated),
		hashFieldV1("options", hashSortedSliceV1(value.Options)),
		hashFieldV1("definition", value.Definition))
}

func snapshotHashCatalogRoutineV1(value CatalogRoutine) snapshotHashRecordV1 {
	return hashRecordV1("routine", hashFieldV1("oid", value.OID),
		hashFieldV1("schema_oid", value.SchemaOID),
		hashFieldV1("schema_name", value.SchemaName), hashFieldV1("name", value.Name),
		hashFieldV1("owner_oid", value.OwnerOID), hashFieldV1("owner_name", value.OwnerName),
		hashFieldV1("kind", value.Kind), hashFieldV1("language_oid", value.LanguageOID),
		hashFieldV1("language_name", value.LanguageName),
		hashFieldV1("identity_arguments", value.IdentityArguments),
		hashFieldV1("arguments", value.Arguments), hashFieldV1("result", value.Result),
		hashFieldV1("input_argument_type_oids", hashOrderedSliceV1(value.InputArgumentTypeOIDs)),
		hashFieldV1("all_argument_type_oids", hashOrderedSliceV1(value.AllArgumentTypeOIDs)),
		hashFieldV1("argument_modes", hashOrderedSliceV1(value.ArgumentModes)),
		hashFieldV1("argument_names", hashOrderedSliceV1(value.ArgumentNames)),
		hashFieldV1("result_type_oid", value.ResultTypeOID),
		hashFieldV1("returns_set", value.ReturnsSet),
		hashFieldV1("source", value.Source), hashFieldV1("binary", value.Binary),
		hashFieldV1("has_sql_body", value.HasSQLBody), hashFieldV1("sql_body", value.SQLBody),
		hashFieldV1("configuration", hashSortedSliceV1(value.Configuration)),
		hashFieldV1("definition", value.Definition),
		hashFieldV1("body_form", string(value.BodyForm)),
		hashFieldV1("reference_trackability", string(value.ReferenceTrackability)))
}

func snapshotHashCatalogRangeV1(value CatalogRange) snapshotHashRecordV1 {
	return hashRecordV1("range", hashFieldV1("type_oid", value.TypeOID),
		hashFieldV1("subtype_oid", value.SubtypeOID),
		hashFieldV1("multirange_type_oid", value.MultirangeTypeOID),
		hashFieldV1("collation_oid", value.CollationOID),
		hashFieldV1("operator_class_oid", value.OperatorClassOID),
		hashFieldV1("operator_class_schema_name", value.OperatorClassSchemaName),
		hashFieldV1("operator_class_name", value.OperatorClassName),
		hashFieldV1("canonical_function_oid", value.CanonicalFunctionOID),
		hashFieldV1("subtype_diff_function_oid", value.SubtypeDiffFunctionOID))
}

func snapshotHashCatalogCollationV1(value CatalogCollation) snapshotHashRecordV1 {
	return hashRecordV1("collation", hashFieldV1("oid", value.OID),
		hashFieldV1("schema_oid", value.SchemaOID),
		hashFieldV1("schema_name", value.SchemaName), hashFieldV1("name", value.Name),
		hashFieldV1("owner_oid", value.OwnerOID), hashFieldV1("owner_name", value.OwnerName),
		hashFieldV1("provider", value.Provider), hashFieldV1("is_deterministic", value.IsDeterministic),
		hashFieldV1("encoding", value.Encoding), hashFieldV1("collate", value.Collate),
		hashFieldV1("ctype", value.CType), hashFieldV1("locale", value.Locale),
		hashFieldV1("icu_rules", value.ICURules), hashFieldV1("version", value.Version))
}

func snapshotHashCatalogOperatorV1(value CatalogOperator) snapshotHashRecordV1 {
	return hashRecordV1("operator", hashFieldV1("oid", value.OID),
		hashFieldV1("schema_oid", value.SchemaOID),
		hashFieldV1("schema_name", value.SchemaName), hashFieldV1("name", value.Name),
		hashFieldV1("owner_oid", value.OwnerOID), hashFieldV1("owner_name", value.OwnerName),
		hashFieldV1("kind", value.Kind), hashFieldV1("can_merge", value.CanMerge),
		hashFieldV1("can_hash", value.CanHash), hashFieldV1("left_type_oid", value.LeftTypeOID),
		hashFieldV1("right_type_oid", value.RightTypeOID),
		hashFieldV1("result_type_oid", value.ResultTypeOID),
		hashFieldV1("function_oid", value.FunctionOID),
		hashFieldV1("restriction_function_oid", value.RestrictionFunctionOID),
		hashFieldV1("join_function_oid", value.JoinFunctionOID),
		hashFieldV1("commutator_operator_oid", value.CommutatorOperatorOID),
		hashFieldV1("negator_operator_oid", value.NegatorOperatorOID),
		hashFieldV1("left_type", value.LeftType), hashFieldV1("right_type", value.RightType),
		hashFieldV1("result_type", value.ResultType))
}

func snapshotHashCatalogExtensionIdentityV1(value CatalogExtensionIdentity) snapshotHashRecordV1 {
	configuration := make([]snapshotHashRecordV1, max(len(value.ConfigurationRelationOIDs), len(value.ConfigurationConditions)))
	for idx := range configuration {
		var relationOID uint32
		if idx < len(value.ConfigurationRelationOIDs) {
			relationOID = value.ConfigurationRelationOIDs[idx]
		}
		condition := ""
		if idx < len(value.ConfigurationConditions) {
			condition = value.ConfigurationConditions[idx]
		}
		configuration[idx] = hashRecordV1("configuration",
			hashFieldV1("relation_oid", relationOID),
			hashFieldV1("condition", condition))
	}
	return hashRecordV1("extension", hashFieldV1("oid", value.OID), hashFieldV1("name", value.Name),
		hashFieldV1("owner_oid", value.OwnerOID), hashFieldV1("owner_name", value.OwnerName),
		hashFieldV1("schema_oid", value.SchemaOID), hashFieldV1("schema_name", value.SchemaName),
		hashFieldV1("is_relocatable", value.IsRelocatable), hashFieldV1("version", value.Version),
		hashFieldV1("configuration", hashSortRecordsV1(configuration)))
}

func snapshotHashCatalogPublicationV1(value CatalogPublication) snapshotHashRecordV1 {
	return hashRecordV1("publication", hashFieldV1("oid", value.OID), hashFieldV1("name", value.Name),
		hashFieldV1("owner_oid", value.OwnerOID), hashFieldV1("owner_name", value.OwnerName),
		hashFieldV1("publishes_all_tables", value.PublishesAllTables),
		hashFieldV1("publishes_inserts", value.PublishesInserts),
		hashFieldV1("publishes_updates", value.PublishesUpdates),
		hashFieldV1("publishes_deletes", value.PublishesDeletes),
		hashFieldV1("publishes_truncates", value.PublishesTruncates),
		hashFieldV1("publishes_via_partition_root", value.PublishesViaPartitionRoot))
}

func snapshotHashCatalogPublicationRelationV1(value CatalogPublicationRelation) snapshotHashRecordV1 {
	columns := make([]snapshotHashRecordV1, max(len(value.ColumnNumbers), len(value.ColumnNames)))
	for idx := range columns {
		var number int16
		if idx < len(value.ColumnNumbers) {
			number = value.ColumnNumbers[idx]
		}
		name := ""
		if idx < len(value.ColumnNames) {
			name = value.ColumnNames[idx]
		}
		columns[idx] = hashRecordV1("publication_column", hashFieldV1("number", number), hashFieldV1("name", name))
	}
	return hashRecordV1("publication_relation", hashFieldV1("oid", value.OID),
		hashFieldV1("publication_oid", value.PublicationOID),
		hashFieldV1("publication_name", value.PublicationName),
		hashFieldV1("relation_oid", value.RelationOID),
		hashFieldV1("relation_schema_name", value.RelationSchemaName),
		hashFieldV1("relation_name", value.RelationName),
		hashFieldV1("columns", hashSortRecordsV1(columns)),
		hashFieldV1("row_filter", value.RowFilter))
}

func snapshotHashCatalogACLGrantV1(value CatalogACLGrant) snapshotHashRecordV1 {
	return hashRecordV1("acl_grant", hashFieldV1("object_class", string(value.ObjectClass)),
		hashFieldV1("object", snapshotHashCatalogDependencyObjectV1(value.Object)),
		hashFieldV1("owner_oid", value.OwnerOID), hashFieldV1("owner_name", value.OwnerName),
		hashFieldV1("grantor_oid", value.GrantorOID),
		hashFieldV1("grantor_name", value.GrantorName),
		hashFieldV1("grantee_oid", value.GranteeOID),
		hashFieldV1("grantee_name", value.GranteeName),
		hashFieldV1("grantee_is_public", value.GranteeIsPublic),
		hashFieldV1("privilege", value.Privilege),
		hashFieldV1("is_grantable", value.IsGrantable))
}

func snapshotHashCatalogDefaultACLV1(value CatalogDefaultACL) snapshotHashRecordV1 {
	return hashRecordV1("default_acl", hashFieldV1("oid", value.OID),
		hashFieldV1("owner_oid", value.OwnerOID),
		hashFieldV1("owner_name", value.OwnerName), hashFieldV1("schema_oid", value.SchemaOID),
		hashFieldV1("schema_name", value.SchemaName), hashFieldV1("is_global", value.IsGlobal),
		hashFieldV1("object_type", string(value.ObjectType)),
		hashFieldV1("grantor_oid", value.GrantorOID),
		hashFieldV1("grantor_name", value.GrantorName),
		hashFieldV1("grantee_oid", value.GranteeOID),
		hashFieldV1("grantee_name", value.GranteeName),
		hashFieldV1("grantee_is_public", value.GranteeIsPublic),
		hashFieldV1("privilege", value.Privilege), hashFieldV1("is_grantable", value.IsGrantable))
}

func snapshotHashCatalogRoleV1(value CatalogRole) snapshotHashRecordV1 {
	return hashRecordV1("role", hashFieldV1("oid", value.OID), hashFieldV1("name", value.Name),
		hashFieldV1("is_superuser", value.IsSuperuser),
		hashFieldV1("inherits_privileges", value.InheritsPrivileges),
		hashFieldV1("can_create_roles", value.CanCreateRoles),
		hashFieldV1("can_login", value.CanLogin),
		hashFieldV1("can_replicate", value.CanReplicate),
		hashFieldV1("bypasses_rls", value.BypassesRLS))
}

func snapshotHashCatalogRoleMembershipV1(value CatalogRoleMembership) snapshotHashRecordV1 {
	return hashRecordV1("role_membership", hashFieldV1("role_oid", value.RoleOID),
		hashFieldV1("role_name", value.RoleName), hashFieldV1("member_oid", value.MemberOID),
		hashFieldV1("member_name", value.MemberName), hashFieldV1("grantor_oid", value.GrantorOID),
		hashFieldV1("grantor_name", value.GrantorName),
		hashFieldV1("admin_option", value.AdminOption),
		hashFieldV1("inherit_option", value.InheritOption),
		hashFieldV1("set_option", value.SetOption))
}

func snapshotHashCatalogExtensionV1(value *CatalogExtension) any {
	if value == nil {
		return nil
	}
	return hashRecordV1("extension", hashFieldV1("oid", value.OID), hashFieldV1("name", value.Name))
}

func snapshotHashTrustedGroupsV1(
	inventory CatalogInventory,
	groups []SnapshotHashTrustedArchivalGroup,
) ([]snapshotHashRecordV1, error) {
	schemasByName := make(map[string]CatalogSchema, len(inventory.Schemas))
	for _, catalogSchema := range inventory.Schemas {
		if existing, duplicate := schemasByName[catalogSchema.Name]; duplicate {
			return nil, fmt.Errorf("building snapshot hash trust material: duplicate schema %q with OIDs %d and %d",
				catalogSchema.Name, existing.OID, catalogSchema.OID)
		}
		schemasByName[catalogSchema.Name] = catalogSchema
	}
	seenGroupIDs := make(map[string]struct{}, len(groups))
	seenSchemas := make(map[string]string)
	records := make([]snapshotHashRecordV1, 0, len(groups))
	for _, group := range groups {
		if group.GroupID == "" {
			return nil, fmt.Errorf("building snapshot hash trust material: trusted archival group ID is empty")
		}
		if _, duplicate := seenGroupIDs[group.GroupID]; duplicate {
			return nil, fmt.Errorf("building snapshot hash trust material: trusted archival group %q is duplicated", group.GroupID)
		}
		seenGroupIDs[group.GroupID] = struct{}{}
		names := hashSortedSliceV1(group.SchemaNames)
		if len(names) == 0 {
			return nil, fmt.Errorf("building snapshot hash trust material: trusted archival group %q has no schemas", group.GroupID)
		}
		for idx := 1; idx < len(names); idx++ {
			if names[idx] == names[idx-1] {
				return nil, fmt.Errorf("building snapshot hash trust material: trusted archival group %q repeats schema %q",
					group.GroupID, names[idx])
			}
		}
		schemaRecords := make([]snapshotHashRecordV1, 0, len(names))
		for _, name := range names {
			catalogSchema, exists := schemasByName[name]
			if !exists {
				return nil, fmt.Errorf("building snapshot hash trust material: trusted archival schema %q is absent from inventory", name)
			}
			if catalogSchema.Comment == "" {
				return nil, fmt.Errorf("building snapshot hash trust material: trusted archival schema %q has no marker comment", name)
			}
			if ownerGroupID, duplicate := seenSchemas[name]; duplicate {
				return nil, fmt.Errorf("building snapshot hash trust material: schema %q belongs to both groups %q and %q",
					name, ownerGroupID, group.GroupID)
			}
			seenSchemas[name] = group.GroupID
			schemaRecords = append(schemaRecords, hashRecordV1("trusted_archival_schema",
				hashFieldV1("oid", catalogSchema.OID),
				hashFieldV1("name", catalogSchema.Name),
				hashFieldV1("marker_comment", catalogSchema.Comment)))
		}
		records = append(records, hashRecordV1("trusted_archival_group",
			hashFieldV1("group_id", group.GroupID), hashFieldV1("schemas", schemaRecords)))
	}
	return hashSortRecordsV1(records), nil
}

func snapshotHashQualifiedNameV1(value SchemaQualifiedName) snapshotHashRecordV1 {
	return hashRecordV1("qualified_name", hashFieldV1("schema_name", value.SchemaName),
		hashFieldV1("escaped_name", value.EscapedName))
}

func snapshotHashStringMapV1(values map[string]string) []snapshotHashRecordV1 {
	records := make([]snapshotHashRecordV1, 0, len(values))
	for key, value := range values {
		records = append(records, hashRecordV1("option", hashFieldV1("key", key), hashFieldV1("value", value)))
	}
	return hashSortRecordsV1(records)
}

func hashSectionV1(name string, records []snapshotHashRecordV1) snapshotHashSectionV1 {
	return snapshotHashSectionV1{Name: name, Records: records}
}

func hashRecordV1(kind string, fields ...snapshotHashFieldV1) snapshotHashRecordV1 {
	if fields == nil {
		fields = []snapshotHashFieldV1{}
	}
	return snapshotHashRecordV1{Kind: kind, Fields: fields}
}

func hashFieldV1(name string, value any) snapshotHashFieldV1 {
	return snapshotHashFieldV1{Name: name, Value: value}
}

func hashRecordsV1[T any](values []T, build func(T) snapshotHashRecordV1) []snapshotHashRecordV1 {
	return hashSortRecordsV1(hashOrderedRecordsV1(values, build))
}

func hashOrderedRecordsV1[T any](values []T, build func(T) snapshotHashRecordV1) []snapshotHashRecordV1 {
	records := make([]snapshotHashRecordV1, len(values))
	for idx, value := range values {
		records[idx] = build(value)
	}
	return records
}

func hashSortRecordsV1(records []snapshotHashRecordV1) []snapshotHashRecordV1 {
	result := slices.Clone(records)
	if result == nil {
		result = []snapshotHashRecordV1{}
	}
	slices.SortFunc(result, func(a, b snapshotHashRecordV1) int {
		encodedA, _ := json.Marshal(a)
		encodedB, _ := json.Marshal(b)
		return bytes.Compare(encodedA, encodedB)
	})
	return result
}

func hashOrderedSliceV1[T any](values []T) []T {
	result := slices.Clone(values)
	if result == nil {
		result = []T{}
	}
	return result
}

func hashSortedSliceV1[T ~string | ~int16 | ~uint32](values []T) []T {
	result := hashOrderedSliceV1(values)
	slices.Sort(result)
	return result
}
