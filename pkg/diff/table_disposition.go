package diff

import (
	"fmt"
	"slices"

	"github.com/stripe/pg-schema-diff/internal/schema"
)

type tableDispositionKind uint8

const (
	tableDispositionKindUnknown tableDispositionKind = iota
	tableDispositionKindPhysicalDelete
	tableDispositionKindArchivalMove
	tableDispositionKindCleanupOnly
)

type tableDisposition struct {
	Kind    tableDispositionKind
	GroupID archivalGroupID
}

type tableDispositions map[string]tableDisposition

func physicalTableDispositions(tables []schema.Table) tableDispositions {
	dispositions := make(tableDispositions, len(tables))
	for _, table := range tables {
		dispositions[table.GetName()] = tableDisposition{
			Kind: tableDispositionKindPhysicalDelete,
		}
	}
	return dispositions
}

func resolveTableDispositions(
	deletedTables []schema.Table,
	configured tableDispositions,
	archivalGroups []preparedArchivalGroup,
) (tableDispositions, error) {
	if configured == nil {
		if len(archivalGroups) != 0 {
			return nil, fmt.Errorf("table dispositions are required when integrating archival groups")
		}
		return physicalTableDispositions(deletedTables), nil
	}

	deletedByName := buildSchemaObjByNameMap(deletedTables)
	groupsByTableName := make(map[string]preparedArchivalGroup)
	membersByTableName := make(map[string]preparedArchivalMember)
	for _, group := range archivalGroups {
		for _, member := range group.members {
			tableName := markerTableName(member.marker.SourceTable).GetName()
			if _, duplicate := groupsByTableName[tableName]; duplicate {
				return nil, fmt.Errorf("multiple archival groups target table %s", tableName)
			}
			groupsByTableName[tableName] = group
			membersByTableName[tableName] = member
		}
	}

	for tableName := range deletedByName {
		if _, ok := configured[tableName]; !ok {
			return nil, fmt.Errorf("missing disposition for deleted table %s", tableName)
		}
	}

	configuredNames := make([]string, 0, len(configured))
	for tableName := range configured {
		configuredNames = append(configuredNames, tableName)
	}
	slices.Sort(configuredNames)
	for _, tableName := range configuredNames {
		disposition := configured[tableName]
		_, deleted := deletedByName[tableName]
		group, hasGroup := groupsByTableName[tableName]
		member := membersByTableName[tableName]
		switch disposition.Kind {
		case tableDispositionKindPhysicalDelete:
			if disposition.GroupID != "" {
				return nil, fmt.Errorf("physical-delete disposition for table %s must not name archival group %q",
					tableName, disposition.GroupID)
			}
			if !deleted {
				return nil, fmt.Errorf("physical-delete disposition references table %s without a delete diff", tableName)
			}
			if hasGroup {
				return nil, fmt.Errorf("physical-delete disposition for table %s conflicts with archival group %q",
					tableName, group.id)
			}
		case tableDispositionKindArchivalMove:
			if !deleted {
				return nil, fmt.Errorf("archival-move disposition references table %s without a delete diff", tableName)
			}
			if err := validateDispositionGroup(tableName, disposition, group, hasGroup); err != nil {
				return nil, err
			}
			if member.remainingMove == nil {
				return nil, fmt.Errorf("archival-move disposition for table %s has no remaining table move", tableName)
			}
		case tableDispositionKindCleanupOnly:
			if err := validateDispositionGroup(tableName, disposition, group, hasGroup); err != nil {
				return nil, err
			}
			if member.remainingMove != nil {
				return nil, fmt.Errorf("cleanup-only disposition for table %s still requires an ordinary table move", tableName)
			}
		default:
			return nil, fmt.Errorf("table %s has unknown disposition %d", tableName, disposition.Kind)
		}
	}

	for tableName, group := range groupsByTableName {
		disposition, ok := configured[tableName]
		if !ok {
			return nil, fmt.Errorf("archival group %q for table %s has no disposition", group.id, tableName)
		}
		if disposition.Kind != tableDispositionKindArchivalMove &&
			disposition.Kind != tableDispositionKindCleanupOnly {
			return nil, fmt.Errorf("archival group %q for table %s has non-archival disposition %d",
				group.id, tableName, disposition.Kind)
		}
	}
	return configured, nil
}

func validateDispositionGroup(
	tableName string,
	disposition tableDisposition,
	group preparedArchivalGroup,
	hasGroup bool,
) error {
	if disposition.GroupID == "" {
		return fmt.Errorf("archival disposition for table %s must name an archival group", tableName)
	}
	if !hasGroup {
		return fmt.Errorf("archival disposition for table %s references missing group %q",
			tableName, disposition.GroupID)
	}
	if disposition.GroupID != group.id {
		return fmt.Errorf("archival disposition for table %s references group %q instead of %q",
			tableName, disposition.GroupID, group.id)
	}
	return nil
}

func markerTableName(identity archivalMarkerObjectIdentity) schema.SchemaQualifiedName {
	return schema.SchemaQualifiedName{
		SchemaName:  identity.SchemaName,
		EscapedName: schema.EscapeIdentifier(identity.Name),
	}
}

func tableIsLogicallyRemoved(dispositions tableDispositions, tableName string) bool {
	_, ok := dispositions[tableName]
	return ok
}

func tableIsPreserved(dispositions tableDispositions, tableName string) bool {
	disposition, ok := dispositions[tableName]
	return ok && (disposition.Kind == tableDispositionKindArchivalMove ||
		disposition.Kind == tableDispositionKindCleanupOnly)
}
