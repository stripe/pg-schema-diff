package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/internal/testdb"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

func TestSourceSafetyPreflightDatabaseDependentIntent(t *testing.T) {
	t.Parallel()

	factory := testdb.MustNewFactory(t)
	for _, tc := range []struct {
		name         string
		dependentDDL string
		include      []string
		targetDDL    string
		expectError  string
		expectedKind sourceSafetyIncomingDependencyKind
	}{
		{
			name: "managed view explicitly deleted",
			dependentDDL: `CREATE VIEW dependent.object AS
				SELECT id FROM managed.archived`,
			include:      []string{"managed", "dependent"},
			expectedKind: sourceSafetyIncomingDependencyKindView,
		},
		{
			name: "managed view persists against replacement table",
			dependentDDL: `CREATE VIEW dependent.object AS
				SELECT id FROM managed.archived`,
			include: []string{"managed", "dependent"},
			targetDDL: `
				CREATE SCHEMA managed;
				CREATE SCHEMA dependent;
				CREATE TABLE managed.archived (id integer PRIMARY KEY);
				CREATE VIEW dependent.object AS SELECT id FROM managed.archived;
			`,
			expectError: "persistent view",
		},
		{
			name: "excluded view persists",
			dependentDDL: `CREATE VIEW dependent.object AS
				SELECT id FROM managed.archived`,
			include:     []string{"managed"},
			expectError: "persistent view",
		},
		{
			name: "excluded materialized view persists",
			dependentDDL: `CREATE MATERIALIZED VIEW dependent.object AS
				SELECT id FROM managed.archived WITH NO DATA`,
			include:     []string{"managed"},
			expectError: "persistent materialized_view",
		},
		{
			name: "excluded rewrite rule persists",
			dependentDDL: `
				CREATE TABLE dependent.audit (id integer);
				CREATE TABLE dependent.rule_owner (id integer);
				CREATE RULE object AS ON INSERT TO dependent.rule_owner
					DO ALSO INSERT INTO dependent.audit
					SELECT id FROM managed.archived;
			`,
			include:     []string{"managed"},
			expectError: "persistent rule",
		},
		{
			name: "excluded row type table consumer persists",
			dependentDDL: `CREATE TABLE dependent.object (
				archived_row managed.archived
			)`,
			include:     []string{"managed"},
			expectError: "persistent row_type_consumer",
		},
		{
			name: "excluded composite type consumer persists",
			dependentDDL: `CREATE TYPE dependent.object AS (
				archived_row managed.archived
			)`,
			include:     []string{"managed"},
			expectError: "persistent row_type_consumer",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			currentDB := factory.CreateDatabase(t)
			_, err := currentDB.ConnPool.Exec(t.Context(), `
				CREATE SCHEMA managed;
				CREATE SCHEMA dependent;
				CREATE TABLE managed.archived (id integer PRIMARY KEY);
			`+tc.dependentDDL)
			require.NoError(t, err)
			targetDB := factory.CreateDatabase(t)
			if tc.targetDDL != "" {
				_, err = targetDB.ConnPool.Exec(t.Context(), tc.targetDDL)
				require.NoError(t, err)
			}

			current := sourceSafetyDatabaseSnapshot(t, currentDB, tc.include...)
			target := sourceSafetyDatabaseSnapshot(t, targetDB, tc.include...)
			relationOID := sourceSafetyRelationOID(t, current.Inventory, "managed", "archived")
			result, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
				CurrentSnapshot: current, TargetSnapshot: target,
				ProposedTableRelationOIDs: []uint32{relationOID},
			})
			if tc.expectError != "" {
				require.ErrorContains(t, err, tc.expectError)
				assert.ErrorContains(t, err, "managed.archived")
				return
			}
			require.NoError(t, err)
			require.Len(t, result.IncomingDependencies, 1)
			assert.Equal(t, tc.expectedKind, result.IncomingDependencies[0].Dependent.Kind)
			assert.Equal(t, sourceSafetyTargetIntentExplicitlyAbsent,
				result.IncomingDependencies[0].TargetIntent)
		})
	}
}

func TestSourceSafetyPreflightDatabaseRoutineTrackability(t *testing.T) {
	t.Parallel()

	factory := testdb.MustNewFactory(t)
	for _, tc := range []struct {
		name        string
		routineDDL  string
		expectError string
	}{
		{
			name: "catalog backed BEGIN ATOMIC",
			routineDDL: `
				CREATE FUNCTION dependent.object() RETURNS integer
				LANGUAGE SQL BEGIN ATOMIC
					SELECT id FROM managed.archived ORDER BY id LIMIT 1;
				END;
			`,
			expectError: "persistent routine",
		},
		{
			name: "SQL string body",
			routineDDL: `
				CREATE FUNCTION dependent.object() RETURNS integer LANGUAGE SQL
				AS 'SELECT id FROM managed.archived ORDER BY id LIMIT 1';
			`,
			expectError: "persistent routine",
		},
		{
			name: "PL/pgSQL body",
			routineDDL: `
				CREATE FUNCTION dependent.object() RETURNS integer LANGUAGE plpgsql AS $body$
				BEGIN
					RETURN (SELECT id FROM managed.archived ORDER BY id LIMIT 1);
				END
				$body$;
			`,
			expectError: "persistent routine",
		},
		{
			name: "dynamic SQL body",
			routineDDL: `
				CREATE FUNCTION dependent.object() RETURNS integer LANGUAGE plpgsql AS $body$
				DECLARE result integer;
				BEGIN
					EXECUTE 'SELECT id FROM managed.archived ORDER BY id LIMIT 1' INTO result;
					RETURN result;
				END
				$body$;
			`,
			expectError: "persistent routine",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			currentDB := factory.CreateDatabase(t)
			_, err := currentDB.ConnPool.Exec(t.Context(), `
				CREATE SCHEMA managed;
				CREATE SCHEMA dependent;
				CREATE TABLE managed.archived (id integer PRIMARY KEY);
			`+tc.routineDDL)
			require.NoError(t, err)
			targetDB := factory.CreateDatabase(t)

			current := sourceSafetyDatabaseSnapshot(t, currentDB, "managed")
			target := sourceSafetyDatabaseSnapshot(t, targetDB, "managed")
			relationOID := sourceSafetyRelationOID(t, current.Inventory, "managed", "archived")
			_, err = runSourceSafetyPreflight(sourceSafetyPreflightRequest{
				CurrentSnapshot: current, TargetSnapshot: target,
				ProposedTableRelationOIDs: []uint32{relationOID},
			})
			require.ErrorContains(t, err, tc.expectError)
			assert.ErrorContains(t, err, "dependent.object()")
		})
	}
}

func TestSourceSafetyPreflightDatabaseForeignKeys(t *testing.T) {
	t.Parallel()

	factory := testdb.MustNewFactory(t)
	currentDB := factory.CreateDatabase(t)
	_, err := currentDB.ConnPool.Exec(t.Context(), `
		CREATE SCHEMA managed;
		CREATE TABLE managed.referenced (id integer PRIMARY KEY);
		CREATE TABLE managed.archived (
			id integer PRIMARY KEY,
			parent_id integer CONSTRAINT self_fk REFERENCES managed.archived,
			referenced_id integer CONSTRAINT outgoing_fk REFERENCES managed.referenced
		);
		CREATE TABLE managed.owner (
			id integer PRIMARY KEY,
			archived_id integer CONSTRAINT incoming_fk REFERENCES managed.archived
		);
	`)
	require.NoError(t, err)
	targetDB := factory.CreateDatabase(t)
	current := sourceSafetyDatabaseSnapshot(t, currentDB, "managed")
	target := sourceSafetyDatabaseSnapshot(t, targetDB, "managed")
	relationOID := sourceSafetyRelationOID(t, current.Inventory, "managed", "archived")

	result, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
		CurrentSnapshot: current, TargetSnapshot: target,
		ProposedTableRelationOIDs: []uint32{relationOID},
	})
	require.NoError(t, err)
	require.Len(t, result.ForeignKeys, 3)
	directionsByName := make(map[string]sourceSafetyForeignKeyDirection)
	for _, foreignKey := range result.ForeignKeys {
		directionsByName[foreignKey.ForeignKey.Name] = foreignKey.Direction
	}
	assert.Equal(t, sourceSafetyForeignKeyDirectionIncoming, directionsByName["incoming_fk"])
	assert.Equal(t, sourceSafetyForeignKeyDirectionOutgoing, directionsByName["outgoing_fk"])
	assert.Equal(t, sourceSafetyForeignKeyDirectionSelf, directionsByName["self_fk"])
}

func TestSourceSafetyPreflightDatabaseEventTriggers(t *testing.T) {
	t.Parallel()

	factory := testdb.MustNewFactory(t)
	for _, tc := range []struct {
		name        string
		alter       string
		expectError bool
	}{
		{name: "origin enabled", expectError: true},
		{name: "replica enabled", alter: "ENABLE REPLICA", expectError: true},
		{name: "always enabled", alter: "ENABLE ALWAYS", expectError: true},
		{name: "disabled", alter: "DISABLE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			currentDB := factory.CreateDatabase(t)
			_, err := currentDB.ConnPool.Exec(t.Context(), `
				CREATE SCHEMA managed;
				CREATE TABLE managed.archived (id integer);
				CREATE FUNCTION public.stage10_event_trigger() RETURNS event_trigger
					LANGUAGE plpgsql AS $body$ BEGIN END $body$;
				CREATE EVENT TRIGGER stage10_event_trigger ON ddl_command_end
					EXECUTE FUNCTION public.stage10_event_trigger();
			`)
			require.NoError(t, err)
			if tc.alter != "" {
				_, err = currentDB.ConnPool.Exec(t.Context(),
					"ALTER EVENT TRIGGER stage10_event_trigger "+tc.alter)
				require.NoError(t, err)
			}
			targetDB := factory.CreateDatabase(t)
			current := sourceSafetyDatabaseSnapshot(t, currentDB, "managed", "public")
			target := sourceSafetyDatabaseSnapshot(t, targetDB, "managed", "public")
			relationOID := sourceSafetyRelationOID(t, current.Inventory, "managed", "archived")

			_, err = runSourceSafetyPreflight(sourceSafetyPreflightRequest{
				CurrentSnapshot: current, TargetSnapshot: target,
				ProposedTableRelationOIDs: []uint32{relationOID},
			})
			if tc.expectError {
				require.ErrorContains(t, err, "event trigger \"stage10_event_trigger\" is enabled")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSourceSafetyPreflightDatabaseExtensionAndPublications(t *testing.T) {
	t.Parallel()

	factory := testdb.MustNewFactory(t)
	t.Run("extension member table", func(t *testing.T) {
		t.Parallel()
		currentDB := factory.CreateDatabase(t)
		_, err := currentDB.ConnPool.Exec(t.Context(), `
			CREATE EXTENSION pg_trgm;
			CREATE SCHEMA managed;
			CREATE TABLE managed.archived (id integer);
			ALTER EXTENSION pg_trgm ADD TABLE managed.archived;
		`)
		require.NoError(t, err)
		targetDB := factory.CreateDatabase(t)
		_, err = targetDB.ConnPool.Exec(t.Context(), "CREATE EXTENSION pg_trgm")
		require.NoError(t, err)
		current := sourceSafetyDatabaseSnapshot(t, currentDB, "managed")
		target := sourceSafetyDatabaseSnapshot(t, targetDB, "managed")
		relationOID := sourceSafetyRelationOID(t, current.Inventory, "managed", "archived")

		_, err = runSourceSafetyPreflight(sourceSafetyPreflightRequest{
			CurrentSnapshot: current, TargetSnapshot: target,
			ProposedTableRelationOIDs: []uint32{relationOID},
		})
		require.ErrorContains(t, err, "extension member table managed.archived")
	})

	t.Run("extension change", func(t *testing.T) {
		t.Parallel()
		currentDB := factory.CreateDatabase(t)
		_, err := currentDB.ConnPool.Exec(t.Context(), `
			CREATE EXTENSION pg_trgm;
			CREATE SCHEMA managed;
			CREATE TABLE managed.archived (id integer);
		`)
		require.NoError(t, err)
		targetDB := factory.CreateDatabase(t)
		current := sourceSafetyDatabaseSnapshot(t, currentDB, "managed")
		target := sourceSafetyDatabaseSnapshot(t, targetDB, "managed")
		relationOID := sourceSafetyRelationOID(t, current.Inventory, "managed", "archived")

		_, err = runSourceSafetyPreflight(sourceSafetyPreflightRequest{
			CurrentSnapshot: current, TargetSnapshot: target,
			ProposedTableRelationOIDs: []uint32{relationOID},
		})
		require.ErrorContains(t, err, "extension \"pg_trgm\" is dropped")
	})

	t.Run("explicit and schema publications", func(t *testing.T) {
		t.Parallel()
		currentDB := factory.CreateDatabase(t)
		_, err := currentDB.ConnPool.Exec(t.Context(), `
			CREATE SCHEMA managed;
			CREATE TABLE managed.archived (id integer, payload text);
			CREATE PUBLICATION explicit_publication
				FOR TABLE managed.archived (id) WHERE (id > 0);
			CREATE PUBLICATION schema_publication FOR TABLES IN SCHEMA managed;
		`)
		require.NoError(t, err)
		targetDB := factory.CreateDatabase(t)
		current := sourceSafetyDatabaseSnapshot(t, currentDB, "managed")
		target := sourceSafetyDatabaseSnapshot(t, targetDB, "managed")
		relationOID := sourceSafetyRelationOID(t, current.Inventory, "managed", "archived")

		result, err := runSourceSafetyPreflight(sourceSafetyPreflightRequest{
			CurrentSnapshot: current, TargetSnapshot: target,
			ProposedTableRelationOIDs: []uint32{relationOID},
		})
		require.NoError(t, err)
		require.Len(t, result.PublicationRelations, 1)
		assert.Equal(t, "explicit_publication", result.PublicationRelations[0].PublicationName)
		require.Len(t, result.PublicationSchemas, 1)
		assert.Equal(t, "schema_publication", result.PublicationSchemas[0].PublicationName)
	})

	t.Run("all tables publication", func(t *testing.T) {
		t.Parallel()
		currentDB := factory.CreateDatabase(t)
		_, err := currentDB.ConnPool.Exec(t.Context(), `
			CREATE SCHEMA managed;
			CREATE TABLE managed.archived (id integer);
			CREATE PUBLICATION all_tables_publication FOR ALL TABLES;
		`)
		require.NoError(t, err)
		targetDB := factory.CreateDatabase(t)
		current := sourceSafetyDatabaseSnapshot(t, currentDB, "managed")
		target := sourceSafetyDatabaseSnapshot(t, targetDB, "managed")
		relationOID := sourceSafetyRelationOID(t, current.Inventory, "managed", "archived")

		_, err = runSourceSafetyPreflight(sourceSafetyPreflightRequest{
			CurrentSnapshot: current, TargetSnapshot: target,
			ProposedTableRelationOIDs: []uint32{relationOID},
		})
		require.ErrorContains(t, err, "FOR ALL TABLES publication \"all_tables_publication\"")
	})
}

func sourceSafetyDatabaseSnapshot(
	t *testing.T,
	db *tempdb.Database,
	includeSchemas ...string,
) schema.SchemaSnapshot {
	t.Helper()
	options := make([]schema.GetSchemaOpt, 0, 1)
	if len(includeSchemas) > 0 {
		options = append(options, schema.WithIncludeSchemaPatterns(includeSchemas...))
	}
	snapshot, err := schema.GetSchemaSnapshot(t.Context(), db.ConnPool, options...)
	require.NoError(t, err)
	return snapshot
}

func sourceSafetyRelationOID(
	t *testing.T,
	inventory schema.CatalogInventory,
	schemaName string,
	relationName string,
) uint32 {
	t.Helper()
	for _, relation := range inventory.Relations {
		if relation.SchemaName == schemaName && relation.Name == relationName {
			return relation.OID
		}
	}
	require.FailNow(t, "relation not found", "%s.%s", schemaName, relationName)
	return 0
}
