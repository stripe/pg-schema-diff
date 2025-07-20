package migration_acceptance_tests

import "github.com/stripe/pg-schema-diff/pkg/diff"

var triggerAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "No-op",
		oldSchemaDDL: []string{
			`
           CREATE TABLE foo (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE FUNCTION increment_version() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER some_update_trigger
               BEFORE UPDATE ON foo
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE increment_version();
    
           CREATE FUNCTION check_content() RETURNS TRIGGER AS $$
               BEGIN
                   IF LENGTH(NEW.content) == 0 THEN
                       RAISE EXCEPTION 'content is empty';
                   END IF;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER some_check_trigger
               BEFORE UPDATE ON foo
               FOR EACH ROW
               EXECUTE FUNCTION check_content();
			`,
		},
		newSchemaDDL: []string{
			`
           CREATE TABLE foo (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE FUNCTION increment_version() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER some_update_trigger
               BEFORE UPDATE ON foo
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE increment_version();
    
           CREATE FUNCTION check_content() RETURNS TRIGGER AS $$
               BEGIN
                   IF LENGTH(NEW.content) == 0 THEN
                       RAISE EXCEPTION 'content is empty';
                   END IF;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER some_check_trigger
               BEFORE UPDATE ON foo
               FOR EACH ROW
               EXECUTE FUNCTION check_content();
			`,
		},
		expectEmptyPlan: true,
	},
	{
		name: "Create trigger with quoted name",
		oldSchemaDDL: []string{
			`
           CREATE SCHEMA schema_1;
           CREATE TABLE schema_1."some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
			`,
		},
		newSchemaDDL: []string{
			`
           CREATE SCHEMA schema_1;
           CREATE TABLE schema_1."some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE SCHEMA schema_2;
           CREATE FUNCTION schema_2."increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON schema_1."some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE schema_2."increment version"();
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Create triggers with quoted names on partitioned table and partition",
		oldSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           ) PARTITION BY LIST (content);
    
           CREATE TABLE "foobar 1" PARTITION OF "some foo" FOR VALUES IN ('foo_2');
    
			`,
		},
		newSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           ) PARTITION BY LIST (content);
    
           CREATE TABLE "foobar 1" PARTITION OF "some foo" FOR VALUES IN ('foo_2');
    
           CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
    
           CREATE TRIGGER "some partition trigger"
               BEFORE UPDATE ON "foobar 1"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Create two triggers depending on the same function",
		oldSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
			`,
		},
		newSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
    
           CREATE TRIGGER "some other trigger"
               BEFORE UPDATE ON "some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Create two triggers with the same name",
		oldSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE TABLE "some other foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
			`,
		},
		newSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE TABLE "some other foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some other foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Drop trigger with quoted names",
		oldSchemaDDL: []string{
			`
           CREATE SCHEMA schema_1;
           CREATE TABLE schema_1."some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE SCHEMA schema_2;
           CREATE FUNCTION schema_2."increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON schema_1."some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE schema_2."increment version"();
			`,
		},
		newSchemaDDL: []string{
			`
           CREATE SCHEMA schema_1;
           CREATE TABLE schema_1."some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE SCHEMA schema_2;
           CREATE FUNCTION schema_2."increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
			`,
		},
	},
	{
		name: "Drop triggers with quoted names on partitioned table and partition",
		oldSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           ) PARTITION BY LIST (content);
    
           CREATE TABLE "foobar 1" PARTITION OF "some foo" FOR VALUES IN ('foo_2');
    
           CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
    
           CREATE TRIGGER "some partition trigger"
               BEFORE UPDATE ON "foobar 1"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
			`,
		},
		newSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           ) PARTITION BY LIST (content);
    
           CREATE TABLE "foobar 1" PARTITION OF "some foo" FOR VALUES IN ('foo_2');
    
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Drop two triggers with the same name",
		oldSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE TABLE "some other foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some other foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
			`,
		},
		newSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE TABLE "some other foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Add and drop trigger - conflicting schemas",
		oldSchemaDDL: []string{
			`
           CREATE SCHEMA schema_1;
           CREATE TABLE schema_1."some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE SCHEMA schema_2;
           CREATE FUNCTION schema_2."increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON schema_1."some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE schema_2."increment version"();
    
           CREATE SCHEMA schema_3;
           CREATE TABLE schema_3."some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE SCHEMA schema_4;
           CREATE FUNCTION schema_4."increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
			`,
		},
		newSchemaDDL: []string{
			`
           CREATE SCHEMA schema_1;
           CREATE TABLE schema_1."some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE SCHEMA schema_2;
           CREATE FUNCTION schema_2."increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE SCHEMA schema_3;
           CREATE TABLE schema_3."some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE SCHEMA schema_4;
           CREATE FUNCTION schema_4."increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON schema_3."some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE schema_4."increment version"();
			`,
		},
	},
	{
		name: "Alter trigger when clause",
		oldSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
			`,
		},
		newSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some foo"
               FOR EACH ROW
               WHEN (NEW.author != 'fizz')
               EXECUTE PROCEDURE "increment version"();
			`,
		},
	},
	{
		name: "Alter trigger table",
		oldSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
           CREATE TABLE "some other foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
			`,
		},
		newSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
           CREATE TABLE "some other foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some other foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
			`,
		},
	},
	{
		name: "Change trigger function and keep old function",
		oldSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
		`},
		newSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
    
           CREATE FUNCTION "decrement version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "decrement version"();
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Change trigger function and drop old function",
		oldSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
		`},
		newSchemaDDL: []string{
			`
           CREATE TABLE "some foo" (
               id INTEGER PRIMARY KEY,
               author TEXT,
               content TEXT NOT NULL DEFAULT '',
               version INT NOT NULL DEFAULT 0
           );
    
           CREATE FUNCTION "decrement version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some foo"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "decrement version"();
		`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeHasUntrackableDependencies},
	},
	{
		name: "Trigger on re-created table is re-created",
		oldSchemaDDL: []string{
			`
           CREATE TABLE "some foobar" (
               id INTEGER,
               version INT NOT NULL DEFAULT 0,
               author TEXT,
               content TEXT NOT NULL DEFAULT ''
           ) PARTITION BY LIST (content);
    
           CREATE TABLE "foobar 1" PARTITION OF "some foobar" FOR VALUES IN ('foo_2');
    
           CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some foobar"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
    
           CREATE TRIGGER "some partition trigger"
               BEFORE UPDATE ON "foobar 1"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
			`,
		},
		newSchemaDDL: []string{
			`
           CREATE TABLE "some other foobar" (
               id INTEGER,
               version INT NOT NULL DEFAULT 0,
               author TEXT,
               content TEXT NOT NULL DEFAULT ''
           ) PARTITION BY LIST (content);
    
           CREATE TABLE "foobar 1" PARTITION OF "some other foobar" FOR VALUES IN ('foo_2');
    
           CREATE FUNCTION "increment version"() RETURNS TRIGGER AS $$
               BEGIN
                   NEW.version = OLD.version + 1;
                   RETURN NEW;
               END;
           $$ language 'plpgsql';
    
           CREATE TRIGGER "some trigger"
               BEFORE UPDATE ON "some other foobar"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
    
           CREATE TRIGGER "some partition trigger"
               BEFORE UPDATE ON "foobar 1"
               FOR EACH ROW
               WHEN (OLD.* IS DISTINCT FROM NEW.*)
               EXECUTE PROCEDURE "increment version"();
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{
			diff.MigrationHazardTypeDeletesData,
		},
	},
	{
		name: "alter - non-constraint trigger to constraint trigger",
		oldSchemaDDL: []string{
			`
             CREATE TABLE some_foo (
                id INTEGER PRIMARY KEY,
                version INT NOT NULL DEFAULT 0
            );

            CREATE FUNCTION check_version() RETURNS TRIGGER AS $$
                BEGIN
                    IF NEW.VERSION <= OLD.VERSION THEN
                        RAISE EXCEPTION 'New version must be greater';
                    END IF;
                    RETURN NEW;
                END;
            $$ language 'plpgsql';

            CREATE TRIGGER some_trigger
                BEFORE UPDATE ON some_foo
                FOR EACH ROW
                WHEN (OLD.* IS DISTINCT FROM NEW.*)
                EXECUTE PROCEDURE check_version();
			`,
		},
		newSchemaDDL: []string{
			`
             CREATE TABLE some_foo (
                id INTEGER PRIMARY KEY,
                version INT NOT NULL DEFAULT 0
            );

            CREATE FUNCTION check_version() RETURNS TRIGGER AS $$
                BEGIN
                    IF NEW.VERSION <= OLD.VERSION THEN
                        RAISE EXCEPTION 'New version must be greater';
                    END IF;
                    RETURN NEW;
                END;
            $$ language 'plpgsql';

            CREATE CONSTRAINT TRIGGER some_trigger
                AFTER UPDATE ON some_foo
                DEFERRABLE INITIALLY DEFERRED
                FOR EACH ROW
                WHEN (OLD.* IS DISTINCT FROM NEW.*)
                EXECUTE PROCEDURE check_version()
			`,
		},
	},
	{
		name: "alter - constraint trigger",
		oldSchemaDDL: []string{
			`
             CREATE TABLE some_foo (
                id INTEGER PRIMARY KEY,
                version INT NOT NULL DEFAULT 0
            );

            CREATE FUNCTION check_version() RETURNS TRIGGER AS $$
                BEGIN
                    IF NEW.VERSION <= OLD.VERSION THEN
                        RAISE EXCEPTION 'New version must be greater';
                    END IF;
                    RETURN NEW;
                END;
            $$ language 'plpgsql';

            CREATE CONSTRAINT TRIGGER some_trigger
                AFTER UPDATE ON some_foo
                DEFERRABLE INITIALLY DEFERRED
                FOR EACH ROW
                -- Remove when condition.
                EXECUTE PROCEDURE check_version()
			`,
		},
		newSchemaDDL: []string{
			`
             CREATE TABLE some_foo (
                id INTEGER PRIMARY KEY,
                version INT NOT NULL DEFAULT 0
            );

            CREATE FUNCTION check_version() RETURNS TRIGGER AS $$
                BEGIN
                    IF NEW.VERSION <= OLD.VERSION THEN
                        RAISE EXCEPTION 'New version must be greater';
                    END IF;
                    RETURN NEW;
                END;
            $$ language 'plpgsql';

            CREATE CONSTRAINT TRIGGER some_trigger
                AFTER UPDATE ON some_foo
                DEFERRABLE INITIALLY DEFERRED
                FOR EACH ROW
                WHEN (OLD.* IS DISTINCT FROM NEW.*)
                EXECUTE PROCEDURE check_version()
			`,
		},
	},
	{
		name: "alter - constraint trigger to non-constraint trigger",
		oldSchemaDDL: []string{
			`
             CREATE TABLE some_foo (
                id INTEGER PRIMARY KEY,
                version INT NOT NULL DEFAULT 0
            );

            CREATE FUNCTION check_version() RETURNS TRIGGER AS $$
                BEGIN
                    IF NEW.VERSION <= OLD.VERSION THEN
                        RAISE EXCEPTION 'New version must be greater';
                    END IF;
                    RETURN NEW;
                END;
            $$ language 'plpgsql';

            CREATE CONSTRAINT TRIGGER some_trigger
                AFTER UPDATE ON some_foo
                DEFERRABLE INITIALLY DEFERRED
                FOR EACH ROW
                WHEN (OLD.* IS DISTINCT FROM NEW.*)
                EXECUTE PROCEDURE check_version()
			`,
		},
		newSchemaDDL: []string{
			`
             CREATE TABLE some_foo (
                id INTEGER PRIMARY KEY,
                version INT NOT NULL DEFAULT 0
            );

            CREATE FUNCTION check_version() RETURNS TRIGGER AS $$
                BEGIN
                    IF NEW.VERSION <= OLD.VERSION THEN
                        RAISE EXCEPTION 'New version must be greater';
                    END IF;
                    RETURN NEW;
                END;
            $$ language 'plpgsql';

            CREATE TRIGGER some_trigger
                BEFORE UPDATE ON some_foo
                FOR EACH ROW
                WHEN (OLD.* IS DISTINCT FROM NEW.*)
                EXECUTE PROCEDURE check_version();
			`,
		},
	},
}

func (suite *acceptanceTestSuite) TestTriggerTestCases() {
	suite.runTestCases(triggerAcceptanceTestCases)
}
