-- postgres=# \d pg_catalog.pg_namespace;
-- Table "pg_catalog.pg_namespace"
-- Column  |   Type    | Collation | Nullable | Default
-- ----------+-----------+-----------+----------+---------
--  oid      | oid       |           | not null |
--  nspname  | name      |           | not null |
--  nspowner | oid       |           | not null |
--  nspacl   | aclitem[] |           |          |

CREATE TABLE pg_catalog.pg_namespace
(
  oid     OID  NOT NULL,
  nspname TEXT NOT NULL
);


-- postgres=# \d pg_catalog.pg_class;
-- Table "pg_catalog.pg_class"
-- Column        |     Type     | Collation | Nullable | Default
-- ---------------------+--------------+-----------+----------+---------
--  oid                 | oid          |           | not null |
--  relname             | name         |           | not null |
--  relnamespace        | oid          |           | not null |
--  reltype             | oid          |           | not null |
--  reloftype           | oid          |           | not null |
--  relowner            | oid          |           | not null |
--  relam               | oid          |           | not null |
--  relfilenode         | oid          |           | not null |
--  reltablespace       | oid          |           | not null |
--  relpages            | integer      |           | not null |
--  reltuples           | real         |           | not null |
--  relallvisible       | integer      |           | not null |
--  reltoastrelid       | oid          |           | not null |
--  relhasindex         | boolean      |           | not null |
--  relisshared         | boolean      |           | not null |
--  relpersistence      | "char"       |           | not null |
--  relkind             | "char"       |           | not null |
--  relnatts            | smallint     |           | not null |
--  relchecks           | smallint     |           | not null |
--  relhasrules         | boolean      |           | not null |
--  relhastriggers      | boolean      |           | not null |
--  relhassubclass      | boolean      |           | not null |
--  relrowsecurity      | boolean      |           | not null |
--  relforcerowsecurity | boolean      |           | not null |
--  relispopulated      | boolean      |           | not null |
--  relreplident        | "char"       |           | not null |
--  relispartition      | boolean      |           | not null |
--  relrewrite          | oid          |           | not null |
--  relfrozenxid        | xid          |           | not null |
--  relminmxid          | xid          |           | not null |
--  relacl              | aclitem[]    |           |          |
--  reloptions          | text[]       | C         |          |
--  relpartbound        | pg_node_tree | C         |          |
-- Indexes:
--     "pg_class_oid_index" PRIMARY KEY, btree (oid)
--     "pg_class_relname_nsp_index" UNIQUE CONSTRAINT, btree (relname, relnamespace)
--     "pg_class_tblspc_relfilenode_index" btree (reltablespace, relfilenode)

CREATE TABLE pg_catalog.pg_class
(
  oid          OID  NOT NULL,
  relname      TEXT NOT NULL,
  relnamespace INT  NOT NULL,
  reltype      INT  NOT NULL
);

-- postgres=# \d pg_catalog.pg_inherits
-- Column      |  Type   | Collation | Nullable | Default
-- ------------------+---------+-----------+----------+---------
--  inhrelid         | oid     |           | not null |
--  inhparent        | oid     |           | not null |
--  inhseqno         | integer |           | not null |
--  inhdetachpending | boolean |           | not null |
-- Indexes:
--     "pg_inherits_relid_seqno_index" PRIMARY KEY, btree (inhrelid, inhseqno)
--     "pg_inherits_parent_index" btree (inhparent)
--
CREATE TABLE pg_catalog.pg_inherits
(
  inhrelid  OID NOT NULL,
  inhparent OID NOT NULL
);

-- postgres=# \d pg_catalog.pg_attribute;
-- Table "pg_catalog.pg_attribute"
-- Column     |   Type    | Collation | Nullable | Default
-- ----------------+-----------+-----------+----------+---------
--  attrelid       | oid       |           | not null |
--  attname        | name      |           | not null |
--  atttypid       | oid       |           | not null |
--  attstattarget  | integer   |           | not null |
--  attlen         | smallint  |           | not null |
--  attnum         | smallint  |           | not null |
--  attndims       | integer   |           | not null |
--  attcacheoff    | integer   |           | not null |
--  atttypmod      | integer   |           | not null |
--  attbyval       | boolean   |           | not null |
--  attalign       | "char"    |           | not null |
--  attstorage     | "char"    |           | not null |
--  attcompression | "char"    |           | not null |
--  attnotnull     | boolean   |           | not null |
--  atthasdef      | boolean   |           | not null |
--  atthasmissing  | boolean   |           | not null |
--  attidentity    | "char"    |           | not null |
--  attgenerated   | "char"    |           | not null |
--  attisdropped   | boolean   |           | not null |
--  attislocal     | boolean   |           | not null |
--  attinhcount    | integer   |           | not null |
--  attcollation   | oid       |           | not null |
--  attacl         | aclitem[] |           |          |
--  attoptions     | text[]    | C         |          |
--  attfdwoptions  | text[]    | C         |          |
--  attmissingval  | anyarray  |           |          |
-- Indexes:
--     "pg_attribute_relid_attnum_index" PRIMARY KEY, btree (attrelid, attnum)
--     "pg_attribute_relid_attnam_index" UNIQUE CONSTRAINT, btree (attrelid, attname)

CREATE TABLE pg_catalog.pg_attribute
(
  attrelid     OID      NOT NULL,
  attname      TEXT     NOT NULL,
  attlen       SMALLINT NOT NULL,
  attnum       SMALLINT NOT NULL,
  atttypid     INTEGER  NOT NULL,
  atttypmod    INTEGER  NOT NULL,
  attnotnull   BOOLEAN  NOT NULL,
  attcollation OID      NOT NULL
);

-- postgres=# \d pg_catalog.pg_collation;
-- Table "pg_catalog.pg_collation"
-- Column        |  Type   | Collation | Nullable | Default
-- ---------------------+---------+-----------+----------+---------
--  oid                 | oid     |           | not null |
--  collname            | name    |           | not null |
--  collnamespace       | oid     |           | not null |
--  collowner           | oid     |           | not null |
--  collprovider        | "char"  |           | not null |
--  collisdeterministic | boolean |           | not null |
--  collencoding        | integer |           | not null |
--  collcollate         | name    |           | not null |
--  collctype           | name    |           | not null |
--  collversion         | text    | C         |          |
-- Indexes:
--     "pg_collation_oid_index" PRIMARY KEY, btree (oid)
--     "pg_collation_name_enc_nsp_index" UNIQUE CONSTRAINT, btree (collname, collencoding, collnamespace)
CREATE TABLE pg_catalog.pg_collation
(
  oid           OID  NOT NULL,
  collname      TEXT NOT NULL,
  collnamespace OID  NOT NULL
);

-- postgres=# \d pg_index;
-- Table "pg_catalog.pg_index"
-- Column     |     Type     | Collation | Nullable | Default
-- ----------------+--------------+-----------+----------+---------
--  indexrelid     | oid          |           | not null |
--  indrelid       | oid          |           | not null |
--  indnatts       | smallint     |           | not null |
--  indnkeyatts    | smallint     |           | not null |
--  indisunique    | boolean      |           | not null |
--  indisprimary   | boolean      |           | not null |
--  indisexclusion | boolean      |           | not null |
--  indimmediate   | boolean      |           | not null |
--  indisclustered | boolean      |           | not null |
--  indisvalid     | boolean      |           | not null |
--  indcheckxmin   | boolean      |           | not null |
--  indisready     | boolean      |           | not null |
--  indislive      | boolean      |           | not null |
--  indisreplident | boolean      |           | not null |
--  indkey         | int2vector   |           | not null |
--  indcollation   | oidvector    |           | not null |
--  indclass       | oidvector    |           | not null |
--  indoption      | int2vector   |           | not null |
--  indexprs       | pg_node_tree | C         |          |
--  indpred        | pg_node_tree | C         |          |
-- Indexes:
--     "pg_index_indexrelid_index" PRIMARY KEY, btree (indexrelid)
--     "pg_index_indrelid_index" btree (indrelid)
CREATE TABLE pg_catalog.pg_index
(
  indexrelid   OID     NOT NULL,
  indrelid     OID     NOT NULL,
  indisunique  BOOLEAN NOT NULL,
  indisprimary BOOLEAN NOT NULL,
  indisvalid   BOOLEAN NOT NULL
);

-- postgres=# \d pg_catalog.pg_attrdef;
-- Table "pg_catalog.pg_attrdef"
-- Column  |     Type     | Collation | Nullable | Default
-- ---------+--------------+-----------+----------+---------
--  oid     | oid          |           | not null |
--  adrelid | oid          |           | not null |
--  adnum   | smallint     |           | not null |
--  adbin   | pg_node_tree | C         | not null |
-- Indexes:
--     "pg_attrdef_oid_index" PRIMARY KEY, btree (oid)
--     "pg_attrdef_adrelid_adnum_index" UNIQUE CONSTRAINT, btree (adrelid, adnum)
CREATE TABLE pg_catalog.pg_attrdef
(
  adrelid OID          NOT NULL,
  admin   INT          NOT NULL,
  adbin   PG_NODE_TREE NOT NULL
);

-- postgres=# \d pg_catalog.pg_depend
-- Table "pg_catalog.pg_depend"
-- Column    |  Type   | Collation | Nullable | Default
-- -------------+---------+-----------+----------+---------
--  classid     | oid     |           | not null |
--  objid       | oid     |           | not null |
--  objsubid    | integer |           | not null |
--  refclassid  | oid     |           | not null |
--  refobjid    | oid     |           | not null |
--  refobjsubid | integer |           | not null |
--  deptype     | "char"  |           | not null |
-- Indexes:
--     "pg_depend_depender_index" btree (classid, objid, objsubid)
--     "pg_depend_reference_index" btree (refclassid, refobjid, refobjsubid)
CREATE TABLE pg_catalog.pg_depend
(
  objid    OID,
  refobjid OID,
  deptype  char
);


-- postgres=# \d pg_catalog.pg_constraint
-- Table "pg_catalog.pg_constraint"
-- Column     |     Type     | Collation | Nullable | Default
-- ---------------+--------------+-----------+----------+---------
--  oid           | oid          |           | not null |
--  conname       | name         |           | not null |
--  connamespace  | oid          |           | not null |
--  contype       | "char"       |           | not null |
--  condeferrable | boolean      |           | not null |
--  condeferred   | boolean      |           | not null |
--  convalidated  | boolean      |           | not null |
--  conrelid      | oid          |           | not null |
--  contypid      | oid          |           | not null |
--  conindid      | oid          |           | not null |
--  conparentid   | oid          |           | not null |
--  confrelid     | oid          |           | not null |
--  confupdtype   | "char"       |           | not null |
--  confdeltype   | "char"       |           | not null |
--  confmatchtype | "char"       |           | not null |
--  conislocal    | boolean      |           | not null |
--  coninhcount   | integer      |           | not null |
--  connoinherit  | boolean      |           | not null |
--  conkey        | smallint[]   |           |          |
--  confkey       | smallint[]   |           |          |
--  conpfeqop     | oid[]        |           |          |
--  conppeqop     | oid[]        |           |          |
--  conffeqop     | oid[]        |           |          |
--  conexclop     | oid[]        |           |          |
--  conbin        | pg_node_tree | C         |          |
-- Indexes:
--     "pg_constraint_oid_index" PRIMARY KEY, btree (oid)
--     "pg_constraint_conname_nsp_index" btree (conname, connamespace)
--     "pg_constraint_conparentid_index" btree (conparentid)
--     "pg_constraint_conrelid_contypid_conname_index" UNIQUE CONSTRAINT, btree (conrelid, contypid, conname)
--     "pg_constraint_contypid_index" btree (contypid)
CREATE TABLE pg_catalog.pg_constraint
(
  oid           OID          NOT NULL,
  conname       TEXT         NOT NULL,
  conindid      OID          NOT NULL,
  contype       CHAR         NOT NULL,
  condeferrable BOOLEAN      NOT NULL,
  condeferred   BOOLEAN      NOT NULL,
  convalidated  BOOLEAN      NOT NULL,
  conrelid      OID          NOT NULL,
  conislocal    BOOLEAN      NOT NULL,
  connoinherit  BOOLEAN      NOT NULL,
  conbin        PG_NODE_TREE NOT NULL
);

-- postgres=# \d pg_catalog.pg_proc
-- Table "pg_catalog.pg_proc"
-- Column      |     Type     | Collation | Nullable | Default
-- -----------------+--------------+-----------+----------+---------
--  oid             | oid          |           | not null |
--  proname         | name         |           | not null |
--  pronamespace    | oid          |           | not null |
--  proowner        | oid          |           | not null |
--  prolang         | oid          |           | not null |
--  procost         | real         |           | not null |
--  prorows         | real         |           | not null |
--  provariadic     | oid          |           | not null |
--  prosupport      | regproc      |           | not null |
--  prokind         | "char"       |           | not null |
--  prosecdef       | boolean      |           | not null |
--  proleakproof    | boolean      |           | not null |
--  proisstrict     | boolean      |           | not null |
--  proretset       | boolean      |           | not null |
--  provolatile     | "char"       |           | not null |
--  proparallel     | "char"       |           | not null |
--  pronargs        | smallint     |           | not null |
--  pronargdefaults | smallint     |           | not null |
--  prorettype      | oid          |           | not null |
--  proargtypes     | oidvector    |           | not null |
--  proallargtypes  | oid[]        |           |          |
--  proargmodes     | "char"[]     |           |          |
--  proargnames     | text[]       | C         |          |
--  proargdefaults  | pg_node_tree | C         |          |
--  protrftypes     | oid[]        |           |          |
--  prosrc          | text         | C         | not null |
--  probin          | text         | C         |          |
--  prosqlbody      | pg_node_tree | C         |          |
--  proconfig       | text[]       | C         |          |
--  proacl          | aclitem[]    |           |          |
-- Indexes:
--     "pg_proc_oid_index" PRIMARY KEY, btree (oid)
--     "pg_proc_proname_args_nsp_index" UNIQUE CONSTRAINT, btree (proname, proargtypes, pronamespace)
CREATE TABLE pg_catalog.pg_proc
(
  oid          OID  NOT NULL,
  proname      TEXT NOT NULL,
  pronamespace OID  NOT NULL,
  prolang      OID  NOT NULL,
  prokind      CHAR NOT NULL
);


-- postgres=# \d pg_catalog.pg_language
-- Table "pg_catalog.pg_language"
-- Column     |   Type    | Collation | Nullable | Default
-- ---------------+-----------+-----------+----------+---------
--  oid           | oid       |           | not null |
--  lanname       | name      |           | not null |
--  lanowner      | oid       |           | not null |
--  lanispl       | boolean   |           | not null |
--  lanpltrusted  | boolean   |           | not null |
--  lanplcallfoid | oid       |           | not null |
--  laninline     | oid       |           | not null |
--  lanvalidator  | oid       |           | not null |
--  lanacl        | aclitem[] |           |          |
-- Indexes:
--     "pg_language_oid_index" PRIMARY KEY, btree (oid)
--     "pg_language_name_index" UNIQUE CONSTRAINT, btree (lanname)
CREATE TABLE pg_catalog.pg_language
(
  oid     OID  NOT NULL,
  lanname TEXT NOT NULL
);


-- postgres=# \d pg_catalog.pg_trigger;
-- Table "pg_catalog.pg_trigger"
-- Column     |     Type     | Collation | Nullable | Default
-- ----------------+--------------+-----------+----------+---------
--  oid            | oid          |           | not null |
--  tgrelid        | oid          |           | not null |
--  tgparentid     | oid          |           | not null |
--  tgname         | name         |           | not null |
--  tgfoid         | oid          |           | not null |
--  tgtype         | smallint     |           | not null |
--  tgenabled      | "char"       |           | not null |
--  tgisinternal   | boolean      |           | not null |
--  tgconstrrelid  | oid          |           | not null |
--  tgconstrindid  | oid          |           | not null |
--  tgconstraint   | oid          |           | not null |
--  tgdeferrable   | boolean      |           | not null |
--  tginitdeferred | boolean      |           | not null |
--  tgnargs        | smallint     |           | not null |
--  tgattr         | int2vector   |           | not null |
--  tgargs         | bytea        |           | not null |
--  tgqual         | pg_node_tree | C         |          |
--  tgoldtable     | name         |           |          |
--  tgnewtable     | name         |           |          |
-- Indexes:
--     "pg_trigger_oid_index" PRIMARY KEY, btree (oid)
--     "pg_trigger_tgrelid_tgname_index" UNIQUE CONSTRAINT, btree (tgrelid, tgname)
--     "pg_trigger_tgconstraint_index" btree (tgconstraint)
CREATE TABLE pg_catalog.pg_trigger
(
  oid          OID     NOT NULL,
  tgrelid      OID     NOT NULL,
  tgparentid   OID     NOT NULL,
  tgname       TEXT    NOT NULL,
  tfoid        OID     NOT NULL,
  tgisinternal BOOLEAN NOT NULL
);
