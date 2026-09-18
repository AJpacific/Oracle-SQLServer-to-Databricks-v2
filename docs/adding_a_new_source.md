# Adding a new source

This accelerator is modular by source. Adding a source requires **no change to
any shared notebook**. If you find yourself editing something under
`notebooks/shared/`, that is a signal the behavior belongs behind the adapter
contract instead.

PostgreSQL is used below purely as an illustration. It is **not implemented** in
this repository.

## What is shared and what is source-specific

| Layer | Owns | Location |
|---|---|---|
| Shared notebooks | Registration, normalization, mapping orchestration, validation, decisions, Bronze provisioning, full + delta loading, reconciliation, checkpoints, ETL, quarantine, retries, notifications, dashboards, control DDL | `notebooks/shared/` |
| Shared pure modules | Record/row normalization, reconciliation rules, DQ rules, failure classification, ETL work unit, watermark serialization, source-neutral mapper contract | `src/` and `src/type_mappers/base.py` |
| Source type mapper | Built-in datatype rules, family normalization, precision/scale policy, source-qualified YAML loading | `src/type_mappers/<source>.py` |
| Source adapter | Dialect SQL, connection probe, type-rules filename, column policy, watermark policy, partition policy, SQL-object typing | `src/source_adapters/<source>.py` |
| Source notebooks | Connection onboarding, broad assessment, metadata inventory, SQL-object assessment, diagnostics | `notebooks/sources/<source>/` |

Shared notebooks never compare `source_system` to a source literal. A test
(`tests/test_modularity.py`) parses the shared code with `ast` and fails the
build if such a comparison, a dialect catalog string, a source credential key,
a dialect query-builder import, or a direct concrete-adapter construction
appears.

Connections are data, not code branches. Adding another connection for any
registered source requires no adapter, mapper, or shared-notebook change. One
endpoint may have several connection IDs and secret scopes. Each selected table
is assigned a v2 ID over `connection_id + source identity`, so registrations of
the same physical object remain independent.

## Steps

### 1. Write the adapter

Create `src/source_adapters/postgresql.py` subclassing `SourceAdapter`. The base
class declares everything shared code may call. You must implement:

**Connection**
- `get_jdbc_url_and_props()` - builds the URL from the connection's secret scope
- `connection_probe_query()` - e.g. `"(SELECT 1 AS CONNECTION_OK) q"`
- `extra_read_options()` - optional JDBC read options

**Metadata and extraction**
- `columns_metadata_query()`, `primary_key_query()`
- `list_schemas_query()`, `list_tables_query()`, `list_views_query()`,
  `list_routines_query()`, `table_statistics_query()`
- `full_extract_query()`, `incremental_extract_query()`,
  `upper_watermark_query()`, `count_query()`, `min_max_query()`,
  `top_n_probe_query()`

All metadata queries must return the **neutral aliases** shared code expects
(`COLUMN_NAME`, `ORDINAL_POSITION`, `IS_NULLABLE`, `DATA_TYPE`,
`CHARACTER_MAXIMUM_LENGTH`, `NUMERIC_PRECISION`, `NUMERIC_SCALE`,
`DATETIME_PRECISION`, and for discovery `SCHEMA_NAME`, `OBJECT_NAME`,
`OBJECT_TYPE`, `ROW_COUNT`, `SIZE_MB`, `ROW_COUNT_METHOD`).
`inventory_common.validate_metadata_aliases()` enforces the required set.

**Policy**
- `normalize_watermark_type()`, `is_supported_watermark_type()`,
  `watermark_type_rank()`, `initial_watermark_value()`
- `resolve_partition_plan()`
- `load_type_mapper()` and **`type_rules_file()`** - the filename only; shared
  code locates it under `config/` and fails loudly if it is missing. The loader
  must instantiate this source's concrete mapper, not the compatibility facade
- `apply_column_policy(column_metadata, proposed_mapping)` - return a
  `ColumnPolicyResult`. Override only if the source has non-writable, hidden,
  generated, or version columns; otherwise inherit the base behavior. Map your
  dialect concepts to the canonical codes `SOURCE_GENERATED_COLUMN`,
  `SOURCE_HIDDEN_COLUMN`, `SOURCE_NON_WRITABLE_COLUMN`,
  `SOURCE_BINARY_VERSION_COLUMN`
- `validate_connection_metadata(connection)` - raise `ValueError` for unusable
  non-secret metadata (never reference a credential in the message)
- `legacy_secret_scope_widget()` - return `None` for a new source; the
  registered `source_connection.secret_scope` is authoritative

**SQL objects**
- `SQL_OBJECT_TYPES` and, if the codes differ from the shared vocabulary,
  `normalize_sql_object_type()`. An unrecognized code must normalize to `""` so
  it is skipped explicitly rather than mislabelled.

### 2. Add the source type mapper and rules

Create `src/type_mappers/postgresql.py` with a class implementing
`SourceTypeMapper`:

```python
from src.type_mappers.base import ColumnMappingResult, SourceTypeMapper


class PostgreSqlTypeMapper(SourceTypeMapper):
  def __init__(self, rules=None):
    self._rules = rules or {}

  def map_column(self, source_type, precision=None, scale=None,
           length=None, is_nullable=True):
    # Apply PostgreSQL policy only; return ColumnMappingResult.
    ...
```

Keep every PostgreSQL family name, fallback rule, and numeric/temporal special
case in this module. Do not edit the Oracle or SQL Server mapper.

Create `config/type_rules_postgresql.yaml` with `source_dialect: postgresql` and
the type mappings. The filename must match `type_rules_file()`. Validate that
`source_dialect` matches the adapter, `target` is `databricks_delta`, every
mapping status is `AUTO`/`REVIEW`/`BLOCKED`, and every fidelity is
`EXACT`/`WIDENED`/`LOSSY`/`UNKNOWN`.

The adapter should load its mapper explicitly:

```python
def load_type_mapper(self):
  return PostgreSqlTypeMapper.from_yaml_path(self._type_rules_path())
```

`src/crosssourcetypemapper.py` is compatibility-only. Do not add rules or a
source branch to it.

### 3. Register the source

Three explicit entries - no dynamic discovery, no directory scanning, no
`eval`/`exec`:

```python
# src/source_adapters/factory.py
_ADAPTERS = {
    "oracle": OracleSourceAdapter,
    "sqlserver": SqlServerSourceAdapter,
    "postgresql": PostgreSqlSourceAdapter,
}
```

```python
# src/type_mappers/factory.py
_MAPPERS = {
  "oracle": OracleTypeMapper,
  "sqlserver": SqlServerTypeMapper,
  "postgresql": PostgreSqlTypeMapper,
}
```

```python
# src/source_registry.py
SOURCE_DEFINITIONS["postgresql"] = {
    "display_name": "PostgreSQL",
    "adapter": PostgreSqlSourceAdapter,
    "notebooks": {
        "validate_connection": "sources/postgresql/NB00A_UpsertAndValidateConnection",
        "assessment":          "sources/postgresql/NB01A_SourceAssessment",
        "inventory":           "sources/postgresql/NB01_SourceInventory",
        "sql_objects":         "sources/postgresql/NB13_SQLObjectAssessmentAndConversion",
        "diagnostic":          "sources/postgresql/TEST_CONNECTION",
    },
    "capabilities": {
        "sql_object_assessment": True,
        "packages": False,
        "catalog_row_counts": True,
    },
}
```

Also add the token to `src/source_identity.py` (`normalize_source_system` must
recognize it) and, if the source requires an explicit database, to
`SOURCES_REQUIRING_DATABASE`. Never add a fallback token: a missing or unknown
`source_system` must continue to fail.

### 4. Write the five source notebooks

Copy the shape of `notebooks/sources/sqlserver/`. Each begins with
`%run ../../shared/_common` and stays thin - dialect calls plus shared helpers:

| Notebook | Must do | Must NOT do |
|---|---|---|
| `NB00A_UpsertAndValidateConnection` | Fix `SOURCE_SYSTEM` internally, call `normalize_connection_input()`, `repo.upsert_connection()`, `probe_connection()`; sanitize failures with `failcls.sanitize_message` | Accept a `source_system` widget; return or print a secret |
| `NB01_SourceInventory` | Call adapter metadata queries, build one complete table batch with `inv_common.normalize_inventory_row()`, then call `persist_inventory_rows()` before updating control state | Write `source_inventory` directly; mark `INVENTORIED` before persistence |
| `NB01A_SourceAssessment` | Discover objects, call `assess_common.build_assessment_record()` and `summarize_table_compatibility()`, `persist_assessment_records()` | Run a per-table `COUNT(*)`; claim an exact row count |
| `NB13_SQLObjectAssessmentAndConversion` | Extract definitions, call `sqlobj_common.build_sql_object_record()`, `persist_sql_object_records()` | Execute or deploy generated SQL |
| `TEST_CONNECTION` | Treat a registered `connection_id` as authoritative for server/database/scope; require explicit `allow_legacy_mode=true` for manual fallback | Let a widget override a registered connection |

### 5. Point the job at the new notebooks

In your Databricks job definition, set the source-specific tasks (`T01`, `T02`,
`T04`, `T14` in `docs/databricks_job_task_mapping.md`) at
`sources/postgresql/...`. Every shared task is unchanged.

Do not add source-specific worklist, Full Load, or Delta notebooks. Global
worklists contain only `run_id`, `connection_id`, and `source_table_id`; the
shared table task resolves the adapter lazily from that registered connection.

### 6. Prove it

`tests/test_modularity.py` contains a test-only `_FakeAdapter` demonstrating
that a third source satisfies the contract and flows through
`assessment_common`, `inventory_common`, and `sql_object_assessment_common`
without shared code learning about it. Its test-only mapper is returned directly
from `load_type_mapper()` without changing either existing concrete mapper.
Extend the contract-parity and datatype regression tests to include your source
so inventory, assessment, SQL-object records, and mapping outcomes are verified.

## What is still required beyond unit tests

Pure Python tests validate query construction and policy. They do **not**
validate JDBC connectivity, driver behavior, catalog permissions, Spark
execution, or Databricks job orchestration. Run the live checklist in
`docs/production_readiness_checklist.md` before using a new source in
production. Runtime rows start as `NOT_EXECUTED`; never infer a pass from pure
tests.
