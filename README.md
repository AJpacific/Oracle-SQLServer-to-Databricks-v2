# Oracle and SQL Server to Databricks Migration Accelerator

A metadata-driven accelerator for migrating Oracle and Microsoft SQL Server tables to Databricks Delta tables governed by Unity Catalog. Shared notebooks route every control-table row through a source-specific adapter, while target provisioning, audit, reconciliation, and incremental orchestration remain common.

## Repository layout

```text
config/
  type_rules.yaml
  type_rules_oracle.yaml
  type_rules_sqlserver.yaml
notebooks/
  _common.py
  00_TEST_ORACLE_CONNECTION.py
  00_TEST_SQLSERVER_CONNECTION.py
  NB00_ControlTableInit.py
  NB01_SourceInventory.py
  NB02_TypeNormalization.py
  NB03_MappingRulesGeneration.py
  NB04_MappingValidation.py
  NB07_TableDecisionGeneration.py
  NB08_TargetProvisioning.py
  NB09_FullLoad.py
  NB10_PostFullLoadState.py
  NB11a_DeltaSyncPrep.py
  NB11b_DeltaSyncApply.py
  NB12_ValidationAndReconciliation.py
src/
  source_adapters/
    base.py
    factory.py
    oracle.py
    sqlserver.py
  control_repository.py
  crosssourcetypemapper.py
  ddl_builder.py
  identifiers.py
  partitioning.py
  source_identity.py
  sql_builder.py
  sqlserver_sql_builder.py
  strategy.py
tests/
  test_accelerator.py
  test_sqlserver.py
requirements-dev.txt
```

Pipeline JSON files may be maintained separately. Confirm notebook paths, task order, compute, and parameters before importing any job definition.

## Source registration

Every source row is identified by:

```text
source_system
source_server
source_database
source_schema
source_table
```

A deterministic `source_table_id` is derived from that identity. This keeps Oracle and SQL Server tables separate even when schema and table names match.

Supported `source_system` values:

```text
oracle
sqlserver
sql_server
mssql
```

Aliases normalize to `sqlserver`. Unknown systems fail explicitly.

## Unity Catalog organization

The default control namespace is:

```text
da_accelerators.control
```

Target location is selected per control row using:

```text
target_catalog
target_schema
target_table
```

If multiple active source rows resolve to the same target fully qualified name, target provisioning marks a configuration error instead of allowing an overwrite collision.

## Secret scopes

Default scopes:

```text
Oracle:     oracle-migration
SQL Server: sqlserver-migration
```

Oracle keys:

```text
oracle-user
oracle-password
oracle-jdbc-url
```

If `oracle-jdbc-url` is absent, the adapter uses:

```text
oracle-host
oracle-port
oracle-service
```

SQL Server keys:

```text
sqlserver-user
sqlserver-password
sqlserver-jdbc-url
```

If `sqlserver-jdbc-url` is absent, the adapter uses:

```text
sqlserver-host
sqlserver-port
```

`source_database` is required for SQL Server control rows. Credentials must remain in secret scopes and must not be committed to Git.

## Connection validation

Run the matching connection notebook before onboarding tables:

```text
00_TEST_ORACLE_CONNECTION.py
00_TEST_SQLSERVER_CONNECTION.py
```

The SQL Server adapter uses the Microsoft JDBC driver:

```text
com.microsoft.sqlserver.jdbc.SQLServerDriver
```

The generated direct JDBC URL enables encryption and does not trust the server certificate by default.

## Pipeline 1: onboarding and initial load

Run in this order:

```text
NB00_ControlTableInit
NB01_SourceInventory
NB02_TypeNormalization
NB03_MappingRulesGeneration
NB04_MappingValidation
NB07_TableDecisionGeneration
NB08_TargetProvisioning
NB09_FullLoad
NB12_ValidationAndReconciliation with mode=full
NB10_PostFullLoadState
```

NB09 performs an overwrite-only initial snapshot load. NB10 commits initial state only for successfully loaded and reconciled tables.

## Pipeline 2: recurring synchronization

Run in this order:

```text
NB11a_DeltaSyncPrep
NB11b_DeltaSyncApply
NB12_ValidationAndReconciliation with mode=delta
```

Supported strategies:

```text
FULL_LOAD
WATERMARK
PRIMARY_KEY
HYBRID
```

Behavior:

- `FULL_LOAD`: complete source extract and complete target overwrite.
- `WATERMARK`: bounded temporal interval replacement and append.
- `PRIMARY_KEY`: complete source extract and Delta MERGE by primary key.
- `HYBRID`: bounded temporal extract and Delta MERGE by primary key.

Only supported temporal source types are eligible as automatic watermarks. Numeric IDs, quantities, strings, SQL Server `timestamp`, and SQL Server `rowversion` are not temporal watermarks.

## SQL Server datetime2 policy

Databricks Delta `TIMESTAMP` uses microsecond precision. SQL Server `datetime2(7)` contains a seventh fractional digit, so this accelerator applies an explicit automatic lossy policy:

```text
SQL Server datetime2
-> source-side datetime2(6)
-> Databricks TIMESTAMP
-> mapping status AUTO
-> fidelity LOSSY
```

The SQL Server full-load projection, upper-watermark query, incremental predicate, and returned incremental watermark column all use the same six-digit policy for a selected `datetime2` watermark.

Other supported SQL Server temporal families retain their family-specific query behavior. SQL Server `timestamp` and `rowversion` map to `BINARY` and are never temporal watermarks.

## Empty temporal tables

A successfully loaded empty `WATERMARK` or `HYBRID` table receives the adapter-defined canonical bootstrap checkpoint:

```text
1900-01-01T00:00:00.000000Z
```

This permits later inserts to be discovered by Pipeline 2 instead of leaving the table permanently outside incremental processing.

## Column safety policy

SQL Server metadata records identity, computed, hidden, rowversion, and source type schema properties.

```text
Hidden column:   BLOCKED
Computed column: REVIEW
Identity column: retained as metadata
Rowversion:      BINARY, never temporal
```

Only approved `AUTO` mappings are selected for automatic extraction and target provisioning.

## Type mapping highlights

Oracle examples:

```text
NUMBER(p,0) -> SMALLINT, INT, BIGINT, or DECIMAL(p,0)
Unconstrained NUMBER -> DECIMAL(38,0), AUTO
DATE -> TIMESTAMP, WIDENED
TIMESTAMP WITH TIME ZONE -> TIMESTAMP, REVIEW/LOSSY
BFILE, ANYDATA, SDO_GEOMETRY -> BLOCKED
```

SQL Server examples:

```text
int -> INT
bigint -> BIGINT
decimal(p,s) -> DECIMAL(p,s), up to precision 38
money -> DECIMAL(19,4)
datetime2 -> TIMESTAMP, AUTO/LOSSY at six digits
uniqueidentifier -> STRING
rowversion/timestamp -> BINARY
sql_variant, hierarchyid, geometry, geography -> BLOCKED
```

The YAML rules and `crosssourcetypemapper.py` are the source of truth for mapping behavior.

## Delete policy

Default behavior keeps target rows that no longer exist at the source.

```text
delete_policy = IGNORE_DELETES
```

For a `PRIMARY_KEY` strategy processing a complete source snapshot, `HARD_DELETE` can request:

```sql
WHEN NOT MATCHED BY SOURCE THEN DELETE
```

This is not applied to bounded `WATERMARK` or `HYBRID` slices.

## Testing

Install development dependencies and run:

```bash
python -m pip install -r requirements-dev.txt
python -m pytest tests -q
```

The current repository tests cover Oracle and SQL Server adapters, source identity, identifiers, source SQL generation, type mapping, watermark strategy, partition planning, and shared-notebook wiring. Always run the suite after changing adapter contracts, query builders, mappings, or notebook calls.

## Deployment validation

Before production use, complete live validation against the intended source platform:

1. Run the matching connection notebook.
2. Onboard one small table for each intended strategy.
3. Validate initial row counts and target schema.
4. Insert and update controlled source rows.
5. Run Pipeline 2 and verify data, queue state, audit state, and checkpoint movement.
6. Restart compute and repeat the relevant connection and synchronization checks.
7. Validate the final SQL Server deployment against the chosen Azure SQL, Cloud SQL for SQL Server, VM-hosted SQL Server, or on-premises network path.

Unit tests validate pure logic and static wiring. They do not replace live JDBC, permissions, TLS, networking, source-dialect, or Databricks job validation.
