# Source identity v2 migration

Source identity v2 makes each table registration owned by one `connection_id`.
The deterministic ID is a SHA-256 digest of:

```text
v2
connection_id
normalized source_system
normalized source_server
normalized source_database
source_schema
source_table
```

Schema and table casing remains significant. The same physical table registered
through two connection IDs therefore receives two different IDs and keeps
separate control state, inventory, mappings, targets, audit history,
reconciliation, retries, DQ history, and checkpoints.

## Upgrade order

1. Stop onboarding, Full Load, Delta, retry, and ETL jobs that write the control
   schema.
2. Back up or clone the control schema according to your platform policy.
3. Deploy this repository revision.
4. Run `shared/NB00_ControlTableInit` once. It adds the v2 lineage and ownership
   columns but does not rewrite an identity. A validation failure reporting
   `SOURCE_IDENTITY_MIGRATION_REQUIRED` is expected for legacy rows.
5. Run `deployment/NB_MigrateSourceTableIdentityV2` with `dry_run=true`.
6. Resolve every blocked row. Missing ownership must be reviewed; never infer a
   connection from object names or credentials.
7. Run the migration with `dry_run=false`. Use `only_connection_ids` and
   `batch_size` only when an operationally reviewed batch is required.
8. Rerun the dry run. Every in-scope row must report `ALREADY_MIGRATED`, with no
   blocked rows or errors.
9. Run the live validation scenarios in the production-readiness checklist
   before enabling global operational Jobs.

## Updated tables

The migration discovers the tables that exist and updates these identity-bearing
tables by `connection_id + old_source_table_id`:

- `source_table_control` (updated last)
- `source_inventory`
- `normalized_source_inventory`
- `resolved_column_mappings`
- `mapping_validation_results`
- `table_load_decisions`
- `review_queue`
- `table_run_log`
- `delta_sync_queue`
- `reconciliation_results`
- `dq_rule`
- `dq_result`
- `dq_quarantine`

`source_assessment` and `sql_object_assessment` already use connection-owned
assessment keys and do not contain `source_table_id`.

The migration preserves `legacy_source_table_id` on `source_table_control` and
sets `source_identity_version=2`. It creates
`source_table_identity_migration` only in execution mode. The mapping table
contains IDs and migration status only; it never stores a secret scope or
credential.

## Safety behavior

Dry-run mode performs no table mutation. Before execution, the notebook blocks
missing ownership, missing connections, source-system conflicts, duplicate new
IDs, conflicts with unrelated registrations, missing child `connection_id`
columns, and child rows whose ownership is blank. It displays bounded sanitized
errors and safe mapping metadata.

Execution updates child history first and the owner row last. Each replacement
uses both `connection_id` and the old ID. A failure is recorded as `PARTIAL` and
the notebook fails; rerunning is safe because already-updated child rows are no
longer matched and a correctly updated owner is reported as already migrated.

This repository does not provide a cross-table Delta transaction. Retain the
pre-migration backup until final verification confirms one v2 owner per
registration, no remaining old child references, no duplicate ownership keys,
and preserved Full Load, checkpoint, retry, reconciliation, and DQ history.

All live migration checklist entries remain `NOT_EXECUTED` until run in the
target Databricks workspace.
