# Production readiness checklist

This checklist separates repository evidence from live runtime evidence. Pure
Python tests and static scans do not prove live Spark, JDBC, Delta, Oracle,
SQL Server, Unity Catalog, or Databricks Job readiness.

Allowed status values:

- `NOT_EXECUTED`
- `PASSED`
- `FAILED`
- `BLOCKED`
- `NOT_APPLICABLE`

Do not mark an item `PASSED` without recording the actual result and durable
evidence. Required runtime checks default to `NOT_EXECUTED`. The release verdict
cannot be `READY` while required live checks remain `NOT_EXECUTED`.

## A. Repository checks

| Check | Environment | Expected result | Actual result | Evidence | Status | Owner or reviewer | Notes |
|---|---|---|---|---|---|---|---|
| Compile source and tests | Windows 11 Enterprise build 26200, Python 3.12.10 | `python -m compileall -q src tests notebooks` exits 0 | Exit 0 | `artifacts/test-results/compileall-output.txt` | PASSED | Release reviewer | Quiet mode produced no diagnostic output |
| Run pytest suite | Windows 11 Enterprise build 26200, Python 3.12.10, pytest 9.0.2 | `python -m pytest tests -q` exits 0 with runner-reported counts | pytest: Exit 0, 650 passed, 188 subtests passed | `artifacts/test-results/pytest-output.txt` | PASSED | Release reviewer | Runner emitted no failed, skipped, or warning count |
| Run unittest discovery | Windows 11 Enterprise build 26200, Python 3.12.10 | `python -m unittest discover -s tests -v` exits 0 with runner-reported total | unittest: Exit 0, 650 tests passed, OK | `artifacts/test-results/unittest-output.txt` | PASSED | Release reviewer | No failures or errors reported |
| Review test summary | Windows 11 Enterprise build 26200 | JSON records UTC timestamp, commands, exits, counts, and overall status | Summary records all exits as 0 and overall status PASSED | `artifacts/test-results/test-summary.json` | PASSED | Release reviewer | Unreported pytest zero categories remain null rather than inferred |
| Run modularity scan | Repository source review and pytest | Shared code has no source branches, defaults, dialect SQL, credentials, or concrete adapter construction | Executable AST, text, and ownership scans passed | `artifacts/test-results/pytest-output.txt` and release delivery report | PASSED | Release reviewer | Compatibility facade and concrete mappers classified separately |
| Run secret and raw-error scan | Repository source review | No committed secret and no unsanitized exception print/persistence | No raw exception print/persistence or credential value found; approved source keys and sanitizer patterns remain | Release delivery report | PASSED | Release reviewer | Synthetic test fixtures classified separately |
| Review Delta schemas | Repository review | Identity-v2 additions and control-table-driven onboarding additions are explicit, additive, and documented without dropping history | Added `source_identity_version`, `legacy_source_table_id`, ownership `connection_id` columns, `accelerator_target_config` table, `source_assessment` selection lifecycle columns (`selection_status`, `selected_ts`, `selected_by`, `onboarding_started_ts`, `onboarding_completed_ts`, `onboarding_error_message`, `onboarding_run_id`, `onboarding_attempt_id`, `registration_completed_ts`, `onboarding_failed_stage`), and execution-only `source_table_identity_migration` mapping table | Git diff, NB00, and migration notebook review | PASSED | Release reviewer | Existing tables are upgraded additively; migration table is created only for an approved execution |
| Review documentation | Repository review | Paths, source requirements, mapper ownership, evidence, and limitations match code | Required files and checklist updated; live checks remain NOT_EXECUTED | Documentation diff | PASSED | Release reviewer | No production claim from pure tests |

## B. Live Spark and Delta checks

| Check | Environment | Expected result | Actual result | Evidence | Status | Owner or reviewer | Notes |
|---|---|---|---|---|---|---|---|
| Control-table creation | Live Databricks workspace | NB00 creates/upgrades the existing control schema idempotently without dropping history | Not recorded | Run URL, logs, and table list | NOT_EXECUTED | TBD | Confirm catalog permissions |
| Delta MERGE | Live Databricks workspace | Registration, assessment, mappings, queue, and keyed load MERGEs produce expected rows on retry | Not recorded | Run URL and before/after queries | NOT_EXECUTED | TBD | Verify unrelated scopes stay unchanged |
| Delta DELETE | Live Databricks workspace | Scoped inventory, watermark, quarantine, and ETL interval deletes affect only intended rows | Not recorded | Run URL and before/after queries | NOT_EXECUTED | TBD | Verify run/table bounds |
| Full overwrite | Live Databricks workspace | Full source and ETL snapshots replace targets with expected schema and counts | Not recorded | Run URL, history, and count queries | NOT_EXECUTED | TBD | Verify overwrite semantics unchanged |
| Table schema provisioning | Live Databricks workspace | AUTO mappings create the expected Delta schema and unsafe mappings do not provision | Not recorded | DDL, DESCRIBE output, run URL | NOT_EXECUTED | TBD | Include nullable/type checks |
| Quarantine replacement | Live Databricks workspace | Same run/table retry replaces quarantine rows without duplicates | Not recorded | Before/after queries | NOT_EXECUTED | TBD | Other runs remain |
| Reconciliation persistence | Live Databricks workspace | Named checks persist exact values and failures block checkpoint movement | Not recorded | Reconciliation rows and run URL | NOT_EXECUTED | TBD | No target-greater-than-source shortcut |
| Checkpoint ordering | Live Databricks workspace | Data apply precedes reconciliation, then checkpoint commit, then queue finalization | Not recorded | Queue timestamps, control row, audit rows | NOT_EXECUTED | TBD | Verify failure recovery paths |
| Identity-v2 migration dry run | Live Databricks workspace | Every legacy owner and child row maps unambiguously; no table is mutated | Not recorded | Migration output and row-count snapshots | NOT_EXECUTED | TBD | Resolve every blocker before execution |
| Identity-v2 migration execution | Live Databricks workspace | Control and all child histories use the v2 ID with no orphans; checkpoints and audit history remain intact | Not recorded | Before/after queries and migration table | NOT_EXECUTED | TBD | Run manually, never from an operational Job |

## C. Oracle checks

| Check | Environment | Expected result | Actual result | Evidence | Status | Owner or reviewer | Notes |
|---|---|---|---|---|---|---|---|
| Oracle JDBC driver | Databricks compute | `oracle.jdbc.OracleDriver` loads | Not recorded | Diagnostic run URL | NOT_EXECUTED | TBD | Record driver version |
| Oracle network | Databricks to Oracle | Host, port, routing, and firewall permit connectivity | Not recorded | Network test evidence | NOT_EXECUTED | TBD | No credential in evidence |
| Oracle TLS | Databricks to Oracle | Certificate and negotiated TLS meet deployment policy | Not recorded | Connection diagnostics | NOT_EXECUTED | TBD | Record trust configuration |
| Oracle authentication | Databricks secret-backed connection | Login succeeds through the registered secret scope | Not recorded | Connection-validation run | NOT_EXECUTED | TBD | Do not print user/password |
| Oracle secret resolution | Databricks workspace | Required secret keys resolve only at runtime | Not recorded | Redacted validation evidence | NOT_EXECUTED | TBD | No secret stored in Delta |
| Oracle data dictionary permissions | Oracle | Required `ALL_*` metadata and definition views are readable | Not recorded | Permission review and queries | NOT_EXECUTED | TBD | Least privilege |
| Oracle metadata discovery | Databricks and Oracle | Schemas, tables, columns, keys, statistics, and object types are discovered correctly | Not recorded | Assessment/inventory runs | NOT_EXECUTED | TBD | Include casing behavior |
| Oracle SIZE_MB estimate | Databricks and Oracle | `ESTIMATED_8K_BLOCKS` is understood and compared with approved metadata for representative tables | Not recorded | Comparison output | NOT_EXECUTED | TBD | Do not treat the 8 KiB assumption as exact |
| Oracle diagnostic sample privacy | Databricks and Oracle | Default diagnostic output contains row count and column names but no source values | Not recorded | Redacted diagnostic run | NOT_EXECUTED | TBD | Leave `show_sample_values=false` |
| Oracle full extraction | Databricks and Oracle | Small selected table extracts and overwrites Bronze with exact count | Not recorded | Source/target queries and run URL | NOT_EXECUTED | TBD | Verify no cross-connection work |
| Oracle incremental extraction | Databricks and Oracle | Frozen temporal interval returns exactly expected rows | Not recorded | Queue bounds and source query evidence | NOT_EXECUTED | TBD | Test retry with newer rows |
| Oracle datatype round trips | Databricks and Oracle | Representative mapped values preserve documented fidelity | Not recorded | Source/Bronze comparison | NOT_EXECUTED | TBD | Include NUMBER and temporal families |
| Oracle LOB handling | Databricks and Oracle | CLOB, NCLOB, and BLOB samples stream and round-trip within documented limits | Not recorded | Sample comparison and metrics | NOT_EXECUTED | TBD | Record payload sizes |
| Oracle SQL-object definition access | Databricks and Oracle | View/routine/package text is accessible or honestly marked unable to assess | Not recorded | NB13 output and grants | NOT_EXECUTED | TBD | Generated drafts remain unexecuted |

## D. SQL Server checks

| Check | Environment | Expected result | Actual result | Evidence | Status | Owner or reviewer | Notes |
|---|---|---|---|---|---|---|---|
| SQL Server JDBC driver | Databricks compute | `com.microsoft.sqlserver.jdbc.SQLServerDriver` loads | Not recorded | Diagnostic run URL | NOT_EXECUTED | TBD | Record driver version |
| SQL Server network | Databricks to SQL Server | Host, port, routing, and firewall permit connectivity | Not recorded | Network test evidence | NOT_EXECUTED | TBD | Include named-instance policy if used |
| SQL Server TLS | Databricks to SQL Server | Encryption and certificate validation meet deployment policy | Not recorded | Connection diagnostics | NOT_EXECUTED | TBD | `trustServerCertificate=false` preferred |
| SQL Server authentication | Databricks secret-backed connection | Login succeeds through the registered secret scope | Not recorded | Connection-validation run | NOT_EXECUTED | TBD | No credential in output |
| SQL Server secret resolution | Databricks workspace | Required secret keys resolve only at runtime | Not recorded | Redacted validation evidence | NOT_EXECUTED | TBD | No secret stored in Delta |
| SQL Server database selection | Databricks and SQL Server | Registered `source_database` is authoritative and queries target it | Not recorded | Diagnostic and catalog queries | NOT_EXECUTED | TBD | Blank database must fail |
| SQL Server catalog permissions | SQL Server | Required `sys.*` metadata and definition views are readable | Not recorded | Permission review and queries | NOT_EXECUTED | TBD | Include VIEW DEFINITION |
| SQL Server diagnostic sample privacy | Databricks and SQL Server | Default diagnostic output contains row count and column names but no source values | Not recorded | Redacted diagnostic run | NOT_EXECUTED | TBD | Leave `show_sample_values=false` |
| Hidden/computed metadata | Databricks and SQL Server | Hidden columns are blocked/excluded and computed columns require review | Not recorded | Inventory/mapping rows | NOT_EXECUTED | TBD | Verify identity metadata too |
| Rowversion handling | Databricks and SQL Server | `timestamp`/`rowversion` maps to BINARY and is never a temporal watermark | Not recorded | Mapping and extraction comparison | NOT_EXECUTED | TBD | Verify 8-byte values |
| Datetime2 precision | Databricks and SQL Server | `datetime2` uses documented six-digit source policy and Delta microseconds | Not recorded | Boundary and round-trip evidence | NOT_EXECUTED | TBD | Include a seventh-digit sample |
| SQL Server full extraction | Databricks and SQL Server | Small selected table overwrites Bronze with exact count | Not recorded | Source/target queries and run URL | NOT_EXECUTED | TBD | Verify database scoping |
| SQL Server incremental extraction | Databricks and SQL Server | Frozen temporal interval returns exactly expected rows | Not recorded | Queue bounds and source query evidence | NOT_EXECUTED | TBD | Test retry with newer rows |
| SQL Server SQL-object definition access | Databricks and SQL Server | Module text is read or encrypted/inaccessible objects are honestly classified | Not recorded | NB13 output and grants | NOT_EXECUTED | TBD | Drafts remain unexecuted |
| SQL Server SIZE_MB live comparison | SQL Server | Accelerator reserved-size result agrees with trusted queries for approved cases | Not recorded | Comparison workbook/query output | NOT_EXECUTED | TBD | No accuracy claim until passed |
| SQL Server catalog row-count comparison | SQL Server | Catalog row count agrees with trusted metadata/count method for test cases | Not recorded | Comparison query output | NOT_EXECUTED | TBD | Record expected metadata lag |

## E. Databricks Job checks

| Check | Environment | Expected result | Actual result | Evidence | Status | Owner or reviewer | Notes |
|---|---|---|---|---|---|---|---|
| Correct notebook paths | Deployed Databricks Jobs | Every task points to current `shared/` or `sources/<source>/` path | Not recorded | Exported job JSON and UI | NOT_EXECUTED | TBD | No removed root paths |
| Correct dependencies | Deployed Databricks Jobs | Task dependencies enforce documented onboarding, delta, ETL, and retry order | Not recorded | Job graph export | NOT_EXECUTED | TBD | Include reconciliation gates |
| run_id propagation | Deployed Databricks Jobs | One workflow run uses the intended run ID and retries use child lineage | Not recorded | Task parameters/values and audit rows | NOT_EXECUTED | TBD | Widget value is authoritative |
| connection_id propagation | Deployed Databricks Jobs | Connection-scoped tasks process only the selected connection | Not recorded | Task inputs and control queries | NOT_EXECUTED | TBD | No secret parameters |
| source_table_id propagation | Deployed Databricks Jobs | Per-table tasks resolve exactly one eligible work unit | Not recorded | ForEach inputs and task logs | NOT_EXECUTED | TBD | Missing/unknown source fails |
| ForEach one-table isolation | Deployed Databricks Jobs | Each iteration reads/writes/audits only its source_table_id | Not recorded | Parallel iteration evidence | NOT_EXECUTED | TBD | Test same schema/table names |
| Global Full Load worklist | Deployed Databricks Jobs | Eligible Oracle and SQL Server registrations produce separate three-field work items; unused connections are absent | Not recorded | Task values and run output | NOT_EXECUTED | TBD | Include two IDs for one physical table |
| Global Delta worklist | Deployed Databricks Jobs | Current-run QUEUED registrations produce unique connection-owned work items | Not recorded | Queue rows and task values | NOT_EXECUTED | TBD | Validate task-value size limit |
| Lazy connection resolution | Deployed Databricks Jobs | Connections without eligible work do not create adapters, read scopes, open JDBC, or update state | Not recorded | Driver logs and audit queries | NOT_EXECUTED | TBD | Exercise inactive and FAILED connections |
| Mixed-source ForEach | Deployed Databricks Jobs | One run processes Oracle and SQL Server work items using each item's registered scope and target | Not recorded | Run graph, audit rows, target checks | NOT_EXECUTED | TBD | No global connection parameter |
| Retry worklist routing | Deployed Databricks Jobs | Each recovery action invokes the existing correct notebook | Not recorded | Worklist and task-run graph | NOT_EXECUTED | TBD | No duplicate engine |
| Child run lineage | Deployed Databricks Jobs | Child run records parent_run_id, attempt, and frozen bounds | Not recorded | table_run_log rows | NOT_EXECUTED | TBD | State-only recovery is audited |
| Task retry configuration | Deployed Databricks Jobs | Platform retries align with application retry/idempotency policy | Not recorded | Exported task settings | NOT_EXECUTED | TBD | Avoid widening intervals |
| Failure notification run condition | Deployed Databricks Jobs | Notification tasks use `ALL_DONE` and never hide the original failure | Not recorded | Job JSON and notification run | NOT_EXECUTED | TBD | Webhook stays secret-backed |

## F. Retry tests

| Check | Environment | Expected result | Actual result | Evidence | Status | Owner or reviewer | Notes |
|---|---|---|---|---|---|---|---|
| Delta retry using frozen parent interval | Live Databricks and source | Retry reuses parent lower/upper bounds and source query | Not recorded | Parent/child queue and audit rows | NOT_EXECUTED | TBD | No MAX recapture |
| ETL retry using frozen Bronze interval | Live Databricks | Retry reuses original Bronze lower/upper bounds | Not recorded | Parent/child audit rows | NOT_EXECUTED | TBD | No Bronze MAX recompute |
| New source rows after failure | Live source and Databricks | Rows after frozen upper bound are excluded from delta retry | Not recorded | Source inserts and target comparison | NOT_EXECUTED | TBD | Process in later run only |
| New Bronze rows after failure | Live Databricks | Rows after frozen ETL upper bound are excluded from ETL retry | Not recorded | Bronze inserts and Silver comparison | NOT_EXECUTED | TBD | Process in later run only |
| Checkpoint-only recovery | Live Databricks | Recovery commits state without source read or data reapply | Not recorded | Audit operation and Delta history | NOT_EXECUTED | TBD | Reconciliation already passed |
| Queue-finalization-only recovery | Live Databricks | Recovery finalizes queue without source read or data reapply | Not recorded | Audit operation and queue timestamps | NOT_EXECUTED | TBD | Committed checkpoint unchanged |
| Quarantine idempotency | Live Databricks | Same run/table retry replaces quarantine rows exactly | Not recorded | Before/after row identities | NOT_EXECUTED | TBD | Other runs preserved |
| Inventory idempotency | Live Databricks and source | Same run/table retry has no duplicates and removes dropped columns | Not recorded | Before/after inventory queries | NOT_EXECUTED | TBD | New run preserves history |

## G. Required live workflow scenarios

### G1. Modular notebook validation

| Check | Environment | Expected result | Actual result | Evidence | Status | Owner or reviewer | Notes |
|---|---|---|---|---|---|---|---|
| Run shared control initialization | Live Databricks | NB00 succeeds idempotently after identity migration; before migration it adds columns then fails with a safe migration-required result | Not recorded | Run URL | NOT_EXECUTED | TBD | Do not treat the expected pre-migration failure as readiness |
| Validate Oracle connection | Live Databricks and Oracle | Registered Oracle connection becomes VALID | Not recorded | Run URL and connection row | NOT_EXECUTED | TBD | Output sanitized |
| Validate SQL Server connection | Live Databricks and SQL Server | Registered SQL Server connection becomes VALID | Not recorded | Run URL and connection row | NOT_EXECUTED | TBD | Output sanitized |
| Run Oracle source assessment | Live Databricks and Oracle | Complete discovery returns `business_status=COMPLETE`; optional failure returns `PARTIAL`; mandatory failure fails the task | Not recorded | Assessment rows and run URL | NOT_EXECUTED | TBD | No per-table count claim; SIZE_MB assumes 8 KiB blocks |
| Run SQL Server source assessment | Live Databricks and SQL Server | Complete discovery returns `business_status=COMPLETE`; optional failure returns `PARTIAL`; mandatory failure fails the task | Not recorded | Assessment rows and run URL | NOT_EXECUTED | TBD | SIZE_MB remains unclaimed pending comparison |
| Run Oracle inventory twice with same run ID | Live Databricks and Oracle | Second run exactly replaces each same-run table snapshot | Not recorded | Two run attempts | NOT_EXECUTED | TBD | Use stable source metadata first |
| Verify no duplicate Oracle inventory rows | Live Databricks | Grouped merge keys have count 1 | Not recorded | SQL query output | NOT_EXECUTED | TBD | Key is run/table/column |
| Run SQL Server inventory twice with same run ID | Live Databricks and SQL Server | Second run exactly replaces each same-run table snapshot | Not recorded | Two run attempts | NOT_EXECUTED | TBD | Include computed/hidden metadata |
| Verify no duplicate SQL Server inventory rows | Live Databricks | Grouped merge keys have count 1 | Not recorded | SQL query output | NOT_EXECUTED | TBD | Key is run/table/column |
| Verify new run preserves inventory history | Live Databricks | New run ID creates a separate complete snapshot | Not recorded | SQL query output | NOT_EXECUTED | TBD | Prior run remains unchanged |

### G2. Full-load validation

| Check | Environment | Expected result | Actual result | Evidence | Status | Owner or reviewer | Notes |
|---|---|---|---|---|---|---|---|
| Select one small Oracle table | Live Oracle and Databricks | Test table and connection are reviewed and isolated | Not recorded | Test manifest | NOT_EXECUTED | TBD | Non-sensitive sample |
| Select one small SQL Server table | Live SQL Server and Databricks | Test table and connection are reviewed and isolated | Not recorded | Test manifest | NOT_EXECUTED | TBD | Non-sensitive sample |
| Inventory both tables | Live Databricks and sources | Complete current metadata persists | Not recorded | Inventory rows | NOT_EXECUTED | TBD | One connection at a time |
| Force one inventory table failure | Live Databricks and sources | Remaining intended tables are attempted, counts are safe, and the inventory task fails | Not recorded | Failed task run and inventory audit | NOT_EXECUTED | TBD | Downstream tasks must not continue |
| Normalize both tables | Live Databricks | Target-neutral rows and schema hashes persist | Not recorded | Normalized rows | NOT_EXECUTED | TBD | Explicit source_system retained |
| Map both tables | Live Databricks | Registered adapter mapper produces expected outcomes | Not recorded | Mapping rows | NOT_EXECUTED | TBD | Compare regression expectations |
| Validate mappings | Live Databricks | Policy findings match approved source behavior | Not recorded | Validation rows | NOT_EXECUTED | TBD | Unsafe columns do not pass |
| Generate decisions | Live Databricks | Eligible tables become AUTO_MIGRATE; others remain reviewed/blocked | Not recorded | Decision/control rows | NOT_EXECUTED | TBD | No status coercion |
| Provision targets | Live Databricks | Target schemas/tables match approved mappings | Not recorded | DESCRIBE output | NOT_EXECUTED | TBD | No collision |
| Run full loads | Live Databricks and sources | Both targets are exact overwrites of selected source snapshots | Not recorded | Run URLs and Delta history | NOT_EXECUTED | TBD | Per-table ForEach |
| Reconcile exact counts | Live Databricks and sources | Exact source and Bronze counts pass | Not recorded | Reconciliation rows | NOT_EXECUTED | TBD | No greater-than shortcut |
| Commit initial state | Live Databricks | Initial state commits only after reconciliation | Not recorded | Control row timestamps | NOT_EXECUTED | TBD | Empty temporal table policy checked |
| Verify no cross-connection processing | Live Databricks | Each work item touches only its supplied connection/table | Not recorded | Before/after control and target queries | NOT_EXECUTED | TBD | Include the same physical object under two IDs |

### G3. Delta validation

| Check | Environment | Expected result | Actual result | Evidence | Status | Owner or reviewer | Notes |
|---|---|---|---|---|---|---|---|
| Select WATERMARK or HYBRID table | Live source and Databricks | Table has approved temporal policy and initial checkpoint | Not recorded | Control row | NOT_EXECUTED | TBD | Use representative source |
| Prepare frozen lower/upper bounds | Live source and Databricks | NB11a captures one immutable interval | Not recorded | Queue row | NOT_EXECUTED | TBD | Record source query |
| Apply exact interval | Live Databricks | Only rows inside `(lower, upper]` are applied | Not recorded | Source/Bronze comparison | NOT_EXECUTED | TBD | Strategy-specific write |
| Reconcile delta work unit | Live Databricks | Exact named checks pass before state change | Not recorded | Reconciliation rows | NOT_EXECUTED | TBD | Record metrics |
| Commit ingest checkpoint | Live Databricks | Checkpoint advances to frozen upper only | Not recorded | Control and queue rows | NOT_EXECUTED | TBD | Never before reconciliation |
| Finalize delta queue | Live Databricks | Queue succeeds after checkpoint commit | Not recorded | Queue timestamps | NOT_EXECUTED | TBD | Ordering visible |
| Verify timestamp/status order | Live Databricks | Apply, reconcile, checkpoint, finalization order is monotonic | Not recorded | Queue/audit timestamps | NOT_EXECUTED | TBD | Review clock/timezone |
| Force failure after queue creation | Live Databricks | Failed frozen work unit remains recoverable | Not recorded | Failure run URL and rows | NOT_EXECUTED | TBD | Do not alter source query |
| Insert rows beyond frozen upper | Live source | Newer rows exist after failed interval | Not recorded | Source insert/query evidence | NOT_EXECUTED | TBD | Record timestamps |
| Retry failed interval | Live Databricks and source | Parent interval is reused exactly | Not recorded | Parent/child rows | NOT_EXECUTED | TBD | No MAX recapture |
| Verify retry excludes newer rows | Live Databricks and source | Rows beyond upper are absent until a later run | Not recorded | Bronze comparison | NOT_EXECUTED | TBD | Later run may ingest them |

### G4. ETL validation

| Check | Environment | Expected result | Actual result | Evidence | Status | Owner or reviewer | Notes |
|---|---|---|---|---|---|---|---|
| Configure Silver target | Live Databricks | Reviewed Silver catalog/schema/table and ETL policy are active | Not recorded | Control row | NOT_EXECUTED | TBD | Preserve Bronze |
| Add valid DQ rules | Live Databricks | Fixed supported rule set validates | Not recorded | dq_rule rows | NOT_EXECUTED | TBD | No arbitrary SQL |
| Run full ETL | Live Databricks | Complete Bronze snapshot transforms to Silver | Not recorded | Run URL and Delta history | NOT_EXECUTED | TBD | Full overwrite |
| Verify Silver and quarantine | Live Databricks | Valid and rejected rows match rule policy | Not recorded | Count/sample queries | NOT_EXECUTED | TBD | Apply column exclusions |
| Verify ETL reconciliation | Live Databricks | Input equals valid plus distinct rejected and target checks pass | Not recorded | Reconciliation rows | NOT_EXECUTED | TBD | Before ETL checkpoint |
| Inspect DQ results | Live Databricks | Per-rule counts/statuses are complete and consistent | Not recorded | dq_result rows | NOT_EXECUTED | TBD | Include no-rule behavior if applicable |
| Force incremental ETL failure | Live Databricks | Failed attempt records original bounds and leaves checkpoint unchanged | Not recorded | Failure audit/control rows | NOT_EXECUTED | TBD | Choose recoverable stage |
| Add newer Bronze rows | Live Databricks | Rows exist beyond failed ETL upper bound | Not recorded | Bronze query evidence | NOT_EXECUTED | TBD | Record timestamps |
| Retry original ETL bounds | Live Databricks | Retry uses recorded lower/upper bounds | Not recorded | Parent/child audit rows | NOT_EXECUTED | TBD | No MAX recompute |
| Verify newer Bronze rows excluded | Live Databricks | Newer rows are absent from retry output | Not recorded | Silver comparison | NOT_EXECUTED | TBD | Process in later run |
| Verify ETL checkpoint upper bound | Live Databricks | Checkpoint advances only to frozen upper after pass | Not recorded | Control/audit rows | NOT_EXECUTED | TBD | Separate from ingest checkpoint |

### G5. SQL Server size validation matrix

| Check | Environment | Expected result | Actual result | Evidence | Status | Owner or reviewer | Notes |
|---|---|---|---|---|---|---|---|
| Empty table size/count | Live SQL Server | Accelerator and trusted catalog queries agree | Not recorded | Comparison output | NOT_EXECUTED | TBD | Record engine version |
| Heap size/count | Live SQL Server | Base allocation and rows agree | Not recorded | Comparison output | NOT_EXECUTED | TBD | Include allocation units |
| Clustered table size/count | Live SQL Server | Clustered base allocation and rows agree | Not recorded | Comparison output | NOT_EXECUTED | TBD | Record index metadata |
| Nonclustered indexes | Live SQL Server | SIZE_MB includes approved nonclustered index allocations once | Not recorded | Comparison output | NOT_EXECUTED | TBD | Row count not multiplied |
| MAX LOB columns | Live SQL Server | varchar(max), nvarchar(max), or varbinary(max) LOB allocation agrees | Not recorded | Comparison output | NOT_EXECUTED | TBD | Include LOB_DATA |
| Row-overflow table | Live SQL Server | ROW_OVERFLOW_DATA allocation is included once | Not recorded | Comparison output | NOT_EXECUTED | TBD | Verify container relationship |
| Partitioned table | Live SQL Server | Every supported partition is aggregated correctly | Not recorded | Comparison output | NOT_EXECUTED | TBD | Use NOT_APPLICABLE only if unsupported |