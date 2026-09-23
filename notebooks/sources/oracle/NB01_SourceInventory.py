# Databricks notebook source
# MAGIC %md
# MAGIC # Oracle / NB01_SourceInventory
# MAGIC Reads Oracle column and primary-key metadata for every registered active
# MAGIC table through the Oracle adapter, resolves the load strategy using the
# MAGIC Oracle watermark policy, and writes the **shared** normalized
# MAGIC `source_inventory` shape. Normalization, persistence, and control updates
# MAGIC are shared, so this notebook owns only the Oracle metadata calls.

# COMMAND ----------

# MAGIC %run ../../shared/_common

# COMMAND ----------

SOURCE_SYSTEM = "oracle"

run_id = get_run_id()
connection_id = CONNECTION_ID
connection_id = require_connection_id(connection_id, "Oracle inventory")
require_valid_connection(connection_id, SOURCE_SYSTEM)
repo = control_repo()
print("run_id:", run_id, "| connection_id:", connection_id)

# COMMAND ----------

active = [
    r for r in (
        repo.registered_tables_for_onboarding_run(connection_id, run_id)
        if run_id
        else repo.active_tables_for_connection(connection_id, include_onboarding=True)
    ).collect()
    if require_source_system(
        r.asDict().get("source_system"), "source_table_control row")
    == SOURCE_SYSTEM
]
print(f"Active Oracle tables to inventory: {len(active)}")

# COMMAND ----------

# Group candidates by source_database for batched JDBC metadata queries
tables_by_db = {}
for r in active:
    d = r.asDict()
    src_db = d.get("source_database") or ""
    tables_by_db.setdefault(src_db, []).append(r)

batch_col_cache = {}
batch_pk_cache = {}

for db_name, db_candidates in tables_by_db.items():
    table_specs = [(r["source_schema"], r["source_table"]) for r in db_candidates]
    try:
        sample_adapter = get_source_adapter_routed(db_candidates[0])
        batch_cols = read_source_jdbc(
            sample_adapter,
            sample_adapter.batch_columns_metadata_query(db_name, table_specs),
            source_server=sample_adapter.source_server,
            source_database=db_name,
        ).collect()
        for c in batch_cols:
            cd = c.asDict(recursive=True)
            sch = cd.get("TABLE_SCHEMA") or cd.get("table_schema")
            tbl = cd.get("TABLE_NAME") or cd.get("table_name")
            batch_col_cache.setdefault((str(sch).casefold(), str(tbl).casefold()), []).append(cd)

        batch_pks = read_source_jdbc(
            sample_adapter,
            sample_adapter.batch_primary_key_query(db_name, table_specs),
            source_server=sample_adapter.source_server,
            source_database=db_name,
        ).collect()
        for pk in batch_pks:
            pkd = pk.asDict(recursive=True)
            sch = pkd.get("TABLE_SCHEMA") or pkd.get("table_schema")
            tbl = pkd.get("TABLE_NAME") or pkd.get("table_name")
            col = pkd.get("COLUMN_NAME") or pkd.get("column_name")
            batch_pk_cache.setdefault((str(sch).casefold(), str(tbl).casefold()), []).append(col)
    except Exception as batch_exc:
        print(f"  [info] Database {db_name!r} batch metadata fallback to per-table: {failcls.sanitize_message(batch_exc)[:200]}")

written = 0
succeeded, failed = 0, 0

for r in active:
    d = r.asDict()
    src_server = d.get("source_server")
    src_db = d.get("source_database")
    src_schema, src_table = r["source_schema"], r["source_table"]
    src_id = d.get("source_table_id")
    conn_id = d.get("connection_id")

    try:
        if not src_id:
            raise ValueError(
                "source_table_id is required; run the identity-v2 migration "
                "for legacy registrations")
        adapter = get_source_adapter_routed(r)
        src_server = adapter.source_server
        src_db = adapter.source_database
    except Exception as e:
        failed += 1
        safe = failcls.sanitize_message(e)
        if src_id:
            try:
                repo.update_control_for_connection(conn_id, src_id, {
                    "current_status": "INVENTORY_FAILED",
                    "error_message": f"source routing failed: {safe[:900]}"})
            except Exception as ctrl_err:
                safe_ctrl_error = failcls.sanitize_message(ctrl_err)
                print(f"  [warn] control update failed: {safe_ctrl_error[:300]}")
        print(f"  FAILED routing {src_schema}.{src_table}: {safe[:300]}")
        continue

    try:
        # ---- Oracle metadata (batched with per-table fallback) ----
        cache_key = (str(src_schema).casefold(), str(src_table).casefold())
        col_dicts = batch_col_cache.get(cache_key)
        if not col_dicts:
            cols = read_source_jdbc(
                adapter, adapter.columns_metadata_query(src_db, src_schema, src_table),
                source_server=src_server, source_database=src_db).collect()
            if not cols:
                raise ValueError(
                    "No columns returned from Oracle metadata (check owner casing "
                    "and SELECT grants)")
            col_dicts = [c.asDict(recursive=True) for c in cols]
        inv_common.validate_metadata_aliases(col_dicts[0].keys())

        if cache_key in batch_pk_cache:
            pk_cols = batch_pk_cache[cache_key]
        else:
            pk_cols = [
                pr["COLUMN_NAME"] for pr in read_source_jdbc(
                    adapter, adapter.primary_key_query(src_db, src_schema, src_table),
                    source_server=src_server, source_database=src_db).collect()
            ]

        # ---- shared normalization + shared strategy resolution ----
        identity = {"run_id": run_id, "source_table_id": src_id,
                    "connection_id": conn_id, "source_system": SOURCE_SYSTEM,
                    "source_server": src_server, "source_database": src_db,
                    "source_schema": src_schema, "source_table": src_table}
        table_inventory_rows = [
            inv_common.normalize_inventory_row(c, identity) for c in col_dicts
        ]

        decision = adapter.resolve_watermark_decision(
            inv_common.strategy_columns(col_dicts), pk_cols,
            d.get("watermark_column"))
        table_written = persist_inventory_rows(table_inventory_rows)

        strategy_payload = inv_common.build_strategy_payload(
            src_id, decision, pk_cols
        )

        # Identity fields are immutable and are already passed separately.
        strategy_payload.pop("source_table_id", None)
        strategy_payload.pop("connection_id", None)

        repo.update_control_for_connection(
            conn_id,
            src_id,
            strategy_payload
        )
        written += table_written
        succeeded += 1
        print(f"  [oracle] {src_schema}.{src_table}: {len(col_dicts)} cols, "
              f"strategy={decision['strategy']}, wm={decision['watermark_column']}; "
              f"reason={decision['reason']}")
    except Exception as e:
        failed += 1
        safe = failcls.sanitize_message(e)
        try:
            repo.update_control_for_connection(
                conn_id, src_id, {"current_status": "INVENTORY_FAILED",
                                  "error_message": safe[:1000]})
        except Exception as ctrl_err:
            safe_ctrl_error = failcls.sanitize_message(ctrl_err)
            print(f"  [warn] control update failed: {safe_ctrl_error[:300]}")
        print(f"  FAILED oracle {src_schema}.{src_table}: {safe[:300]}")

print(f"Wrote {written} inventory rows. succeeded={succeeded} failed={failed}")

try:
    dbutils.jobs.taskValues.set(key="run_id", value=run_id)
    dbutils.jobs.taskValues.set(key="connection_id", value=connection_id)
    dbutils.jobs.taskValues.set(key="candidate_table_count", value=len(active))
    dbutils.jobs.taskValues.set(key="inventoried_table_count", value=succeeded)
    dbutils.jobs.taskValues.set(key="failed_table_count", value=failed)
    dbutils.jobs.taskValues.set(key="columns_written", value=written)
    dbutils.jobs.taskValues.set(key="databases_processed", value=len(tables_by_db))
    dbutils.jobs.taskValues.set(key="status", value="FAILED" if failed else "SUCCEEDED")
    dbutils.jobs.taskValues.set(key="business_status", value="PARTIAL" if failed and succeeded else ("FAILED" if failed else "COMPLETE"))
except Exception:
    pass

inventory_result = {
    "status": "FAILED" if failed else "SUCCEEDED",
    "execution_status": "FAILED" if failed else "SUCCEEDED",
    "business_status": "PARTIAL" if failed and succeeded else
                       ("FAILED" if failed else "COMPLETE"),
    "run_id": run_id,
    "connection_id": connection_id,
    "source_system": SOURCE_SYSTEM,
    "candidate_table_count": len(active),
    "inventoried_table_count": succeeded,
    "failed_table_count": failed,
    "databases_processed": len(tables_by_db),
    "tables_succeeded": succeeded,
    "tables_failed": failed,
    "columns_written": written,
    # Backward-compatible aliases.
    "tables": succeeded,
    "failed": failed,
    "columns": written,
}
if failed:
    print(json.dumps(inventory_result))
    raise RuntimeError(
        f"Source inventory failed: tables_succeeded={succeeded}, "
        f"tables_failed={failed}")

dbutils.notebook.exit(json.dumps(inventory_result))