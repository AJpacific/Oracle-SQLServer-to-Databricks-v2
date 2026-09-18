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
    r for r in repo.active_tables_for_connection(
        connection_id, include_onboarding=True).collect()
    if require_source_system(
        r.asDict().get("source_system"), "source_table_control row")
    == SOURCE_SYSTEM
]
print(f"Active Oracle tables to inventory: {len(active)}")

# COMMAND ----------

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
        # ---- Oracle metadata (ALL_TAB_COLUMNS / ALL_CONS_COLUMNS) ----
        cols = read_source_jdbc(
            adapter, adapter.columns_metadata_query(src_db, src_schema, src_table),
            source_server=src_server, source_database=src_db).collect()
        if not cols:
            raise ValueError(
                "No columns returned from Oracle metadata (check owner casing "
                "and SELECT grants)")
        col_dicts = [c.asDict(recursive=True) for c in cols]
        inv_common.validate_metadata_aliases(col_dicts[0].keys())

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
        repo.update_control_for_connection(
            conn_id, src_id,
            inv_common.build_strategy_payload(src_id, decision, pk_cols))
        written += table_written
        succeeded += 1
        print(f"  [oracle] {src_schema}.{src_table}: {len(cols)} cols, "
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

inventory_result = {
    "status": "FAILED" if failed else "SUCCEEDED",
    "execution_status": "FAILED" if failed else "SUCCEEDED",
    "business_status": "PARTIAL" if failed and succeeded else
                       ("FAILED" if failed else "COMPLETE"),
    "run_id": run_id,
    "connection_id": connection_id,
    "source_system": SOURCE_SYSTEM,
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