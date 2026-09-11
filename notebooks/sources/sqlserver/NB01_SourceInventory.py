# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server / NB01_SourceInventory
# MAGIC Reads SQL Server column and primary-key metadata for every registered
# MAGIC active table through the SQL Server adapter, interprets the computed /
# MAGIC hidden / identity / rowversion flags, resolves the load strategy using the
# MAGIC SQL Server watermark policy, and writes the **shared** normalized
# MAGIC `source_inventory` shape. Normalization, persistence, and control updates
# MAGIC are shared, so this notebook owns only the SQL Server metadata calls.

# COMMAND ----------

# MAGIC %run ../../shared/_common

# COMMAND ----------

SOURCE_SYSTEM = "sqlserver"

run_id = get_run_id()
connection_id = CONNECTION_ID
repo = control_repo()
print("run_id:", run_id, "| connection_id:", connection_id or "(all SQL Server rows)")

# COMMAND ----------

active = [
    r for r in repo.active_tables(connection_id=(connection_id or None)).collect()
    if normalize_source_system(r.asDict().get("source_system") or SOURCE_SYSTEM)
    == SOURCE_SYSTEM
]
print(f"Active SQL Server tables to inventory: {len(active)}")

# COMMAND ----------

inventory_rows = []
succeeded, failed = 0, 0

for r in active:
    d = r.asDict()
    src_server = d.get("source_server")
    src_db = d.get("source_database")
    src_schema, src_table = r["source_schema"], r["source_table"]
    src_id = d.get("source_table_id")
    conn_id = d.get("connection_id")

    try:
        if not src_db:
            raise ValueError("SQL Server rows require source_database")
        if not src_id:
            src_id = compute_source_table_id(SOURCE_SYSTEM, src_server, src_db,
                                             src_schema, src_table)
            repo.update_control_by_identity(SOURCE_SYSTEM, src_server, src_db,
                                            src_schema, src_table,
                                            {"source_table_id": src_id})
        adapter = get_source_adapter_routed(r)
    except Exception as e:
        failed += 1
        safe = failcls.sanitize_message(e)
        if src_id:
            try:
                repo.update_control(src_id, {
                    "current_status": "INVENTORY_FAILED",
                    "error_message": f"source routing failed: {safe[:900]}"})
            except Exception as ctrl_err:
                print(f"  [warn] control update failed: {ctrl_err}")
        print(f"  FAILED routing {src_schema}.{src_table}: {safe[:300]}")
        continue

    try:
        # ---- SQL Server metadata (sys.columns / sys.indexes catalog views) ----
        cols = read_source_jdbc(
            adapter, adapter.columns_metadata_query(src_db, src_schema, src_table),
            source_server=src_server, source_database=src_db).collect()
        if not cols:
            raise ValueError(
                "No columns returned from SQL Server metadata (check the "
                "database/schema/table names and SELECT grants)")
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
        inventory_rows.extend(
            inv_common.normalize_inventory_row(c, identity) for c in col_dicts)

        decision = adapter.resolve_watermark_decision(
            inv_common.strategy_columns(col_dicts), pk_cols,
            d.get("watermark_column"))
        repo.update_control(
            src_id, inv_common.build_strategy_payload(src_id, decision, pk_cols))
        succeeded += 1
        print(f"  [sqlserver] {src_schema}.{src_table}: {len(cols)} cols, "
              f"strategy={decision['strategy']}, wm={decision['watermark_column']}; "
              f"reason={decision['reason']}")
    except Exception as e:
        failed += 1
        safe = failcls.sanitize_message(e)
        try:
            repo.update_control(src_id, {"current_status": "INVENTORY_FAILED",
                                         "error_message": safe[:1000]})
        except Exception as ctrl_err:
            print(f"  [warn] control update failed: {ctrl_err}")
        print(f"  FAILED sqlserver {src_schema}.{src_table}: {safe[:300]}")

# COMMAND ----------

written = persist_inventory_rows(inventory_rows)
print(f"Wrote {written} inventory rows. succeeded={succeeded} failed={failed}")

dbutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "run_id": run_id, "connection_id": connection_id,
    "source_system": SOURCE_SYSTEM, "tables": succeeded, "failed": failed,
    "columns": written,
}))
