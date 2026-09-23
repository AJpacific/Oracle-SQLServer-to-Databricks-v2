# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # NB08_TargetProvisioning
# MAGIC For every AUTO_MIGRATE table, creates the target schema and a Delta table
# MAGIC whose columns come from the approved mappings. Idempotent (IF NOT EXISTS).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

run_id = get_run_id()
connection_id = require_connection_id(CONNECTION_ID, "target provisioning")
connection = require_valid_connection(connection_id)
print("run_id:", run_id, "| connection_id:", connection_id)
repo = control_repo()

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

# Onboarding is scoped to exactly one connection. Collision detection remains
# global across all active registrations.
# Equivalent to: repo.active_tables_for_connection(connection_id, decision="AUTO_MIGRATE", include_onboarding=True)
auto = spark.sql(f"""
    SELECT *
    FROM {ctrl('source_table_control')}
    WHERE connection_id = {escape_string_literal(connection_id)}
      AND upper(trim(coalesce(table_decision, ''))) = 'AUTO_MIGRATE'
      AND upper(trim(coalesce(current_status, ''))) IN (
          'READY_FOR_PROVISIONING',
          'PROVISION_FAILED'
      )
""").collect()

print("AUTO_MIGRATE tables to provision:", len(auto))

# COMMAND ----------

# Collision guard: if another registration reserves the same target FQN it is
# a configuration error (they would overwrite each other), so mark rather than provisioning.
# Intentionally retained batch collection: finding target owners in a single query
# avoids executing N separate Spark queries for N tables.
_all_target_rows = spark.sql(f"""
    SELECT connection_id, source_table_id,
           lower(concat_ws('.', coalesce(target_catalog, '{CATALOG}'),
                 coalesce(target_schema, lower(source_schema)),
                 coalesce(target_table, lower(source_table)))) AS target_fqn
    FROM {ctrl('source_table_control')}
    WHERE target_catalog IS NOT NULL AND trim(target_catalog) <> ''
      AND target_schema IS NOT NULL AND trim(target_schema) <> ''
      AND target_table IS NOT NULL AND trim(target_table) <> ''
      AND coalesce(current_status, '') NOT IN ('RETIRED', 'DECOMMISSIONED')
""").collect()

_fqn_to_ids = {}
for r in _all_target_rows:
    key = (r["connection_id"], r["source_table_id"])
    _fqn_to_ids.setdefault(normalize_target_component(r["target_fqn"]), set()).add(key)

# COMMAND ----------

provisioned, failed = 0, 0
for r in auto:
    assert_table_connection_match(r, connection_id)
    assert_current_source_table_identity(r, connection)
    conn_id = r["connection_id"]
    src_id = r["source_table_id"]
    require_source_system(r["source_system"], "source_table_control row")
    s_schema, s_table = r["source_schema"], r["source_table"]
    t_catalog = r["target_catalog"] or CATALOG
    t_schema = r["target_schema"] or s_schema.lower()
    t_table = r["target_table"] or s_table.lower()
    target_fqn = f"{t_catalog}.{t_schema}.{t_table}"

    target_owners = _fqn_to_ids.get(normalize_target_component(target_fqn), set())
    if target_owners - {(conn_id, src_id)}:
        failed += 1
        repo.update_control_for_connection(conn_id, src_id, {
            "current_status": "PROVISION_CONFIG_ERROR",
            "error_message": ("target FQN collision: multiple registrations "
                              f"resolve to {target_fqn}; disambiguate target_schema/"
                              "target_table before provisioning"),
        })
        print(f"  CONFIG ERROR (collision) {s_schema}.{s_table} -> {target_fqn}")
        continue

    try:
        # Use the latest available approved mapping run for this source table.
        mapping_run_rows = spark.sql(f"""
            SELECT run_id
            FROM {ctrl('resolved_column_mappings')}
            WHERE connection_id = {escape_string_literal(conn_id)}
              AND source_table_id = {escape_string_literal(src_id)}
            GROUP BY run_id
            ORDER BY run_id DESC
            LIMIT 1
        """).collect()

        if not mapping_run_rows:
            raise Exception(
                "no resolved mappings found for this source table"
            )

        mapping_run_id = mapping_run_rows[0]["run_id"]

        cols = spark.sql(f"""
            SELECT column_name, databricks_delta_type, mapping_status,
                   is_nullable, ordinal_position, include_column, is_writable
            FROM {ctrl('resolved_column_mappings')}
            WHERE run_id = {escape_string_literal(mapping_run_id)}
              AND connection_id = {escape_string_literal(conn_id)}
              AND source_table_id = {escape_string_literal(src_id)}
            ORDER BY ordinal_position
        """).collect()

        if not cols:
            raise Exception(
                f"no resolved mappings found for mapping run {mapping_run_id}"
            )
        # A column the source policy excluded is simply not provisioned; any
        # other non-AUTO or untyped column is a hard configuration error.
        cols = [c for c in cols if c["include_column"] is not False]
        if not cols:
            raise Exception("source column policy excluded every column")
        unsafe = [
            c["column_name"]
            for c in cols
            if (c["mapping_status"] or "").upper() != "AUTO"
            or not c["databricks_delta_type"]
        ]
        if unsafe:
            raise Exception(
                "unsafe or incomplete mappings: " + ", ".join(unsafe))

        col_specs = [(c["column_name"], c["databricks_delta_type"], bool(c["is_nullable"]))
                     for c in cols]

        spark.sql(ddl.build_create_schema(t_catalog, t_schema, "migrated data"))
        spark.sql(ddl.build_create_table(t_catalog, t_schema, t_table, col_specs))

        repo.update_control_for_connection(
            conn_id, src_id,
            {
                "target_catalog": t_catalog,
                "target_schema": t_schema,
                "target_table": t_table,
                "is_active": True,
                "current_status": "PROVISIONED",
                "error_message": None,
            },
        )
        provisioned += 1
        print(f"  provisioned {t_catalog}.{t_schema}.{t_table} ({len(col_specs)} cols)")
    except Exception as e:
        failed += 1
        safe_error = failcls.sanitize_message(e)
        repo.update_control_for_connection(
            conn_id, src_id,
            {
                "current_status": "PROVISION_FAILED",
                "error_message": safe_error[:1000],
            },
        )
        print(
            f"FAILED {s_schema}.{s_table}: "
            f"{type(e).__name__}: {safe_error[:300]}"
        )

# COMMAND ----------

print(f"Provisioned={provisioned} Failed={failed}")

if failed > 0:
    raise Exception(
        f"{failed} table(s) failed provisioning; see control table."
    )

if len(auto) == 0:
    raise RuntimeError(
        "No AUTO_MIGRATE tables found in READY_FOR_PROVISIONING or "
        "PROVISION_FAILED state. Provisioning cannot report success."
    )

dbutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED",
    "run_id": run_id,
    "connection_id": connection_id,
    "provisioning_candidates": len(auto),
    "provisioned": provisioned,
    "failed": failed,
}))