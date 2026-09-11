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
print("run_id:", run_id)
repo = control_repo()

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

# Scoped to the onboarding connection when supplied, so one connection's run
# never provisions another connection's active tables.
auto = repo.active_tables(connection_id=(CONNECTION_ID or None),
                          decision="AUTO_MIGRATE").collect()
print("AUTO_MIGRATE tables:", len(auto))

# COMMAND ----------

# Collision guard: if two active source rows resolve to the same target FQN it is
# a configuration error (they would silently overwrite each other), so mark both
# rather than provisioning either.
def _target_fqn(r):
    t_catalog = r["target_catalog"] or CATALOG
    t_schema = r["target_schema"] or r["source_schema"].lower()
    t_table = r["target_table"] or r["source_table"].lower()
    return f"{t_catalog}.{t_schema}.{t_table}"

_fqn_to_ids = {}
for r in auto:
    _fqn_to_ids.setdefault(_target_fqn(r).lower(), set()).add(r["source_table_id"])
_collided_fqns = {fqn for fqn, ids in _fqn_to_ids.items() if len(ids) > 1}

# COMMAND ----------

provisioned, failed = 0, 0
for r in auto:
    src_id = r["source_table_id"]
    s_schema, s_table = r["source_schema"], r["source_table"]
    t_catalog = r["target_catalog"] or CATALOG
    t_schema = r["target_schema"] or s_schema.lower()
    t_table = r["target_table"] or s_table.lower()
    target_fqn = f"{t_catalog}.{t_schema}.{t_table}"

    if target_fqn.lower() in _collided_fqns:
        failed += 1
        repo.update_control(src_id, {
            "current_status": "PROVISION_CONFIG_ERROR",
            "error_message": ("target FQN collision: multiple active source rows "
                              f"resolve to {target_fqn}; disambiguate target_schema/"
                              "target_table before provisioning"),
        })
        print(f"  CONFIG ERROR (collision) {s_schema}.{s_table} -> {target_fqn}")
        continue

    try:
        # Pull the approved mapping (latest run for this source table), ordered.
        cols = spark.sql(f"""
            SELECT column_name, databricks_delta_type, mapping_status,
                   is_nullable, ordinal_position, is_computed, is_hidden
            FROM {ctrl('resolved_column_mappings')}
            WHERE run_id = {escape_string_literal(run_id)}
              AND source_table_id = {escape_string_literal(src_id)}
            ORDER BY ordinal_position
        """).collect()
        if not cols:
            raise Exception("no resolved mappings found for this run")
        unsafe = [
            c["column_name"]
            for c in cols
            if (c["mapping_status"] or "").upper() != "AUTO"
            or not c["databricks_delta_type"]
            or bool(c["is_computed"]) or bool(c["is_hidden"])
        ]
        if unsafe:
            raise Exception(
                "unsafe or incomplete mappings: " + ", ".join(unsafe))

        col_specs = [(c["column_name"], c["databricks_delta_type"], bool(c["is_nullable"]))
                     for c in cols]

        spark.sql(ddl.build_create_schema(t_catalog, t_schema, "migrated data"))
        spark.sql(ddl.build_create_table(t_catalog, t_schema, t_table, col_specs))

        repo.update_control(
            src_id,
            {
                "target_catalog": t_catalog,
                "target_schema": t_schema,
                "target_table": t_table,
                "current_status": "PROVISIONED",
                "error_message": None,
            },
        )
        provisioned += 1
        print(f"  provisioned {t_catalog}.{t_schema}.{t_table} ({len(col_specs)} cols)")
    except Exception as e:
        failed += 1
        repo.update_control(src_id, {"current_status": "PROVISION_FAILED",
                                     "error_message": str(e)[:1000]})
        print(f"  FAILED {s_schema}.{s_table}: {e}")

# COMMAND ----------

print(f"Provisioned={provisioned} Failed={failed}")
if failed > 0:
    raise Exception(f"{failed} table(s) failed provisioning; see control table.")

dbutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "run_id": run_id,
                                  "provisioned": provisioned}))