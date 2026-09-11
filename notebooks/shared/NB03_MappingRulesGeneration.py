# Databricks notebook source
# MAGIC %md
# MAGIC # NB03_MappingRulesGeneration
# MAGIC Applies the source-specific Oracle or SQL Server rules to every normalized
# MAGIC column and writes resolved_column_mappings. SQL Server computed columns are
# MAGIC REVIEW and hidden columns are BLOCKED before target provisioning.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F

run_id = get_run_id()
print("run_id:", run_id)

# One adapter per source dialect, resolved through the factory. The adapter
# supplies both the type rules and the column policy, so this notebook contains
# no dialect branch.
_adapter_cache = {}

def adapter_for(source_system):
    key = require_source_system(source_system, "normalized inventory row")
    if key not in _adapter_cache:
        _adapter_cache[key] = build_adapter(key)
    return _adapter_cache[key]

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

_conn_filter = (f" AND connection_id = {escape_string_literal(CONNECTION_ID)}"
                if CONNECTION_ID else "")

norm = spark.sql(
    f"SELECT * FROM {ctrl('normalized_source_inventory')} "
    f"WHERE run_id = {escape_string_literal(run_id)}{_conn_filter}"
).collect()
print("Columns to map:", len(norm))

# COMMAND ----------

mapped = []
for r in norm:
    src_system = require_source_system(
        r["source_system"], "normalized inventory row")
    try:
        adapter = adapter_for(src_system)
        res = adapter.load_type_mapper().map_column(
            source_type=r["raw_type"],
            precision=r["precision"],
            scale=r["scale"],
            length=r["length"],
            is_nullable=r["is_nullable"],
        )
        # The source owns its own column policy; this notebook persists only the
        # normalized result and never interprets a dialect metadata concept.
        policy = adapter.apply_column_policy(r.asDict(), res)
        mapped.append((
            run_id, r["source_table_id"], r["connection_id"], src_system,
            r["source_schema"], r["source_table"], r["column_name"],
            int(r["ordinal_position"]), res.source_type, res.databricks_delta_type,
            policy.mapping_status, policy.mapping_fidelity, policy.notes,
            bool(r["is_nullable"]),
            bool(r["is_identity"]), bool(r["is_computed"]), bool(r["is_hidden"]),
            bool(r["is_rowversion"]), r["source_type_schema"],
            bool(policy.include_column), bool(policy.is_writable),
            bool(policy.requires_review), policy.policy_code,
        ))
    except Exception as exc:
        safe_error = failcls.sanitize_message(exc)
        mapped.append((
            run_id, r["source_table_id"], r["connection_id"], src_system,
            r["source_schema"], r["source_table"], r["column_name"],
            int(r["ordinal_position"]), r["raw_type"], None,
            "BLOCKED", "UNKNOWN",
            f"Mapping failed with {type(exc).__name__}: "
            f"{safe_error[:500]}",
            bool(r["is_nullable"]), bool(r["is_identity"]),
            bool(r["is_computed"]), bool(r["is_hidden"]),
            bool(r["is_rowversion"]), r["source_type_schema"],
            False, False, True, None,
        ))
        print(
            "BLOCKED mapping:",
            f"[{src_system}] {r['source_schema']}.{r['source_table']}.{r['column_name']}",
            type(exc).__name__, safe_error[:500],
        )

if norm and not mapped:
    raise RuntimeError("Normalized input exists but no mapping rows were produced.")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType
)

if mapped:

    resolved_mapping_schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("source_table_id", StringType(), True),
        StructField("connection_id", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("source_schema", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("ordinal_position", IntegerType(), True),
        StructField("source_type", StringType(), True),
        StructField("databricks_delta_type", StringType(), True),
        StructField("mapping_status", StringType(), True),
        StructField("fidelity", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("is_nullable", BooleanType(), True),
        StructField("is_identity", BooleanType(), True),
        StructField("is_computed", BooleanType(), True),
        StructField("is_hidden", BooleanType(), True),
        StructField("is_rowversion", BooleanType(), True),
        StructField("source_type_schema", StringType(), True),
        StructField("include_column", BooleanType(), True),
        StructField("is_writable", BooleanType(), True),
        StructField("requires_review", BooleanType(), True),
        StructField("policy_code", StringType(), True)
    ])

    df = (
        spark.createDataFrame(
            mapped,
            schema=resolved_mapping_schema
        )
        .withColumn("captured_ts", F.current_timestamp())
    )

    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(
            ctrl("resolved_column_mappings").replace("`", "")
        )

    # quick summary
    df.groupBy("mapping_status").count().show()

    print(f"Wrote {len(mapped)} mappings.")

else:
    print("No columns to map.")

dbutils.notebook.exit(
    json.dumps({
        "status": "SUCCEEDED",
        "run_id": run_id,
        "columns": len(mapped)
    })
)