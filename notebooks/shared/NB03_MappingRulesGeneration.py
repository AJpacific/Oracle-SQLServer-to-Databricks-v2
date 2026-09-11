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

# One mapper per source dialect; selected per row from source_system. The Oracle
# NUMBER path never runs for SQL Server numeric/decimal and vice versa.
_mapper_cache = {}

def mapper_for(source_system):
    key = normalize_source_system(source_system or "oracle")
    if key not in _mapper_cache:
        _mapper_cache[key] = load_type_mapper(key)
    return _mapper_cache[key]

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
    src_system = r["source_system"] or "oracle"
    try:
        mapper = mapper_for(src_system)
        res = mapper.map_column(
            source_type=r["raw_type"],
            precision=r["precision"],
            scale=r["scale"],
            length=r["length"],
            is_nullable=r["is_nullable"],
        )
        status, fidelity, notes = res.status, res.fidelity, res.notes
        if normalize_source_system(src_system) == "sqlserver":
            if bool(r["is_hidden"]):
                status, fidelity = "BLOCKED", "UNKNOWN"
                notes = "SQL Server hidden/system-generated column is not migrated automatically"
            elif bool(r["is_computed"]):
                status, fidelity = "REVIEW", "UNKNOWN"
                notes = "SQL Server computed column requires explicit approval before materialization"
        mapped.append((
            run_id, r["source_table_id"], r["connection_id"], src_system,
            r["source_schema"], r["source_table"], r["column_name"],
            int(r["ordinal_position"]), res.source_type, res.databricks_delta_type,
            status, fidelity, notes, bool(r["is_nullable"]),
            bool(r["is_identity"]), bool(r["is_computed"]), bool(r["is_hidden"]),
            bool(r["is_rowversion"]), r["source_type_schema"],
        ))
    except Exception as exc:
        mapped.append((
            run_id, r["source_table_id"], r["connection_id"], src_system,
            r["source_schema"], r["source_table"], r["column_name"],
            int(r["ordinal_position"]), r["raw_type"], None,
            "BLOCKED", "UNKNOWN",
            f"Mapping failed with {type(exc).__name__}: {str(exc)[:500]}",
            bool(r["is_nullable"]), bool(r["is_identity"]),
            bool(r["is_computed"]), bool(r["is_hidden"]),
            bool(r["is_rowversion"]), r["source_type_schema"],
        ))
        print(
            "BLOCKED mapping:",
            f"[{src_system}] {r['source_schema']}.{r['source_table']}.{r['column_name']}",
            type(exc).__name__, str(exc)[:500],
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
        StructField("source_type_schema", StringType(), True)
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