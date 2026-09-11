# Databricks notebook source
# MAGIC %md
# MAGIC # NB02_TypeNormalization
# MAGIC Converts Oracle or SQL Server column metadata into a target-neutral
# MAGIC representation, preserves source-specific safety flags, and computes a
# MAGIC per-table schema hash. Idempotent per run_id.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import hashlib
from pyspark.sql import functions as F, Row

run_id = get_run_id()
print("run_id:", run_id)

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

# Optional connection scoping: a per-connection onboarding run must not consume
# another connection's rows from the same run_id.
_conn_filter = (f" AND connection_id = {escape_string_literal(CONNECTION_ID)}"
                if CONNECTION_ID else "")

inv = spark.sql(f"SELECT * FROM {ctrl('source_inventory')} "
                f"WHERE run_id = {escape_string_literal(run_id)}{_conn_filter}")
rows = inv.collect()
print("Inventory rows to normalize:", len(rows))

# COMMAND ----------

def _to_int(v):
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None

out = []
by_table = {}
for r in rows:
    raw = r["data_type"]
    # Type names are lower-cased to form rule keys. The source dialect is NEVER
    # derived from a datatype name; it is carried explicitly as source_system.
    normalized = raw.strip().lower() if raw else raw
    prec = _to_int(r["numeric_precision"])
    scale = _to_int(r["numeric_scale"])
    length = _to_int(r["character_maximum_length"])
    nullable = (str(r["is_nullable"]).upper() == "YES")
    src_id = r["source_table_id"]
    src_system = require_source_system(
        r["source_system"], "source_inventory row")
    conn_id = r["connection_id"]
    # Group by the source-qualified id so the same schema.table on two sources
    # never share a signature or collide.
    key = src_id
    # Include every schema-defining attribute in the deterministic signature.
    by_table.setdefault(key, []).append((
        int(r["ordinal_position"]),
        f"{r['ordinal_position']}:{r['column_name']}:{raw}:{normalized}:"
        f"{prec}:{scale}:{length}:{nullable}:{bool(r['is_identity'])}:"
        f"{bool(r['is_computed'])}:{bool(r['is_hidden'])}:"
        f"{bool(r['is_rowversion'])}:{r['source_type_schema']}"
    ))
    out.append((run_id, src_id, conn_id, src_system, r["source_schema"], r["source_table"],
                r["column_name"], int(r["ordinal_position"]), raw, normalized,
                prec, scale, length, nullable, bool(r["is_identity"]),
                bool(r["is_computed"]), bool(r["is_hidden"]),
                bool(r["is_rowversion"]), r["source_type_schema"]))

# per-source-table schema hash (keyed by source_table_id)
hashes = {}
for key, parts in by_table.items():
    ordered_parts = [
        signature_part
        for _, signature_part in sorted(parts, key=lambda item: item[0])
    ]
    signature = "|".join(ordered_parts)
    hashes[key] = hashlib.sha256(signature.encode("utf-8")).hexdigest()

# COMMAND ----------

from pyspark.sql import functions as F, Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType
)

if out:

    normalized_schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("source_table_id", StringType(), True),
        StructField("connection_id", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("source_schema", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("ordinal_position", IntegerType(), True),
        StructField("raw_type", StringType(), True),
        StructField("normalized_type", StringType(), True),
        StructField("precision", IntegerType(), True),
        StructField("scale", IntegerType(), True),
        StructField("length", IntegerType(), True),
        StructField("is_nullable", BooleanType(), True),
        StructField("is_identity", BooleanType(), True),
        StructField("is_computed", BooleanType(), True),
        StructField("is_hidden", BooleanType(), True),
        StructField("is_rowversion", BooleanType(), True),
        StructField("source_type_schema", StringType(), True)
    ])

    df = spark.createDataFrame(
        out,
        schema=normalized_schema
    )

    hash_rows = [
        Row(
            source_table_id=k,
            schema_hash=v
        )
        for k, v in hashes.items()
    ]

    hdf = spark.createDataFrame(hash_rows)

    df = (
        df.join(
            hdf,
            ["source_table_id"],
            "left"
        )
        .withColumn("captured_ts", F.current_timestamp())
    )

    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(
            ctrl("normalized_source_inventory").replace("`", "")
        )

    print(
        f"Wrote {len(out)} normalized rows for {len(hashes)} tables."
    )

else:
    print("Nothing to normalize.")

dbutils.notebook.exit(
    json.dumps({
        "status": "SUCCEEDED",
        "run_id": run_id,
        "columns": len(out)
    })
)