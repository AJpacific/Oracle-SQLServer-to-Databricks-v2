# Databricks notebook source
# MAGIC %md
# MAGIC # NB02_TypeNormalization
# MAGIC Converts normalized source column metadata into the shared target-neutral
# MAGIC representation, preserves adapter-supplied safety flags, and computes a
# MAGIC per-table schema hash. Idempotent per run_id.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import hashlib
from pyspark.sql import functions as F, Row

run_id = get_run_id()
connection_id = require_connection_id(CONNECTION_ID, "type normalization")
connection = require_valid_connection(connection_id)
print("run_id:", run_id, "| connection_id:", connection_id)

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

inv = spark.sql(f"SELECT * FROM {ctrl('source_inventory')} "
                f"WHERE run_id = {escape_string_literal(run_id)} "
                f"AND connection_id = {escape_string_literal(connection_id)}")
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
    assert_table_connection_match(r, connection_id)
    assert_source_identity_match(r, connection)
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
    key = (conn_id, src_id)
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

spark.sql(f"""
    DELETE FROM {ctrl('normalized_source_inventory')}
    WHERE run_id = {escape_string_literal(run_id)}
      AND connection_id = {escape_string_literal(connection_id)}
""")

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
        Row(connection_id=key[0], source_table_id=key[1], schema_hash=value)
        for key, value in hashes.items()
    ]

    hdf = spark.createDataFrame(hash_rows)

    df = (
        df.join(
            hdf,
            ["connection_id", "source_table_id"],
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
        "connection_id": connection_id,
        "columns": len(out)
    })
)