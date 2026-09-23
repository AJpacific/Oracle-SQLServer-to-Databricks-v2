# Databricks notebook source
# MAGIC %md
# MAGIC # NB07_TableDecisionGeneration
# MAGIC Classifies each table as AUTO_MIGRATE / MANUAL_REVIEW / BLOCKED based on
# MAGIC its column mapping statuses, writes table_load_decisions, and updates
# MAGIC source_table_control.table_decision + mapping_status.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F
from collections import defaultdict
run_id = get_run_id()
connection_id = require_connection_id(CONNECTION_ID, "table decision generation")
connection = require_valid_connection(connection_id)
print("run_id:", run_id, "| connection_id:", connection_id)
repo = control_repo()

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

maps = spark.sql(
    f"SELECT source_table_id, connection_id, source_system, source_server, source_database, "
    f"source_schema, source_table, mapping_status FROM {ctrl('resolved_column_mappings')} "
    f"WHERE run_id = {escape_string_literal(run_id)} "
    f"AND connection_id = {escape_string_literal(connection_id)}"
).collect()

# Aggregate by the source-qualified id so two sources that share a schema.table
# are decided independently and never merged together.
agg = defaultdict(lambda: {"total": 0, "blocked": 0, "review": 0,
                           "system": None, "server": None, "database": None,
                           "schema": None, "table": None,
                           "connection_id": None})
for r in maps:
    assert_table_connection_match(r, connection_id)
    assert_source_identity_match(r, connection)
    key = (r["connection_id"], r["source_table_id"])
    source_system = require_source_system(
        r["source_system"], "resolved mapping row")
    agg[key]["total"] += 1
    agg[key]["system"] = source_system
    agg[key]["server"] = r["source_server"]
    agg[key]["database"] = r["source_database"]
    agg[key]["schema"] = r["source_schema"]
    agg[key]["table"] = r["source_table"]
    agg[key]["connection_id"] = r["connection_id"]
    st = (r["mapping_status"] or "").upper()
    if st == "BLOCKED":
        agg[key]["blocked"] += 1
    elif st == "REVIEW":
        agg[key]["review"] += 1

# COMMAND ----------

decisions = []
for ownership_key, c in agg.items():
    conn_id, src_id = ownership_key
    schema, table = c["schema"], c["table"]
    if c["total"] == 0:
        decision, reason = "BLOCKED", "No columns mapped"
    elif c["blocked"] > 0:
        decision = "BLOCKED"
        reason = f"{c['blocked']} column(s) have no safe mapping"
    elif c["review"] > 0:
        decision = "MANUAL_REVIEW"
        reason = f"{c['review']} column(s) need review before migration"
    else:
        decision = "AUTO_MIGRATE"
        reason = "All columns map safely"

    decisions.append((run_id, src_id, c["connection_id"], c["system"],
                      c["server"], c["database"], schema, table,
                      decision, reason, c["blocked"], c["review"], c["total"]))

    # Write the decision back to the master control table (keyed by id).
    repo.update_control_for_connection(conn_id, src_id, {
        "table_decision": decision,
        "mapping_status": "OK" if decision == "AUTO_MIGRATE" else decision,
        "current_status": (
            "READY_FOR_PROVISIONING"
            if decision == "AUTO_MIGRATE"
            else decision
        ),
        "error_message": None,
    })

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

spark.sql(f"""
    DELETE FROM {ctrl('table_load_decisions')}
    WHERE run_id = {escape_string_literal(run_id)}
      AND connection_id = {escape_string_literal(connection_id)}
""")
spark.sql(f"""
        UPDATE {ctrl('review_queue')}
        SET review_status = 'RESOLVED', captured_ts = current_timestamp()
        WHERE run_id = {escape_string_literal(run_id)}
            AND connection_id = {escape_string_literal(connection_id)}
""")

if decisions:

    table_decisions_schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("source_table_id", StringType(), True),
        StructField("connection_id", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("source_server", StringType(), True),
        StructField("source_database", StringType(), True),
        StructField("source_schema", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("decision", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("blocked_columns", IntegerType(), True),
        StructField("review_columns", IntegerType(), True),
        StructField("total_columns", IntegerType(), True)
    ])

    df = (
        spark.createDataFrame(
            decisions,
            schema=table_decisions_schema
        )
        .withColumn("captured_ts", F.current_timestamp())
    )

    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(
            ctrl("table_load_decisions").replace("`", "")
        )

    df.groupBy("decision").count().show()

    # Maintain the review_queue as an explicit human-review boundary.
    # Tables that can't auto-migrate are surfaced here instead of silently skipped.
    pending = (
        df.filter(F.col("decision") != "AUTO_MIGRATE")
          .select(
              "connection_id",
              "source_table_id",
              "source_system",
              "source_server",
              "source_database",
              "source_schema",
              "source_table",
              "decision",
              "reason",
              "blocked_columns",
              "review_columns",
              "total_columns",
              F.lit("PENDING_REVIEW").alias("review_status"),
              "run_id",
              "captured_ts"
          )
    )

    if pending.count() > 0:
        pending.createOrReplaceTempView("rq_pending")

        spark.sql(f"""
            MERGE INTO {ctrl('review_queue')} tgt
            USING rq_pending src
            ON tgt.run_id = src.run_id
           AND tgt.connection_id = src.connection_id
           AND tgt.source_table_id = src.source_table_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    # Tables that now auto-migrate are resolved out of the queue.
    resolved = (
        df.filter(F.col("decision") == "AUTO_MIGRATE")
          .select(
              "run_id", "connection_id", "source_table_id"
          )
    )

    if resolved.count() > 0:
        resolved.createOrReplaceTempView("rq_resolved")

        spark.sql(f"""
            MERGE INTO {ctrl('review_queue')} tgt
            USING rq_resolved src
            ON tgt.run_id = src.run_id
           AND tgt.connection_id = src.connection_id
           AND tgt.source_table_id = src.source_table_id
            WHEN MATCHED THEN UPDATE SET review_status = 'RESOLVED',
                                         captured_ts = current_timestamp()
        """)

else:
    print("No table decisions produced.")

dbutils.notebook.exit(
    json.dumps({
        "status": "SUCCEEDED",
        "run_id": run_id,
        "connection_id": connection_id,
        "tables": len(decisions)
    })
)