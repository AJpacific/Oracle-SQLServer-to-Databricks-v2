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
print("run_id:", run_id)
repo = control_repo()

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

maps = spark.sql(
    f"SELECT source_table_id, source_system, source_schema, source_table, "
    f"mapping_status FROM {ctrl('resolved_column_mappings')} "
    f"WHERE run_id = {escape_string_literal(run_id)}"
).collect()

# Aggregate by the source-qualified id so two sources that share a schema.table
# are decided independently and never merged together.
agg = defaultdict(lambda: {"total": 0, "blocked": 0, "review": 0,
                           "system": None, "schema": None, "table": None})
for r in maps:
    key = r["source_table_id"]
    agg[key]["total"] += 1
    agg[key]["system"] = r["source_system"]
    agg[key]["schema"] = r["source_schema"]
    agg[key]["table"] = r["source_table"]
    st = (r["mapping_status"] or "").upper()
    if st == "BLOCKED":
        agg[key]["blocked"] += 1
    elif st == "REVIEW":
        agg[key]["review"] += 1

# COMMAND ----------

decisions = []
for src_id, c in agg.items():
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

    decisions.append((run_id, src_id, c["system"], schema, table, decision, reason,
                      c["blocked"], c["review"], c["total"]))

    # Write the decision back to the master control table (keyed by id).
    repo.update_control(src_id, {
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

if decisions:

    table_decisions_schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("source_table_id", StringType(), True),
        StructField("source_system", StringType(), True),
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
        .saveAsTable(
            ctrl("table_load_decisions").replace("`", "")
        )

    df.groupBy("decision").count().show()

    # Maintain the review_queue as an explicit human-review boundary.
    # Tables that can't auto-migrate are surfaced here instead of silently skipped.
    pending = (
        df.filter(F.col("decision") != "AUTO_MIGRATE")
          .select(
              "source_table_id",
              "source_system",
              "source_schema",
              "source_table",
              "decision",
              "reason",
              "blocked_columns",
              "review_columns",
              "total_columns",
              "run_id",
              "captured_ts"
          )
          .withColumn("review_status", F.lit("PENDING_REVIEW"))
    )

    if pending.count() > 0:
        pending.createOrReplaceTempView("rq_pending")

        spark.sql(f"""
            MERGE INTO {ctrl('review_queue')} tgt
            USING rq_pending src
            ON tgt.source_table_id = src.source_table_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    # Tables that now auto-migrate are resolved out of the queue.
    resolved = (
        df.filter(F.col("decision") == "AUTO_MIGRATE")
          .select(
              "source_table_id"
          )
    )

    if resolved.count() > 0:
        resolved.createOrReplaceTempView("rq_resolved")

        spark.sql(f"""
            MERGE INTO {ctrl('review_queue')} tgt
            USING rq_resolved src
            ON tgt.source_table_id = src.source_table_id
            WHEN MATCHED THEN UPDATE SET review_status = 'RESOLVED',
                                         captured_ts = current_timestamp()
        """)

else:
    print("No table decisions produced.")

dbutils.notebook.exit(
    json.dumps({
        "status": "SUCCEEDED",
        "run_id": run_id,
        "tables": len(decisions)
    })
)