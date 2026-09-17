# Databricks notebook source
# MAGIC %md
# MAGIC # NB01B_RegisterSelectedTables
# MAGIC INGEST-pipeline task that registers explicitly selected assessed tables
# MAGIC into `source_table_control`. Only COMPATIBLE and REVIEW tables are
# MAGIC registered; every new row is inactive (`is_active=false`,
# MAGIC `current_status=REGISTERED`) and must be reviewed/activated separately.
# MAGIC It preserves existing watermark, initial-load, successful-run, and manually
# MAGIC assigned target names, computes the deterministic connection-owned
# MAGIC `source_table_id`, and blocks target-name collisions before the MERGE.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

dbutils.widgets.text("assessment_id", "")
dbutils.widgets.text("connection_id", "")
dbutils.widgets.text("selected_schemas", "")
dbutils.widgets.text("selected_tables", "")
dbutils.widgets.text("target_catalog", "")
dbutils.widgets.dropdown("target_schema_mode", "SOURCE_SCHEMA",
                         ["SOURCE_SCHEMA", "PREFIX_WITH_DATABASE", "EXPLICIT"])
dbutils.widgets.text("target_schema", "")

assessment_id = dbutils.widgets.get("assessment_id").strip()
connection_id = dbutils.widgets.get("connection_id").strip()
selected_schemas = {s.strip() for s in dbutils.widgets.get("selected_schemas").split(",") if s.strip()}
selected_tables = {s.strip() for s in dbutils.widgets.get("selected_tables").split(",") if s.strip()}
target_catalog = dbutils.widgets.get("target_catalog").strip() or CATALOG
target_schema_mode = dbutils.widgets.get("target_schema_mode").strip()
explicit_target_schema = dbutils.widgets.get("target_schema").strip()

if not assessment_id or not connection_id:
    raise ValueError("assessment_id and connection_id are required")
if target_schema_mode == "EXPLICIT" and not explicit_target_schema:
    raise ValueError("EXPLICIT target_schema_mode requires target_schema")

repo = control_repo()
connection = require_valid_connection(connection_id)
connection_data = connection.asDict()

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

# The assessment ID must have one connection owner. Filtering only the table
# rows would otherwise turn a wrong assessment/connection pair into a silent
# no-op.
assessment_owners = spark.sql(f"""
    SELECT DISTINCT connection_id, source_system, source_server, source_database
    FROM {ctrl('source_assessment')}
    WHERE assessment_id = {escape_string_literal(assessment_id)}
      AND connection_id = {escape_string_literal(connection_id)}
""").collect()
if len(assessment_owners) != 1:
    raise ValueError(
        f"assessment_id {assessment_id!r} resolves to "
        f"{len(assessment_owners)} metadata owner(s) for connection_id "
        f"{connection_id!r}; expected exactly one")
assessment_owner = assessment_owners[0]
assert_table_connection_match(assessment_owner, connection_id)
assert_source_identity_match(assessment_owner, connection)

# Read TABLE objects for the exact assessment + connection; register only those
# explicitly selected and only when COMPATIBLE or REVIEW.
assessed = spark.sql(f"""
    SELECT connection_id, source_schema, object_name, source_system, source_server,
           source_database, compatibility_status
    FROM {ctrl('source_assessment')}
    WHERE assessment_id = {escape_string_literal(assessment_id)}
      AND connection_id = {escape_string_literal(connection_id)}
      AND object_type = 'TABLE'
""").collect()
print(f"Assessed tables in scope: {len(assessed)}")

# COMMAND ----------

def _selected(schema, obj):
    if selected_tables:
        return f"{schema}.{obj}" in selected_tables or obj in selected_tables
    if selected_schemas:
        return schema in selected_schemas
    return False

def _target_schema(schema, database):
    if target_schema_mode == "EXPLICIT":
        return explicit_target_schema
    if target_schema_mode == "PREFIX_WITH_DATABASE":
        return f"{(database or '')}_{schema}".lower().strip("_")
    return schema.lower()

candidates = []
skipped = []
for r in assessed:
    assert_source_identity_match(r, connection)
    schema, obj = r["source_schema"], r["object_name"]
    if not _selected(schema, obj):
        continue
    comp = r["compatibility_status"]
    if comp not in ("COMPATIBLE", "REVIEW"):
        skipped.append((schema, obj, f"not registerable ({comp})"))
        continue
    src_server = connection_data.get("source_server")
    src_db = connection_data.get("source_database")
    src_system = require_source_system(
        connection_data.get("source_system"), "registered connection")
    sid = compute_source_table_id(
        connection_id, src_system, src_server,
        src_db, schema, obj)
    t_schema = _target_schema(schema, src_db)
    t_table = obj.lower()
    candidates.append({
        "source_table_id": sid, "connection_id": connection_id,
        "source_system": src_system, "source_server": src_server,
        "source_database": src_db, "source_schema": schema, "source_table": obj,
        "target_catalog": target_catalog, "target_schema": t_schema,
        "target_table": t_table, "mapping_status": comp,
        "target_fqn": f"{target_catalog}.{t_schema}.{t_table}".lower(),
    })

print(f"Selected {len(candidates)} table(s); skipped {len(skipped)}.")

# COMMAND ----------

# ---- collision detection BEFORE any MERGE ----------------------------------
# 1) Two selected tables resolving to the same target FQN.
from collections import Counter
fqn_counts = Counter(c["target_fqn"] for c in candidates)
internal_collisions = {f for f, n in fqn_counts.items() if n > 1}

# 2) An existing active control row (different identity) using the same target FQN,
#    plus the existing connection_id so a conflicting one can be blocked.
existing = spark.sql(f"""
    SELECT source_table_id, connection_id,
           lower(concat_ws('.', coalesce(target_catalog, '{CATALOG}'),
                 coalesce(target_schema, lower(source_schema)),
                 coalesce(target_table, lower(source_table)))) AS target_fqn
    FROM {ctrl('source_table_control')}
    WHERE is_active = true
""").collect()
existing_fqn = {}
for e in existing:
    existing_fqn.setdefault(e["target_fqn"], set()).add(
        (e["connection_id"], e["source_table_id"]))

valid = []
conflicts = []
for c in candidates:
    if c["target_fqn"] in internal_collisions:
        skipped.append((c["source_schema"], c["source_table"],
                        f"target collision within selection: {c['target_fqn']}"))
        continue
    owners = existing_fqn.get(c["target_fqn"], set())
    if owners - {(connection_id, c["source_table_id"])}:
        skipped.append((c["source_schema"], c["source_table"],
                        f"target collision with existing row: {c['target_fqn']}"))
        continue
    valid.append(c)

print(f"Registerable after collision check: {len(valid)}; "
      f"connection conflicts: {len(conflicts)}")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F

worklist = []
if valid:
    reg_rows = [Row(
        source_table_id=c["source_table_id"], connection_id=c["connection_id"],
        source_system=c["source_system"], source_server=c["source_server"],
        source_database=c["source_database"], source_schema=c["source_schema"],
        source_table=c["source_table"], target_catalog=c["target_catalog"],
        target_schema=c["target_schema"], target_table=c["target_table"],
        mapping_status=c["mapping_status"],
        source_identity_version=SOURCE_IDENTITY_VERSION,
        legacy_source_table_id=None) for c in valid]
    src_df = (spark.createDataFrame(reg_rows)
              .withColumn("is_active", F.lit(False))
              .withColumn("current_status", F.lit("REGISTERED"))
              .withColumn("initial_load_completed", F.lit(False))
              .withColumn("delete_policy", F.lit("IGNORE_DELETES"))
              .withColumn("created_ts", F.current_timestamp())
              .withColumn("updated_ts", F.current_timestamp()))
    src_df.createOrReplaceTempView("_register_rows")
    # An existing row keeps ALL operational state (watermark, initial-load
    # completion, successful-run info, decision, manual target names). The only
    # New rows start inactive and are never auto-activated.
    spark.sql(f"""
        MERGE INTO {ctrl('source_table_control')} t
        USING _register_rows s
                    ON t.connection_id = s.connection_id
                 AND t.source_table_id = s.source_table_id
        WHEN NOT MATCHED THEN INSERT (
            source_table_id, connection_id, source_system, source_server,
            source_database, source_schema, source_table, target_catalog,
            target_schema, target_table, mapping_status, is_active,
            current_status, initial_load_completed, delete_policy,
                        source_identity_version, legacy_source_table_id,
                        created_ts, updated_ts
        ) VALUES (
            s.source_table_id, s.connection_id, s.source_system, s.source_server,
            s.source_database, s.source_schema, s.source_table, s.target_catalog,
            s.target_schema, s.target_table, s.mapping_status, s.is_active,
            s.current_status, s.initial_load_completed, s.delete_policy,
            s.source_identity_version, s.legacy_source_table_id,
            s.created_ts, s.updated_ts
        )
    """)
    for c in valid:
        worklist.append({"connection_id": connection_id,
                         "source_table_id": c["source_table_id"]})

# COMMAND ----------

# Mark ONLY the assessment rows that were actually registered. Unrequested,
# skipped, collided, conflicting, MANUAL, and UNABLE_TO_ASSESS rows stay false.
if valid:
    sel_rows = [Row(assessment_id=assessment_id, connection_id=connection_id,
                    source_schema=c["source_schema"], object_name=c["source_table"])
                for c in valid]
    spark.createDataFrame(sel_rows).createOrReplaceTempView("_selected_objects")
    spark.sql(f"""
        MERGE INTO {ctrl('source_assessment')} t
        USING _selected_objects s
          ON t.assessment_id = s.assessment_id
         AND t.connection_id = s.connection_id
         AND t.source_schema = s.source_schema
         AND t.object_name  = s.object_name
         AND t.object_type  = 'TABLE'
        WHEN MATCHED THEN UPDATE SET t.is_selected = true
    """)

for s in skipped:
    print("  SKIP", s)

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "assessment_id": assessment_id,
    "connection_id": connection_id, "registered": len(valid),
    "skipped": len(skipped), "conflicts": conflicts, "worklist": worklist,
}))
