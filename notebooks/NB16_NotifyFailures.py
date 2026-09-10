# Databricks notebook source
# MAGIC %md
# MAGIC # NB16_NotifyFailures
# MAGIC Shared task that summarizes failures for one run across job_run_log,
# MAGIC table_run_log, reconciliation_results and dq_result, and optionally posts a
# MAGIC concise message to a Microsoft Teams incoming webhook. The webhook is read
# MAGIC only from a secret scope and is never printed or logged. The summary never
# MAGIC includes source data or credentials. No escalation/paging logic.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("pipeline_name", "")
dbutils.widgets.dropdown("notification_mode", "NONE", ["NONE", "TEAMS_WEBHOOK"])
dbutils.widgets.text("teams_webhook_secret_scope", "")
dbutils.widgets.text("teams_webhook_secret_key", "")

run_id = dbutils.widgets.get("run_id").strip() or get_run_id()
pipeline_name = dbutils.widgets.get("pipeline_name").strip() or "PIPELINE"
mode = dbutils.widgets.get("notification_mode").strip()
wh_scope = dbutils.widgets.get("teams_webhook_secret_scope").strip()
wh_key = dbutils.widgets.get("teams_webhook_secret_key").strip()

def ctrl(t):
    return f"{quote_databricks(CATALOG)}.{quote_databricks(CONTROL_SCHEMA)}.{quote_databricks(t)}"

# COMMAND ----------

# ---- collect failures (sanitized, no source data / credentials) ------------
table_failures = spark.sql(f"""
    SELECT source_table_id, connection_id, source_schema, source_table, operation,
           failure_stage, error_category, retry_eligible, error_message
    FROM {ctrl('table_run_log')}
    WHERE run_id = {escape_string_literal(run_id)} AND status = 'FAILED'
""").collect()

recon_failures = spark.sql(f"""
    SELECT source_table_id, check_type, status, message
    FROM {ctrl('reconciliation_results')}
    WHERE run_id = {escape_string_literal(run_id)} AND status = 'FAIL'
""").collect()

dq_failures = spark.sql(f"""
    SELECT source_table_id, rule_type, failed_count, status
    FROM {ctrl('dq_result')}
    WHERE run_id = {escape_string_literal(run_id)} AND status = 'FAIL'
""").collect()

job_failures = spark.sql(f"""
    SELECT job_name, status, message FROM {ctrl('job_run_log')}
    WHERE run_id = {escape_string_literal(run_id)} AND status <> 'SUCCEEDED'
""").collect()

total = len(table_failures) + len(recon_failures) + len(dq_failures) + len(job_failures)

# COMMAND ----------

lines = [f"[{pipeline_name}] run {run_id}: {total} failure(s)"]
for r in table_failures[:50]:
    lines.append(
        f"- TABLE {r['source_schema']}.{r['source_table']} "
        f"(conn={r['connection_id']}) op={r['operation']} "
        f"stage={r['failure_stage']} cat={r['error_category']} "
        f"retry={r['retry_eligible']}: {failcls.sanitize_message(r['error_message'])[:200]}")
for r in recon_failures[:50]:
    lines.append(f"- RECON {r['source_table_id'][:12]} {r['check_type']}: "
                 f"{failcls.sanitize_message(r['message'])[:160]}")
for r in dq_failures[:50]:
    lines.append(f"- DQ {r['source_table_id'][:12]} {r['rule_type']} "
                 f"failed={r['failed_count']}")
for r in job_failures[:20]:
    lines.append(f"- JOB {r['job_name']} {r['status']}: "
                 f"{failcls.sanitize_message(r['message'])[:160]}")
summary = "\n".join(lines)
print(summary)

# COMMAND ----------

notification_status = "NO_FAILURES" if total == 0 else "PRINTED"

if total > 0 and mode == "TEAMS_WEBHOOK":
    notification_status = "NOT_SENT"
    if not wh_scope or not wh_key:
        print("TEAMS_WEBHOOK requested but no webhook secret configured; NOT_SENT.")
    else:
        try:
            import json as _json
            import urllib.request as _rq
            webhook = dbutils.secrets.get(wh_scope, wh_key)   # never printed
            payload = _json.dumps({"text": summary}).encode("utf-8")
            req = _rq.Request(webhook, data=payload,
                              headers={"Content-Type": "application/json"})
            with _rq.urlopen(req, timeout=15) as resp:
                if 200 <= resp.status < 300:
                    notification_status = "SENT"
                else:
                    print(f"Teams webhook returned HTTP {resp.status}; NOT_SENT.")
        except Exception as e:
            # Sanitize: a failing webhook must not leak the URL into logs.
            print(f"Teams notification failed (NOT_SENT): "
                  f"{failcls.sanitize_message(type(e).__name__)}")

# COMMAND ----------

# A notification problem must never hide the underlying pipeline failure.
dbutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "run_id": run_id, "pipeline_name": pipeline_name,
    "failures": total, "notification_status": notification_status,
}))
