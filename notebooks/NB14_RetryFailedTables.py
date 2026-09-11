# Databricks notebook source
# MAGIC %md
# MAGIC # NB14_RetryFailedTables (compatibility wrapper)
# MAGIC The authoritative implementation moved to `notebooks/shared/NB14_RetryFailedTables`.
# MAGIC This wrapper keeps existing job definitions working; update them to the
# MAGIC shared path. There is exactly one implementation - none is duplicated here.

# COMMAND ----------

# MAGIC %run ./shared/NB14_RetryFailedTables
