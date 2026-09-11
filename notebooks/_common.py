# Databricks notebook source
# MAGIC %md
# MAGIC # _common (compatibility wrapper)
# MAGIC The authoritative implementation moved to `notebooks/shared/_common`.
# MAGIC This wrapper keeps existing job definitions working; update them to the
# MAGIC shared path. There is exactly one implementation - none is duplicated here.

# COMMAND ----------

# MAGIC %run ./shared/_common
