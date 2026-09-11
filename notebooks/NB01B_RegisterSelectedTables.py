# Databricks notebook source
# MAGIC %md
# MAGIC # NB01B_RegisterSelectedTables (compatibility wrapper)
# MAGIC The authoritative implementation moved to `notebooks/shared/NB01B_RegisterSelectedTables`.
# MAGIC This wrapper keeps existing job definitions working; update them to the
# MAGIC shared path. There is exactly one implementation - none is duplicated here.

# COMMAND ----------

# MAGIC %run ./shared/NB01B_RegisterSelectedTables
