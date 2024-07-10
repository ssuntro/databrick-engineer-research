-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Delta Time Travel

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

SELECT * 
FROM employees VERSION AS OF 5

-- COMMAND ----------

select * from employees timestamp as of "2024-07-04T09:09:37.000+00:00"

-- COMMAND ----------

SELECT * FROM employees@v4

-- COMMAND ----------

DELETE FROM employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

RESTORE TABLE employees TO VERSION AS OF 5

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## OPTIMIZE Command
-- MAGIC
-- MAGIC aka compating small files into larger ones in order to improve this speed/performance.
-- MAGIC
-- MAGIC where z-order in data lakes is about co-locating and reorganising column info in the same set of files

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

OPTIMIZE employees
ZORDER BY id

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## VACUUM Command
-- MAGIC
-- MAGIC cleaning up unused data files such as 
-- MAGIC - uncomitted files
-- MAGIC - files that are no longer in in the lastest table state
-- MAGIC
-- MAGIC  
-- MAGIC

-- COMMAND ----------

VACUUM employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

SELECT * FROM employees@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Tables

-- COMMAND ----------

DROP TABLE employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'
