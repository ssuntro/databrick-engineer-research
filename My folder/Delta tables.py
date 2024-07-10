# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE employees
# MAGIC (id INT, name STRING, salary DOUBLE);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees
# MAGIC VALUES 
# MAGIC   (1, "Adam", 3500.0),
# MAGIC   (2, "Sarah", 4020.5);
# MAGIC
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (3, "John", 2999.3),
# MAGIC   (4, "Thomas", 4000.3);
# MAGIC
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (5, "Anna", 2500.0);
# MAGIC
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (6, "Kim", 6200.3)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED employees

# COMMAND ----------

new_dataframe_name = _sqldf
display(new_dataframe_name)

# COMMAND ----------

# MAGIC %md
# MAGIC # Failed: to access table directory

# COMMAND ----------

#show directory of employee table
# https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage?WT.mc_id=Portal-Microsoft_Azure_Databricks
#https://community.databricks.com/t5/data-engineering/unity-catalog-issues/td-p/66696

# files = dbutils.fs.ls("abfss://unity-catalog-storage@dbstorageb7k4upl2jqi3u.dfs.core.windows.net/354120185967681")

files = dbutils.fs.ls("abfss://unity-catalog-storage@dbstorageb7k4upl2jqi3u.dfs.core.windows.net/354120185967681/__unitystorage/catalogs/6f476597-854a-4225-ad3c-6fa8d0fcdbc4/tables/60cf0968-2d4e-4acf-9d44-362029bb2b6f")


display(files)



# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employees 
# MAGIC SET salary = salary + 100
# MAGIC WHERE name LIKE "A%"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees
