# Databricks notebook source
print("Hallo world!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "DDD"

# COMMAND ----------

# MAGIC %md
# MAGIC # Title1
# MAGIC ## Title2

# COMMAND ----------

# "./../Setup"
%run "/Users/soulyu_shine@hotmail.com/Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC # Title 3

# COMMAND ----------


print(full_name)


# COMMAND ----------

# list all files and folders in the default datast location
%fs ls '/databricks-datasets'

# COMMAND ----------

#file system utility
dbutils.help()

# COMMAND ----------

dbutils.fs.help()


# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets')
display(files)
