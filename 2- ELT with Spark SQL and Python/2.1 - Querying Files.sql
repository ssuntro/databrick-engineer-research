-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying JSON 
-- MAGIC
-- MAGIC Run Copy-Datasets to ensure files are ready in dbfs prior. The main goal of this notebook is given there is files in dbfs how to import them as a Delta table.

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_*.json`

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT count(*) FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

 SELECT *,
    input_file_name() source_file
  FROM json.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying text Format

-- COMMAND ----------

SELECT * FROM text.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Querying binaryFile Format

-- COMMAND ----------

SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Querying CSV 

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

CREATE TABLE books_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
LOCATION "${dataset.bookstore}/books-csv"

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limitations of Non-Delta Tables
-- MAGIC
-- MAGIC No time travel feature or garantee that always provided latest committed data excepts manually refresh the Non-Delta tables.

-- COMMAND ----------

DESCRIBE EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC         .table("books_csv")
-- MAGIC       .write
-- MAGIC         .mode("append")
-- MAGIC         .format("csv")
-- MAGIC         .option('header', 'true')
-- MAGIC         .option('delimiter', ';')
-- MAGIC         .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Refresh table peration
-- MAGIC This external CSV file is not configured to tell Spark that it should refresh this data.
-- MAGIC
-- MAGIC But remember, refreshing a table will invalidate its cache, meaning that we will need to scan our original data source and pull all data back into memory. For a very large dataset,
-- MAGIC
-- MAGIC this may take a significant amount of time.

-- COMMAND ----------

REFRESH TABLE books_csv

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTAS Statements
-- MAGIC
-- MAGIC Load files into Delta tables

-- COMMAND ----------

CREATE TABLE customers 
AS
SELECT * FROM json.`${dataset.bookstore}/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_vw
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

CREATE TABLE books 
AS
  SELECT * FROM books_tmp_vw;
  
SELECT * FROM books

-- COMMAND ----------

DESCRIBE EXTENDED books
