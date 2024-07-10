-- Databricks notebook source
-- MAGIC %md
-- MAGIC Delta technology provides ACID compliant updates to Delta tables.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

CREATE TABLE orders 
AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overwriting Tables
-- MAGIC
-- MAGIC overwriting vs (deleting and recreating) the table
-- MAGIC - the old version of the table still exists and can easily retrieve all data using Time Travel.
-- MAGIC -  overwriting a table is much faster because it does not need to list the directory recursively or delete any files.
-- MAGIC -  it's an atomic operation. Concurrent queries can still read the table while you are overwriting it.
-- MAGIC - due to the ACID transaction guarantees, if overwriting the table fails, the table will be in its previous state.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Option1.CRAS statement

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Option2. INSERT OVERWRITE
-- MAGIC
-- MAGIC However, INSERT OVERWRITE statement has some differences.
-- MAGIC
-- MAGIC For example, it can only overwrite an existing table and not creating a new one like our CREATE OR REPLACE statement.
-- MAGIC
-- MAGIC And it can override only the new records that match the current table schema, which means that it is a safer technique for overwriting an existing table without the risk of modifying the table schema.

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *, current_timestamp() FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Appending Data
-- MAGIC
-- MAGIC
-- MAGIC The INSERT INTO statement is a simple and efficient operation for inserting new data.
-- MAGIC
-- MAGIC However, it does not have any built in guarantees to prevent inserting the same records multiple times.
-- MAGIC Validate by running below statement mutliple times in order to +700 records each time
-- MAGIC
-- MAGIC INSERT INTO orders
-- MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders-new`
-- MAGIC
-- MAGIC To fix it, using Merging data statement

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

select * from parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merging Data
-- MAGIC
-- MAGIC In a merge operation, updates, inserts and deletes are completed in a single atomic transaction.
-- MAGIC
-- MAGIC In addition, merge operation is a great solution for avoiding duplicates when inserting records.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We are creating a temporary view of the new customer data.
-- MAGIC
-- MAGIC And now we can apply the merge operation that says MERGE INTO customers
-- MAGIC
-- MAGIC the new changes coming from customer_updates temp view on the customer ID key.
-- MAGIC
-- MAGIC And we have two actions here.
-- MAGIC
-- MAGIC When match, we do an update and when not match, we do an insert.
-- MAGIC
-- MAGIC In addition, we add extra conditions.
-- MAGIC
-- MAGIC In this case, we are checking that the current row has a null email while the new record does not.
-- MAGIC
-- MAGIC In such a case, we update the email and we also update the last updated timestamp.
-- MAGIC
-- MAGIC And again, if the new record does not match any existing customers based on the customer ID, in this
-- MAGIC
-- MAGIC case, we will insert this new record.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS 
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
  UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv-new",
  header = "true",
  delimiter = ";"
);

SELECT * FROM books_updates

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN 
  INSERT *

-- COMMAND ----------

-- merge into second time will impact no record
MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN 
  INSERT *
