-- Databricks notebook source
SET datasets.path=dbfs:/mnt/demo-datasets/bookstore;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Bronze Layer Tables
-- MAGIC
-- MAGIC ${datasets.path}/books-cdc -> books_bronze(source of cdc feed) -> CDC -> LIVE.books_silver(target table)

-- COMMAND ----------

-- load data from new json file incrementally
CREATE OR REFRESH STREAMING LIVE TABLE books_bronze
COMMENT "The raw books data, ingested from CDC feed"
AS SELECT * FROM cloud_files("${datasets.path}/books-cdc", "json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Silver Layer Tables
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE books_silver;

APPLY CHANGES INTO LIVE.books_silver
  FROM STREAM(LIVE.books_bronze)
  KEYS (book_id)
  APPLY AS DELETE WHEN row_status = "DELETE"
  SEQUENCE BY row_time
  COLUMNS * EXCEPT (row_status, row_time)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold Layer Tables
-- MAGIC
-- MAGIC we define a simple aggregate query to create a live table from the data in our book_silver table.
-- MAGIC
-- MAGIC Notice here that this is not a streaming table.
-- MAGIC
-- MAGIC Since data is being updated and deleted from our book_silver table, it is no more valid to be a streaming
-- MAGIC
-- MAGIC source for this new table.
-- MAGIC
-- MAGIC Remember streaming sources must be append only tables.

-- COMMAND ----------

CREATE LIVE TABLE author_counts_state
  COMMENT "Number of books per author"
AS SELECT author, count(*) as books_count, current_timestamp() updated_time
  FROM LIVE.books_silver
  GROUP BY author

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DLT Views
-- MAGIC
-- MAGIC DLT views are temporary views
-- MAGIC
-- MAGIC scoped to the DLT pipeline they are a part of, so they are not persisted to the metastore.
-- MAGIC
-- MAGIC Views can still be used to enforce data equality. And metrics for views will be collected and reported
-- MAGIC
-- MAGIC as they would be for tables.

-- COMMAND ----------

CREATE LIVE VIEW books_sales
  AS SELECT b.title, o.quantity
    FROM (
      SELECT *, explode(books) AS book 
      FROM LIVE.orders_cleaned) o
    INNER JOIN LIVE.books_silver b
    ON o.book.book_id = b.book_id;
