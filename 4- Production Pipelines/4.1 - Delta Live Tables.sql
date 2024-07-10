-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Delta Live Tables
-- MAGIC Delta Live Tables or DLT is a framework for building reliable and maintainable data processing pipelines.
-- MAGIC DLT simplifies the hard work of building large scale ETL while maintaining table dependencies and data quality.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

SET datasets.path=dbfs:/mnt/demo-datasets/bookstore;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer Tables
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### orders_raw
-- MAGIC
-- MAGIC run below command only validate the syntax. to exe them you must create a DLT pipeline.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
COMMENT "The raw books orders, ingested from orders-raw"
AS SELECT * FROM cloud_files("${datasets.path}/orders-json-raw", "json",
                             map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
-- COMMENT "The raw books orders, ingested from orders-raw"
-- AS SELECT * FROM cloud_files("${datasets.path}/orders-raw", "parquet",
--                              map("schema", "order_id STRING, order_timestamp LONG, customer_id STRING, quantity LONG"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### customers

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE customers
COMMENT "The customers lookup table, ingested from customers-json"
AS SELECT * FROM json.`${datasets.path}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Silver Layer Tables
-- MAGIC This layer produce a refined copy of data from the bronze layer.
-- MAGIC
-- MAGIC At this level, we apply operations like data cleansing and enrichment.
-- MAGIC
-- MAGIC Here we declare our silver table orders_cleaned, which enriches the order's data with customer information.
-- MAGIC
-- MAGIC In addition, we implement quality control using constraint keywords.
-- MAGIC
-- MAGIC Here we reject records with no order_id.
-- MAGIC
-- MAGIC The Constraint keyword enables DLT to collect metrics on constraint violations.
-- MAGIC
-- MAGIC It provides an optional On Violation clause specifying an action to take on records that violate
-- MAGIC
-- MAGIC the constraints.
-- MAGIC
-- MAGIC The three modes currently supported by Delta are included in this table.
-- MAGIC
-- MAGIC DROP ROW where we discard records that violate constraints.
-- MAGIC
-- MAGIC FAIL UPDATE where the pipeline fail when constraint is violated.
-- MAGIC
-- MAGIC And finally, when omitted, records violating constraint will be included, but violation will be reported
-- MAGIC
-- MAGIC in the metrics.
-- MAGIC
-- MAGIC Notice also that we need to use the LIVE prefix in order to refer to other DLT tables.
-- MAGIC
-- MAGIC And for streaming DTL tables, we need to use the STREAM method.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### orders_cleaned

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned (
  CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned books orders with valid order_id"
AS
  SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books,
         c.profile:address:country as country
  FROM STREAM(LIVE.orders_raw) o -- LIVE.orders_raw to refer to live tables + STREAM for streaming DTL tables.
  LEFT JOIN LIVE.customers c
    ON o.customer_id = c.customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC >> Constraint violation
-- MAGIC
-- MAGIC | **`ON VIOLATION`** | Behavior |
-- MAGIC | --- | --- |
-- MAGIC | **`DROP ROW`** | Discard records that violate constraints |
-- MAGIC | **`FAIL UPDATE`** | Violated constraint causes the pipeline to fail  |
-- MAGIC | Omitted | Records violating constraints will be kept, and reported in metrics |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold Tables
-- MAGIC
-- MAGIC the expected outcome is a table that represent the daily number of books per customer in a specific region. aka a table that can anwser business question.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
COMMENT "Daily number of books per customer in China"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.orders_cleaned
  WHERE country = "China"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fr_daily_customer_books
COMMENT "Daily number of books per customer in France"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.orders_cleaned
  WHERE country = "France"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

-- COMMAND ----------


