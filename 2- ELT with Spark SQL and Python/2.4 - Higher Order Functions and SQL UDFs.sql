-- Databricks notebook source
-- MAGIC %md
-- MAGIC UDF = User defined functions
-- MAGIC
-- MAGIC
-- MAGIC Higher order functions allow you to work directly with hierarchical data like arrays and map type objects.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Filtering Arrays

-- COMMAND ----------

SELECT
  order_id,
  books,
  FILTER (books, i -> i.quantity >= 2) AS multiple_copies
FROM orders

-- COMMAND ----------

-- No more empty array in multiple_copies column
SELECT order_id, multiple_copies
FROM (
  SELECT
    order_id,
    FILTER (books, i -> i.quantity >= 2) AS multiple_copies
  FROM orders)
WHERE size(multiple_copies) > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Transforming Arrays

-- COMMAND ----------

SELECT
  order_id,
  books,
  TRANSFORM (
    books,
    b -> CAST(b.subtotal * 0.8 AS INT)
  ) AS subtotal_after_discount
FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## User Defined Functions (UDF)
-- MAGIC
-- MAGIC allow you to register a custom combination of SQL logic as function in a database, making these methods reusable in any SQL query.
-- MAGIC In addition, UDF functions leverage spark SQL directly maintaining all the optimization of Spark when applying your custom logic to large datasets.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Note that user defined functions are permanent objects that are persisted to the database, so you can use them between different Spark sessions and notebooks. With Describe Function command, we can see where it was registered and basic information about expected inputs and the expected return type.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING

RETURN concat("https://www.", split(email, "@")[1])

-- COMMAND ----------

SELECT email, get_url(email) domain
FROM customers

-- COMMAND ----------

DESCRIBE FUNCTION get_url

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED get_url

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC UDF with complex logic such as pattern matching

-- COMMAND ----------

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
          WHEN email like "%.com" THEN "Commercial business"
          WHEN email like "%.org" THEN "Non-profits organization"
          WHEN email like "%.edu" THEN "Educational institution"
          ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
       END;

-- COMMAND ----------

SELECT email, site_type(email) as domain_category
FROM customers

-- COMMAND ----------

DROP FUNCTION get_url;
DROP FUNCTION site_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Remark: 
-- MAGIC # Remember, everything is evaluated natively in Spark.
-- MAGIC # And so it's optimized for parallel execution.
