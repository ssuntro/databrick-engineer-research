-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Parsing JSON Data

-- COMMAND ----------

SELECT * FROM customers

-- COMMAND ----------

DESCRIBE customers

-- COMMAND ----------

SELECT customer_id, profile:first_name, profile:address:country 
FROM customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Spark SQL also has the ability to parse JSON objects into struct types.
-- MAGIC
-- MAGIC Struct is a native spark type with nested attributes.
-- MAGIC struct provided ability to work with nested obj.

-- COMMAND ----------

-- error due to missing json schema
SELECT from_json(profile) AS profile_struct
  FROM customers;

-- COMMAND ----------

SELECT profile 
FROM customers 
LIMIT 1

-- COMMAND ----------

-- derive json schema using the data
CREATE OR REPLACE TEMP VIEW parsed_customers AS
  SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')) AS profile_struct
  FROM customers;
  
SELECT * FROM parsed_customers

-- COMMAND ----------

DESCRIBE parsed_customers

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name, profile_struct.address.country
FROM parsed_customers

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_final AS
  SELECT customer_id, profile_struct.*
  FROM parsed_customers;
  
SELECT * FROM customers_final

-- COMMAND ----------

SELECT order_id, customer_id, books
FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explode Function
-- MAGIC
-- MAGIC The most important one is the explode function that allows us to put each element of an array on its own row.
-- MAGIC
-- MAGIC So now each element of the book's array has its own row and we are repeating the other information like the customer ID and the order ID.

-- COMMAND ----------

SELECT order_id, customer_id, explode(books) AS book 
FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Collecting Rows
-- MAGIC
-- MAGIC Another interesting function is the collect_set aggregation function that allows us to collect unique values for a field, including fields within arrays.

-- COMMAND ----------

SELECT customer_id,
  collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS books_set
FROM orders
GROUP BY customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Flatten Arrays

-- COMMAND ----------

SELECT customer_id,
  collect_set(books.book_id) As before_flatten,
  array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
FROM orders
GROUP BY customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Join Operations

-- COMMAND ----------

-- As a result, orders_enriched will contain all column from both o and b
CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM (
  SELECT *, explode(books) AS book 
  FROM orders) o
INNER JOIN books b
ON o.book.book_id = b.book_id;

SELECT * FROM orders_enriched

-- COMMAND ----------

describe books

-- COMMAND ----------

DESCRIBE (
  SELECT *, explode(books) AS book 
  FROM orders)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set Operations
-- MAGIC
-- MAGIC can perform only if schema is similar for both operant

-- COMMAND ----------

describe orders; 

-- COMMAND ----------

describe orders_updates

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;

SELECT * FROM orders
UNION 
SELECT * FROM orders_updates

-- COMMAND ----------

SELECT * FROM orders 
INTERSECT 
SELECT * FROM orders_updates 

-- COMMAND ----------

SELECT * FROM orders 
MINUS 
SELECT * FROM orders_updates 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reshaping Data with Pivot
-- MAGIC
-- MAGIC Spark SQL also support pivot clause, which is used to change data perspective.
-- MAGIC
-- MAGIC We can get the aggregated values based on a specific column values, which will be turned to multiple
-- MAGIC
-- MAGIC columns used in Select clause.
-- MAGIC
-- MAGIC The pivot table can be specified after the table name or subquery.
-- MAGIC
-- MAGIC So here we have SELECT * FROM
-- MAGIC
-- MAGIC and we specify between two parenthesis the Select statement that will be the input for this table.
-- MAGIC
-- MAGIC In the pivot clause,
-- MAGIC
-- MAGIC the first argument is an aggregation function,
-- MAGIC
-- MAGIC and the column to be aggregated.
-- MAGIC
-- MAGIC Then we specify the pivot column in the FOR subclause.
-- MAGIC
-- MAGIC The IN operator contains the pivot columns values.
-- MAGIC
-- MAGIC So here we use the Pivot Command to create a new transactions table that flatten out the information
-- MAGIC
-- MAGIC contained in the orders table for each customer.
-- MAGIC
-- MAGIC Such a flatten data format can be useful for dashboarding, but also useful for applying machine learning
-- MAGIC
-- MAGIC algorithms for inference and predictions.

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

SELECT * FROM transactions

-- COMMAND ----------

SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
