-- Create the hr_db schema if it does not exist
CREATE SCHEMA IF NOT EXISTS hive_metastore.hr_db;

-- Create the employees table with the appropriate schema
CREATE TABLE IF NOT EXISTS hive_metastore.hr_db.employees (
    id INT,
    name STRING,
    salary DOUBLE,
    city STRING
);
-- location 'dbfs:/mnt/demo/employees2';

-- Insert data into the employees table
INSERT INTO hive_metastore.hr_db.employees
VALUES
(1, 'Anna', 2500, 'Paris'),
(2, 'Thomas', 3000, 'London'),
(3, 'Bilal', 3500, 'Paris'),
(4, 'Maya', 2000, 'Paris'),
(5, 'Sophie', 2500, 'London'),
(6, 'Adam', 3500, 'London'),
(7, 'Ali', 3000, 'Paris');

-- Create the paris_employee_vw view
CREATE VIEW hive_metastore.hr_db.paris_employee_vw
AS SELECT * FROM hive_metastore.hr_db.employees WHERE city = 'Paris';
