-- Run these commands in your Snowflake worksheet (Snowsight)

-- 1. Create a Warehouse (if you don't already have one)
CREATE OR REPLACE WAREHOUSE ECOMM_WH 
WITH WAREHOUSE_SIZE = 'XSMALL' 
AUTO_SUSPEND = 60 
AUTO_RESUME = TRUE;

-- 2. Create the Database and Schema
CREATE OR REPLACE DATABASE ECOMM_DB;
CREATE OR REPLACE SCHEMA ECOMM_DB.SALES;

-- 3. Use the newly created objects
USE WAREHOUSE ECOMM_WH;
USE DATABASE ECOMM_DB;
USE SCHEMA SALES;

-- Note: We will let PySpark automatically create the tables for us during the load phase
-- to avoid manually managing table schemas.
