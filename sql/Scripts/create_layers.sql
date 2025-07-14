-- Create three layers for database
-- - Bronze layer is to store raw data from different sources
-- - Silver layer is to process the data before being consumed
-- - Gold layer is to create a read-only final data to be consumed

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;