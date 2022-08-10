-- Databricks notebook source
--  %md-sandbox
--  
--  <div style="text-align: center; line-height: 0; padding-top: 9px;">
--    <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
--  </div>

-- COMMAND ----------

--  %md
--  
--  
--  # Databases and Tables on Databricks
--  In this demonstration, you will create and explore databases and tables.
--  
--  ## Learning Objectives
--  By the end of this lesson, you should be able to:
--  * Use Spark SQL DDL to define databases and tables
--  * Describe how the **`LOCATION`** keyword impacts the default storage directory
--  
--  
--  
--  **Resources**
--  * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Databases and Tables - Databricks Docs</a>
--  * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">Managed and Unmanaged Tables</a>
--  * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">Creating a Table with the UI</a>
--  * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">Create a Local Table</a>
--  * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">Saving to Persistent Tables</a>

-- COMMAND ----------

--  %md
--  
--  
--  ## Lesson Setup
--  The following script clears out previous runs of this demo and configures some Hive variables that will be used in our SQL queries.

-- COMMAND ----------

--  %run ../Includes/Classroom-Setup-03.1

-- COMMAND ----------

--  %md
--  
--  
--  ## Using Hive Variables
--  
--  While not a pattern that is generally recommended in Spark SQL, this notebook will use some Hive variables to substitute in string values derived from the account email of the current user.
--  
--  The following cell demonstrates this pattern.

-- COMMAND ----------

SELECT "${da.db_name}" AS db_name,
       "${da.paths.working_dir}" AS working_dir

-- COMMAND ----------

--  %md
--  
--  
--  Because you may be working in a shared workspace, this course uses variables derived from your username so the databases don't conflict with other users. Again, consider this use of Hive variables a hack for our lesson environment rather than a good practice for development.

-- COMMAND ----------

--  %md
--  
--   
--  ## Databases
--  Let's start by creating two databases:
--  - One with no **`LOCATION`** specified
--  - One with **`LOCATION`** specified

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${da.db_name}_default_location;
CREATE DATABASE IF NOT EXISTS ${da.db_name}_custom_location LOCATION '${da.paths.working_dir}/_custom_location.db';

-- COMMAND ----------

--  %md
--  
--   
--  Note that the location of the first database is in the default location under **`dbfs:/user/hive/warehouse/`** and that the database directory is the name of the database with the **`.db`** extension

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED ${da.db_name}_default_location;

-- COMMAND ----------

--  %md
--  
--  
--  Note that the location of the second database is in the directory specified after the **`LOCATION`** keyword.

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED ${da.db_name}_custom_location;

-- COMMAND ----------

--  %md
--  
--   
--  We will create a table in the database with default location and insert data. 
--  
--  Note that the schema must be provided because there is no data from which to infer the schema.

-- COMMAND ----------

USE ${da.db_name}_default_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_default_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_default_location 
VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_default_location;

-- COMMAND ----------

--  %md
--  
--   
--  We can look at the extended table description to find the location (you'll need to scroll down in the results).

-- COMMAND ----------

DESCRIBE EXTENDED managed_table_in_db_with_default_location;

-- COMMAND ----------

--  %md
--  
--  
--  By default, managed tables in a database without the location specified will be created in the **`dbfs:/user/hive/warehouse/<database_name>.db/`** directory.
--  
--  We can see that, as expected, the data and metadata for our Delta Table are stored in that location.

-- COMMAND ----------

--  %python 
--  hive_root =  f"dbfs:/user/hive/warehouse"
--  db_name =    f"{DA.db_name}_default_location.db"
--  table_name = f"managed_table_in_db_with_default_location"
--  
--  tbl_location = f"{hive_root}/{db_name}/{table_name}"
--  print(tbl_location)
--  
--  files = dbutils.fs.ls(tbl_location)
--  display(files)

-- COMMAND ----------

--  %md
--  
--   
--  Drop the table.

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_default_location;

-- COMMAND ----------

--  %md
--  
--   
--  Note the table's directory and its log and data files are deleted. Only the database directory remains.

-- COMMAND ----------

--  %python 
--  
--  db_location = f"{hive_root}/{db_name}"
--  print(db_location)
--  dbutils.fs.ls(db_location)

-- COMMAND ----------

--  %md
--  
--   
--  We now create a table in  the database with custom location and insert data. 
--  
--  Note that the schema must be provided because there is no data from which to infer the schema.

-- COMMAND ----------

USE ${da.db_name}_custom_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_custom_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_custom_location VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_custom_location;

-- COMMAND ----------

--  %md
--  
--   
--  Again, we'll look at the description to find the table location.

-- COMMAND ----------

DESCRIBE EXTENDED managed_table_in_db_with_custom_location;

-- COMMAND ----------

--  %md
--  
--   
--  As expected, this managed table is created in the path specified with the **`LOCATION`** keyword during database creation. As such, the data and metadata for the table are persisted in a directory here.

-- COMMAND ----------

--  %python 
--  
--  table_name = f"managed_table_in_db_with_custom_location"
--  tbl_location =   f"{DA.paths.working_dir}/_custom_location.db/{table_name}"
--  print(tbl_location)
--  
--  files = dbutils.fs.ls(tbl_location)
--  display(files)

-- COMMAND ----------

--  %md
--  
--   
--  Let's drop the table.

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_custom_location;

-- COMMAND ----------

--  %md
--  
--   
--  Note the table's folder and the log file and data file are deleted.  
--    
--  Only the database location remains

-- COMMAND ----------

--  %python 
--  
--  db_location =   f"{DA.paths.working_dir}/_custom_location.db"
--  print(db_location)
--  
--  dbutils.fs.ls(db_location)

-- COMMAND ----------

--  %md
--  
--   
--  ## Tables
--  We will create an external (unmanaged) table from sample data. 
--  
--  The data we are going to use are in CSV format. We want to create a Delta table with a **`LOCATION`** provided in the directory of our choice.

-- COMMAND ----------

USE ${da.db_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${DA.paths.datasets}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;

SELECT * FROM external_table;

-- COMMAND ----------

--  %md
--  
--   
--  Let's note the location of the table's data in this lesson's working directory.

-- COMMAND ----------

DESCRIBE TABLE EXTENDED external_table;

-- COMMAND ----------

--  %md
--  
--   
--  Now, we drop the table.

-- COMMAND ----------

DROP TABLE external_table;

-- COMMAND ----------

--  %md
--  
--   
--  The table definition no longer exists in the metastore, but the underlying data remain intact.

-- COMMAND ----------

--  %python 
--  tbl_path = f"{DA.paths.working_dir}/external_table"
--  files = dbutils.fs.ls(tbl_path)
--  display(files)

-- COMMAND ----------

--  %md
--  
--   
--  ## Clean up
--  Drop both databases.

-- COMMAND ----------

DROP DATABASE ${da.db_name}_default_location CASCADE;
DROP DATABASE ${da.db_name}_custom_location CASCADE;

-- COMMAND ----------

--  %md
--  
--   
--  Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

--  %python 
--  DA.cleanup()

-- COMMAND ----------

--  %md-sandbox
--  &copy; 2022 Databricks, Inc. All rights reserved.<br/>
--  Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
--  <br/>
--  <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
