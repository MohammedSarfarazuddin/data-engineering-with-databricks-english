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
--  # Databases, Tables, and Views Lab
--  
--  ## Learning Objectives
--  By the end of this lab, you should be able to:
--  - Create and explore interactions between various relational entities, including:
--    - Databases
--    - Tables (managed and external)
--    - Views (views, temp views, and global temp views)
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
--  ### Getting Started
--  
--  Run the following cell to configure variables and datasets for this lesson.

-- COMMAND ----------

--  %run ../Includes/Classroom-Setup-03.3L

-- COMMAND ----------

--  %md
--  
--  
--  ## Overview of the Data
--  
--  The data include multiple entries from a selection of weather stations, including average temperatures recorded in either Fahrenheit or Celsius. The schema for the table:
--  
--  |ColumnName  | DataType| Description|
--  |------------|---------|------------|
--  |NAME        |string   | Station name |
--  |STATION     |string   | Unique ID |
--  |LATITUDE    |float    | Latitude |
--  |LONGITUDE   |float    | Longitude |
--  |ELEVATION   |float    | Elevation |
--  |DATE        |date     | YYYY-MM-DD |
--  |UNIT        |string   | Temperature units |
--  |TAVG        |float    | Average temperature |
--  
--  This data is stored in the Parquet format; preview the data with the query below.

-- COMMAND ----------

SELECT * 
FROM parquet.`${DA.paths.datasets}/weather/StationData-parquet`

-- COMMAND ----------

--  %md
--  
--  
--  ## Create a Database
--  
--  Create a database in the default location using the **`da.db_name`** variable defined in setup script.

-- COMMAND ----------

-- TODO

<FILL-IN> ${da.db_name}

-- COMMAND ----------

--  %md
--  
--  Run the cell below to check your work.

-- COMMAND ----------

--  %python 
--  assert spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{DA.db_name}'").count() == 1, "Database not present"

-- COMMAND ----------

--  %md
--  
--  
--  ## Change to Your New Database
--  
--  **`USE`** your newly created database.

-- COMMAND ----------

-- TODO

<FILL-IN> ${da.db_name}

-- COMMAND ----------

--  %md
--  
--  Run the cell below to check your work.

-- COMMAND ----------

--  %python
--  assert spark.sql(f"SHOW CURRENT DATABASE").first()["namespace"] == DA.db_name, "Not using the correct database"

-- COMMAND ----------

--  %md
--  
--  
--  ## Create a Managed Table
--  Use a CTAS statement to create a managed table named **`weather_managed`**.

-- COMMAND ----------

-- TODO

<FILL-IN>
SELECT * 
FROM parquet.`${DA.paths.datasets}/weather/StationData-parquet`

-- COMMAND ----------

--  %md
--  
--  Run the cell below to check your work.

-- COMMAND ----------

--  %python
--  assert spark.table("weather_managed"), "Table named `weather_managed` does not exist"
--  assert spark.table("weather_managed").count() == 2559, "Incorrect row count"

-- COMMAND ----------

--  %md
--  
--  
--  ## Create an External Table
--  
--  Recall that an external table differs from a managed table through specification of a location. Create an external table called **`weather_external`** below.

-- COMMAND ----------

-- TODO

<FILL-IN>
LOCATION "${da.paths.working_dir}/lab/external"
AS SELECT * 
FROM parquet.`${DA.paths.datasets}/weather/StationData-parquet`

-- COMMAND ----------

--  %md
--  
--  Run the cell below to check your work.

-- COMMAND ----------

--  %python
--  assert spark.table("weather_external"), "Table named `weather_external` does not exist"
--  assert spark.table("weather_external").count() == 2559, "Incorrect row count"

-- COMMAND ----------

--  %md
--  
--  
--  ## Examine Table Details
--  Use the SQL command **`DESCRIBE EXTENDED table_name`** to examine the two weather tables.

-- COMMAND ----------

DESCRIBE EXTENDED weather_managed

-- COMMAND ----------

DESCRIBE EXTENDED weather_external

-- COMMAND ----------

--  %md
--  
--  
--  Run the following helper code to extract and compare the table locations.

-- COMMAND ----------

--  %python
--  def getTableLocation(tableName):
--      return spark.sql(f"DESCRIBE DETAIL {tableName}").select("location").first()[0]

-- COMMAND ----------

--  %python
--  managedTablePath = getTableLocation("weather_managed")
--  externalTablePath = getTableLocation("weather_external")
--  
--  print(f"""The weather_managed table is saved at: 
--  
--      {managedTablePath}
--  
--  The weather_external table is saved at:
--  
--      {externalTablePath}""")

-- COMMAND ----------

--  %md
--  
--  
--  List the contents of these directories to confirm that data exists in both locations.

-- COMMAND ----------

--  %python
--  files = dbutils.fs.ls(managedTablePath)
--  display(files)

-- COMMAND ----------

--  %python
--  files = dbutils.fs.ls(externalTablePath)
--  display(files)

-- COMMAND ----------

--  %md
--  
--  
--  ### Check Directory Contents after Dropping Database and All Tables
--  The **`CASCADE`** keyword will accomplish this.

-- COMMAND ----------

-- TODO

<FILL_IN> ${da.db_name}

-- COMMAND ----------

--  %md
--  
--  Run the cell below to check your work.

-- COMMAND ----------

--  %python
--  assert spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{DA.db_name}'").count() == 0, "Database present"

-- COMMAND ----------

--  %md
--  
--  
--  With the database dropped, the files will have been deleted as well.
--  
--  Uncomment and run the following cell, which will throw a **`FileNotFoundException`** as your confirmation.

-- COMMAND ----------

--  %python
--  # files = dbutils.fs.ls(managedTablePath)
--  # display(files)

-- COMMAND ----------

--  %python
--  files = dbutils.fs.ls(externalTablePath)
--  display(files)

-- COMMAND ----------

--  %python
--  files = dbutils.fs.ls(DA.paths.working_dir)
--  display(files)

-- COMMAND ----------

--  %md
--  
--  
--  **This highlights the main differences between managed and external tables.** By default, the files associated with managed tables will be stored to this location on the root DBFS storage linked to the workspace, and will be deleted when a table is dropped.
--  
--  Files for external tables will be persisted in the location provided at table creation, preventing users from inadvertently deleting underlying files. **External tables can easily be migrated to other databases or renamed, but these operations with managed tables will require rewriting ALL underlying files.**

-- COMMAND ----------

--  %md
--  
--  
--  ## Create a Database with a Specified Path
--  
--  Assuming you dropped your database in the last step, you can use the same **`database`** name.

-- COMMAND ----------

CREATE DATABASE ${da.db_name} LOCATION '${da.paths.working_dir}/${da.db_name}';
USE ${da.db_name};

-- COMMAND ----------

--  %md
--  
--  
--  Recreate your **`weather_managed`** table in this new database and print out the location of this table.

-- COMMAND ----------

-- TODO

<FILL_IN>

-- COMMAND ----------

--  %python
--  getTableLocation("weather_managed")

-- COMMAND ----------

--  %md
--  
--  Run the cell below to check your work.

-- COMMAND ----------

--  %python
--  assert spark.table("weather_managed"), "Table named `weather_managed` does not exist"
--  assert spark.table("weather_managed").count() == 2559, "Incorrect row count"

-- COMMAND ----------

--  %md
--  
--  
--  While here we're using the **`working_dir`** directory created on the DBFS root, _any_ object store can be used as the database directory. **Defining database directories for groups of users can greatly reduce the chances of accidental data exfiltration**.

-- COMMAND ----------

--  %md
--  
--  
--  ## Views and their Scoping
--  
--  Using the provided **`AS`** clause, register:
--  - a view named **`celsius`**
--  - a temporary view named **`celsius_temp`**
--  - a global temp view named **`celsius_global`**

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

--  %md
--  
--  Run the cell below to check your work.

-- COMMAND ----------

--  %python
--  assert spark.table("celsius"), "Table named `celsius` does not exist"
--  assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'celsius'").first()["isTemporary"] == False, "Table is temporary"

-- COMMAND ----------

--  %md
--  
--  
--  Now create a temporary view.

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

--  %md
--  
--  Run the cell below to check your work.

-- COMMAND ----------

--  %python
--  assert spark.table("celsius_temp"), "Table named `celsius_temp` does not exist"
--  assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'celsius_temp'").first()["isTemporary"] == True, "Table is not temporary"

-- COMMAND ----------

--  %md
--  
--  
--  Now register a global temp view.

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

--  %md
--  
--  Run the cell below to check your work.

-- COMMAND ----------

--  %python
--  assert spark.table("global_temp.celsius_global"), "Global temporary view named `celsius_global` does not exist"

-- COMMAND ----------

--  %md
--  
--  
--  Views will be displayed alongside tables when listing from the catalog.

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

--  %md
--  
--  
--  Note the following:
--  - The view is associated with the current database. This view will be available to any user that can access this database and will persist between sessions.
--  - The temp view is not associated with any database. The temp view is ephemeral and is only accessible in the current SparkSession.
--  - The global temp view does not appear in our catalog. **Global temp views will always register to the **`global_temp`** database**. The **`global_temp`** database is ephemeral but tied to the lifetime of the cluster; however, it is only accessible by notebooks attached to the same cluster on which it was created.

-- COMMAND ----------

SELECT * FROM global_temp.celsius_global

-- COMMAND ----------

--  %md
--  
--  
--  While no job was triggered when defining these views, a job is triggered _each time_ a query is executed against the view.

-- COMMAND ----------

--  %md
--  
--  
--  ## Clean Up
--  Drop the database and all tables to clean up your workspace.

-- COMMAND ----------

DROP DATABASE ${da.db_name} CASCADE

-- COMMAND ----------

--  %md
--  
--  
--  ## Synopsis
--  
--  In this lab we:
--  - Created and deleted databases
--  - Explored behavior of managed and external tables
--  - Learned about the scoping of views

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
