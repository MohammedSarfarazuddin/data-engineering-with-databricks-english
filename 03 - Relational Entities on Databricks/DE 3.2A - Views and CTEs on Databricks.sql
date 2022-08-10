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
--  # Views and CTEs on Databricks
--  In this demonstration, you will create and explore views and common table expressions (CTEs).
--  
--  ## Learning Objectives
--  By the end of this lesson, you should be able to:
--  * Use Spark SQL DDL to define views
--  * Run queries that use common table expressions
--  
--  
--  
--  **Resources**
--  * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-view.html" target="_blank">Create View - Databricks Docs</a>
--  * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-qry-select-cte.html" target="_blank">Common Table Expressions - Databricks Docs</a>

-- COMMAND ----------

--  %md
--  
--  
--  ## Classroom Setup
--  The following script clears out previous runs of this demo and configures some Hive variables that will be used in our SQL queries.

-- COMMAND ----------

--  %run ../Includes/Classroom-Setup-03.2A

-- COMMAND ----------

--  %md
--  
--   
--  We start by creating a table of data we can use for the demonstration.

-- COMMAND ----------

-- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
CREATE TABLE external_table
USING CSV OPTIONS (
  path = '${da.paths.datasets}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST"
);

SELECT * FROM external_table;

-- COMMAND ----------

--  %md
--  
--  
--  
--  To show a list of tables (and views), we use the **`SHOW TABLES`** command also demonstrated below.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

--  %md
--  
--  
--  
--  ## Views, Temp Views & Global Temp Views
--  
--  To set this demonstration up, we are going to first create one of each type of view.
--  
--  Then in the next notebook, we will explore the differences between how each one behaves.

-- COMMAND ----------

--  %md
--  
--  
--  ### Views
--  Let's create a view that contains only the data where the origin is "ABQ" and the destination is "LAX".

-- COMMAND ----------

CREATE VIEW view_delays_abq_lax AS
  SELECT * 
  FROM external_table 
  WHERE origin = 'ABQ' AND destination = 'LAX';

SELECT * FROM view_delays_abq_lax;

-- COMMAND ----------

--  %md
--  
--   
--   
--  Note that the **`view_delays_abq_lax`** view has been added to the list below:

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

--  %md
--  
--   
--  ### Temporary Views
--  
--  Next we'll create a temporary view. 
--  
--  The syntax is very similar but adds **`TEMPORARY`** to the command.

-- COMMAND ----------

CREATE TEMPORARY VIEW temp_view_delays_gt_120
AS SELECT * FROM external_table WHERE delay > 120 ORDER BY delay ASC;

SELECT * FROM temp_view_delays_gt_120;

-- COMMAND ----------

--  %md
--  
--  
--  
--  Now if we show our tables again, we will see the one table and both views.
--  
--  Make note of the values in the **`isTemporary`** column.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

--  %md
--  
--  
--  
--  ### Global Temp Views
--  
--  Lastly, we'll create a global temp view. 
--  
--  Here we simply add **`GLOBAL`** to the command. 
--  
--  Also note the **`global_temp`** database qualifer in the subsequent **`SELECT`** statement.

-- COMMAND ----------

CREATE GLOBAL TEMPORARY VIEW global_temp_view_dist_gt_1000 
AS SELECT * FROM external_table WHERE distance > 1000;

SELECT * FROM global_temp.global_temp_view_dist_gt_1000;

-- COMMAND ----------

--  %md
--  
--  
--  
--  Before we move on, review one last time the database's tables and views...

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

--  %md
--  
--  
--  
--  ...and the tables and views in the **`global_temp`** database:

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

--  %md
--  
--  
--  
--  Next we are going to demonstrate how tables and views are persisted across multiple sessions and how temp views are not.
--  
--  To do this simply open the next notebook, [DE 3.2B - Views and CTEs on Databricks, Cont]($./DE 3.2B - Views and CTEs on Databricks, Cont), and continue with the lesson.
--  
--  <img src="https://files.training.databricks.com/images/icon_note_24.png"> Note: There are several scenarios in which a new session may be created:
--  * Restarting a cluster
--  * Detaching and reataching to a cluster
--  * Installing a python package which in turn restarts the Python interpreter
--  * Or simply opening a new notebook

-- COMMAND ----------

--  %md-sandbox
--  &copy; 2022 Databricks, Inc. All rights reserved.<br/>
--  Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
--  <br/>
--  <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
