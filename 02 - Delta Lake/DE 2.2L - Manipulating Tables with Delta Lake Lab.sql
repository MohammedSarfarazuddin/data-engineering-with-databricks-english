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
--  
--  # Manipulating Tables with Delta Lake
--  
--  This notebook provides a hands-on review of some of the basic functionality of Delta Lake.
--  
--  ## Learning Objectives
--  By the end of this lab, you should be able to:
--  - Execute standard operations to create and manipulate Delta Lake tables, including:
--    - **`CREATE TABLE`**
--    - **`INSERT INTO`**
--    - **`SELECT FROM`**
--    - **`UPDATE`**
--    - **`DELETE`**
--    - **`MERGE`**
--    - **`DROP TABLE`**

-- COMMAND ----------

--  %md
--  
--  
--  
--  ## Setup
--  Run the following script to setup necessary variables and clear out past runs of this notebook. Note that re-executing this cell will allow you to start the lab over.

-- COMMAND ----------

--  %run ../Includes/Classroom-Setup-02.2L

-- COMMAND ----------

--  %md
--  
--  
--  
--  ## Create a Table
--  
--  In this notebook, we'll be creating a table to track our bean collection.
--  
--  Use the cell below to create a managed Delta Lake table named **`beans`**.
--  
--  Provide the following schema:
--  
--  | Field Name | Field type |
--  | --- | --- |
--  | name | STRING |
--  | color | STRING |
--  | grams | FLOAT |
--  | delicious | BOOLEAN |

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

--  %md
--  
--  
--  
--  **NOTE**: We'll use Python to run checks occasionally throughout the lab. The following cell will return as error with a message on what needs to change if you have not followed instructions. No output from cell execution means that you have completed this step.

-- COMMAND ----------

--  %python
--  assert spark.table("beans"), "Table named `beans` does not exist"
--  assert spark.table("beans").columns == ["name", "color", "grams", "delicious"], "Please name the columns in the order provided above"
--  assert spark.table("beans").dtypes == [("name", "string"), ("color", "string"), ("grams", "float"), ("delicious", "boolean")], "Please make sure the column types are identical to those provided above"

-- COMMAND ----------

--  %md
--  
--  
--  ## Insert Data
--  
--  Run the following cell to insert three rows into the table.

-- COMMAND ----------

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false)

-- COMMAND ----------

--  %md
--  
--  
--  
--  Manually review the table contents to ensure data was written as expected.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

--  %md
--  
--  
--  
--  Insert the additional records provided below. Make sure you execute this as a single transaction.

-- COMMAND ----------

-- TODO
<FILL-IN>
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false)

-- COMMAND ----------

--  %md
--  
--  
--  
--  Run the cell below to confirm the data is in the proper state.

-- COMMAND ----------

--  %python
--  assert spark.table("beans").count() == 6, "The table should have 6 records"
--  assert spark.conf.get("spark.databricks.delta.lastCommitVersionInSession") == "2", "Only 3 commits should have been made to the table"
--  assert set(row["name"] for row in spark.table("beans").select("name").collect()) == {'beanbag chair', 'black', 'green', 'jelly', 'lentils', 'pinto'}, "Make sure you have not modified the data provided"

-- COMMAND ----------

--  %md
--  
--  
--  
--  ## Update Records
--  
--  A friend is reviewing your inventory of beans. After much debate, you agree that jelly beans are delicious.
--  
--  Run the following cell to update this record.

-- COMMAND ----------

UPDATE beans
SET delicious = true
WHERE name = "jelly"

-- COMMAND ----------

--  %md
--  
--  
--  
--  You realize that you've accidentally entered the weight of your pinto beans incorrectly.
--  
--  Update the **`grams`** column for this record to the correct weight of 1500.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

--  %md
--  
--  
--  
--  Run the cell below to confirm this has completed properly.

-- COMMAND ----------

--  %python
--  assert spark.table("beans").filter("name='pinto'").count() == 1, "There should only be 1 entry for pinto beans"
--  row = spark.table("beans").filter("name='pinto'").first()
--  assert row["color"] == "brown", "The pinto bean should be labeled as the color brown"
--  assert row["grams"] == 1500, "Make sure you correctly specified the `grams` as 1500"
--  assert row["delicious"] == True, "The pinto bean is a delicious bean"

-- COMMAND ----------

--  %md
--  
--  
--  
--  ## Delete Records
--  
--  You've decided that you only want to keep track of delicious beans.
--  
--  Execute a query to drop all beans that are not delicious.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

--  %md
--  
--  
--  
--  Run the following cell to confirm this operation was successful.

-- COMMAND ----------

--  %python
--  assert spark.table("beans").filter("delicious=true").count() == 5, "There should be 5 delicious beans in your table"
--  assert spark.table("beans").filter("name='beanbag chair'").count() == 0, "Make sure your logic deletes non-delicious beans"

-- COMMAND ----------

--  %md
--  
--  
--  
--  ## Using Merge to Upsert Records
--  
--  Your friend gives you some new beans. The cell below registers these as a temporary view.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

SELECT * FROM new_beans

-- COMMAND ----------

--  %md
--  
--  
--  
--  In the cell below, use the above view to write a merge statement to update and insert new records to your **`beans`** table as one transaction.
--  
--  Make sure your logic:
--  - Matches beans by name **and** color
--  - Updates existing beans by adding the new weight to the existing weight
--  - Inserts new beans only if they are delicious

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

--  %md
--  
--  
--  
--  Run the cell below to check your work.

-- COMMAND ----------

--  %python
--  version = spark.sql("DESCRIBE HISTORY beans").selectExpr("max(version)").first()[0]
--  last_tx = spark.sql("DESCRIBE HISTORY beans").filter(f"version={version}")
--  assert last_tx.select("operation").first()[0] == "MERGE", "Transaction should be completed as a merge"
--  metrics = last_tx.select("operationMetrics").first()[0]
--  assert metrics["numOutputRows"] == "3", "Make sure you only insert delicious beans"
--  assert metrics["numTargetRowsUpdated"] == "1", "Make sure you match on name and color"
--  assert metrics["numTargetRowsInserted"] == "2", "Make sure you insert newly collected beans"
--  assert metrics["numTargetRowsDeleted"] == "0", "No rows should be deleted by this operation"

-- COMMAND ----------

--  %md
--  
--  
--  
--  ## Dropping Tables
--  
--  When working with managed Delta Lake tables, dropping a table results in permanently deleting access to the table and all underlying data files.
--  
--  **NOTE**: Later in the course, we'll learn about external tables, which approach Delta Lake tables as a collection of files and have different persistence guarantees.
--  
--  In the cell below, write a query to drop the **`beans`** table.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

--  %md
--  
--  
--  
--  Run the cell below to assert that your table no longer exists.

-- COMMAND ----------

--  %python
--  assert spark.sql("SHOW TABLES LIKE 'beans'").collect() == [], "Confirm that you have dropped the `beans` table from your current database"

-- COMMAND ----------

--  %md
--  
--  
--  
--  ## Wrapping Up
--  
--  By completing this lab, you should now feel comfortable:
--  * Completing standard Delta Lake table creation and data manipulation commands

-- COMMAND ----------

--  %md
--  
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
