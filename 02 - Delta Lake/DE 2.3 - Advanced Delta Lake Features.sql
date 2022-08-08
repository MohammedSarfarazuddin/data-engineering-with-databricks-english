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
--  # Advanced Delta Lake Features
--  
--  Now that you feel comfortable performing basic data tasks with Delta Lake, we can discuss a few features unique to Delta Lake.
--  
--  Note that while some of the keywords used here aren't part of standard ANSI SQL, all Delta Lake operations can be run on Databricks using SQL
--  
--  ## Learning Objectives
--  By the end of this lesson, you should be able to:
--  * Use **`OPTIMIZE`** to compact small files
--  * Use **`ZORDER`** to index tables
--  * Describe the directory structure of Delta Lake files
--  * Review a history of table transactions
--  * Query and roll back to previous table version
--  * Clean up stale data files with **`VACUUM`**
--  
--  **Resources**
--  * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html" target="_blank">Delta Optimize - Databricks Docs</a>
--  * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html" target="_blank">Delta Vacuum - Databricks Docs</a>

-- COMMAND ----------

--  %md
--  
--  
--  ## Run Setup
--  The first thing we're going to do is run a setup script. It will define a username, userhome, and database that is scoped to each user.

-- COMMAND ----------

--  %run ../Includes/Classroom-Setup-02.3

-- COMMAND ----------

--  %md
--  
--  
--  ## Creating a Delta Table with History
--  
--  The cell below condenses all the transactions from the previous lesson into a single cell. (Except for the **`DROP TABLE`**!)
--  
--  As you're waiting for this query to run, see if you can identify the total number of transactions being executed.

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
  
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);
  
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%";

DELETE FROM students 
WHERE value > 6;

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *;

-- COMMAND ----------

--  %md
--  
--  
--  ## Examine Table Details
--  
--  Databricks uses a Hive metastore by default to register databases, tables, and views.
--  
--  Using **`DESCRIBE EXTENDED`** allows us to see important metadata about our table.

-- COMMAND ----------

DESCRIBE EXTENDED students

-- COMMAND ----------

--  %md
--  
--  
--  **`DESCRIBE DETAIL`** is another command that allows us to explore table metadata.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

--  %md
--  
--  
--  Note the **`Location`** field.
--  
--  While we've so far been thinking about our table as just a relational entity within a database, a Delta Lake table is actually backed by a collection of files stored in cloud object storage.

-- COMMAND ----------

--  %md
--  
--  
--  ## Explore Delta Lake Files
--  
--  We can see the files backing our Delta Lake table by using a Databricks Utilities function.
--  
--  **NOTE**: It's not important right now to know everything about these files to work with Delta Lake, but it will help you gain a greater appreciation for how the technology is implemented.

-- COMMAND ----------

--  %python
--  display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

--  %md
--  
--  
--  Note that our directory contains a number of Parquet data files and a directory named **`_delta_log`**.
--  
--  Records in Delta Lake tables are stored as data in Parquet files.
--  
--  Transactions to Delta Lake tables are recorded in the **`_delta_log`**.
--  
--  We can peek inside the **`_delta_log`** to see more.

-- COMMAND ----------

--  %python
--  display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

--  %md
--  
--  
--  Each transaction results in a new JSON file being written to the Delta Lake transaction log. Here, we can see that there are 8 total transactions against this table (Delta Lake is 0 indexed).

-- COMMAND ----------

--  %md
--  
--  
--  ## Reasoning about Data Files
--  
--  We just saw a lot of data files for what is obviously a very small table.
--  
--  **`DESCRIBE DETAIL`** allows us to see some other details about our Delta table, including the number of files.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

--  %md
--  
--  
--  Here we see that our table currently contains 4 data files in its present version. So what are all those other Parquet files doing in our table directory? 
--  
--  Rather than overwriting or immediately deleting files containing changed data, Delta Lake uses the transaction log to indicate whether or not files are valid in a current version of the table.
--  
--  Here, we'll look at the transaction log corresponding the **`MERGE`** statement above, where records were inserted, updated, and deleted.

-- COMMAND ----------

--  %python
--  display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))

-- COMMAND ----------

--  %md
--  
--  
--  The **`add`** column contains a list of all the new files written to our table; the **`remove`** column indicates those files that no longer should be included in our table.
--  
--  When we query a Delta Lake table, the query engine uses the transaction logs to resolve all the files that are valid in the current version, and ignores all other data files.

-- COMMAND ----------

--  %md
--  
--  
--  ## Compacting Small Files and Indexing
--  
--  Small files can occur for a variety of reasons; in our case, we performed a number of operations where only one or several records were inserted.
--  
--  Files will be combined toward an optimal size (scaled based on the size of the table) by using the **`OPTIMIZE`** command.
--  
--  **`OPTIMIZE`** will replace existing data files by combining records and rewriting the results.
--  
--  When executing **`OPTIMIZE`**, users can optionally specify one or several fields for **`ZORDER`** indexing. While the specific math of Z-order is unimportant, it speeds up data retrieval when filtering on provided fields by colocating data with similar values within data files.

-- COMMAND ----------

OPTIMIZE students
ZORDER BY id

-- COMMAND ----------

--  %md
--  
--  
--  Given how small our data is, **`ZORDER`** does not provide any benefit, but we can see all of the metrics that result from this operation.

-- COMMAND ----------

--  %md
--  
--  
--  ## Reviewing Delta Lake Transactions
--  
--  Because all changes to the Delta Lake table are stored in the transaction log, we can easily review the <a href="https://docs.databricks.com/spark/2.x/spark-sql/language-manual/describe-history.html" target="_blank">table history</a>.

-- COMMAND ----------

DESCRIBE HISTORY students

-- COMMAND ----------

--  %md
--  
--  
--  As expected, **`OPTIMIZE`** created another version of our table, meaning that version 8 is our most current version.
--  
--  Remember all of those extra data files that had been marked as removed in our transaction log? These provide us with the ability to query previous versions of our table.
--  
--  These time travel queries can be performed by specifying either the integer version or a timestamp.
--  
--  **NOTE**: In most cases, you'll use a timestamp to recreate data at a time of interest. For our demo we'll use version, as this is deterministic (whereas you may be running this demo at any time in the future).

-- COMMAND ----------

SELECT * 
FROM students VERSION AS OF 3

-- COMMAND ----------

--  %md
--  
--  
--  What's important to note about time travel is that we're not recreating a previous state of the table by undoing transactions against our current version; rather, we're just querying all those data files that were indicated as valid as of the specified version.

-- COMMAND ----------

--  %md
--  
--  
--  ## Rollback Versions
--  
--  Suppose you're typing up query to manually delete some records from a table and you accidentally execute this query in the following state.

-- COMMAND ----------

DELETE FROM students

-- COMMAND ----------

--  %md
--  
--  
--  Note that when we see a **`-1`** for number of rows affected by a delete, this means an entire directory of data has been removed.
--  
--  Let's confirm this below.

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

--  %md
--  
--  
--  Deleting all the records in your table is probably not a desired outcome. Luckily, we can simply rollback this commit.

-- COMMAND ----------

RESTORE TABLE students TO VERSION AS OF 8 

-- COMMAND ----------

--  %md
--  
--  
--  Note that a **`RESTORE`** <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html" target="_blank">command</a> is recorded as a transaction; you won't be able to completely hide the fact that you accidentally deleted all the records in the table, but you will be able to undo the operation and bring your table back to a desired state.

-- COMMAND ----------

--  %md
--  
--  
--  ## Cleaning Up Stale Files
--  
--  Databricks will automatically clean up stale files in Delta Lake tables.
--  
--  While Delta Lake versioning and time travel are great for querying recent versions and rolling back queries, keeping the data files for all versions of large production tables around indefinitely is very expensive (and can lead to compliance issues if PII is present).
--  
--  If you wish to manually purge old data files, this can be performed with the **`VACUUM`** operation.
--  
--  Uncomment the following cell and execute it with a retention of **`0 HOURS`** to keep only the current version:

-- COMMAND ----------

-- VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

--  %md
--  
--  
--  By default, **`VACUUM`** will prevent you from deleting files less than 7 days old, just to ensure that no long-running operations are still referencing any of the files to be deleted. If you run **`VACUUM`** on a Delta table, you lose the ability time travel back to a version older than the specified data retention period.  In our demos, you may see Databricks executing code that specifies a retention of **`0 HOURS`**. This is simply to demonstrate the feature and is not typically done in production.  
--  
--  In the following cell, we:
--  1. Turn off a check to prevent premature deletion of data files
--  1. Make sure that logging of **`VACUUM`** commands is enabled
--  1. Use the **`DRY RUN`** version of vacuum to print out all records to be deleted

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM students RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

--  %md
--  
--  
--  By running **`VACUUM`** and deleting the 10 files above, we will permanently remove access to versions of the table that require these files to materialize.

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

--  %md
--  
--  
--  Check the table directory to show that files have been successfully deleted.

-- COMMAND ----------

--  %python
--  display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

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
