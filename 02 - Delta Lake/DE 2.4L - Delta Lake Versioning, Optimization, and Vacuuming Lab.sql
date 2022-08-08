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
--  # Delta Lake Versioning, Optimization, and Vacuuming
--  
--  This notebook provides a hands-on review of some of the more esoteric features Delta Lake brings to the data lakehouse.
--  
--  ## Learning Objectives
--  By the end of this lab, you should be able to:
--  - Review table history
--  - Query previous table versions and rollback a table to a specific version
--  - Perform file compaction and Z-order indexing
--  - Preview files marked for permanent deletion and commit these deletes

-- COMMAND ----------

--  %md
--  
--  
--  ## Setup
--  Run the following script to setup necessary variables and clear out past runs of this notebook. Note that re-executing this cell will allow you to start the lab over.

-- COMMAND ----------

--  %run ../Includes/Classroom-Setup-02.4L

-- COMMAND ----------

--  %md
--  
--  
--  ## Recreate the History of your Bean Collection
--  
--  This lab picks up where the last lab left off. The cell below condenses all the operations from the last lab into a single cell (other than the final **`DROP TABLE`** statement).
--  
--  For quick reference, the schema of the **`beans`** table created is:
--  
--  | Field Name | Field type |
--  | --- | --- |
--  | name | STRING |
--  | color | STRING |
--  | grams | FLOAT |
--  | delicious | BOOLEAN |

-- COMMAND ----------

CREATE TABLE beans 
(name STRING, color STRING, grams FLOAT, delicious BOOLEAN);

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false);

INSERT INTO beans VALUES
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false);

UPDATE beans
SET delicious = true
WHERE name = "jelly";

UPDATE beans
SET grams = 1500
WHERE name = 'pinto';

DELETE FROM beans
WHERE delicious = false;

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

MERGE INTO beans a
USING new_beans b
ON a.name=b.name AND a.color = b.color
WHEN MATCHED THEN
  UPDATE SET grams = a.grams + b.grams
WHEN NOT MATCHED AND b.delicious = true THEN
  INSERT *;

-- COMMAND ----------

--  %md
--  
--  
--  ## Review the Table History
--  
--  Delta Lake's transaction log stores information about each transaction that modifies a table's contents or settings.
--  
--  Review the history of the **`beans`** table below.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

--  %md
--  
--  
--  If all the previous operations were completed as described you should see 7 versions of the table (**NOTE**: Delta Lake versioning starts with 0, so the max version number will be 6).
--  
--  The operations should be as follows:
--  
--  | version | operation |
--  | --- | --- |
--  | 0 | CREATE TABLE |
--  | 1 | WRITE |
--  | 2 | WRITE |
--  | 3 | UPDATE |
--  | 4 | UPDATE |
--  | 5 | DELETE |
--  | 6 | MERGE |
--  
--  The **`operationsParameters`** column will let you review predicates used for updates, deletes, and merges. The **`operationMetrics`** column indicates how many rows and files are added in each operation.
--  
--  Spend some time reviewing the Delta Lake history to understand which table version matches with a given transaction.
--  
--  **NOTE**: The **`version`** column designates the state of a table once a given transaction completes. The **`readVersion`** column indicates the version of the table an operation executed against. In this simple demo (with no concurrent transactions), this relationship should always increment by 1.

-- COMMAND ----------

--  %md
--  
--  
--  ## Query a Specific Version
--  
--  After reviewing the table history, you decide you want to view the state of your table after your very first data was inserted.
--  
--  Run the query below to see this.

-- COMMAND ----------

SELECT * FROM beans VERSION AS OF 1

-- COMMAND ----------

--  %md
--  
--  
--  And now review the current state of your data.

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

--  %md
--  
--  
--  You want to review the weights of your beans before you deleted any records.
--  
--  Fill in the statement below to register a temporary view of the version just before data was deleted, then run the following cell to query the view.

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TEMP VIEW pre_delete_vw AS
<FILL-IN>

-- COMMAND ----------

SELECT * FROM pre_delete_vw

-- COMMAND ----------

--  %md
--  
--  
--  Run the cell below to check that you have captured the correct version.

-- COMMAND ----------

--  %python
--  assert spark.table("pre_delete_vw"), "Make sure you have registered the temporary view with the provided name `pre_delete_vw`"
--  assert spark.table("pre_delete_vw").count() == 6, "Make sure you're querying a version of the table with 6 records"
--  assert spark.table("pre_delete_vw").selectExpr("int(sum(grams))").first()[0] == 43220, "Make sure you query the version of the table after updates were applied"

-- COMMAND ----------

--  %md
--  
--  
--  ## Restore a Previous Version
--  
--  Apparently there was a misunderstanding; the beans your friend gave you that you merged into your collection were not intended for you to keep.
--  
--  Revert your table to the version before this **`MERGE`** statement completed.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

--  %md
--  
--  
--  Review the history of your table. Make note of the fact that restoring to a previous version adds another table version.

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

--  %python
--  last_tx = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
--  assert spark.sql(f"DESCRIBE HISTORY beans").select("operation").first()[0] == "RESTORE", "Make sure you reverted your table with the `RESTORE` keyword"
--  assert spark.table("beans").count() == 5, "Make sure you reverted to the version after deleting records but before merging"

-- COMMAND ----------

--  %md
--  
--  
--  ## File Compaction
--  Looking at the transaction metrics during your reversion, you are surprised you have some many files for such a small collection of data.
--  
--  While indexing on a table of this size is unlikely to improve performance, you decide to add a Z-order index on the **`name`** field in anticipation of your bean collection growing exponentially over time.
--  
--  Use the cell below to perform file compaction and Z-order indexing.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

--  %md
--  
--  
--  Your data should have been compacted to a single file; confirm this manually by running the following cell.

-- COMMAND ----------

DESCRIBE DETAIL beans

-- COMMAND ----------

--  %md
--  
--  
--  Run the cell below to check that you've successfully optimized and indexed your table.

-- COMMAND ----------

--  %python
--  last_tx = spark.sql("DESCRIBE HISTORY beans").first()
--  assert last_tx["operation"] == "OPTIMIZE", "Make sure you used the `OPTIMIZE` command to perform file compaction"
--  assert last_tx["operationParameters"]["zOrderBy"] == '["name"]', "Use `ZORDER BY name` with your optimize command to index your table"

-- COMMAND ----------

--  %md
--  
--  
--  ## Cleaning Up Stale Data Files
--  
--  You know that while all your data now resides in 1 data file, the data files from previous versions of your table are still being stored alongside this. You wish to remove these files and remove access to previous versions of the table by running **`VACUUM`** on the table.
--  
--  Executing **`VACUUM`** performs garbage cleanup on the table directory. By default, a retention threshold of 7 days will be enforced.
--  
--  The cell below modifies some Spark configurations. The first command overrides the retention threshold check to allow us to demonstrate permanent removal of data. 
--  
--  **NOTE**: Vacuuming a production table with a short retention can lead to data corruption and/or failure of long-running queries. This is for demonstration purposes only and extreme caution should be used when disabling this setting.
--  
--  The second command sets **`spark.databricks.delta.vacuum.logging.enabled`** to **`true`** to ensure that the **`VACUUM`** operation is recorded in the transaction log.
--  
--  **NOTE**: Because of slight differences in storage protocols on various clouds, logging **`VACUUM`** commands is not on by default for some clouds as of DBR 9.1.

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

-- COMMAND ----------

--  %md
--  
--  
--  Before permanently deleting data files, review them manually using the **`DRY RUN`** option.

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

--  %md
--  
--  
--  All data files not in the current version of the table will be shown in the preview above.
--  
--  Run the command again without **`DRY RUN`** to permanently delete these files.
--  
--  **NOTE**: All previous versions of the table will no longer be accessible.

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS

-- COMMAND ----------

--  %md
--  
--  
--  Because **`VACUUM`** can be such a destructive act for important datasets, it's always a good idea to turn the retention duration check back on. Run the cell below to reactive this setting.

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = true

-- COMMAND ----------

--  %md
--  
--  
--  Note that the table history will indicate the user that completed the **`VACUUM`** operation, the number of files deleted, and log that the retention check was disabled during this operation.

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

--  %md
--  
--  
--  Query your table again to confirm you still have access to the current version.

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

--  %md
--  
--  
--  <img src="https://files.training.databricks.com/images/icon_warn_32.png"> Because Delta Cache stores copies of files queried in the current session on storage volumes deployed to your currently active cluster, you may still be able to temporarily access previous table versions (though systems should **not** be designed to expect this behavior). 
--  
--  Restarting the cluster will ensure that these cached data files are permanently purged.
--  
--  You can see an example of this by uncommenting and running the following cell that may, or may not, fail
--  (depending on the state of the cache).

-- COMMAND ----------

-- SELECT * FROM beans@v1

-- COMMAND ----------

--  %md
--  
--  
--  By completing this lab, you should now feel comfortable:
--  * Completing standard Delta Lake table creation and data manipulation commands
--  * Reviewing table metadata including table history
--  * Leverage Delta Lake versioning for snapshot queries and rollbacks
--  * Compacting small files and indexing tables
--  * Using **`VACUUM`** to review files marked for deletion and committing these deletes

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
