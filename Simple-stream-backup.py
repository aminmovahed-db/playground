# Databricks notebook source
checkpoint_path = "s3://one-env-uc-external-location/am-auto-loader-test/checkpoints/test/"
dbutils.fs.mkdirs(checkpoint_path)
checkpoint_backup_path = "s3://one-env-uc-external-location/am-auto-loader-test/checkpoints_backup/"
dbutils.fs.mkdirs(checkpoint_backup_path)

# COMMAND ----------

dbutils.fs.rm(checkpoint_path, True)

# COMMAND ----------

# MAGIC %sql 
# MAGIC use catalog main;
# MAGIC use schema nab_bronze_test_am;

# COMMAND ----------

from pyspark.sql import functions as F
def update_silver():
    query = (spark.readStream
                  .table("bronze_test")
                  .withColumn("processed_time", F.current_timestamp())
                  .writeStream.option("checkpointLocation", checkpoint_path)
                  .trigger(availableNow=True)
                  .table("silver_test"))
    
    query.awaitTermination()

# COMMAND ----------

from pyspark.sql import functions as F
def update_silver_backup():
    query = (spark.readStream
                  .table("bronze_backup")
                  .withColumn("processed_time", F.current_timestamp())
                  .writeStream.option("checkpointLocation", checkpoint_backup_path)
                  .trigger(availableNow=True)
                  .table("silver_backup"))
    
    query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE bronze_test
# MAGIC (id INT, name STRING, value DOUBLE); 
# MAGIC
# MAGIC INSERT INTO bronze_test
# MAGIC VALUES (1, "Yve", 1.0),
# MAGIC        (2, "Omar", 2.5),
# MAGIC        (3, "Elia", 3.3)

# COMMAND ----------

update_silver()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze_test
# MAGIC VALUES (4, "Ted", 4.7),
# MAGIC        (5, "Tiffany", 5.5),
# MAGIC        (6, "Vini", 6.3)

# COMMAND ----------

update_silver()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table silver_backup deep clone silver_test

# COMMAND ----------

# dbutils.fs.cp(checkpoint_path, checkpoint_backup_path,recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze_test
# MAGIC VALUES (7, "Emmy", 3.2),
# MAGIC        (8, "Brad", 7.1),
# MAGIC        (9, "John", 9.3)

# COMMAND ----------

update_silver()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table silver_backup deep clone silver_test

# COMMAND ----------

# MAGIC %sql
# MAGIC create table bronze_backup deep clone bronze_test

# COMMAND ----------

update_silver_backup()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for rows in table1 that are not in table2
# MAGIC SELECT id, name, value
# MAGIC FROM silver_test
# MAGIC EXCEPT
# MAGIC SELECT id, name, value
# MAGIC FROM silver_backup;
# MAGIC -- -- Check for rows in table2 that are not in table1
# MAGIC SELECT id, name, value
# MAGIC FROM silver_backup
# MAGIC EXCEPT
# MAGIC SELECT id, name, value 
# MAGIC FROM silver_test;

# COMMAND ----------

spark.sql("Drop table if exists bronze_test")
spark.sql("Drop table if exists silver_test")
spark.sql("Drop table if exists silver_backup")
spark.sql("Drop table if exists bronze_backup")
dbutils.fs.rm(checkpoint_path,recurse=True)
dbutils.fs.rm(checkpoint_backup_path,recurse=True)

