# Databricks notebook source
catalog = 'main'
bronze_schema = 'nab_shared_mode_test'
silver_schema = 'nab_shared_mode_test'

spark.conf.set ('db.bronze_schema', f'{catalog}.{bronze_schema}')
spark.conf.set ('db.silver_schema', f'{catalog}.{silver_schema}')

spark.conf.set("spark.databricks.analyzer.noSyncIsStreamingToCteReference", "true")
checkpoint_path = f'/Volumes/{catalog}/{bronze_schema}/checkpoints'
checkpoint_backup_path = f"/Volumes/{catalog}/{bronze_schema}/checkpoints_backup"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${db.bronze_schema};
# MAGIC CREATE SCHEMA IF NOT EXISTS ${db.silver_schema};
# MAGIC CREATE VOLUME IF NOT EXISTS ${db.bronze_schema}.checkpoints;
# MAGIC CREATE VOLUME IF NOT EXISTS ${db.bronze_schema}.checkpoints_backup;

# COMMAND ----------

dbutils.fs.rm(checkpoint_path, True)
dbutils.fs.rm(checkpoint_backup_path, True)

# COMMAND ----------

def simulate_framework_read():
    emp_df = spark.readStream.table(f"{bronze_schema}.employee")
    emp_df.createOrReplaceGlobalTempView("employee")

    mgr_df = spark.readStream.table(f"{bronze_schema}.employee_phone")
    mgr_df.createOrReplaceGlobalTempView("employee_phone")

    return spark.sql("""
                     WITH driver as (
                       SELECT DISTINCT * FROM (
                          select emp_id FROM global_temp.employee  
                          UNION ALL
                          select emp_id FROM global_temp.employee_phone
                       ) 
                     ),
                     src_raw AS (
                      SELECT 
                      e.emp_id
                      , e.emp_name
                      , e.emp_batch_no
                      , p.phone
                      , e.extract_dttm as EXTRACT_DTTM
                      FROM driver d 
                        LEFT JOIN ${db.bronze_schema}.employee e 
                          ON d.emp_id = e.emp_id 
                        LEFT JOIN ${db.bronze_schema}.employee_phone p 
                          ON d.emp_id = p.emp_id
                     )
                     SELECT 
                      src.* 
                     FROM src_raw src
                  """)

def upsertToDelta(microBatchOutputDF, batchId):
  microBatchOutputDF.createOrReplaceTempView("updates")
  microBatchOutputDF.sparkSession.sql("""
    MERGE INTO ${db.silver_schema}.target t
    USING updates s
    ON s.emp_id = t.emp_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)

def simulate_framework_write(df):
      streaming_query = (df.writeStream
                        .trigger(once=True)
                        .option("checkpointLocation", checkpoint_path)
                        .foreachBatch(upsertToDelta)
                        .outputMode("update")
                        .start()
                        )
      streaming_query.awaitTermination()

# COMMAND ----------

from pyspark.sql.functions import max, col
def checkpoints_read(checkpoint_path):
      max_commit = spark.read.json(f'{checkpoint_path}/commits').selectExpr('_metadata.file_name').agg(max('_metadata.file_name')).collect()[0][0]

      return (spark.read
            .json(f'{checkpoint_path}/offsets')
            .where("reservoirId is not null")
            .where(f"_metadata.file_name = '{max_commit}'")).withColumn("Version", col("reservoirVersion") + col("index")).selectExpr('reservoirId', 'Version','_metadata.file_name').withColumnRenamed('_metadata.file_name', 'commit_version')
      


def check_version(table_name):
    return spark.sql(f"describe history {table_name}").first()["version"]

# COMMAND ----------

def get_max_commit(checkpoints_path):
    return spark.read.json(f'{checkpoints_path}/commits').selectExpr('_metadata.file_name').agg(max('_metadata.file_name')).collect()[0][0]

def backup_checkpoints(checkpoints_path, backup_path, max_commit):
    
    # Check if the checkpoint location exists
    if not dbutils.fs.ls(checkpoints_path):
        raise ValueError(f"Checkpoint path '{checkpoints_path}' does not exist.")

    # Create the backup path if it doesn't exist
    try:
        dbutils.fs.ls(backup_path)
        print(f"Backup path '{backup_path}' already exists.")
    except Exception:
        print(f"Backup path '{backup_path}' does not exist. Creating it.")
        dbutils.fs.mkdirs(backup_path)
    
    folders_dict = {
    "metadata": "metadata",
    "commits": "commits",
    "offsets": "offsets",
    "state": f"state/state_{max_commit}"
    }


    for folder in folders_dict:
        try:
            dbutils.fs.cp(f"{checkpoints_path}/{folder}", f"{backup_path}/{folders_dict[folder]}", recurse=True)
        except Exception as e:
            print(f"Error copying {folder}: {e}")



# COMMAND ----------

def run_pipeline():
    df = simulate_framework_read()
    df = df.distinct()
    simulate_framework_write(df)
    
def run_backup():
    employee_v = check_version(f"{bronze_schema}.employee")
    employee_phone_v = check_version(f"{bronze_schema}.employee_phone")
    target_v = check_version(f"{silver_schema}.target")
    print(f"Employee version: {employee_v}\nEmployee_phone version: {employee_phone_v}\nTarget version: {target_v}")
    display(checkpoints_read(checkpoint_path))
    backup_checkpoints(checkpoint_path, checkpoint_backup_path, get_max_commit(checkpoint_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ${db.bronze_schema}.employee;
# MAGIC CREATE TABLE ${db.bronze_schema}.employee (
# MAGIC   EMP_ID string,
# MAGIC   EMP_NAME string,
# MAGIC   EMP_BATCH_NO string,
# MAGIC   MANAGER_ID string,
# MAGIC   EXTRACT_DTTM timestamp)
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC DROP TABLE IF EXISTS ${db.bronze_schema}.employee_phone;
# MAGIC CREATE TABLE ${db.bronze_schema}.employee_phone (
# MAGIC   EMP_ID string,
# MAGIC   PHONE string,
# MAGIC   EXTRACT_DTTM timestamp)
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC DROP TABLE IF EXISTS ${db.silver_schema}.target;
# MAGIC CREATE TABLE ${db.silver_schema}.target (
# MAGIC   EMP_ID string,
# MAGIC   EMP_NAME string,
# MAGIC   EMP_BATCH_NO string,
# MAGIC   PHONE string,
# MAGIC   EXTRACT_DTTM timestamp)
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${db.bronze_schema}.employee (
# MAGIC   EMP_ID,
# MAGIC   EMP_NAME,
# MAGIC   EMP_BATCH_NO,
# MAGIC   MANAGER_ID,
# MAGIC   EXTRACT_DTTM)
# MAGIC VALUES
# MAGIC   ('E1', 'E1-Name', 'B1-E1-1111', 'M1', '2024-01-02 00:00:00');
# MAGIC
# MAGIC
# MAGIC INSERT INTO TABLE ${db.bronze_schema}.employee_phone (
# MAGIC   EMP_ID,
# MAGIC   PHONE,
# MAGIC   EXTRACT_DTTM)
# MAGIC VALUES
# MAGIC   ('E1', '0411111111', '2024-01-02 00:00:00');

# COMMAND ----------

run_pipeline()
run_backup()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE ${db.bronze_schema}.employee (
# MAGIC   EMP_ID,
# MAGIC   EMP_NAME,
# MAGIC   EMP_BATCH_NO,
# MAGIC   MANAGER_ID,
# MAGIC   EXTRACT_DTTM)
# MAGIC VALUES
# MAGIC   ('E2', 'E2-Name', 'B2-E2-1111', 'M2', '2024-01-02 00:00:00');
# MAGIC
# MAGIC INSERT INTO TABLE ${db.bronze_schema}.employee_phone (
# MAGIC   EMP_ID,
# MAGIC   PHONE,
# MAGIC   EXTRACT_DTTM)
# MAGIC VALUES
# MAGIC   ('E2', '0422222222', '2024-01-02 00:00:00');

# COMMAND ----------

# DBTITLE 1,single user mode test
run_pipeline()
run_backup()
