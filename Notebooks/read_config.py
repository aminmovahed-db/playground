import os
import yaml
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql import Row
from pyspark.sql import functions as F

def yml_to_df(config_path, schema):
    try:
        with open(config_path, 'r') as file:
            file_content = file.read().replace('\t', '    ')
            config_content = yaml.safe_load(file_content)
            df = create_spark_df_from_dict(config_content, schema)
    except Exception as e:
        print(f"Error reading {config_path}: {e}")
        return None
    return df
      
def create_spark_df_from_dict(data, schema):
    models = data.get('config', {}).get('models', [])
    rows = [Row(
        target_table=model.get('target_table', None),
        filename=model.get('filename', None),
        incr_source_table=model.get('incr_source_table', None),
        multi_source_db_tbls=model.get('multi_source_db_tbls', None)
    ) for model in models]
    return spark.createDataFrame(rows, schema)


def read_config_files_in_subfolders(folder):
    config_data = {}
    schema = StructType([
        StructField("target_table", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("incr_source_table", StringType(), True),
        StructField("multi_source_db_tbls", StringType(), True)
    ])
    df = spark.createDataFrame([], schema)
    for root, dirs, files in os.walk(folder):
        if 'config.yaml' in files:
            config_path = os.path.join(root, 'config.yaml')
            df = df.union(yml_to_df(config_path, schema))
    return df

def add_checkpoint_paths(df):
    df = df.withColumn(
        "multi_source_db_tbls",
        F.split(F.regexp_replace(F.col("multi_source_db_tbls"), r'[\[\]]', ''), ',\s*')
    )
    df = df.withColumn(
        "multi_source_db_tbls",
        F.explode_outer("multi_source_db_tbls")
    )

    df = df.withColumn(
            "checkpoints_path",
            F.when(
                F.col("incr_source_table").isNotNull(),  # if incr_source_table is not null
                F.concat_ws("/", F.col("target_table"), F.col("incr_source_table"))
            ).when(
                F.col("filename").isNotNull(),  # if incr_source_table is null but filename is not null
                F.concat_ws("/", F.col("target_table"), F.col("filename"), F.col("multi_source_db_tbls"))
            ).otherwise(  # if incr_source_table and filename are both null
                F.concat_ws("/", F.col("target_table"), F.lit(F.col("target_table") + ".sql"), F.col("multi_source_db_tbls"))
            )
        )

    return df
  

