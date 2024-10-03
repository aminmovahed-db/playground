import os
import yaml
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row


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
        incr_source_table=model.get('incr_source_table', None)
    ) for model in models]
    return spark.createDataFrame(rows, schema)


def read_config_files_in_subfolders(folder):
    config_data = {}
    schema = StructType([
        StructField("target_table", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("incr_source_table", StringType(), True)
    ])
    df = spark.createDataFrame([], schema)
    for root, dirs, files in os.walk(folder):
        if 'config.yml' in files:
            config_path = os.path.join(root, 'config.yml')
            df = df.union(yml_to_df(config_path, schema))
    return df
  

