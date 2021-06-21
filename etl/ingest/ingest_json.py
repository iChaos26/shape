import pyspark
import json

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import udf

#from logging import GlobalLog

spark = SparkSession.builder \
         .appName('SparkCassandraApp') \
         .getOrCreate()

sc = SQLContext(spark)
# Save schema from the original DataFrame into json:
#schema_json = df.schema.json()

# Restore schema from json:
#new_schema = StructType.fromJson(json.loads(schema_json))

schema = StructType([StructField("equipment_id", IntegerType(), True),
                     StructField("code", StringType(), True),
                     StructField("group_name", StringType(), True)])

df = sc.read.option("multiLine", "true").option("mode", "PERMISSIVE") \
                    .json("/home/joao/dev/shape/Data/equipment.json", schema=schema) 
print(df.show())                    
#root
# |-- code: string (nullable = true)
# |-- equipment_id: long (nullable = true)
# |-- group_name: string (nullable = true)