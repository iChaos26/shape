import pyspark

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructField, StructType, IntegerType
from etl.utils.sparkutils import Spark, SparkSession


class CsvHander:

    def __init__(self):
        




#spark = SparkSession.builder \
#         .appName('SparkCassandraApp') \
#         .getOrCreate()
#
#sc = SQLContext(spark)

# Infer Schema

#cassandra = Cassandra()
#df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table=".options(table="csv_data", keyspace="test")kv", keyspace="test").save()

def csv_schema():
    schema = StructType([StructField("equipment_id", IntegerType(), True),
                         StructField("sensor_id", IntegerType(), True)])
 
df = sc.read \
    .option("delimiter" , ";") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/home/joao/dev/shape/Data/*csv")

#df.show(20)

def export_csv(df):
    return df


#df.printSchema()
#df.show(truncate=False, vertical=True)
#root
# |-- equipment_id: integer (nullable = true)
# |-- sensor_id: integer (nullable = true)
