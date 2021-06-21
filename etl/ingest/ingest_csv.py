import pyspark

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructField, StructType, IntegerType
from pyspark.sql.functions import udf

spark = SparkSession.builder \
         .appName('SparkCassandraApp') \
         .getOrCreate()

sc = SQLContext(spark)

# Infer Schema

schema = StructType([StructField("equipment_id", IntegerType(), True),
                     StructField("sensor_id", IntegerType(), True)])
 
df = sc.read \
    .option("delimiter" , ";") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/home/joao/dev/shape/Data/*csv")

df.show()
#root
# |-- equipment_id: integer (nullable = true)
# |-- sensor_id: integer (nullable = true)
