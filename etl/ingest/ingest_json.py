import pyspark as pk
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf

#from logging import GlobalLog

spark = (SparkSession.builder
         .appName('SparkCassandraApp')
         .getOrCreate())

conf = pk.SparkConf()
# conf.set('spark.app.name', app_name)

sc = pk.SparkContext.getOrCreate(conf=conf)
sqlcontext = SQLContext(sc)
path = "/home/joao/dev/shape/Data/equipment.json"

schema = StructType([StructField("equipment_id", IntergerType(), True),
                     StructField("code", StringType(), True),
                     StructField("group_name", StringType(), True)])

print(sqlcontext.read.options(table="equipment",multiline=True) /
                    .json(path) /
                    .printSchema() /
                    .load())
