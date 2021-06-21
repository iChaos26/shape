import pyspark
import re

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import regexp_extract, col

#(36979, 1)
#root
# |-- value: string (nullable = true)
#row = ['[2019-12-10 10:46:09]\tERROR\tsensor[5]:\t(temperature\t365.26, vibration\t-6305.32)',..]

spark = SparkSession \
    .builder \
    .appName("LogCount") \
    .getOrCreate()

sc = SQLContext(spark)

logs = sc.read \
    .text("/home/joao/dev/shape/Data/*.log")

#Data Wrangling
def regex_extractor(dataframe, pattern, aliases):
    return dataframe.select(regexp_extract('value', pattern, 1).alias('timestamp'))
    
#timestamp = [re.search(ts_rx, item).group(1) for item in logs]

log_ts = logs.select(regexp_extract('value', ts_rx, 1).alias('timestamp'))

#! PROXIMOS PASSOS:
# - terminar regex extractor for all possibilites
# - fazer join, montar novo structured schema e tabelao de LOGAS
# - Começar a montar container app + spark master and slaves
# - Montar docker cassandra container
# - Montar redshift container
# - Wrapper (datastax) e conectar redshift com cassandra. 