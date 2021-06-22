import pyspark

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import regexp_extract,regexp_replace, col


spark = SparkSession \
    .builder \
    .appName("LogCount") \
    .getOrCreate()

sc = SQLContext(spark)

logs = sc.read \
    .text("/home/joao/dev/shape/Data/*.log")

#Data Wrangling
    
pattern_ts = r'\[(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})]'
pattern_error = r'([A-Z]*[A-Z])'
pattern_sensor = r'((?<=sensor\[).?\d{1,2})'
pattern_temp = r'((?<=temperature\t).?\d{1,6}.\d{1,2})'
pattern_vib = r'((?<=vibration\t).?\d{1,6}.\d{1,2})'

logs_df = logs.select(regexp_extract('value', pattern_ts, 1).alias('timestamp'),
                         regexp_extract('value', pattern_error, 1).alias('status'),
                         regexp_extract('value', pattern_sensor, 1).cast('integer').alias('sensor_id'),
                         regexp_extract('value', pattern_temp, 1).cast('double').alias('temperature'),
                         regexp_extract('value', pattern_vib, 1).cast('double').alias('vibration'))

#logs_df.printSchema()
#logs_df.show(truncate=False, vertical=True)

#root
# |-- timestamp: string (nullable = true)
# |-- status: string (nullable = true)
# |-- sensor_id: integer (nullable = true)
# |-- temperature: double (nullable = true)
# |-- vibration: double (nullable = true)

