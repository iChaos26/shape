import pyspark
from app.utils.spark.sparkutils import Spark

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import regexp_extract,regexp_replace, col

class LogsHandler:
    
    def __init__(self):
        self.sk_session = Spark.get_spark_session()
        self.sql_context = Spark.get_sql_context(self.sk_session)
        self.pattern_timestamp = r'\[(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})]'
        pattern_error = r'([A-Z]*[A-Z])'
        pattern_sensor = r'((?<=sensor\[).?\d{1,2})'
        pattern_temp = r'((?<=temperature\t).?\d{1,6}.\d{1,2})'
        pattern_vib = r'((?<=vibration\t).?\d{1,6}.\d{1,2})'

    def logs_reader(self):
        df_no_schema = self.sql_context.read \
            .text("/home/joao/dev/shape/Data/*.log")
        
    def logs_normalizer(self, dataframe):
        logs_df = dataframe.select(regexp_extract('value', self.pattern_ts, 1).alias('timestamp'),
                        regexp_extract('value', self.pattern_error, 1).alias('status'),
                        regexp_extract('value', self.pattern_sensor, 1).cast('integer').alias('sensor_id'),
                        regexp_extract('value', self.pattern_temp, 1).cast('double').alias('temperature'),
                        regexp_extract('value', self.pattern_vib, 1).cast('double').alias('vibration'))
        return logs_df


#root
# |-- timestamp: string (nullable = true)
# |-- status: string (nullable = true)
# |-- sensor_id: integer (nullable = true)
# |-- temperature: double (nullable = true)
# |-- vibration: double (nullable = true)

