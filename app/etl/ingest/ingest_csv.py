import pyspark

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructField, StructType, IntegerType
from utils.spark.sparkutils import Spark

class CsvHandler:

    def __init__(self):
        self.sk_session = Spark.get_spark_session()
        self.sql_context = Spark.get_sql_context(self.sk_session)
        self.schema = StructType([StructField("equipment_id", IntegerType(), True),
                        StructField("sensor_id", IntegerType(), True)])

    def csv_reader(self):
        df = self.sql_context.read \
            .option("delimiter" , ";") \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv("/home/joao/dev/shape/Data/*csv", schema=self.schema)
        return df


#root
# |-- equipment_id: integer (nullable = true)
# |-- sensor_id: integer (nullable = true)
