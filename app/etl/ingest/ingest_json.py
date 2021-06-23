import pyspark
import json
from app.utils.spark.sparkutils import Spark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType


class JsonHandler:

    def __init__(self):
        self.sk_session = Spark.get_spark_session()
        self.sql_context = Spark.get_sql_context(self.sk_session)
        self.schema = StructType([StructField("equipment_id", IntegerType(), True),
                StructField("code", StringType(), True),
                StructField("group_name", StringType(), True)])

    def json_reader(self):
        df = self.sql_context.read \
            .option("multiLine", "true") \
            .option("mode", "PERMISSIVE") \
            .json("/home/joao/dev/shape/Data/equipment.json", schema=self.schema)
        return df
#root
# |-- code: string (nullable = true)
# |-- equipment_id: long (nullable = true)
# |-- group_name: string (nullable = true)