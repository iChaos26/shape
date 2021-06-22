from utils.sparkutils import Spark


spark = Spark.get_new_spark_session()
print(spark)

#To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
#<pyspark.sql.session.SparkSession object at 0x7fc2260226a0>
