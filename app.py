from utils.sparkutils import Spark
#from etl.ingest import 
#class Job:





session = Spark.get_spark_session()
sc = Spark.get_spark_sqlcontext(session)
print(sc)

#To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
#<pyspark.sql.session.SparkSession object at 0x7fc2260226a0>
#<pyspark.sql.context.SQLContext object at 0x7f41066a3c40>
