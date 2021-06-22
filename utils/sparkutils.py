from pyspark import SparkFiles
from pyspark.sql import SparkSession, SQLContext


class Spark:

    __spark_session = None

    @staticmethod
    def get_spark_session(packages=None, configs=None):
        """
        Get Spark Session
        """
        if Spark.__spark_session:
            return Spark.__spark_session

        try:
            builder = SparkSession.builder.appName('shapetest')

            if packages:
                builder = builder.config('spark.jars.packages', ','.join(packages))

            if configs:
                for config in configs:
                    builder = builder.config(config[0], config[1])

            session = builder.getOrCreate()

            Spark.__spark_session = session
            return Spark.__spark_session
        except OSError:
            raise Exception('Invalid Spark Session')

    @staticmethod
    def get_new_spark_session(packages=None, configs=None):
        if Spark.__spark_session is not None:
            Spark.__spark_session.stop()

        Spark.__spark_session = None

        return Spark.get_spark_session(packages, configs)

    @staticmethod
    def get_spark_context():
        """
        Get Spark Context
        """
        if Spark.__spark_session:
            return Spark.__spark_session.sparkContext

    @staticmethod
    def get_spark_sqlcontext():
        """
        Get Spark SQLContext
        """
        if Spark.__spark_session:
            return SQLContext(Spark.__spark_session)

if __name__ == '__main__':
    spark = Spark()
    spark_session = spark.get_spark_session()
    print(spark_session)