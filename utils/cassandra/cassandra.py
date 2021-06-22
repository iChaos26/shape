from .utils.sparkutils import Spark


class Cassandra:

    def __init__(self):
        self.spark = Spark.get_spark_session()

    def load_cassandra_table(self, table_name, keyspace='localhost'):
        return self.spark.read\
                       .format("org.apache.spark.sql.cassandra")\
                       .options(table=table_name, keyspace=keyspace).load()