import logging

from app.utils.spark.sparkutils import Spark
from utils.cassandra.cassandra import Cassandra
from app.etl.ingest.ingest_csv import CsvHandler
from app.etl.ingest.ingest_json import JsonHandler
from app.etl.ingest.ingest_log import LogsHandler
#from cassandra.cluster import Cluster
#from cassandra.query import tuple_factory
#from cassandra.auth import PlainTextAuthProvider
#from cassandra.concurrent import execute_concurrent
#from cassandra.policies import DCAwareRoundRobinPolicy, AddressTranslator

from config.config_manager import ConfigManager
from app_settings import AppSettings
logging.basicConfig(level=logging.INFO)
class Job:
    
    def __init__(self):
        self.sk_session = Spark.get_spark_session()
        self.sql_context = Spark.get_sql_context(self.sk_session)
        self.config_manager = ConfigManager(AppSettings())
        self.auth_provider = PlainTextAuthProvider(username=self.config_manager.cassandra_connection.user,password=self.config_manager.cassandra_connection.password) 
        self.cluster = self.get_cassandra_cluster_connection()
        self.session = self.cluster.connect(self.config_manager.cassandra_connection.dbname, wait_for_all_pools=True)

    def execute(self):
        try:
            pass
        except Exception as e:
            raise e
        finally:
            logging.info('Closing connection to Cassandra')
            self.session.shutdown()
            self.cluster.shutdown()
# from cassandra.cluster import Cluster
# cluster = Cluster(['192.168.1.1', '192.168.1.2'])
# session = cluster.connect()
# session.execute("CREATE KEYSPACE ...")
# ...
# cluster.shutdown()

#<pyspark.sql.session.SparkSession object at 0x7fc2260226a0>
#<pyspark.sql.context.SQLContext object at 0x7f41066a3c40>
