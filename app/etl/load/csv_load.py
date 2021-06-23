
class CsvLoader:

    
    def csv_load_cassandra(self, dataframe):
        return dataframe.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="shape", keyspace="test")\
            .save()