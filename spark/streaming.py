import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, max as max_, last, avg, max_by, date_format, current_timestamp
from pyspark.sql.column import Column, _to_java_column

def create_spark_connection():
    spark = None
    try:
        spark = SparkSession \
            .builder \
            .appName("Streaming") \
            .config('spark.jars.packages',
                    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
                    'za.co.absa:abris_2.12:6.4.0,'
                    'org.apache.spark:spark-avro_2.12:3.5.0,'
                    '') \
            .config('spark.jars.repositories','https://repo1.maven.org/maven2') \
            .config('spark.sql.streaming.statefulOperator.checkCorrectness.enabled', 'false') \
            .config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions') \
            .config('spark.jars', './spark/jars/spark-cassandra-connector-assembly-3.4.1-4-g05ca11a5.jar') \
            .config('spark.sql.catalog.cassandra', 'com.datastax.spark.connector.datasource.CassandraCatalog') \
            .config('spark.sql.catalog.cassandra.spark.cassandra.connection.host', 'cassandra') \
            .config('spark.sql.catalog.cassandra.spark.cassandra.auth.username', 'cassandra') \
            .config('spark.sql.catalog.cassandra.spark.cassandra.auth.password', 'cassandra') \
            .getOrCreate()
        logging.info("Spark connection created")

    except Exception as e:
        logging.error(f"Unable to create spark session due to {e}")
    
    return spark

def from_avro(col, config):
    """
    This function is copied from ABRIS github, with modification on sparkcontext
    """
    jvm_gateway = spark.sparkContext._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro

    return Column(abris_avro.functions.from_avro(_to_java_column(col), config))

def from_avro_abris_config(config_map):
    """
    This function is copied from ABRIS github, with modification on sparkcontext
    """
    jvm_gateway = spark.sparkContext._active_spark_context._gateway.jvm
    scala_map = jvm_gateway.PythonUtils.toScalaMap(config_map)

    return jvm_gateway.za.co.absa.abris.config \
        .AbrisConfig \
        .fromConfluentAvro() \
        .downloadReaderSchemaByLatestVersion() \
        .andTopicNameStrategy("pmflow", False) \
        .usingSchemaRegistry(scala_map)

def apd_cassandra_format(df):
    
    ''' The function is for formatting average per district data '''

    return df.selectExpr( "max(timestamp) as timestamp", "collect_set( struct(district, aqi, time, date)) as records")\
             .withColumn("date", date_format("timestamp", "yyyy-MM-dd")) \
             .withColumn("time", date_format("timestamp", "HH:mm:ss")) \
             .drop("timestamp")


def average_per_district(df):
    return df.groupBy("district").agg(avg("aqi").alias("aqi"),
                                      max_("timestamp").alias("timestamp"),
                                      max_by("time","timestamp").alias("time"),
                                      max_by("date","timestamp").alias("date"))

def writeToCassandra(writeDF, epochID):
    writeDF.write \
           .mode("append") \
           .saveAsTable("cassandra.pmflow.average_per_district_by_date")

def doTask(df, epochID):
    # this will keep sending even if there is no update in data
    formatted = apd_cassandra_format( average_per_district(df) )
    writeToCassandra(formatted, epochID)

if __name__ == "__main__":
    
    spark = create_spark_connection()
    from_avro_abris_settings = from_avro_abris_config({'schema.registry.url': 'http://schema-registry:8081'})
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:9092") \
        .option("subscribe", "pmflow") \
        .option("startingOffsets", "earliest") \
        .load()
    # to do: implement spark hdfs checkpoint!
   

    explodedAvro = df.select(from_avro("value", from_avro_abris_settings).alias("value")).select("value.*")
    convertTime = explodedAvro.withColumn("timestamp", to_timestamp(col("time"))).withWatermark("timestamp", "1 day")

    # get the latest record for each station, assuming there will be no unorderly data
    # Work around is needed since the traditional join doesn't work on streaming data
    # use max_by for spark 3.0+
    latestTable = convertTime.groupBy("uid").agg(max_("timestamp"), 
                                                 max_by("district", "timestamp"), 
                                                 max_by("name", "timestamp"), 
                                                 max_by("aqi", "timestamp"), 
                                                 max_by("lat", "timestamp"), 
                                                 max_by("lon", "timestamp")) \
                             .toDF("uid", "timestamp", "district", "name", "aqi", "lat", "lon") \
                             .withColumn("date", date_format("timestamp", "yyyy-MM-dd")) \
                             .withColumn("time", date_format("timestamp", "HH:mm:ss"))
    
    # after this use foreachbatch instead
    task = latestTable.writeStream \
                      .outputMode("complete") \
                      .foreachBatch(doTask) \
                      .start()

   
   
                      
    task.awaitTermination()
    