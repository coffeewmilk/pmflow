import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, max as max_, last, avg, max_by, date_format 
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
            .config('spark.jars', 'jars/spark-cassandra-connector-assembly-3.4.1-4-g05ca11a5.jar') \
            .config('spark.sql.catalog.cassandra', 'com.datastax.spark.connector.datasource.CassandraCatalog') \
            .config('spark.sql.catalog.cassandra.spark.cassandra.connection.host', 'cassandra') \
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

def writeToCassandra(writeDF, epochID):
    writeDF.write \
           .mode("append") \
           .partitionBy("district") \
           .saveAsTable("cassandra.pmflow.aqi_by_district_date_time")

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
   

    explodedAvro = df.withWatermark("timestamp", "5 minutes").select(from_avro("value", from_avro_abris_settings).alias("value")).select("value.*")
    convertTime = explodedAvro.withColumn("timestamp", to_timestamp(col("time")))

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
    
    averagePerDistrict = latestTable.groupBy("district").agg(avg("aqi").alias("aqi"), last("time").alias("time"), last("date").alias("date"))

   
    # query = latestTable \
    #         .writeStream \
    #         .outputMode("complete") \
    #         .format("console") \
    #         .start()
    
    # sent = averagePerDistrict.select("district", "date", "time", "aqi") \
    #                          .writeStream \
    #                          .outputMode("update") \
    #                          .foreachBatch(writeToCassandra) \
    #                          .start()
                      
    # sent.awaitTermination()

    spark.read.table("cassandra.pmflow.aqi_by_district_date_time").show()
    
