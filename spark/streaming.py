import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
                    'org.apache.spark:spark-avro_2.12:3.5.0') \
            .config('spark.jars.repositories','https://repo1.maven.org/maven2') \
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

if __name__ == "__main__":
    
    spark = create_spark_connection()
    from_avro_abris_settings = from_avro_abris_config({'schema.registry.url': 'http://schema-registry:8081'})
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:9092") \
        .option("subscribe", "pmflow") \
        .load()
    
    fromAvro = df.select(from_avro("value", from_avro_abris_settings)).select("value.*")
    query = fromAvro \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()
    
    query.awaitTermination()

    
