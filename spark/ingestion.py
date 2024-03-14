import requests
import pandas as pd
import json
import time
import os
import logging

from pyspark.sql import SparkSession
from sedona.spark import *
from sedona.sql.st_predicates import ST_Contains
from sedona.sql.st_constructors import ST_MakePoint
from pyspark.sql.functions import col, struct, isnull, coalesce, to_timestamp
from pyspark.sql.column import Column, _to_java_column
#from pyspark.sql.avro.functions import from_avro, to_avro

def create_spark_connection():
    spark = None
    try:
        # this config will only be used when running the code with python
        spark = SparkSession \
            .builder \
            .appName("Ingestion") \
            .config('spark.jars.packages',
                'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
                'org.apache.spark:spark-avro_2.12:3.5.0,'
                'org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.5.1,'
                'org.datasyslab:geotools-wrapper:1.5.1-28.2,'
                'edu.ucar:cdm-core:5.4.2,'
                'za.co.absa:abris_2.12:6.4.0') \
            .config('spark.jars.repositories',
                     'https://artifacts.unidata.ucar.edu/content/repositories/unidata-releases,'
                     'https://repo1.maven.org/maven2,'
                     'https://packages.confluent.io/maven') \
            .getOrCreate()
            
        # bypass spark with sedona
        sedona = SedonaContext.create(spark)
        logging.info("Spark connection created, actually sedona")
    except Exception as e:
        logging.error(f"Unable to create spark session due to {e}")
    
    return sedona


def read_district_shape():
    district_map = None
    try:
        path = os.path.realpath(os.path.dirname(__file__))
        shape_file_location=f'{path}/data/Bangkok Metropolitan Region'
        districtRDD = ShapefileReader.readToGeometryRDD(spark, shape_file_location)
        # Adapter.toDf(districtRDD, spark).select('geometry', 'ADM2_EN').createOrReplaceTempView("district_map")
        district_map = Adapter.toDf(districtRDD, spark).select('geometry', col('ADM2_EN').alias('district'))
        logging.info("Successfully created map view")
    except Exception as e:
        logging.error(f"Unable to create map view due to {e}")

    return district_map

def label_district_by_df(df):
    # implement a df as lookup table?
    global districtLookup
    labeled = None
    try:
        #df.createOrReplaceTempView("temp")
        #labeled = spark.sql("SELECT * FROM temp t INNER JOIN district_map d ON ST_Contains(d.geometry, ST_MakePoint(t.lon, t.lat))")
        # first find it in lookup table

        
        # labeled = df.join(districtLookup, (df.lat == districtLookup.lat) & (df.lon == districtLookup.lon), 'full') \
        #             .drop(districtLookup.lat, districtLookup.lon) \
        #             .withColumnRenamed('district', 'pre_district')
        # labeled.show()
        # # lable the remaining with shapefile
        # labeled = labeled.filter(isnull(labeled.pre_district)) \
        #                  .join(district_map, ST_Contains(district_map.geometry, ST_MakePoint(labeled.lon, labeled.lat))) \
        #                  .withColumn('district', coalesce(labeled.pre_district, district_map.district)) \
        #                  .drop(district_map.district, labeled.pre_district, district_map.geometry)
        # # add district that is not in lookup table
        # districtLookup = labeled.select('lat', 'lon', 'district').union(districtLookup).distinct()
        # # add checkpoint to prevent unbound dag
        # districtLookup.cache()
        # districtLookup.localCheckpoint()
        labeled = df.join(district_map, ST_Contains(district_map.geometry, ST_MakePoint(df.lon, df.lat))) \
                    .drop(district_map.geometry)

        labeled.show()
    except Exception as e:
        logging.error(f"Unable to label district due to {e}")
    return labeled


def updated_row(dfOld, dfNew):
    if dfOld == None: return None
    columns = ['aqi', 'uid']
    intersectDf = dfOld.select(columns).intersect(dfNew.select(columns))
    rowUpdated = dfOld.join(intersectDf, on='aqi', how='leftanti')
    return rowUpdated


def to_avro(col, config):
    """
    This function is copied from ABRIS github, with modification on sparkcontext
    """
    jvm_gateway = spark.sparkContext._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro

    return Column(abris_avro.functions.to_avro(_to_java_column(col), config))


def toAvroSchema(df):
    # this functin is not use due to conflic in schema
    jvm_gateway = spark.sparkContext._active_spark_context._gateway.jvm
    return jvm_gateway.za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils.toAvroSchema(df._jdf, ('value',))

def to_avro_abris_config(config_map):
    """
    This function is copied from ABRIS github, with modification on sparkcontext
    """
    jvm_gateway = spark.sparkContext._active_spark_context._gateway.jvm
    scala_map = jvm_gateway.PythonUtils.toScalaMap(config_map)

    return jvm_gateway.za.co.absa.abris.config \
        .AbrisConfig \
        .toConfluentAvro() \
        .downloadSchemaByLatestVersion() \
        .andTopicNameStrategy("pmflow", False) \
        .usingSchemaRegistry(scala_map)

def transform_avro_format(df):
    avroFormat = df.withColumn("value",
                  struct(
                         col("aqi").alias("aqi"),
                         col("uid").alias("uid"),
                         col("station").alias("name"),
                         col("district").alias("district"),
                         col("lat").alias("lat"),
                         col("lon").alias("lon"),
                         col("time").alias("time"))).select("value")
    return avroFormat

def send_df_to_kafka(df, config):
    df = transform_avro_format(df)
    df.select(to_avro("value", config).alias("value"))\
       .write \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "broker:9092") \
       .option("topic", "pmflow") \
       .save()



def doTask(df, epochID):
    # filter out empty api row and cast string to uid.
    df = df.filter(df.aqi != '-').withColumn("uid", col('uid').cast('string'))
    # label district
    df_labeled = label_district_by_df(df)
    send_df_to_kafka(df_labeled, to_avro_abris_settings)



if __name__ == '__main__':

    spark = create_spark_connection()
    districtLookup = spark.createDataFrame([], "lat: double, lon: double, district: string")
    district_map = read_district_shape()
    to_avro_abris_settings = to_avro_abris_config({'schema.registry.url': 'http://schema-registry:8081'})

    input = spark \
            .readStream \
            .format("com.pmflow.aqicnSource.read.stream") \
            .option("interval", 5) \
            .option("key", "[aqinKey]") \
            .load()
    
    deduplicated = input.withColumn("timestamp", to_timestamp(col("time"))) \
                        .withWatermark("timestamp", "2 hours") \
                        .dropDuplicates(["uid", "timestamp"])
    
    task = deduplicated.writeStream \
                       .outputMode("update") \
                       .foreachBatch(doTask) \
                       .start()
    
    task.awaitTermination()
    
    

        