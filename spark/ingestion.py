import requests
import pandas as pd
import json
import time
import os
import logging

from pyspark.sql import SparkSession
from sedona.spark import *
from pyspark.sql.functions import col, struct
from pyspark.sql.column import Column, _to_java_column
#from pyspark.sql.avro.functions import from_avro, to_avro

def create_spark_connection():
    spark = None
    try:
        spark = SparkSession \
            .builder \
            .appName("Ingestion").getOrCreate()
            # .config('spark.jars.packages',
            #     'org.apache.sedona:sedona-spark-3.0_2.12:1.5.1,'
            #     'org.datasyslab:geotools-wrapper:1.5.1-28.2,'
            #     'org.apache.spark:spark-avro_2.12:3.5.0') \
            # .config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all') \
            
        # bypass spark with sedona
        sedona = SedonaContext.create(spark)
        logging.info("Spark connection created, actually sedona")
    except Exception as e:
        logging.error(f"Unable to create spark session due to {e}")
    
    return sedona


def fetch_data_from_aqin(spark):
    aqicnKey = '[aqicnKey]' # to be filled
    latlong1 = ['13.898819', '100.415717']
    latlong2 = ['13.579757', '100.683936']
    data = None
    try:
        pmJSON = requests.get(f"https://api.waqi.info/v2/map/bounds?latlng={','.join(latlong1)},{','.join(latlong2)}&token={aqicnKey}").json()['data']
        df = spark.createDataFrame(pmJSON)
        data = df.select('aqi','lat', 'lon', col('uid').cast('int'), col("station.name").alias('name'), col("station.time").alias('time'))
    except Exception as e:
        logging.error(f"Unable to retrive data from aqin due to {e}")
    return data

def create_district_view(spark):
    try:
        path = os.path.realpath(os.path.dirname(__file__))
        shape_file_location=f'{path}/data/Bangkok Metropolitan Region'
        districtRDD = ShapefileReader.readToGeometryRDD(spark, shape_file_location)
        Adapter.toDf(districtRDD, spark).select('geometry', 'ADM2_EN').createOrReplaceTempView("district_map")
        logging.info("Successfully created map view")
    except Exception as e:
        logging.error(f"Unable to create map view due to {e}")

def label_district_by_df(df, spark):
    labeled = None
    try:
        df.createOrReplaceTempView("temp")
        labeled = spark.sql("SELECT * FROM temp t INNER JOIN district_map d ON ST_Contains(d.geometry, ST_MakePoint(t.lon, t.lat))")
    except Exception as e:
        logging.error(f"Unable to label district due to {e}")
    return labeled


def updated_row(dfOld, dfNew):
    columns = ['aqi', 'uid']
    intersectDf = dfOld.select(columns).intersect(dfNew.select(columns))
    rowUpdated = dfOld.join(intersectDf, on='name', how='leftanti')
    return rowUpdated

def create_schemamanager_config():
    # this function is not used due to conflic in schema
    jvm_gateway = spark.sparkContext._active_spark_context._gateway.jvm
    SchemaRegistryClientConfig = {"schema.registry.url":'http://schema-registry:8081'}

    SchemaManagerFactory = jvm_gateway.za.co.absa.abris.avro.read.confluent.SchemaManagerFactory

    return SchemaManagerFactory.create(jvm_gateway.PythonUtils.toScalaMap(SchemaRegistryClientConfig)) 

def register_schema(schemaManager, schema):
    # this function is not use due to conflic in schema
    jvm_gateway = spark.sparkContext._active_spark_context._gateway.jvm
    SchemaSubject = jvm_gateway.za.co.absa.abris.avro.registry.SchemaSubject
    subject = SchemaSubject.usingTopicNameStrategy("pmflow", False) 
    return schemaManager.register(subject, schema) 

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



def send_df_to_kafka(df, config):
    df.select(to_avro("value", config).alias("value"))\
       .write \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "broker:9092") \
       .option("topic", "pmflow") \
       .save()

def transform_avro_format(df):
    avroFormat = df.withColumn("value",
                  struct(
                         col("aqi").alias("aqi"),
                         col("uid").alias("uid"),
                         col("name").alias("name"),
                         col("ADM2_EN").alias("district"),
                         col("lat").alias("lat"),
                         col("lon").alias("lon"),
                         col("time").alias("time"))).select("value")
    avroFormat.printSchema()
    return avroFormat

if __name__ == '__main__':

    spark = create_spark_connection()
    create_district_view(spark)

    while True:
        time.sleep(1)
        data = fetch_data_from_aqin(spark)
        labelData = label_district_by_df(data, spark)
        transformed = transform_avro_format(labelData)
        
        to_avro_abris_settings = to_avro_abris_config({'schema.registry.url': 'http://schema-registry:8081'})
        send_df_to_kafka(transformed, to_avro_abris_settings)