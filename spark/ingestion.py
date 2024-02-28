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
from pyspark.sql.functions import col, struct, isnull, coalesce
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


def fetch_data_from_aqin(spark):
    aqicnKey = '[aqicnKey]' # to be filled
    latlong1 = ['13.898819', '100.415717']
    latlong2 = ['13.579757', '100.683936']
    data = None
    try:
        pmJSON = requests.get(f"https://api.waqi.info/v2/map/bounds?latlng={','.join(latlong1)},{','.join(latlong2)}&token={aqicnKey}").json()['data']
        df = spark.createDataFrame(pmJSON)
        # filter null
        df = df.filter(df.aqi != '-')
        data = df.select('aqi','lat', 'lon', col('uid').cast('string'), col("station.name").alias('name'), col("station.time").alias('time'))
    except Exception as e:
        logging.error(f"Unable to retrive data from aqin due to {e}")
    return data

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
        labeled = df.join(districtLookup, (df.lat == districtLookup.lat) & (df.lon == districtLookup.lon), 'full') \
                    .drop(districtLookup.lat, districtLookup.lon) \
                    .withColumnRenamed('district', 'pre_district')
        # lable the remaining with shapefile
        labeled = labeled.filter(isnull(labeled.pre_district)) \
                         .join(district_map, ST_Contains(district_map.geometry, ST_MakePoint(df.lon, df.lat))) \
                         .withColumn('district', coalesce(labeled.pre_district, district_map.district)) \
                         .drop(district_map.district, labeled.pre_district, district_map.geometry)
        # add district that is not in lookup table
        districtLookup = labeled.select('lat', 'lon', 'district').union(districtLookup)

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
                         col("district").alias("district"),
                         col("lat").alias("lat"),
                         col("lon").alias("lon"),
                         col("time").alias("time"))).select("value")
    return avroFormat

if __name__ == '__main__':

    spark = create_spark_connection()
    districtLookup = spark.createDataFrame([], "lat: double, lon: double, district: string")
    district_map = read_district_shape()
    to_avro_abris_settings = to_avro_abris_config({'schema.registry.url': 'http://schema-registry:8081'})
    
    initialValue = fetch_data_from_aqin(spark)
    initialLabel = label_district_by_df(initialValue)
    initialDf = transform_avro_format(initialLabel)
    send_df_to_kafka(initialDf, to_avro_abris_settings)

    while True:
        time.sleep(1)
        data = fetch_data_from_aqin(spark)
        updatedData = updated_row(initialValue, data)
        initialValue = data

        if updatedData == None: continue
        if updatedData.count() > 0 :
            labelData = label_district_by_df(updatedData)
            transformed = transform_avro_format(labelData)
            send_df_to_kafka(transformed, to_avro_abris_settings)

        