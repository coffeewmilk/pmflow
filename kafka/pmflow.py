import requests
import pandas as pd
import json
import time
import logging
import os

from pyspark.sql import SparkSession
from sedona.spark import *

from kafka import KafkaProducer
from pyspark.sql.functions import col


# a unique appName for each job?
def create_spark_connection():
    spark = None
    try:
        spark = SparkSession \
            .builder \
            .appName("pmflow").getOrCreate()
            # .config('spark.jars.packages',
            #     'org.apache.sedona:sedona-spark-3.0_2.12:1.5.1,'
            #     'org.datasyslab:geotools-wrapper:1.5.1-28.2,'
            #     'org.apache.spark:spark-avro_2.12:3.5.0') \
            # .config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all') \
            
        # bypass spark with sedona
        sedona = sedona = SedonaContext.create(spark)
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
    
        
        

    

