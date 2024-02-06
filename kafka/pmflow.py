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

# this could be a class
def create_kafka_producer():
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=['broker:9092'], max_block_ms = 5000)
        logging.info("successfuly initiate")
    except Exception as e:
        logging.error(f"Unable to create Kafka producer due to {e}")
    
    return producer

def fetch_data_from_aqin(spark):
    aqicnKey = '[aqicnKey]' # to be filled
    latlong1 = ['13.898819', '100.415717']
    latlong2 = ['13.579757', '100.683936']
    data = None
    try:
        pmJSON = requests.get(f"https://api.waqi.info/v2/map/bounds?latlng={','.join(latlong1)},{','.join(latlong2)}&token={aqicnKey}").json()['data']
        df = spark.createDataFrame(pmJSON)
        data = df.select('aqi','lat', 'lon', col("station.name").alias('name'), col("station.time").alias('time'))
    except Exception as e:
        logging.error(f"Unable to retrive data from aqin due to {e}")
    return data

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    print('I am an errback ', exc_info=excp)

def send_data_to_kafka(producer, data):
    producer.send('pmflow', data).add_callback(on_send_success).add_errback(on_send_error)

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

def send_dataframe_to_kafka(producer, df):
    # just send it as avro format
    dataAvro = df.write.format("avro")
    send_data_to_kafka(producer, dataAvro)

def updated_row(dfOld, dfNew):
    columns = ['aqi', 'name']
    intersectDf = dfOld.select(columns).intersect(dfNew.select(columns))
    rowUpdated = dfOld.join(intersectDf, on='name', how='leftanti')
    return rowUpdated
    

if __name__ == "__main__":
    spark = create_spark_connection()
    producer = create_kafka_producer()
    if spark == None or producer == None:
        exit
    
    initilizeData = fetch_data_from_aqin(spark)
    send_dataframe_to_kafka(producer, initilizeData)

    while True:
        time.sleep(1)
        newData = fetch_data_from_aqin(spark)
        updatedRow = updated_row(initilizeData, newData)
        send_dataframe_to_kafka(producer, updatedRow)
        initilizeData = newData
        
        

    

