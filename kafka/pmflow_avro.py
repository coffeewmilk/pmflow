import requests
import pandas as pd
import json
import time
import os
import logging

from pyspark.sql import SparkSession
from sedona.spark import *
from pyspark.sql.functions import col

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from pmflow import create_spark_connection, fetch_data_from_aqin, label_district_by_df, create_district_view

def row_to_dict(row, ctx):
    return dict(station=row.station,
                district=row.district,
                aqi=row.aqi,
                lat=row.lat,
                lon=row.lon,
                time=row.time)

def delivery_report(err, msg):
    if err is not None:
        print(f"Failed to delivery due to {err}")
        return
    print("a record produced successfully")

def create_serializer(filename):
    path = os.path.realpath(os.path.dirname(os.path.dirname(__file__)))
    with open(f"{path}/avro/{filename}") as f:
        schema_str = f.read()
    schema_registry_conf = {'url': 'http://schema-registry:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # alternative way?
    global avro_serializer
    global string_serializer
    
    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     row_to_dict)

    string_serializer = StringSerializer('utf_8')

def create_producer():
    producer_conf = {'bootstrap.servers': 'broker:9092'}
    
    # alternative way?
    global producer
    producer = Producer(producer_conf)

def produce_data(data):
    producer.produce(topic='topic',
                             value=avro_serializer(data, SerializationContext('topic', MessageField.VALUE)),
                             on_delivery=delivery_report)

def produce_data_map_partition(iterator):
    producer_map = Producer({'bootstrap.servers': 'broker:9092'}.copy())  # Configure your Kafka producer here
    for row in iterator:
        producer_map.produce(
            topic='topic',
            value=avro_serializer(row, SerializationContext('topic', MessageField.VALUE)),
            on_delivery=delivery_report
        )
    producer.flush()


def produce_df(df):
    #df.foreach(produce_data)
    test = df.rdd.foreachPartition(produce_data_map_partition)
    test.count()


if __name__ == "__main__":
    try:
        create_producer()
        create_serializer('record.avsc')

    except Exception as e:
        print(f"Unable to create producer or serializer due to {e}")
        exit()
    
    spark = create_spark_connection()
    print("Successfully created spark connection")
    create_district_view(spark)
    
    while True:
        data = fetch_data_from_aqin(spark)
        labeledData = label_district_by_df(data, spark)
        produce_df(labeledData)
        time.sleep(1)
