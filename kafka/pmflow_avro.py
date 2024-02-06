import requests
import pandas as pd
import json
import time
import os
import logging

from pyspark.sql import SparkSession
from sedona.spark import *
from pyspark.sql.functions import col, struct
from pyspark.sql.avro.functions import from_avro, to_avro

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from pmflow import create_spark_connection, fetch_data_from_aqin, label_district_by_df, create_district_view

def send_dataframe_to_kafka(df): #nope this is not right
    pass

def send_df_to_kafka(df):
    df.select(to_avro("value").alias("value"))\
       .write \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "broker:9092") \
       .option("topic", "pmflow") \
       .save()

def transform_avro_format(df):
    avroFormat = df.withColumn("value",
                  struct(
                         col("aqi").alias("aqi"),
                         col("name").alias("name"),
                         col("ADM2_EN").alias("district"),
                         col("lat").alias("lat"),
                         col("lon").alias("lon"),
                         col("time").alias("time"))).select("value")
    avroFormat.printSchema()
    return avroFormat

if __name__ == '__main__':
    spark = create_spark_connection()
    data = fetch_data_from_aqin(spark)
    create_district_view(spark)
    labelData = label_district_by_df(data, spark)
    transformed = transform_avro_format(labelData)
    send_df_to_kafka(transformed)