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

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from pmflow import create_spark_connection, fetch_data_from_aqin, label_district_by_df, create_district_view

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

def to_avro_abris_config(config_map, id):
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
        
        to_avro_abris_settings = to_avro_abris_config({'schema.registry.url': 'http://schema-registry:8081'}, 3)
        send_df_to_kafka(transformed, to_avro_abris_settings)