import requests
import pandas as pd
import json
import time
import logging
import os

from pyspark.sql import SparkSession
from pyspark import SparkContext
from sedona.spark import *

from kafka import KafkaProducer
from pyspark.sql.functions import col

spark = SparkSession \
            .builder \
            .appName("pmflow") \
            .config('spark.jars.packages',
                'org.apache.sedona:sedona-spark-3.0_2.12:1.5.1,'
                'org.datasyslab:geotools-wrapper:1.5.1-28.2,'
                'org.apache.spark:spark-avro_2.12:3.5.0') \
            .config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all') \
            .config('spark.jars', './jars/abris_2.11-5.1.1.jar') \
            .getOrCreate()


jvm_gateway = spark.sparkContext._active_spark_context._gateway.jvm
abris_avro = jvm_gateway.za.co.absa.abris.avros
print("this should work just fine")