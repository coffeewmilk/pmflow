import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
import time

aqicnKey = '[aqicnKey]'
latlong1 = ['13.898819', '100.415717']
latlong2 = ['13.579757', '100.683936']





def pmFetch(sparksession):

    pmJSON = requests.get(f"https://api.waqi.info/v2/map/bounds?latlng={','.join(latlong1)},{','.join(latlong2)}&token={aqicnKey}").json()['data']
    df = sparksession.createDataFrame(pmJSON)
    dfStations = df.select('aqi','lat', 'lon', col("station.name").alias('name'), col("station.time").alias('time'))
    return dfStations


def lableDistrict(df):
    # todo 
    pass


def rowUpdate(dfOld, dfNew):
    # todo
    # only return row which its aqi is updated
    columns = ['aqi', 'name']
    intersectDf = dfOld.select(columns).intersect(dfNew.select(columns))
    rowUpdated = dfOld.join(intersectDf, on='name', how='leftanti')
    return rowUpdated




if __name__ == "__main__":

    # to config
    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

    #pmFetch(spark).show()

    while True:
        currentData = pmFetch(spark)
        time.sleep(0.25)
        newData = pmFetch(spark)
        label_district_by_df(newData).show()
        # rowUpdate(currentData, newData).show()
        time.sleep(0.25)