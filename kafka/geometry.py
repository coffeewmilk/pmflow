

from sedona.spark import *
from sedona.utils.adapter import Adapter

from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.core.enums import FileDataSplitter


from sedona.core.formatMapper.shapefileParser import ShapefileReader
from pmApi import pmFetch


# this create unshaded jar
config = SedonaContext.builder(). \
    config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-3.0_2.12:1.5.1,'
           'org.datasyslab:geotools-wrapper:1.5.1-28.2'). \
    config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all'). \
    getOrCreate()
sedona = SedonaContext.create(config)

shape_file_location='./data/Bangkok Metropolitan Region'

test = ShapefileReader.readToGeometryRDD(sedona, shape_file_location)

# It's already geometry type!
# epsg?
lat = 13.763446
lon = 100.632600

spatial_df = Adapter.toDf(test, sedona).select('geometry', 'ADM2_EN').createOrReplaceTempView("spatial")
# yeah this works!!!


def label_district_by_point(lat, lon):
    district = sedona.sql(f"SELECT * FROM spatial WHERE ST_Contains (geometry, ST_MakePoint({lon}, {lat}))")
    return district

def label_district_by_df(df):
    df.createOrReplaceTempView("temp")
    labeled = sedona.sql("SELECT * FROM temp t INNER JOIN spatial s ON ST_Contains(s.geometry, ST_MakePoint(t.lon, t.lat))")
    return labeled

label_district_by_df(pmFetch(sedona)).show()