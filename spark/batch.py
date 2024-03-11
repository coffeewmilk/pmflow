from pyspark.sql import SparkSession
from datetime import datetime, timedelta, timezone
from pyspark.sql.functions import max, count, first, collect_set, struct, to_timestamp, date_format


def hourRange(start, end):
    return (','.join(list(map(lambda n : f'{n:02}', list(range(start, end))))))

def writeToCassandra(writeDF, table):
    writeDF.write \
           .mode("append") \
           .saveAsTable(f"cassandra.pmflow.{table}")

if __name__ == "__main__" :
    # run every 4hrs
    currentTime = datetime.now(timezone.utc)
    startbatchTime = currentTime - timedelta(hours=4)

    # both are on the same day
    if currentTime.date() == startbatchTime.date():
        range = hourRange(startbatchTime.hour, currentTime.hour)
        path = f'hdfs://hdfs:9820/pmflow/topics/pmflow/year={currentTime.year}/month={currentTime.month:02}/day={currentTime.day:02}/hour={{{range}}}'
    # seperated day
    else:
        firstdayRange = hourRange(startbatchTime.hour, 12)
        seconddayRange = hourRange(0, currentTime.hour)
        path = f'hdfs://hdfs:9820/pmflow/topics/pmflow/year={startbatchTime.year}/month={startbatchTime.month:02}/day={startbatchTime.day:02}/hour={{{firstdayRange}}}' \
            f'hdfs://hdfs:9820/pmflow/topics/pmflow/year={currentTime.year}/month={currentTime.month:02}/day={currentTime.day:02}/hour={{{seconddayRange}}}'

    print(path)

    spark = SparkSession.builder \
                        .appName("Batch") \
                        .config('spark.jars.packages', 'org.apache.spark:spark-avro_2.12:3.5.0,'
                                'com.datastax.spark:spark-cassandra-connector_2.12:3.5.0') \
                        .config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions') \
                        .config('spark.sql.catalog.cassandra', 'com.datastax.spark.connector.datasource.CassandraCatalog') \
                        .config('spark.sql.catalog.cassandra.spark.cassandra.connection.host', 'cassandra') \
                        .config('spark.sql.catalog.cassandra.spark.cassandra.auth.username', 'cassandra') \
                        .config('spark.sql.catalog.cassandra.spark.cassandra.auth.password', 'cassandra') \
                        .getOrCreate()

    input = spark.read.format("avro").load(path).withColumn("timestamp", to_timestamp("time")).withColumnRenamed("name", "station")
    stationsDetails = input.groupby("station").agg( max("aqi").alias("maxAqi"), count("time").alias("timesUpdate"), first("district").alias("district"), max("timestamp").alias("timestamp"))
    districtGrouped = stationsDetails.groupby("district") \
                                     .agg( collect_set(struct ("station", "maxAqi", "timesUpdate")).alias("stations"), max("timestamp").alias("timestamp")) \
                                     .withColumn("date", date_format("timestamp", "yyyy-MM-dd")) \
                                     .withColumn("time", date_format("timestamp", "HH:mm:ss")) \
                                     .drop("timestamp")
                                     
    districtGrouped.show()
    writeToCassandra(districtGrouped, "stations_details_by_date_district")
    