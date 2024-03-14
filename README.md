# Pmflow

### This is my personal data pipeline project. Pmflow works by fetching data from the Aqicn website and aggregating it by Bangkok districts.
### The main purpose of this project is to enhance my understanding of data pipeline and the tech landscape.
The area is only limited to the inner district of Bangkok due to performance issues as well as the distribution of AQI stations.

<div style="text-align: center;">
    <img width="100%" src="/images/dashboard1.jpg">
</div>
<div style="text-align: center;">
    <img width="100%" src="/images/dashboard2.jpg">
</div>

## Tech Stack

<div style="text-align: center;">
    <img width="100%" src="/images/diagram.png">
</div>

### Ingestion layer

- Since Spark doesn't support using REST API as a streaming source, a custom source specifically for this project is written [spark-aqicn-source](https://github.com/coffeewmilk/spark-aqicn-source).
However, take note that this is not good practice. The REST API has no track of offsets, hence it is impossible for Spark to retrieve missing data in case the ingestion layer falls down at some point.

- Another problem is that each station updates AQI data at different intervals. When fetching the API, data from every station is received despite being duplicated data with the earlier fetch. drop_duplicate with a watermark of 2 hours has been used to tackle this problem.

- In this layer, the data is then labeled by district using Apache Sedona on lat and lon data, and the Bangkok district shape file is also included.

- Lastly, The data is sent to Kafka using Avro format.

### Event Streaming Platform

- Kafka is used as an event streaming platform in this project due to its popularity in the tech industry.

- Kafka provides fault tolerance and exactly-once delivery, which are essential for reliable data pipelines.

- Thanks to Confluent, Kafka is now easy to work with Docker along with many extensions such as Kafka Connect, which is used to save data to HDFS storage.

### Batch layer

- This project implemented simple batch processing that aggregates the max AQI number and number of updates for each station categorized by district. The batch processing is triggered every 4 hours using Cron.

- HDFS is used to store data from Kafka. The main reason is the compatibility with Spark, and it's common to set up alongside Spark in distributed systems. It can also be used to store checkpoints for Spark Structured Streaming, which can be implemented later.

### Streaming layer 

- Since data from each station comes at different intervals, the streaming layer has to only keep the last updated for each station.

- The data is then aggregated as an average value for each district and sent to Cassandra.

### Serving layer

- Cassandra is suitable for this project because of its write and read performance as well as simplicity. Since we will not be doing aggregation further from this point and there will be no modification to the data,
Accessing data in Cassandra with a key is more efficient and there are also benefits of using clustering key to order the data by its time

### Dashboard

- In order to fully understand Cassandra, it's useful to understand it from the person who queries data from the database viewpoint. Cassandra data model also requires you to think ahead of how the data will be queried before setting up tables.

- Next.js is chosen because of my past experience with React. It's also the most popular framework in the industry.

- React-simple-maps and MUI X Charts are used to construct the dashboard.
