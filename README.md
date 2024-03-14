# Pmflow

### This is my personal data pipeline project. Pmflow works by fetching data from Aqicn website and aggregated it by Bangkok districts.
### The main purpose of this project is to enchance my understanding of data pipeline and tech landscape. 
 The area are only limited to inner district of Bangkok due to performance issue as well as distribution of aqi stations


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

- Since spark doesn't support using restapi as streaming source, a custom source specifictly for this priject is written [spark-aqicn-source](https://github.com/coffeewmilk/spark-aqicn-source).
However, take note that this is not good practice, The restapi has no track of offsets hence it is impossible for spark to retrieve missing data in-case the ingestion layer falls down at some point.

- Another problem is that each station updates aqi data at different interval, when fetching the api, data from every station is recieve despite being duplicated data with the earlier fetch. drop_duplicate with watermark of 2 hours has been used to tackle this problem.

- In this layer the data is then label by district using Apache Sedona on lat and lon data, the bangkok district shape file is also included.

- Lastly, The data is sent to Kafka using Avro format.

### Event Streaming Platform

- Kafka is used as a even streaming platform in this project due to its popularity in tech industry.

- Kafka provides fault-tolerance and exactly-once delivery which are essential for realiable data-pipe.

- Thanks to confluence, Kafka is now easy to work with docker along with many extensions such as kafka connect which is used to save data to HDFS storage.

### Batch layer

- This project implemented a simple batch processing that aggregate the max aqi number and number of updates for each station catagorized by district. The batch processing is trigger every 4 hours using Cron.

- HDFS is used to store data from kafka. The main reason is the compability with spark and it's common to set-up along side spark in distributed systems, it can also be used to store checkpoints for spark-structure-streaming which can be implemented later.

### Streaming layer 

- Since data from each station comes at different intervals, the streaming layer has to keep the last updated for each station.

- The data is then aggregated as average value for each district and sent to Cassandra.

### Serving layer

- Cassandra suitable for this project beacuse of its write and read performance as well as simplicity. Since we will not be doing aggregation further from this point and there will be no modification to the data,
  Accessing data in cassandra with key is more efficient and there are also benifits of using clustering key to order the data by its time

### Dashboard

- In order to fully understand Cassandra, it's useful to understand it from the person who query data from the database viewpoint. Cassandra data model also require you to think ahead of how the data will be qurery before setting up tables

- NextJs is choosen because of my past experience with React. It's also the most popular framework in the industry.

- React-simple-maps and MUI X Charts are used to construct dashboard
