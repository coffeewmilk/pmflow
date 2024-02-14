curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://connector:8083/connectors/sink-hdfs-pmflow/config \
    -d '
 {
		"connector.class": "io.confluent.connect.hdfs3.Hdfs3SinkConnector",
		"topics": "pmflow",
        "confluent.topic.bootstrap.servers":"broker:9092",
        "hdfs.url": "hdfs://hdfs:9820/pmflow",
		"format.class": "io.confluent.connect.hdfs3.avro.AvroFormat",
	    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
        "path.format": "'\'year\''=YYYY/'\'month\''=MM/'\'day\''=dd/'\'hour\''=HH",
        "partition.duration.ms": "3000",
        "locale": "en-US",
        "timezone": "UTC",
        "timestamp.extractor": "Wallclock",
        "flush.size":"100"
	}
'