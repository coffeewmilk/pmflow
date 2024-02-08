#!/usr/bin/env bash

curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://connector:8083/connectors/sink-s3-pmflow/config \
    -d '
 {
		"connector.class": "io.confluent.connect.s3.S3SinkConnector",
		"topics": "pmflow",
		"s3.region": "ap-southeast-1",
		"s3.bucket.name": "pmflow",
		"storage.class": "io.confluent.connect.s3.storage.S3Storage",
		"format.class": "io.confluent.connect.s3.format.avro.AvroFormat",
	    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
        "path.format": "'\'year\''=YYYY/'\'month\''=MM/'\'day\''=dd/'\'hour\''=HH",
        "partition.duration.ms": "3000",
        "locale": "en-US",
        "timezone": "UTC",
        "timestamp.extractor": "Wallclock",
        "flush.size":"10"
	}
'