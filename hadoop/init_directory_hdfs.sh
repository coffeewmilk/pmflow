curl -i -X PUT "http://hdfs:9870/webhdfs/v1/pmflow/topics?op=MKDIRS"
curl -i -X PUT "http://hdfs:9870/webhdfs/v1/pmflow/logs?op=MKDIRS"
curl -i -X PUT "http://hdfs:9870/webhdfs/v1/pmflow/bash?op=MKDIRS"
curl -i -X PUT "http://hdfs:9870/webhdfs/v1/pmflow/checkpoint?op=MKDIRS"