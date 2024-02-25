echo $PWD

# prototyping manually add jar
INGEST_JAR=$(echo ./spark/jars/ingestion/*.jar | tr ' ' ',')
STREAM_JAR=$(echo ./spark/jars/streaming/*.jar | tr ' ' ',')

spark-submit --properties-file ./spark/conf/spark-ingestion.conf ./spark/ingestion.py 2>&1 | sed 's/^/| INGESTION | /g' 

# & sleep 1 && spark-submit --properties-file ./spark/conf/spark-streaming.conf ./spark/streaming.py 2>&1 | sed 's/^/| STREAMING | /g'