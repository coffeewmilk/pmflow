echo $PWD

# prototyping manually add jar
INGEST_JAR=$(echo ./spark/jars/ingestion/target/dependency/*.jar | tr ' ' ',')
STREAM_JAR=$(echo ./spark/jars/streaming/target/dependency/*.jar | tr ' ' ',')

# spark-submit --properties-file ./spark/conf/spark-ingestion.conf ./spark/ingestion.py 2>&1 | sed 's/^/| INGESTION | /g' 
spark-submit --jars $INGEST_JAR ./spark/ingestion.py 2>&1 | sed 's/^/| INGESTION | /g' 

# spark-submit --jars $STREAM_JAR --properties-file ./spark/conf/spark-streaming.conf ./spark/streaming.py 2>&1 | sed 's/^/| STREAMING | /g'