echo $PWD

spark-submit --properties-file ./spark/conf/spark-ingestion.conf ./spark/ingestion.py 2>&1 | sed 's/^/| INGESTION | /g' 

spark-submit --properties-file ./spark/conf/spark-streaming.conf ./spark/streaming.py 2>&1 | sed 's/^/| STREAMING | /g'