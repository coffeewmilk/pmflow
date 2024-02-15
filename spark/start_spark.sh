echo $PWD

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.spark:spark-avro_2.12:3.5.0,\
org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.5.1,\
org.datasyslab:geotools-wrapper:1.5.1-28.2,\
edu.ucar:cdm-core:5.4.2,\
za.co.absa:abris_2.12:6.4.0 \
            --repositories https://artifacts.unidata.ucar.edu/content/repositories/unidata-releases,\
https://repo1.maven.org/maven2,\
https://packages.confluent.io/maven \
            ./spark/ingestion.py | sed 's/^/| INGESTION | /g' 

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
# za.co.absa:abris_2.12:6.4.0 \
#             --repositories https://repo1.maven.org/maven2 \
#             ./spark/streaming.py | sed 's/^/| STREAM | /g' &