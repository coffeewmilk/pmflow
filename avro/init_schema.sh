curl -v -X POST -H "Content-Type: application/json"\
 --data @/avro/record_avro_schema.avro\
   http://localhost:8081/subjects/pmflow-value/versions\