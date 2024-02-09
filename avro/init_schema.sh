curl -v -X POST -H "Content-Type: application/json"\
 --data @/scripts/avro/record_avro_schema.avro\
   http://schema-registry:8081/subjects/pmflow-value/versions