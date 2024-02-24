host='cassandra'

auth=$(curl -L -X POST "http://$host:8081/v1/auth" \
  -H 'Content-Type: application/json' \
  --data-raw '{
    "username": "cassandra",
    "password": "cassandra"
}' | cut -d ':' -f 2 | sed 's/{//g' | sed 's/}//g' | tr -d '"') &&

export AUTH_TOKEN=$auth &&

echo "Token generated: $AUTH_TOKEN"

# create keyspace
curl -s --location --request POST "http://$host:8082/v2/schemas/keyspaces" \
--header "X-Cassandra-Token: $AUTH_TOKEN" \
--header 'Content-Type: application/json' \
--data '{
    "name": "pmflow"
}' && echo "created keyspace"

# create table
curl -s --location \
--request POST "http://$host:8082/v2/schemas/keyspaces/pmflow/tables" \
--header "X-Cassandra-Token: $AUTH_TOKEN" \
--header "Content-Type: application/json" \
--header "Accept: application/json" \
--data '{
	"name": "aqi_by_district_date_time",
	"columnDefinitions":
	  [
        {
	      "name": "district",
	      "typeDefinition": "text"
	    },
        {
	      "name": "date",
	      "typeDefinition": "date"
	    },
        {
	      "name": "time",
	      "typeDefinition": "time"
	    },
        {
	      "name": "aqi",
	      "typeDefinition": "float"
	    }
	  ],
	"primaryKey":
	  {
	    "partitionKey": ["date","district"],
	    "clusteringKey": ["time"]
	  },
	"tableOptions":
	  {
	    "defaultTimeToLive": 0,
	    "clusteringExpression":
	      [{ "column": "time", "order": "DESC" }]
	  }
}' $$ echo "Table created"
