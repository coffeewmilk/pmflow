host='cassandra_rest'

auth=$(curl -L -X POST "http://cassandra:8081/v1/auth" \
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


# create UDT
curl -s --location --request POST "http://$host:8082/v2/schemas/keyspaces/pmflow/types" \
--header "X-Cassandra-Token: $AUTH_TOKEN" --header 'Content-Type: application/json' \
--data '{"name" : "roughrecord", 
		"fields": 
			[
				{
				"name" : "district", 
				"typeDefinition": "text"
			 	},
				{
				"name" : "aqi", 
				"typeDefinition": "float"
				},
				{
				"name" : "time", 
				"typeDefinition": "time"
				},
				{
				"name" : "date", 
				"typeDefinition": "date"
				}

			]}'

#create table
curl -s --location \
--request POST "http://$host:8082/v2/schemas/keyspaces/pmflow/tables" \
--header "X-Cassandra-Token: $AUTH_TOKEN" \
--header "Content-Type: application/json" \
--header "Accept: application/json" \
--data '{
	"name": "average_per_district_by_date",
	"columnDefinitions":
	  [
        {
	      "name": "date",
	      "typeDefinition": "date"
	    },
        {
	      "name": "time",
	      "typeDefinition": "time"
	    },
        {
	      "name": "records",
	      "typeDefinition": "frozen<set<roughrecord>>"
	    }
	  ],
	"primaryKey":
	  {
	    "partitionKey": ["date"],
	    "clusteringKey": ["time"]
	  },
	"tableOptions":
	  {
	    "defaultTimeToLive": 0,
	    "clusteringExpression":
	      [{ "column": "time", "order": "DESC" }]
	  }
}' 

#create table
curl -s --location \
--request POST "http://$host:8082/v2/schemas/keyspaces/pmflow/tables" \
--header "X-Cassandra-Token: $AUTH_TOKEN" \
--header "Content-Type: application/json" \
--header "Accept: application/json" \
--data '{
	"name": "average_per_district_by_date_district",
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
	      "typeDefinition": "int"
	    }
	  ],
	"primaryKey":
	  {
	    "partitionKey": ["date", "district"],
	    "clusteringKey": ["time"]
	  },
	"tableOptions":
	  {
	    "defaultTimeToLive": 0,
	    "clusteringExpression":
	      [{ "column": "time", "order": "DESC" }]
	  }
}' 


#create table
curl -s --location \
--request POST "http://$host:8082/v2/schemas/keyspaces/pmflow/tables" \
--header "X-Cassandra-Token: $AUTH_TOKEN" \
--header "Content-Type: application/json" \
--header "Accept: application/json" \
--data '{
	"name": "available_districts_by_day",
	"columnDefinitions":
	  [
		{
	      "name": "districts",
	      "typeDefinition": "set<text>"
	    },
        {
	      "name": "date",
	      "typeDefinition": "date"
	    },
        {
	      "name": "time",
	      "typeDefinition": "time"
	    }
	  ],
	"primaryKey":
	  {
	    "partitionKey": ["date"],
	    "clusteringKey": ["time"]
	  },
	"tableOptions":
	  {
	    "defaultTimeToLive": 0,
	    "clusteringExpression":
	      [{ "column": "time", "order": "DESC" }]
	  }
}' 