import { Client } from "cassandra-driver";

const client = new Client({
    contactPoints: ['localhost'],
    localDataCenter: 'datacenter1',
    keyspace: 'pmflow'
});

async function connectToCassandra() {
    try {
      await client.connect();
      console.log('Connected to Cassandra');
    } catch (err) {
      console.error('Error connecting to Cassandra', err);
      throw err; // Rethrow the error to indicate connection failure
    }
  }
    
connectToCassandra()

export async function GET(
    request: Request,
    { params }: { params: { data: string } }
  ) {
    const data = params.data // reserve for different path
    const value =  await client.execute('SELECT * FROM aqi_by_district_date_time GROUP BY district')
    return Response.json(value)
  }