import { Client } from "cassandra-driver";
import { type NextRequest } from 'next/server'
// host.docker.internal
const client = new Client({
    contactPoints: ['localhost'],
    localDataCenter: 'datacenter1',
    keyspace: 'pmflow',
    credentials: { username: 'cassandra', password: 'cassandra' }
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
    request: NextRequest,
    { params }: { params: { data: string } }
  ) 
  {

    const data = params.data // reserve for different path
    const fullTime = new Date();
    const utcDate = ("0"+(fullTime.getUTCDate())).slice(-2);
    const utcMonth = ("0"+(fullTime.getUTCMonth()+1)).slice(-2); //better to cast number first
    const utcYear = fullTime.getUTCFullYear();
    const dateString = `${utcYear}-${utcMonth}-${utcDate}`

    if (data == "AveragesPertime") {
      const value =  await client.execute(`SELECT * FROM average_per_district_by_date WHERE date='${dateString}' LIMIT 1`)
      return Response.json(value)
    }
    if (data == "AveragePerTimeDistrict") {
      const searchParams = request.nextUrl.searchParams
      const district = searchParams.get('district')
      const query = `SELECT * FROM average_per_district_by_date_district WHERE date='${dateString}' AND district=?`
      const value = await client.execute(query, [ district ])
      return Response.json(value)
    }
  }