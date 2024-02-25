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

    const fullTime = new Date();
    const utcDate = fullTime.getUTCDate();
    const utcMonth = ("0"+(fullTime.getUTCMonth()+1)).slice(-2) //better to cast number first
    const utcYear = fullTime.getUTCFullYear();

    const dateString = `${utcYear}-${utcMonth}-${utcDate}`

    const value =  await client.execute(`SELECT * FROM aqi_by_district_date_time WHERE date='${dateString}' GROUP BY district`)
    return Response.json(value)
  }