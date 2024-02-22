import type { NextApiRequest, NextApiResponse } from 'next'
import { Client } from "cassandra-driver";
 
type ResponseData = {
  message: string
}

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
export default function handler(
  req: NextApiRequest,
  res: NextApiResponse<ResponseData>
) {
  const {data} = req.query
  if (data == 'byDistrict') {
    client.execute('SELECT * FROM aqi_by_district_date_time').then(
        (r) => {console.log(r);
        res.status(200).json({message: 'thisIsData'})
        })
  }
  else {
    res.status(404).json({message: 'invalidRequest'})
  }
}

