'use client'

import { LineChart } from "@mui/x-charts"
import { toSeconds, secondsToString } from "@/app/lib/dashboard/convertime"
import { Line } from "react-simple-maps";

type averagePerTimeDistrict = {
    aqi: number;
    time: string;
}

export default function HistroricalChart ({records}: {records:averagePerTimeDistrict[]}) {
    const seconds = records.map( record => ({aqi: record.aqi, second: toSeconds(record.time)}))
    return (
        <LineChart
            xAxis={[{
                dataKey: 'second',
                valueFormatter: (value) => (secondsToString(value))
            },]}
            series={[{
                dataKey: 'aqi'
            },]}
            dataset={seconds} />
            
    )

}