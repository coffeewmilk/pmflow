'use client'

import useSWR, { Fetcher } from 'swr'
import React from "react"
import MapChart from '@/app/ui/dashboard/mapchart'
import RealTimeRowWrapper from '@/app/ui/dashboard/realtimerow'

const fetcher = (request: string) => fetch(`/api/data/${request}`).then(res => res.json()) // specify type later?

export default function RealtimeDash() {
    
    const { data, error } = useSWR('AveragesPertime', fetcher, { refreshInterval: 1000 })
    // const data = {rows: [{time : 'time', records: [{district : 'Pathum Wan', aqi : 90, time : 'time', date : 'date'}]}]}
    
    if (data) { console.log(data) }
    if (!data) return <div>Loading...</div> 
    
    return (
        <div className='flex h-full'>
        <div className="flex my-auto w-2/3 h-full">
            <div className='w-2/3 m-auto'>
                <MapChart records={data.rows[0].records}/>
            </div>
        </div>
        <div className="flex flex-col h-full w-1/3 value-row-bg">
            <div className="flex h-16 header-bg">
                <div className="my-auto"> {data.rows[0].time} </div>
            </div>
            <RealTimeRowWrapper records={data.rows[0].records}/>
        </div>
        </div>
    )
}