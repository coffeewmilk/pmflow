'use client'

import useSWR, { Fetcher } from 'swr'
import React from "react"
import MapChart from '@/app/ui/dashboard/mapchart'
import RealTimeRowWrapper from '@/app/ui/dashboard/realtimerow'

const fetcher = (request: string) => fetch(`/api/data/${request}`).then(res => res.json()) // specify type later?

export default function RealtimeDash() {
    
    const { data, error } = useSWR('AverageByDistrict', fetcher, { refreshInterval: 1000 })
    console.log(data)
    console.log(Date.now())
    
    return (
        <>
        <div className="flex my-auto w-2/3 h-full">
            <div className='w-2/3 m-auto'>
                <MapChart/>
            </div>
        </div>
        <div className="flex flex-col h-full w-1/3 value-row-bg">
            <div className="flex h-16 header-bg">
                <div className="my-auto"> header </div>
            </div>
            <RealTimeRowWrapper/>
        </div>
        </>
    )
}