'use client'

import useSWR, { Fetcher } from 'swr'
import StationRowWrapper from './stationrow'

const StationsFetcher = (request: string, datarequest: string) => fetch('/api/data/StationDetails?' + new URLSearchParams({district: request}))
                                               .then(res => res.json())

export default function StationDataWrapper ({selectedOption} : {selectedOption : string}) {
    const { data : stationData, error: stationError } = useSWR([selectedOption, "stationdetail"], ([request, datarequest]) => StationsFetcher(request, datarequest))
    if (!stationData) return <>Loading...</>
    if (stationData.rowLength == 0) return <>no data</>
    console.log(stationData)
    return (
        <StationRowWrapper stations={stationData.rows[0].stations} />
    )
}