'use client'

import useSWR, { Fetcher } from 'swr'
import {useState, useEffect} from "react"
import HistroricalDropdown from './histroricaldropdown'
import HistroricalDataWrapper from './histroricaldatawrapper'
import StationDataWrapper from './stationdatawrapper'

type averageByDistrict = {
    district: string;
    aqi: number;
    time: string;
    date: string;
}


const dropdownFetcher = (request: string) => fetch(`/api/data/${request}`).then(res => res.json())

export default function HistroricalDash () {
    const { data, error } = useSWR('AveragesPertime', dropdownFetcher)
    const [selectedOption, setSelectedOption] = useState('loading');
    const [options, setOptions] = useState(['loading'])
    
    // set change if data change
    useEffect(() => {
        if(data) {
            const fetchedOptions:string[] = (data.rows[0].records).map((record:averageByDistrict) => record.district)
            setSelectedOption(fetchedOptions[0])
            setOptions(fetchedOptions)}
    }, [data]);
    if (!data) return <div>Loading...</div>
    // console.log(selectedOption)
    // console.log(options) 
    return (
        <>
            <div className='flex flex-col dashboard-single-height dashboard-bg'> 
                <HistroricalDropdown options={options} selectedOption={selectedOption} setSelectedOption = {setSelectedOption}/>
                <div className='flex h-full'>
                    <HistroricalDataWrapper selectedOption={selectedOption}/>
                </div> 
            </div>
            <div className='flex dashboard-single-height value-row-bg mt-10'>
                <StationDataWrapper selectedOption={selectedOption}/>
            </div>
        </>
    ) 
}
