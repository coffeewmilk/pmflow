'use client'

import { ComposableMap, Geographies, Geography } from "react-simple-maps"
import colorScale from "@/app/lib/dashboard/colorscale"

const geoUrl = "/outputTopo.json"

type averageByDistrict = {
    district: string;
    aqi: number;
    time: string;
    date: string;
}

const scale = colorScale()

export default function MapChart({records}: {records:averageByDistrict[]}) {
    return (
        <ComposableMap 
            projection="geoEqualEarth"
            projectionConfig={{
                rotate: [-100.4, -13.85, 0],
                center: [0, 0],
                scale: 32000,
              }}>
            <Geographies geography={geoUrl}>
                {({ geographies }) =>
                geographies.map((geo) => {
                    const theRecord = records.find((record) => record.district === geo.properties.ADM2_EN);
                    return (
                        <Geography key={geo.rsmKey} 
                                   geography={geo} 
                                   fill={theRecord ? scale(theRecord.aqi) : "#383838"}/>
                    )
                })
                 }
            </Geographies>
        </ComposableMap>
    )
}