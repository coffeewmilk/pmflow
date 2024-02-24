'use client'

type averageByDistrict = {
    district: string;
    aqi: number;
    time: string;
}



export default function RealTimeRowWrapper() {
    return (
        <div className="flex flex-col flex-nowrap space-y-4 overflow-y-auto">
            <RealTimeRow name="thisstation" aqi={23} time="currenttime" />
        </div>
    )
}

export function RealTimeRow({
    name,
    aqi,
    time
}: {
    name: string;
    aqi: number;
    time: string;
}) {
    return (
        <div className="flex h-10"> 
                    <p className="my-auto mx-2 w-3/5"> {name} </p>
                    <p className="my-auto w-1/5"> {aqi} </p>
                    <p className="my-auto mx-2 w-2/5"> {time} </p>
        </div>
    )
}