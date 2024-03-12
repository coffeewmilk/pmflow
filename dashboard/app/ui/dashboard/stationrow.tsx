' use client '

type stationdetails = {
    station : string;
    maxAqi : number;
    timesUpdate : number;
}

export default function StationRowWrapper ( {stations} : {stations : stationdetails[]}) {
    return (
        <div className="flex flex-col flex-nowrap w-full space-y-4 overflow-y-auto">
            {stations.map(station =>
                <StationRow
                    key = {station.station}
                    name = {station.station}
                    maxAqi={station.maxAqi}
                    timesupdate={station.timesUpdate}/>)
            }
        </div>
    )
}


export function StationRow ( {name, maxAqi, timesupdate} : {name: string, maxAqi: number, timesupdate: number}) {
    return (
        <div className="flex h-10"> 
                    <p className="my-auto w-3/5"> {name} </p>
                    <p className="my-auto w-1/5"> {maxAqi} </p>
                    <p className="my-auto mx-2 w-1/5"> {timesupdate} </p>
        </div>
    )
}