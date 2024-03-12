' use client '

type stationdetails = {
    station : string;
    maxAqi : number;
    timesUpdate : number;
}

export default function StationRowWrapper ( {stations} : {stations : stationdetails[]}) {
    return (
        <table className="w-full">
            <thead>
                <tr className="h-16">
                    <th>Station</th>
                    <th className="text-right p-2 w-1/5">max aqi</th>
                    <th className="text-right p-2 w-1/5">times updated</th>
                </tr>
            </thead>
            <tbody>
                {stations.map(station =>
                    (<tr className="h-16" key = {station.station}>
                        <td className="p-2">{station.station}</td>
                        <td className="text-right p-2">{station.maxAqi}</td>
                        <td className="text-right p-2">{station.timesUpdate}</td>
                    </tr>))
                }
                <tr></tr>
            </tbody>
        </table>
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