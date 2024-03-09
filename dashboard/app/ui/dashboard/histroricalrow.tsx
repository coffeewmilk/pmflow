'use client'

type averagePerTimeDistrict = {
    aqi: number;
    time: string;
}

export default function HistroricalRowWrapper( {records}: {records:averagePerTimeDistrict[]}) {
    return (
        <div className="flex flex-col flex-nowrap space-y-4 overflow-y-auto">
            {records.map(record => 
                <HistroricalRow
                    key = {record.time}
                    aqi={record.aqi}
                    time={record.time}/>)}
        </div>
    )
}

export function HistroricalRow({
    aqi,
    time
}: {
    aqi: number;
    time: string;
}) {
    return (
        <div className="flex h-10"> 
                    <p className="my-auto w-1/5"> {aqi} </p>
                    <p className="my-auto mx-2 w-2/5"> {time} </p>
        </div>
    )
}