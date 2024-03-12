import useSWR, { Fetcher } from 'swr'
import HistroricalRowWrapper from './histroricalrow'
import HistroricalChart from './histroricalchart'


const dataFetcher = (request: string) => fetch('/api/data/AveragePerTimeDistrict?' + new URLSearchParams({district: request}))
                                               .then(res => res.json())

export default function HistroricalDataWrapper({selectedOption} : {selectedOption:string}) {
    const { data, error } = useSWR(selectedOption, dataFetcher)
    if (!data) return <>Loading...</>
    if (data.rowLength == 0) return <>no data</>
    return (
        <>
        <div className="flex my-auto w-2/3 h-full justify-center">
            <div className='w-2/3 flex-auto'>
                <HistroricalChart records={data.rows} />
            </div>
        </div>
        <div className="flex flex-col w-1/3 h-full value-row-bg">
            <div className="flex h-16 header-bg">
                <div className="my-auto"> {data.rows[0].time} </div>
            </div>
            <HistroricalRowWrapper records={data.rows} />
        </div>
        </>
    )

}