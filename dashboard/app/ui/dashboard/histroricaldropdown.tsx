' use client '

import {Dispatch, SetStateAction} from 'react'


export default function HistroricalDropdown( {options, selectedOption, setSelectedOption} : 
    {options : string[], selectedOption : string, setSelectedOption : Dispatch<SetStateAction<string>>}) {
        return (
            <select
                className='text-black'
                value={selectedOption}
                onChange={e => setSelectedOption(e.target.value)}>
                {options.map(o => (
                    <option key = {o} value={o}>{o}</option>
                ))}
            </select>
        )
}