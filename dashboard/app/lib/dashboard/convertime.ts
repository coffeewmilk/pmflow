

export function toSeconds (timestring : string) {
    const hms = timestring.split(':')
    return parseInt(hms[0]) * 60 * 60 + parseInt(hms[1]) * 60 + (parseInt(hms[2]) || 0)
}

export function secondsToString (seconds: number) {
    return (new Date(seconds * 1000).toISOString().slice(11, 19))
}

