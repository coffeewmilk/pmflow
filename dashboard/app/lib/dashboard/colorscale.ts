import { scaleThreshold } from "d3";

export default function colorScale () {
    return ( scaleThreshold([10, 20, 25, 50, 75], [
        "#73d9e9",
        "#73d36e",
        "#edba3a",
        "#e7263e",
        "#8a042c",
        "#472564"
    ]))
}