const fetch = require("node-fetch")

async function get() {
    const response = await fetch('https://map.vatsim.net/livedata/live.json')
    const data = await response.json()
    i = 0
        for (e in data){
            i++
        }
    return i
}

exports.get = get;
