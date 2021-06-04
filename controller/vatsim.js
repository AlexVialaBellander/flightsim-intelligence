const fetch = require("node-fetch")

async function get() {
    const response = await fetch('https://map.vatsim.net/livedata/live.json')
    const data = await response.json()
    return data
}

exports.get = get();
