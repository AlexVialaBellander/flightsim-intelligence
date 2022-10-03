const fetch = require("node-fetch")

async function get() {
    const response = await fetch('https://api.ivao.aero/v2/tracker/now/connections/stats', {
        headers: {
            apikey: process.env.IVAO_KEY || "wBugpaXUjbXgzQ2BZvrturHTrTfEzLAp"
        }
    })
    const data = await response.json()
    console.log
    return data
}

exports.get = get;
