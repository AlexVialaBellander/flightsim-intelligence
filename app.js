var app = require("express")();
const vatsim = require("./controller/vatsim.js");
const ivao = require("./controller/ivao.js");

app.listen(3000, () => {
    console.log("Server running on port 3000");
});

app.get("/vatsim", (req, res, next) => {
    const b = vatsim.get.then((b) => {
        i = 0
        for (e in b){
            i++
        }
        res.status(200).send({total: i});
    })
});

app.get("/ivao", (req, res, next) => {
    const b = ivao.get.then((b) => {
        res.status(200).send({total: b.now.total});
    })
});