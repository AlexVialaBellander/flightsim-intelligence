var app = require("express")();
const vatsim = require("./controller/vatsim.js");
const ivao = require("./controller/ivao.js");

app.listen(3000, () => {
    console.log("Server running on port 3000");
});

app.get("/vatsim", (req, res, next) => {
    vatsim.get().then((b) => {
        res.status(200).send({total: b});
    })
});

app.get("/ivao", (req, res, next) => {
    ivao.get().then((b) => {
        res.status(200).send({total: b.now.total});
    })
});

app.get("/data", (req, res, next) => {
    ivao.get().then((b) => {
        vatsim.get().then((a) => {
            res.status(200).send(
                {
                    data: {
                        ivao: b.now.total,
                        vatsim: a
                    }
                }
            );
        });
    });
});
