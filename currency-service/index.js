const express = require('express');
const axios = require('axios');
const app = express();

app.use(express.json());

app.get('/convert', async (req, res) => {
    try {
        const { amount, from, to } = req.query;
        if (!from || !to || !amount) {
            return res.status(400).send("Missing parameters");
        }

        const response = await axios.get(`https://open.er-api.com/v6/latest/${from}`);
        const rates = response.data.rates;

        if (!rates || !rates[to]) {
            return res.status(400).send("Currency not supported");
        }

        const rate = rates[to];
        const converted = amount * rate;
        
        res.json({ converted, rate });
    } catch (err) {
        console.error(err);
        res.status(500).send("Conversion error");
    }
});

app.listen(3002, () => console.log("Currency Service running on 3002"));