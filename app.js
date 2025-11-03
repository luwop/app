'use strict';
const http = require('http');
var assert = require('assert');
const express = require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
require('dotenv').config();
const port = Number(process.argv[2]);
const hbase = require('hbase');

const url = new URL(process.argv[3]);
console.log(url);
var hclient = hbase({
    host: url.hostname,
    path: url.pathname ?? "/",
    port: url.port,
    protocol: url.protocol.slice(0, -1),
    encoding: 'latin1'
});

function counterToNumber(c) {
    const str = Buffer.from(c, 'latin1').toString().trim();
    const num = Number(str);
    return isNaN(num) ? 0 : num;
}

function rowToMap(row) {
    var stats = {};
    row.forEach(function (item) {
        stats[item['column']] = counterToNumber(item['$']);
    });
    return stats;
}

app.use(express.static('public'));

app.get('/delays.html', function (req, res) {
    const origin = req.query['origin'];
    const year = req.query['year'];
    const key = `${year}_${origin}`;
    console.log("Key entered:", key);
    hclient.table('grlewis_hw51_delay_by_origin_year_hb').row(key).get(function (err, cells) {
        if (err) {
            console.error(err);
            return res.status(500).send("Error querying HBase.");
        }
        if (!cells || cells.length === 0) {
            console.log(`No data found for ${key}`);
            return res.send(`<h2>No data found for ${origin} in ${year}</h2>`);
        }
        const weatherInfo = rowToMap(cells);
        console.log(weatherInfo);
        function weather_delay(weather) {
            var delays = weatherInfo["delay:" + weather + "_delays"];
            if (delays === undefined || delays === 0) return " - ";
            return delays.toFixed(1);
        }
        var template = filesystem.readFileSync("result.mustache").toString();
        var html = mustache.render(template, {
            origin: origin,
            year: year,
            clear_dly: weather_delay("clear"),
            fog_dly: weather_delay("fog"),
            rain_dly: weather_delay("rain"),
            snow_dly: weather_delay("snow"),
            hail_dly: weather_delay("hail"),
            thunder_dly: weather_delay("thunder"),
            tornado_dly: weather_delay("tornado")
        });
        res.send(html);
    });
});

app.listen(port, () => {
    console.log(`Listening on port ${port}`);
});