'use strict';
const http = require('http');
const express = require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
require('dotenv').config();
const hbase = require('hbase');

const port = Number(process.argv[2]);
const url = new URL(process.argv[3]);

console.log("Connecting to HBase:", url);

// -----------------------------
// HBASE CLIENT
// -----------------------------
const hclient = hbase({
    host: url.hostname,
    path: url.pathname ?? "/",
    port: url.port,
    protocol: url.protocol.slice(0, -1),
    encoding: 'latin1'
});

// -----------------------------
// LOAD CARD NAMES
// -----------------------------
const cardData = JSON.parse(
    filesystem.readFileSync("./data/cards.json").toString()
);

// Make a dictionary: id → name
const CARD_NAME = {};
cardData.forEach(c => {
    CARD_NAME[String(c.id)] = c.name;
});

// -----------------------------
// HELPERS
// -----------------------------
function decodeString(c) {
    if (!c) return "";
    return Buffer.from(c, 'latin1').toString().trim();
}

function decodeNumber(c) {
    if (!c) return 0;
    const s = decodeString(c);
    const n = Number(s);
    return isNaN(n) ? 0 : n;
}

function rowToStats(cells) {
    let stats = {};

    cells.forEach(col => {
        const q = col.column;
        const val = col["$"];

        if (q.startsWith("stats:")) stats[q.replace("stats:", "")] = decodeNumber(val);
        else if (q.startsWith("rel:")) stats[q.replace("rel:", "")] = decodeString(val);
        else stats[q] = decodeString(val);
    });

    return stats;
}

// -----------------------------
// STATIC FILES
// -----------------------------
app.use(express.static('public'));

// -----------------------------
// CARD ANALYTICS ENDPOINT
// -----------------------------
app.get('/stats.html', function (req, res) {
    const cardId = req.query['card_id'];

    if (!cardId) return res.send("<h2>No card ID provided!</h2>");

    console.log("Fetching Card:", cardId);

    hclient.table('grlewis_card_stats_hb')
        .row(cardId)
        .get(function (err, cells) {

            if (err) {
                console.error(err);
                return res.status(500).send("Error querying HBase.");
            }

            if (!cells || cells.length === 0) {
                return res.send(`<h2>No data found for card ${cardId}</h2>`);
            }

            const info = rowToStats(cells);

            // -----------------------------
            // MAP IDs → NAMES
            // -----------------------------
            const partners = [
                info.top_synergy_1,
                info.top_synergy_2,
                info.top_synergy_3
            ].filter(x => x).map(id => ({
                name: CARD_NAME[id] || id,
                count: "" // no count available yet
            }));

            const opponents = [
                info.top_counter_1,
                info.top_counter_2,
                info.top_counter_3
            ].filter(x => x).map(id => ({
                name: CARD_NAME[id] || id,
                count: "" // no count available yet
            }));

            // -----------------------------
            // MUSTACHE CONTEXT
            // -----------------------------
            const context = {
                card_id: cardId,
                card_name: info.card_name || CARD_NAME[cardId] || "Unknown Card",
                usage: info.total_appearances,
                wins: info.winner_appearances,
                win_rate: (info.win_rate * 100).toFixed(2),
                partners,
                opponents
            };

            const template = filesystem.readFileSync("views/card_results.mustache").toString();
            const html = mustache.render(template, context);
            res.send(html);
        });
});

// -----------------------------
// START SERVER
// -----------------------------
app.listen(port, () => {
    console.log(`Card Stats WebApp running on port ${port}`);
});