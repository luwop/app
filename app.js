'use strict';

const http = require('http');
const express = require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
require('dotenv').config();
const hbase = require('hbase');
const { Kafka } = require("kafkajs");

const port = Number(process.argv[2]);
const url = new URL(process.argv[3]);

const hclient = hbase({
    host: url.hostname,
    path: url.pathname ?? "/",
    port: url.port,
    protocol: url.protocol.slice(0, -1),
    encoding: 'latin1'
});

const cardData = JSON.parse(
    filesystem.readFileSync("./data/cards.json").toString()
);

const CARD_NAME = {};
cardData.forEach(c => {
    CARD_NAME[String(c.id)] = c.name;
});

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
    const stats = {};

    cells.forEach(col => {
        const q = col.column;
        const val = col["$"];

        if (q.startsWith("stats:"))
            stats[q.replace("stats:", "")] = decodeNumber(val);
        else if (q.startsWith("rel:"))
            stats[q.replace("rel:", "")] = decodeString(val);
        else
            stats[q] = decodeString(val);
    });

    return stats;
}

app.use(express.static('public'));

app.get('/stats.html', function (req, res) {
    const cardId = req.query['card_id'];
    if (!cardId) return res.send("<h2>No card ID provided!</h2>");

    hclient.table('grlewis_card_stats_hb').row(cardId).get(function (err, mainCells) {
        if (err) return res.status(500).send("HBase read error.");
        if (!mainCells || !Array.isArray(mainCells) || mainCells.length === 0)
            return res.send(`<h2>No data found for card ${cardId}</h2>`);

        const info = rowToStats(mainCells);

        const synergyIDs = [
            info.top_synergy_1,
            info.top_synergy_2,
            info.top_synergy_3
        ].filter(Boolean);

        const counterIDs = [
            info.top_counter_1,
            info.top_counter_2,
            info.top_counter_3
        ].filter(Boolean);

        function fetchSecondaryCard(id, callback) {
            hclient.table('grlewis_card_stats_hb').row(id).get(function (err, cells) {
                if (err) return callback(err);
                if (!cells || !Array.isArray(cells) || cells.length === 0)
                    return callback(null, null);

                const parsed = rowToStats(cells);

                const cardObj = {
                    id,
                    name: CARD_NAME[id] || id,
                    win_rate: (parsed.win_rate * 100).toFixed(2),
                    synergy_list: [
                        CARD_NAME[parsed.top_synergy_1] || parsed.top_synergy_1,
                        CARD_NAME[parsed.top_synergy_2] || parsed.top_synergy_2,
                        CARD_NAME[parsed.top_synergy_3] || parsed.top_synergy_3
                    ].filter(Boolean),
                    counter_list: [
                        CARD_NAME[parsed.top_counter_1] || parsed.top_counter_1,
                        CARD_NAME[parsed.top_counter_2] || parsed.top_counter_2,
                        CARD_NAME[parsed.top_counter_3] || parsed.top_counter_3
                    ].filter(Boolean)
                };

                callback(null, cardObj);
            });
        }

        function fetchAll(list, finalCallback) {
            const results = [];
            let count = 0;

            if (list.length === 0) return finalCallback(null, []);

            list.forEach(id => {
                fetchSecondaryCard(id, function (err, obj) {
                    if (obj) results.push(obj);
                    count++;
                    if (count === list.length) finalCallback(null, results);
                });
            });
        }

        fetchAll(synergyIDs, function (_, partners) {
            fetchAll(counterIDs, function (_, opponents) {
                const context = {
                    card_id: cardId,
                    card_name: CARD_NAME[cardId] || "Unknown Card",
                    usage: info.total_appearances,
                    win_rate: (info.win_rate * 100).toFixed(2),
                    partners,
                    opponents
                };

                const template = filesystem
                    .readFileSync("views/card_results.mustache")
                    .toString();

                const html = mustache.render(template, context);
                res.send(html);
            });
        });
    });
});

const kafka = new Kafka({
    clientId: "clash-producer",
    brokers: ["boot-public-byg.mpcs53014kafka.2siu49.c2.kafka.us-east-1.amazonaws.com:9196"],
    ssl: true,
    sasl: {
        mechanism: "scram-sha-512",
        username: "mpcs53014-2025",
        password: "A3v4rd4@ujjw"
    },
    connectionTimeout: 10000,
    requestTimeout: 30000
});

const producer = kafka.producer();

async function initKafka() {
    try {
        await producer.connect();
    } catch (err) {
        console.error("Kafka connection error:", err);
    }
}

initKafka();

app.get("/submit-match", async function (req, res) {
    const event = {
        timestamp: Date.now(),
        matches: Number(req.query.matches),
        wins: Number(req.query.wins),
        player_cards: [
            req.query.player1, req.query.player2, req.query.player3, req.query.player4,
            req.query.player5, req.query.player6, req.query.player7, req.query.player8
        ].map(Number),
        opponent_cards: [
            req.query.opp1, req.query.opp2, req.query.opp3, req.query.opp4,
            req.query.opp5, req.query.opp6, req.query.opp7, req.query.opp8
        ].map(Number)
    };

    try {
        await producer.send({
            topic: "grlewis_clash_events",
            messages: [{ value: JSON.stringify(event) }]
        });

        res.redirect("/match.html");
    } catch (err) {
        res.status(500).send("Failed to submit match.");
    }
});

app.listen(port, () => {
    console.log(`Card Stats WebApp running on port ${port}`);
});
