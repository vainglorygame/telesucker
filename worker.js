#!/usr/bin/node
/* jshint esnext:true */
/* download data from AWS and push into process queues */
"use strict";

const amqp = require("amqplib"),
    Promise = require("bluebird"),
    winston = require("winston"),
    loggly = require("winston-loggly-bulk"),
    request = require("request-promise"),
    sleep = require("sleep-promise"),
    jsonapi = require("../orm/jsonapi"),
    moment = require("moment"),
    AdmZip = require("adm-zip");

const RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    QUEUE = process.env.QUEUE || "sample",
    PROCESS_QUEUE = process.env.PROCESS_QUEUE || "process",
    PROCESS_BRAWL_QUEUE = process.env.PROCESS_BRAWL_QUEUE || "process_brawl",
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN,
    SAMPLERS = parseInt(process.env.SAMPLERS) || 5;

const logger = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({
            timestamp: true,
            colorize: true
        })
    ]
});

// loggly integration
if (LOGGLY_TOKEN)
    logger.add(winston.transports.Loggly, {
        inputToken: LOGGLY_TOKEN,
        subdomain: "kvahuja",
        tags: ["backend", "sampler", QUEUE],
        json: true
    });

(async () => {
    let rabbit, ch;

    while (true) {
        try {
            rabbit = await amqp.connect(RABBITMQ_URI);
            ch = await rabbit.createChannel();
            await ch.assertQueue(QUEUE, {durable: true});
            break;
        } catch (err) {
            logger.error("error connecting", err);
            await sleep(5000);
        }
    }

    await ch.prefetch(SAMPLERS);
    ch.consume(QUEUE, async (msg) => {
        const payload = JSON.parse(msg.content.toString());
        if (msg.properties.type == "sample")
            await getSample(payload);
        if (msg.properties.type == "telemetry") {
            try {
                await getTelemetry(payload, msg.properties.headers.match_api_id);
            } catch (err) {
                logger.error("Telemetry download error", err);
                ch.nack(msg, false, true);  // TODO how to handle this?
                return;
            }
        }

        logger.info("done", payload);
        ch.ack(msg);
    }, { noAck: false });

    // download a sample ZIP and send to processor
    async function getSample(url) {
        logger.info("downloading sample", url);
        const zipdata = await request({
            uri: url,
            encoding: null
        }),
            zip = new AdmZip(zipdata);
        await Promise.map(zip.getEntries(), async (entry) => {
            if (entry.isDirectory) return;
            const match = jsonapi.parse(JSON.parse(entry.getData().toString("utf8")));
            await sendMatchToProcessor(match);
        });
        logger.info("sample processed", url);
    }

    // send to seperated queues or just to `process`
    async function sendMatchToProcessor(match) {
        if (["casual", "ranked"].indexOf(match.attributes.gameMode) != -1)
            await ch.sendToQueue(PROCESS_QUEUE, new Buffer(JSON.stringify(match)),
                { persistent: true, type: "match" })
        else
            await ch.sendToQueue(PROCESS_BRAWL_QUEUE, new Buffer(JSON.stringify(match)),
                { persistent: true, type: "match" })
    }

    // download Telemetry, filter irrelevant events, forward the rest to `process`
    async function getTelemetry(url, match_api_id) {
        logger.info("downloading Telemetry",
            { url: url, match_api_id: match_api_id });
        // download
        const telemetry = await loggedRequest(url),
            spawn = telemetry.filter((ev) => ev.type == "PlayerFirstSpawn")[0];

        // return telemetry { m_a_id, data, start, end } in an interval
        const gamePhase = (start, end) => { return {
            match_api_id: match_api_id,
            data: telemetry.filter((ev) =>
                moment(ev.time).isBetween(
                    moment(spawn.time).add(start, "seconds"),
                    moment(spawn.time).add(end, "seconds")
                ) ),
            start: start,
            end: end
        } };
        // split into phases
        const phases = [
            gamePhase(0, 90 * 60),  // whole
            gamePhase(0, 1 * 60),  // start
            gamePhase(0, 4 * 60),  // early game
            gamePhase(0, 8 * 60),  // miner full
            gamePhase(0, 12 * 60),  // miner full
            gamePhase(0, 15 * 60),  // Kraken spawn
            gamePhase(0, 20 * 60),  // late mid game
            gamePhase(0, 25 * 60),  // late game
            gamePhase(0, 30 * 60),  // late game
            gamePhase(0, 90 * 60)  // still playing?
        ];
        await Promise.each(phases, async (phase, idx, len) => {
            if (phase.data.length > 0)
                await ch.sendToQueue(PROCESS_QUEUE, new Buffer(
                    JSON.stringify(phase)), {
                        persistent: true, type: "telemetry",
                        headers: idx == len - 1? { notify: "match." + match_api_id } : {}
                    })
        });
        logger.info("Telemetry done",
            { url: url, match_api_id: match_api_id });
    }

    async function loggedRequest(url) {
        let response;
        try {
            const opts = {
                json: true,
                gzip: true,
                time: true,
                forever: true,
                strictSSL: true,
                resolveWithFullResponse: true
            };
            logger.info("AWS request", { uri: url, });
            response = await request(url, opts);
            return response.body;
        } catch (err) {
            logger.warn("AWS error", {
                uri: url,
                error: err.response.body
            });
            throw err;
        } finally {
            if (response != undefined)  // else non-requests error
                logger.info("AWS response", {
                    status: response.statusCode,
                    connection_start: response.timings.connect,
                    connection_end: response.timings.end
                });
        }
    }
})();
