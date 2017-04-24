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
})();
