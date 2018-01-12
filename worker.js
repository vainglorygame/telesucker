#!/usr/bin/node
/* jshint esnext:true */
/* download data from AWS and push into process queues */
"use strict";

const amqp = require("amqplib"),
    Promise = require("bluebird"),
    winston = require("winston"),
    loggly = require("winston-loggly-bulk"),
    request = require("request-promise"),
    moment = require("moment");

const RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    QUEUE = process.env.QUEUE || "telesuck",
    SHRINK_QUEUE = process.env.SHRINK_QUEUE || "shrink",
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN;

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
        tags: ["backend", "telesucker", QUEUE],
        json: true
    });

amqp.connect(RABBITMQ_URI).then(async (rabbit) => {
    process.on("SIGINT", () => {
        rabbit.close();
        process.exit();
    });

    const ch = await rabbit.createChannel();
    await ch.assertQueue(QUEUE, { durable: true });
    await ch.assertQueue(QUEUE + "_failed", { durable: true });
    await ch.assertQueue(SHRINK_QUEUE, { durable: true });
    await ch.prefetch(1);

    logger.info("configuration", { QUEUE, SHRINK_QUEUE });

    ch.consume(QUEUE, async (msg) => {
        const url = msg.content.toString();

        try {
            await getTelemetry(url, msg.properties.headers.match_api_id);
        } catch (err) {
            logger.error("Telemetry download error", err);
            await ch.sendToQueue(QUEUE + "_failed", msg.content, {
                persistent: true,
                headers: msg.properties.headers
            });
            ch.nack(msg, false, false);
            return;
        }

        logger.info("done", url);
        ch.ack(msg);
    }, { noAck: false });

    // download Telemetry, filter irrelevant events, forward the rest to `process`
    async function getTelemetry(url, match_api_id) {
        logger.info("downloading Telemetry",
            { url: url, match_api_id: match_api_id });
        // download
        const telemetry = await loggedRequest(url),
            spawn = telemetry.filter((ev) => ev.type == "PlayerFirstSpawn")[0],
            spawn_time = moment.parseZone(spawn.time);

        const forward_profiler = logger.startTimer();

        // return telemetry { m_a_id, data, start, end } in an interval
        const gamePhase = (start, end) => {
            // TODO: uncommented for better performance with a single game spanning phase
            /*
            let spawn_plus_start = spawn_time.clone()
                    .add(start, "seconds")
                    .format("YYYY-MM-DDTHH:mm:ss"),
                spawn_plus_end = spawn_time.clone()
                    .add(end, "seconds")
                    .format("YYYY-MM-DDTHH:mm:ss");
            */
            return {
                match_api_id: match_api_id,
                /*
                data: telemetry.slice(
                    // assumes Telemetry is ordered by timestamp,
                    // assumes it uses the format as above
                    telemetry.findIndex((ev) => !(ev.time<spawn_plus_start)),
                    telemetry.findIndex((ev) => !(ev.time<spawn_plus_end))
                    // slice does not include end
                ),
                */
                data: telemetry,
                start: start,
                end: end
        } };
        // split into phases
        const phases = [
            // genious idea to put bans into Telemetry.
            gamePhase(-5 * 60, 5400) // bans + all
        ];
        await Promise.each(phases, async (phase) => {
            if (phase.data.length > 0) {
                const notify = "match." + match_api_id;
                await ch.publish("amq.topic", notify, new Buffer("phase_pending"));
                await ch.sendToQueue(SHRINK_QUEUE, new Buffer(
                    JSON.stringify(phase)), {
                        persistent: true, type: "telemetry",
                        headers: { notify }
                    })
            } else {
                logger.info("Warning! No data for this phase.", { phase });
            }
        });
        forward_profiler.done("Telemetry splitting");
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
            if (typeof response.body === "string") {
                if (!response.body.startsWith("[")) {
                    // invalid JSON
                    // https://github.com/gamelocker/vainglory-assets/issues/325
                    let hotfix = response.body.replace(/\{ "time":/g, ', { "time":');
                    // `},` -> `} \n ]`
                    hotfix = "[" + hotfix.substring(2, hotfix.length) + "]";
                    response.body = JSON.parse(hotfix);
                } else {
                    logger.warn("invalid response", { url });
                    //throw { response.body };
JSON.parse(response.body);
                }
            }
            return response.body;
        } catch (err) {
            if (err.response != undefined) {
                logger.warn("AWS error", {
                    uri: url,
                    error: err.response.body
                });
            } else {
                //logger.error(err);
            }
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
});

process.on("unhandledRejection", (err) => {
    logger.error(err);
    process.exit(1);  // fail hard and die
});
