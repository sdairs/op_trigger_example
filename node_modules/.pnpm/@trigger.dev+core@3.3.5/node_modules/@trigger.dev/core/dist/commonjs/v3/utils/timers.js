"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.unboundedTimeout = unboundedTimeout;
exports.checkpointSafeTimeout = checkpointSafeTimeout;
const promises_1 = require("node:timers/promises");
async function unboundedTimeout(delay = 0, value, options) {
    const maxDelay = 2147483647; // Highest value that will fit in a 32-bit signed integer
    const fullTimeouts = Math.floor(delay / maxDelay);
    const remainingDelay = delay % maxDelay;
    let lastTimeoutResult = await (0, promises_1.setTimeout)(remainingDelay, value, options);
    for (let i = 0; i < fullTimeouts; i++) {
        lastTimeoutResult = await (0, promises_1.setTimeout)(maxDelay, value, options);
    }
    return lastTimeoutResult;
}
async function checkpointSafeTimeout(delay = 0) {
    const scanIntervalMs = 1000;
    // Every scanIntervalMs, check if delay has elapsed
    for await (const start of (0, promises_1.setInterval)(scanIntervalMs, Date.now())) {
        if (Date.now() - start > delay) {
            break;
        }
    }
}
//# sourceMappingURL=timers.js.map