"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.defaultFetchRetryOptions = exports.defaultRetryOptions = void 0;
exports.calculateNextRetryDelay = calculateNextRetryDelay;
exports.calculateResetAt = calculateResetAt;
const retry_js_1 = require("../../retry.js");
exports.defaultRetryOptions = {
    maxAttempts: 3,
    factor: 2,
    minTimeoutInMs: 1000,
    maxTimeoutInMs: 60000,
    randomize: true,
};
exports.defaultFetchRetryOptions = {
    byStatus: {
        "429,408,409,5xx": {
            strategy: "backoff",
            ...exports.defaultRetryOptions,
        },
    },
    connectionError: exports.defaultRetryOptions,
    timeout: exports.defaultRetryOptions,
};
/**
 *
 * @param options
 * @param attempt - The current attempt number. If the first attempt has failed, this will be 1.
 * @returns
 */
function calculateNextRetryDelay(options, attempt) {
    const opts = { ...exports.defaultRetryOptions, ...options };
    if (attempt >= opts.maxAttempts) {
        return;
    }
    const { factor, minTimeoutInMs, maxTimeoutInMs, randomize } = opts;
    const random = randomize ? Math.random() + 1 : 1;
    const timeout = Math.min(maxTimeoutInMs, random * minTimeoutInMs * Math.pow(factor, attempt - 1));
    // Round to the nearest integer
    return Math.round(timeout);
}
function calculateResetAt(resets, format, now = Date.now()) {
    const resetAt = (0, retry_js_1.calculateResetAt)(resets, format, new Date(now));
    return resetAt?.getTime();
}
//# sourceMappingURL=retries.js.map