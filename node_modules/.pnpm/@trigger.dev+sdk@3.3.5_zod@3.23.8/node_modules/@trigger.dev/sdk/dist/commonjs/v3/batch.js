"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.batch = void 0;
const v3_1 = require("@trigger.dev/core/v3");
const shared_js_1 = require("./shared.js");
const tracer_js_1 = require("./tracer.js");
exports.batch = {
    trigger: shared_js_1.batchTriggerById,
    triggerAndWait: shared_js_1.batchTriggerByIdAndWait,
    triggerByTask: shared_js_1.batchTriggerTasks,
    triggerByTaskAndWait: shared_js_1.batchTriggerAndWaitTasks,
    retrieve: retrieveBatch,
};
/**
 * Retrieves details about a specific batch by its ID.
 *
 * @param {string} batchId - The unique identifier of the batch to retrieve
 * @param {ApiRequestOptions} [requestOptions] - Optional API request configuration options
 * @returns {ApiPromise<RetrieveBatchResponse>} A promise that resolves with the batch details
 *
 * @example
 * // First trigger a batch
 * const response = await batch.trigger([
 *   { id: "simple-task", payload: { message: "Hello, World!" } }
 * ]);
 *
 * // Then retrieve the batch details
 * const batchDetails = await batch.retrieve(response.batchId);
 * console.log("batch", batchDetails);
 */
function retrieveBatch(batchId, requestOptions) {
    const apiClient = v3_1.apiClientManager.clientOrThrow();
    const $requestOptions = (0, v3_1.mergeRequestOptions)({
        tracer: tracer_js_1.tracer,
        name: "batch.retrieve()",
        icon: "batch",
        attributes: {
            batchId: batchId,
            ...(0, v3_1.accessoryAttributes)({
                items: [
                    {
                        text: batchId,
                        variant: "normal",
                    },
                ],
                style: "codepath",
            }),
        },
    }, requestOptions);
    return apiClient.retrieveBatch(batchId, $requestOptions);
}
//# sourceMappingURL=batch.js.map