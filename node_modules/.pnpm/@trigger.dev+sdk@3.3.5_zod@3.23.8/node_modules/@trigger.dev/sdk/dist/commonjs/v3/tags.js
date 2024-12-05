"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.tags = void 0;
const v3_1 = require("@trigger.dev/core/v3");
const tracer_js_1 = require("./tracer.js");
exports.tags = {
    add: addTags,
};
async function addTags(tags, requestOptions) {
    const apiClient = v3_1.apiClientManager.clientOrThrow();
    const run = v3_1.taskContext.ctx?.run;
    if (!run) {
        throw new Error("Can't set tags outside of a run. You can trigger a task and set tags in the options.");
    }
    const $requestOptions = (0, v3_1.mergeRequestOptions)({
        tracer: tracer_js_1.tracer,
        name: "tags.set()",
        icon: "tag",
        attributes: {
            ...(0, v3_1.accessoryAttributes)({
                items: [
                    {
                        text: typeof tags === "string" ? tags : tags.join(", "),
                        variant: "normal",
                    },
                ],
                style: "codepath",
            }),
        },
    }, requestOptions);
    try {
        await apiClient.addTags(run.id, { tags }, $requestOptions);
    }
    catch (error) {
        if (error instanceof v3_1.UnprocessableEntityError) {
            v3_1.logger.error(error.message, {
                existingTags: run.tags,
                newTags: tags,
            });
            return;
        }
        v3_1.logger.error("Failed to set tags", { error });
        throw error;
    }
}
//# sourceMappingURL=tags.js.map