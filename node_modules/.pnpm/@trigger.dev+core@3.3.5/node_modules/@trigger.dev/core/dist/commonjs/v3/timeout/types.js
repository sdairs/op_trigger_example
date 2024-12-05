"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TaskRunExceededMaxDuration = void 0;
class TaskRunExceededMaxDuration extends Error {
    timeoutInSeconds;
    usageInSeconds;
    constructor(timeoutInSeconds, usageInSeconds) {
        super(`Run exceeded maxDuration of ${timeoutInSeconds} seconds`);
        this.timeoutInSeconds = timeoutInSeconds;
        this.usageInSeconds = usageInSeconds;
        this.name = "TaskRunExceededMaxDuration";
    }
}
exports.TaskRunExceededMaxDuration = TaskRunExceededMaxDuration;
//# sourceMappingURL=types.js.map