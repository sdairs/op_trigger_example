export class TaskRunExceededMaxDuration extends Error {
    timeoutInSeconds;
    usageInSeconds;
    constructor(timeoutInSeconds, usageInSeconds) {
        super(`Run exceeded maxDuration of ${timeoutInSeconds} seconds`);
        this.timeoutInSeconds = timeoutInSeconds;
        this.usageInSeconds = usageInSeconds;
        this.name = "TaskRunExceededMaxDuration";
    }
}
//# sourceMappingURL=types.js.map