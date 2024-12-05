"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TaskRunPromise = exports.SubtaskUnwrapError = void 0;
class SubtaskUnwrapError extends Error {
    taskId;
    runId;
    cause;
    constructor(taskId, runId, subtaskError) {
        if (subtaskError instanceof Error) {
            super(`Error in ${taskId}: ${subtaskError.message}`);
            this.cause = subtaskError;
            this.name = "SubtaskUnwrapError";
        }
        else {
            super(`Error in ${taskId}`);
            this.name = "SubtaskUnwrapError";
            this.cause = subtaskError;
        }
        this.taskId = taskId;
        this.runId = runId;
    }
}
exports.SubtaskUnwrapError = SubtaskUnwrapError;
class TaskRunPromise extends Promise {
    taskId;
    constructor(executor, taskId) {
        super(executor);
        this.taskId = taskId;
    }
    unwrap() {
        return this.then((result) => {
            if (result.ok) {
                return result.output;
            }
            else {
                throw new SubtaskUnwrapError(this.taskId, result.id, result.error);
            }
        });
    }
}
exports.TaskRunPromise = TaskRunPromise;
//# sourceMappingURL=tasks.js.map