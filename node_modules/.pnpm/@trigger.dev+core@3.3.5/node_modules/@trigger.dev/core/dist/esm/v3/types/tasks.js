export class SubtaskUnwrapError extends Error {
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
export class TaskRunPromise extends Promise {
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
//# sourceMappingURL=tasks.js.map