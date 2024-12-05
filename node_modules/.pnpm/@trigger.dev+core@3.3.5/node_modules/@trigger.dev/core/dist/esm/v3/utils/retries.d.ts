import { type RetryOptions } from "../schemas/index.js";
export declare const defaultRetryOptions: {
    maxAttempts: number;
    factor: number;
    minTimeoutInMs: number;
    maxTimeoutInMs: number;
    randomize: true;
};
export declare const defaultFetchRetryOptions: {
    byStatus: {
        "429,408,409,5xx": {
            maxAttempts: number;
            factor: number;
            minTimeoutInMs: number;
            maxTimeoutInMs: number;
            randomize: true;
            strategy: "backoff";
        };
    };
    connectionError: {
        maxAttempts: number;
        factor: number;
        minTimeoutInMs: number;
        maxTimeoutInMs: number;
        randomize: true;
    };
    timeout: {
        maxAttempts: number;
        factor: number;
        minTimeoutInMs: number;
        maxTimeoutInMs: number;
        randomize: true;
    };
};
/**
 *
 * @param options
 * @param attempt - The current attempt number. If the first attempt has failed, this will be 1.
 * @returns
 */
export declare function calculateNextRetryDelay(options: RetryOptions, attempt: number): number | undefined;
export declare function calculateResetAt(resets: string | undefined | null, format: "unix_timestamp" | "iso_8601" | "iso_8601_duration_openai_variant" | "unix_timestamp_in_ms", now?: number): number | undefined;
