type ExponentialBackoffType = "NoJitter" | "FullJitter" | "EqualJitter";
type ExponentialBackoffOptions = {
    base: number;
    factor: number;
    min: number;
    max: number;
    maxRetries: number;
    maxElapsed: number;
};
declare class StopRetrying extends Error {
    constructor(message?: string);
}
declare class RetryLimitExceeded extends Error {
    constructor(message?: string);
}
type YieldType<T> = T extends AsyncGenerator<infer Y, any, any> ? Y : never;
/**
 * Exponential backoff helper class
 * - All time units in seconds unless otherwise specified
 */
export declare class ExponentialBackoff {
    #private;
    constructor(type?: ExponentialBackoffType, opts?: Partial<ExponentialBackoffOptions>);
    type(type?: ExponentialBackoffType): ExponentialBackoff;
    base(base?: number): ExponentialBackoff;
    factor(factor?: number): ExponentialBackoff;
    min(min?: number): ExponentialBackoff;
    max(max?: number): ExponentialBackoff;
    maxRetries(maxRetries?: number): ExponentialBackoff;
    maxElapsed(maxElapsed?: number): ExponentialBackoff;
    retries(retries?: number): ExponentialBackoff;
    retryAsync(maxRetries?: number): AsyncGenerator<{
        delay: {
            seconds: number;
            milliseconds: number;
        };
        retry: number;
    }, void, unknown>;
    [Symbol.asyncIterator](): AsyncGenerator<{
        delay: {
            seconds: number;
            milliseconds: number;
        };
        retry: number;
    }, void, unknown>;
    /** Returns the delay for the current retry in seconds. */
    delay(retries?: number, jitter?: boolean): number;
    /** Waits with the appropriate delay for the current retry. */
    wait(retries?: number, jitter?: boolean): Promise<void>;
    elapsed(retries?: number, jitter?: boolean): {
        seconds: number;
        minutes: number;
        hours: number;
        days: number;
        total: number;
    };
    reset(): this;
    next(): number;
    stop(): void;
    get state(): {
        retries: number;
        type: ExponentialBackoffType;
        base: number;
        factor: number;
        min: number;
        max: number;
        maxRetries: number;
        maxElapsed: number;
    };
    execute<T>(callback: (iteratorReturn: YieldType<ReturnType<ExponentialBackoff["retryAsync"]>> & {
        elapsedMs: number;
    }) => Promise<T>, { attemptTimeoutMs }?: {
        attemptTimeoutMs?: number;
    }): Promise<{
        success: true;
        result: T;
    } | {
        success: false;
        error?: unknown;
        cause: "StopRetrying" | "Timeout" | "MaxRetries";
    }>;
    static RetryLimitExceeded: typeof RetryLimitExceeded;
    static StopRetrying: typeof StopRetrying;
}
export {};
