export type CacheMetadata = {
    createdTime: number;
    ttl?: number | null;
};
export type CacheEntry<Value = unknown> = {
    metadata: CacheMetadata;
    value: Value;
};
export type Eventually<Value> = Value | null | undefined | Promise<Value | null | undefined>;
export type CacheStore<Value = any> = {
    name?: string;
    get: (key: string) => Eventually<CacheEntry<Value>>;
    set: (key: string, value: CacheEntry<Value>) => unknown | Promise<unknown>;
    delete: (key: string) => unknown | Promise<unknown>;
};
export type CacheFunction = <Value>(cacheKey: string, fn: () => Promise<Value> | Value) => Promise<Value> | Value;
export declare class InMemoryCache<Value = any> {
    private _cache;
    get(key: string): Eventually<CacheEntry<Value>>;
    set(key: string, value: CacheEntry<Value>): unknown;
    delete(key: string): unknown;
}
/**
 * Create a cache function that uses the provided store to cache values. Using InMemoryCache is safe because each task run is isolated.
 * @param store
 * @returns
 */
export declare function createCache(store: CacheStore): CacheFunction;
