import { SemanticInternalAttributes } from "@trigger.dev/core/v3";
import { tracer } from "./tracer.js";
export class InMemoryCache {
    _cache = new Map();
    get(key) {
        return this._cache.get(key);
    }
    set(key, value) {
        this._cache.set(key, value);
        return undefined;
    }
    delete(key) {
        this._cache.delete(key);
        return undefined;
    }
}
/**
 * Create a cache function that uses the provided store to cache values. Using InMemoryCache is safe because each task run is isolated.
 * @param store
 * @returns
 */
export function createCache(store) {
    return function cache(cacheKey, fn) {
        return tracer.startActiveSpan("cache", async (span) => {
            span.setAttribute("cache.key", cacheKey);
            span.setAttribute(SemanticInternalAttributes.STYLE_ICON, "device-sd-card");
            const cacheEntry = await store.get(cacheKey);
            if (cacheEntry) {
                span.updateName(`cache.hit ${cacheKey}`);
                return cacheEntry.value;
            }
            span.updateName(`cache.miss ${cacheKey}`);
            const value = await tracer.startActiveSpan("cache.getFreshValue", async (span) => {
                return await fn();
            }, {
                attributes: {
                    "cache.key": cacheKey,
                    [SemanticInternalAttributes.STYLE_ICON]: "device-sd-card",
                },
            });
            await tracer.startActiveSpan("cache.set", async (span) => {
                await store.set(cacheKey, {
                    value,
                    metadata: {
                        createdTime: Date.now(),
                    },
                });
            }, {
                attributes: {
                    "cache.key": cacheKey,
                    [SemanticInternalAttributes.STYLE_ICON]: "device-sd-card",
                },
            });
            return value;
        });
    };
}
//# sourceMappingURL=cache.js.map