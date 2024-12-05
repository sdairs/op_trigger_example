import type { ApiRequestOptions, RunTags } from "@trigger.dev/core/v3";
export declare const tags: {
    add: typeof addTags;
};
declare function addTags(tags: RunTags, requestOptions?: ApiRequestOptions): Promise<void>;
export {};
