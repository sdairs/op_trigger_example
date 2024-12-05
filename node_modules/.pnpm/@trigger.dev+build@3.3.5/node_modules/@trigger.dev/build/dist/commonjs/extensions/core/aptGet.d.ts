import { BuildExtension } from "@trigger.dev/core/v3/build";
export type AptGetOptions = {
    packages: string[];
};
export declare function aptGet(options: AptGetOptions): BuildExtension;
