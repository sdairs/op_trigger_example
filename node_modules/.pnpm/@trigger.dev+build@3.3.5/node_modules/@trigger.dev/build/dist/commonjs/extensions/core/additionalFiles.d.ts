import { BuildExtension } from "@trigger.dev/core/v3/build";
export type AdditionalFilesOptions = {
    files: string[];
};
export declare function additionalFiles(options: AdditionalFilesOptions): BuildExtension;
