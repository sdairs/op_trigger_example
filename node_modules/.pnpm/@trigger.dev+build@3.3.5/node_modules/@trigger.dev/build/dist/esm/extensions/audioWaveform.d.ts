import { BuildExtension } from "@trigger.dev/core/v3/build";
export type AudioWaveformOptions = {
    version?: string;
    checksum?: string;
};
export declare function audioWaveform(options?: AudioWaveformOptions): BuildExtension;
