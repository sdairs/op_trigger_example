import { BuildManifest } from "@trigger.dev/core/v3";
import { BuildContext, BuildExtension } from "@trigger.dev/core/v3/build";
export declare function puppeteer(): PuppeteerExtension;
declare class PuppeteerExtension implements BuildExtension {
    readonly name = "PuppeteerExtension";
    onBuildComplete(context: BuildContext, manifest: BuildManifest): Promise<void>;
}
export {};
