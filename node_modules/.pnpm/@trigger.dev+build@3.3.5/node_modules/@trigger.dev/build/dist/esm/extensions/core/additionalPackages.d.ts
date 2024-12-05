import { BuildExtension } from "@trigger.dev/core/v3/build";
export type AdditionalPackagesOptions = {
    packages: string[];
};
/**
 * Add additional packages to the build when deploying, useful when you are using the bin command of a package by shelling out to it.
 * You can pass the name of the package, and it's version will be resolved from the locally installed version. If the version cannot be automatically resolved, it will resolve to the latest version, or you can specify the version using `@` syntax.
 * @example
 *
 * ```ts
 * additionalPackages({
 *  packages: ["wrangler", "prisma@3.0.0"]
 * });
 */
export declare function additionalPackages(options: AdditionalPackagesOptions): BuildExtension;
