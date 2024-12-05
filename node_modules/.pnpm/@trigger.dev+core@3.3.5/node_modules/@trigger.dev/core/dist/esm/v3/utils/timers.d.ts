import { setTimeout } from "node:timers/promises";
export declare function unboundedTimeout<T = void>(delay?: number, value?: T, options?: Parameters<typeof setTimeout>[2]): Promise<T>;
export declare function checkpointSafeTimeout(delay?: number): Promise<void>;
