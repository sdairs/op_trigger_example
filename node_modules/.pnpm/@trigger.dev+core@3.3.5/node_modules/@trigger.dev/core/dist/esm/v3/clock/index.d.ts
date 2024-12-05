import type { Clock, ClockTime } from "./clock.js";
export declare class ClockAPI {
    #private;
    private static _instance?;
    private constructor();
    static getInstance(): ClockAPI;
    setGlobalClock(clock: Clock): boolean;
    preciseNow(): ClockTime;
    reset(): void;
}
