/**
 * Contains two parts: the first part is the seconds, the second part is the nanoseconds.
 *
 */
export type ClockTime = [number, number];
export interface Clock {
    preciseNow(): ClockTime;
    reset(): void;
}
export declare function calculateDurationInMs(start: ClockTime, end: ClockTime): number;
